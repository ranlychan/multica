package hatchet

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/multica-ai/multica/server/internal/util/secretbox"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

func TestHatchetTenantPinnedLifecycleContract(t *testing.T) {
	data, err := os.ReadFile("testdata/tenant-lifecycle.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		Commit  string
		Sources []struct{ Name, Path, Text string }
	}
	if json.Unmarshal(data, &fixture) != nil || fixture.Commit != UpstreamCommit {
		t.Fatal("invalid lifecycle contract")
	}
	for _, source := range fixture.Sources {
		if source.Name == "routes" {
			if !strings.Contains(source.Text, `router.POST(baseURL+"/api/v1/api-tokens/:api-token", wrapper.ApiTokenUpdateRevoke)`) || strings.Contains(source.Text, `router.DELETE(baseURL+"/api/v1/tenants/:tenant",`) {
				t.Fatal("cleanup route contract changed")
			}
		}
		if dir := os.Getenv("HATCHET_UPSTREAM_DIR"); dir != "" {
			actual, err := exec.Command("git", "-C", dir, "show", UpstreamCommit+":"+source.Path).Output()
			if err != nil {
				t.Fatal(err)
			}
			if source.Name == "routes" {
				for _, line := range strings.Split(source.Text, "\n") {
					if !strings.Contains(string(actual), line) {
						t.Fatalf("route evidence mismatch %s", source.Path)
					}
				}
			} else if string(actual) != source.Text {
				t.Fatalf("source evidence mismatch %s", source.Path)
			}
		}
	}
}

func TestHatchetTenantSQLFences(t *testing.T) {
	dsn := os.Getenv("HATCHET_TENANT_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("requires disposable HATCHET_TENANT_TEST_DATABASE_URL")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	migration, err := os.ReadFile("../../migrations/587_workflow_hatchet_tenant.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, string(migration)); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, "CREATE TABLE IF NOT EXISTS workspace(id UUID PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, "DELETE FROM workflow_hatchet_tenant"); err != nil {
		t.Fatal(err)
	}
	q := db.New(pool)
	var ws pgtype.UUID
	_ = ws.Scan(testRun)
	box, _ := secretbox.New(bytes.Repeat([]byte{1}, 32))
	boundary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("deleted workspace contacted engine")
		w.WriteHeader(500)
	}))
	defer boundary.Close()
	client, _ := NewClient(boundary.URL, boundary.Client())
	service, _ := NewSQLTenantService(client, q, pool, box, "email", "password")
	if _, err = service.Ensure(ctx, ws); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("deleted workspace provision: %v", err)
	}
	_, err = q.CreateWorkflowHatchetTenant(ctx, db.CreateWorkflowHatchetTenantParams{WorkspaceID: ws, TenantID: testTenant, TokenEncrypted: "ciphertext-preserved"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = q.CreateWorkflowHatchetTenant(ctx, db.CreateWorkflowHatchetTenantParams{WorkspaceID: ws, TenantID: "other", TokenEncrypted: "overwritten"}); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("mapping overwritten: %v", err)
	}
	if err = q.QueueWorkflowHatchetTenantDeletion(ctx, ws); err != nil {
		t.Fatal(err)
	}
	claim, err := q.ClaimWorkflowHatchetTenantDeletion(ctx, db.ClaimWorkflowHatchetTenantDeletionParams{WorkspaceID: ws, NextDeletionAttemptAt: pgtype.Timestamptz{Time: time.Now().Add(time.Minute), Valid: true}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = q.FinishWorkflowHatchetTenantDeletion(ctx, db.FinishWorkflowHatchetTenantDeletionParams{WorkspaceID: ws, DeletionState: "deleted", ExpectedAttempt: claim.DeletionAttempts - 1})
	if !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("stale cleanup accepted: %v", err)
	}
	row, err := q.FinishWorkflowHatchetTenantDeletion(ctx, db.FinishWorkflowHatchetTenantDeletionParams{WorkspaceID: ws, DeletionState: "failed", ExpectedAttempt: claim.DeletionAttempts, DeletionError: pgtype.Text{String: "tenant_deletion_unsupported", Valid: true}, NextDeletionAttemptAt: pgtype.Timestamptz{Time: time.Now().Add(24 * time.Hour), Valid: true}})
	if err != nil || row.TokenEncrypted != "ciphertext-preserved" || row.DeletedAt.Valid {
		t.Fatalf("tombstone: %+v %v", row, err)
	}
	_, _ = pool.Exec(ctx, "DELETE FROM workflow_hatchet_tenant WHERE workspace_id=$1", ws)
	if _, err = pool.Exec(ctx, "INSERT INTO workspace(id) VALUES($1)", ws); err != nil {
		t.Fatal(err)
	}
	raceBoundary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/users/login":
			http.SetCookie(w, &http.Cookie{Name: "session", Value: "private", Path: "/"})
			_, _ = w.Write([]byte(`{}`))
		case "/api/v1/users/memberships":
			_, _ = w.Write([]byte(`{"rows":[]}`))
		case "/api/v1/tenants":
			_, _ = w.Write([]byte(`{"metadata":{"id":"` + testTenant + `"}}`))
		case "/api/v1/tenants/" + testTenant + "/api-tokens":
			deleteCtx, cancel := context.WithTimeout(ctx, time.Second)
			defer cancel()
			if _, e := pool.Exec(deleteCtx, "DELETE FROM workspace WHERE id=$1", ws); e != nil {
				t.Errorf("engine request blocked workspace deletion: %v", e)
			}
			_, _ = w.Write([]byte(`{"token":"raced-server-secret"}`))
		default:
			t.Errorf("unexpected race route %s", r.URL.Path)
		}
	}))
	defer raceBoundary.Close()
	raceClient, _ := NewClient(raceBoundary.URL, raceBoundary.Client())
	raceService, _ := NewSQLTenantService(raceClient, q, pool, box, "email", "password")
	if _, err = raceService.Ensure(ctx, ws); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("raced deletion delivered credentials: %v", err)
	}
	raced, err := q.GetWorkflowHatchetTenant(ctx, ws)
	if err != nil || raced.DeletionState != "pending" {
		t.Fatalf("raced cleanup lost: %+v %v", raced, err)
	}
	sealed, _ := base64.StdEncoding.DecodeString(raced.TokenEncrypted)
	plain, err := box.Open(sealed)
	if err != nil || string(plain) != "raced-server-secret" {
		t.Fatalf("raced ciphertext lost: %v", err)
	}
}

type tenantMemory struct {
	row           db.WorkflowHatchetTenant
	exists        bool
	stale         bool
	reads         int
	failReadAfter int
}

func (m *tenantMemory) GetWorkflowHatchetTenant(context.Context, pgtype.UUID) (db.WorkflowHatchetTenant, error) {
	m.reads++
	if m.failReadAfter > 0 && m.reads >= m.failReadAfter {
		return m.row, errors.New("transient storage failure")
	}
	if !m.exists {
		return m.row, pgx.ErrNoRows
	}
	return m.row, nil
}

const testWorkerToken = "eyJhbGciOiJFUzI1NiJ9.eyJ0b2tlbl9pZCI6IjM0NTY3ODkwLTEyMzQtNDIzNC04MjM0LTEyMzQ1Njc4OWFiYyJ9.signature"
const testWorkerTokenID = "34567890-1234-4234-8234-123456789abc"

func TestHatchetWorkerTransientReadOnlyRevokesIssuedToken(t *testing.T) {
	for _, failRevoke := range []bool{false, true} {
		t.Run(fmt.Sprint(failRevoke), func(t *testing.T) {
			revoked := []string{}
			boundary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/api/v1/users/login":
					http.SetCookie(w, &http.Cookie{Name: "session", Value: "private", Path: "/"})
					_, _ = w.Write([]byte(`{}`))
				case "/api/v1/tenants/" + testTenant + "/api-tokens":
					if r.Method == "POST" {
						_, _ = w.Write([]byte(`{"token":"` + testWorkerToken + `"}`))
					} else {
						_, _ = w.Write([]byte(`{"rows":[{"metadata":{"id":"` + testRun + `"},"name":"multica-server"},{"metadata":{"id":"` + testWorkerTokenID + `"},"name":"multica-worker-new"}]}`))
					}
				default:
					revoked = append(revoked, r.URL.Path)
					if failRevoke {
						w.WriteHeader(500)
					} else {
						w.WriteHeader(204)
					}
				}
			}))
			defer boundary.Close()
			client, _ := NewClient(boundary.URL, boundary.Client())
			box, _ := secretbox.New(bytes.Repeat([]byte{1}, 32))
			sealed, _ := box.Seal([]byte("existing-server-secret"))
			store := &tenantMemory{exists: true, failReadAfter: 2, row: db.WorkflowHatchetTenant{TenantID: testTenant, DeletionState: "active", TokenEncrypted: base64.StdEncoding.EncodeToString(sealed)}}
			service, _ := NewTenantService(client, store, box, "email", "password")
			_, err := service.WorkerCredentials(context.Background(), pgtype.UUID{Valid: true}, "new", "1h")
			if err == nil || errors.Is(err, ErrUnavailable) {
				t.Fatalf("transient read became deletion: %v", err)
			}
			if len(revoked) != 1 || revoked[0] != "/api/v1/api-tokens/"+testWorkerTokenID {
				t.Fatalf("revoked healthy credentials: %v", revoked)
			}
			if failRevoke && !strings.Contains(err.Error(), testWorkerTokenID) {
				t.Fatalf("failed revoke receipt missing: %v", err)
			}
			if failRevoke {
				var receipt *WorkerTokenRevocationError
				if !errors.As(err, &receipt) || receipt.TenantID != testTenant || receipt.TokenID != testWorkerTokenID {
					t.Fatalf("unusable retry receipt: %v", err)
				}
				failRevoke = false
				if retryErr := service.CleanupPending(context.Background()); retryErr != nil {
					t.Fatal(retryErr)
				}
				if len(revoked) != 2 || revoked[1] != "/api/v1/api-tokens/"+testWorkerTokenID {
					t.Fatalf("retry revoked unrelated token: %v", revoked)
				}
			}
			if store.row.DeletionState != "active" {
				t.Fatal("healthy mapping changed")
			}
		})
	}
}

func TestHatchetCleanupRevocationFailurePreservesCiphertextAndBackoff(t *testing.T) {
	boundary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
		_, _ = w.Write([]byte("private diagnostics"))
	}))
	defer boundary.Close()
	client, _ := NewClient(boundary.URL, boundary.Client())
	box, _ := secretbox.New(bytes.Repeat([]byte{1}, 32))
	store := &tenantMemory{exists: true, row: db.WorkflowHatchetTenant{TenantID: testTenant, TokenEncrypted: "preserved"}}
	service, _ := NewTenantService(client, store, box, "email", "password")
	now := time.Now()
	for _, attempt := range []int32{0, 10} {
		store.row.DeletionAttempts = attempt
		err := service.Cleanup(context.Background(), pgtype.UUID{Valid: true}, now)
		if err == nil || strings.Contains(err.Error(), "private") || store.row.TokenEncrypted != "preserved" || store.row.DeletionState != "failed" || store.row.DeletionError.String != "tenant_token_revocation_failed" {
			t.Fatalf("failure receipt: %+v %v", store.row, err)
		}
		delay := store.row.NextDeletionAttemptAt.Time.Sub(now)
		if (attempt == 0 && delay != 2*time.Minute) || delay <= 0 || delay > 24*time.Hour {
			t.Fatalf("retry %v", delay)
		}
	}
}
func (m *tenantMemory) CreateWorkflowHatchetTenant(_ context.Context, p db.CreateWorkflowHatchetTenantParams) (db.WorkflowHatchetTenant, error) {
	if m.exists {
		return m.row, pgx.ErrNoRows
	}
	m.exists = true
	m.row = db.WorkflowHatchetTenant{WorkspaceID: p.WorkspaceID, TenantID: p.TenantID, TokenEncrypted: p.TokenEncrypted, DeletionState: "active"}
	return m.row, nil
}
func (m *tenantMemory) ClaimWorkflowHatchetTenantDeletion(_ context.Context, p db.ClaimWorkflowHatchetTenantDeletionParams) (db.WorkflowHatchetTenant, error) {
	m.row.DeletionState = "deleting"
	m.row.DeletionAttempts++
	m.row.NextDeletionAttemptAt = p.NextDeletionAttemptAt
	return m.row, nil
}
func (m *tenantMemory) FinishWorkflowHatchetTenantDeletion(_ context.Context, p db.FinishWorkflowHatchetTenantDeletionParams) (db.WorkflowHatchetTenant, error) {
	if m.stale || p.ExpectedAttempt != m.row.DeletionAttempts {
		return m.row, pgx.ErrNoRows
	}
	m.row.DeletionState = p.DeletionState
	m.row.DeletionError = p.DeletionError
	m.row.NextDeletionAttemptAt = p.NextDeletionAttemptAt
	return m.row, nil
}

func TestHatchetTenantProvisionRestartAndWorkerSeparation(t *testing.T) {
	creates := 0
	tokens := 0
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "" {
			t.Error("provisioning sent bearer")
		}
		if r.URL.Path == "/api/v1/users/login" {
			http.SetCookie(w, &http.Cookie{Name: "session", Value: "private", Path: "/"})
			_, _ = w.Write([]byte(`{}`))
			return
		}
		if _, err := r.Cookie("session"); err != nil {
			t.Error("missing private cookie")
		}
		switch r.URL.Path {
		case "/api/v1/users/memberships":
			_, _ = w.Write([]byte(`{"rows":[]}`))
		case "/api/v1/tenants":
			creates++
			_, _ = w.Write([]byte(`{"metadata":{"id":"` + testTenant + `"},"slug":"multica-` + testRun + `"}`))
		case "/api/v1/tenants/" + testTenant + "/api-tokens":
			tokens++
			name := "server-secret"
			if tokens > 1 {
				name = testWorkerToken
			}
			_ = json.NewEncoder(w).Encode(map[string]string{"token": name})
		default:
			t.Errorf("unexpected %s", r.URL.Path)
		}
	}))
	defer s.Close()
	c, _ := NewClient(s.URL, s.Client())
	box, _ := secretbox.New(bytes.Repeat([]byte{1}, 32))
	store := &tenantMemory{}
	service, err := NewTenantService(c, store, box, "private@example.com", "password")
	if err != nil {
		t.Fatal(err)
	}
	var workspace pgtype.UUID
	_ = workspace.Scan(testRun)
	first, err := service.Ensure(context.Background(), workspace)
	if err != nil {
		t.Fatal(err)
	}
	sealed, _ := base64.StdEncoding.DecodeString(store.row.TokenEncrypted)
	plain, _ := box.Open(sealed)
	if first.Token != "server-secret" || string(plain) != "server-secret" || strings.Contains(store.row.TokenEncrypted, "server-secret") {
		t.Fatal("encrypted mapping wrong")
	}
	restart, _ := NewTenantService(c, store, box, "private@example.com", "password")
	second, err := restart.Ensure(context.Background(), workspace)
	if err != nil || second != first || creates != 1 || tokens != 1 {
		t.Fatalf("restart %v creates=%d tokens=%d", err, creates, tokens)
	}
	worker, err := restart.WorkerCredentials(context.Background(), workspace, "daemon-a", "1h")
	if err != nil || worker.Token != testWorkerToken || worker.TenantID != testTenant || worker.Token == first.Token {
		t.Fatalf("worker separation: %+v %v", worker, err)
	}
}

func TestHatchetTenantCleanupFencedUnsupported(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/users/login":
			http.SetCookie(w, &http.Cookie{Name: "session", Value: "private", Path: "/"})
			_, _ = w.Write([]byte(`{}`))
		case "/api/v1/tenants/" + testTenant + "/api-tokens":
			_, _ = w.Write([]byte(`{"rows":[{"metadata":{"id":"` + testRun + `"},"name":"multica-server","revoked":false}]}`))
		case "/api/v1/api-tokens/" + testRun:
			if r.Method != "POST" {
				t.Error("wrong revoke method")
			}
			w.WriteHeader(204)
		default:
			t.Errorf("guessed cleanup route %s", r.URL.Path)
		}
	}))
	defer s.Close()
	c, _ := NewClient(s.URL, s.Client())
	box, _ := secretbox.New(bytes.Repeat([]byte{1}, 32))
	store := &tenantMemory{exists: true, row: db.WorkflowHatchetTenant{TenantID: testTenant, TokenEncrypted: "preserved"}}
	service, _ := NewTenantService(c, store, box, "email", "password")
	err := service.Cleanup(context.Background(), pgtype.UUID{Valid: true}, time.Now())
	if !errors.Is(err, ErrTenantDeletionUnsupported) || store.row.DeletionState != "failed" || store.row.TokenEncrypted != "preserved" || store.row.DeletionError.String != "tenant_deletion_unsupported" {
		t.Fatalf("cleanup: %+v %v", store.row, err)
	}
	store.stale = true
	err = service.Cleanup(context.Background(), pgtype.UUID{Valid: true}, time.Now())
	if !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("stale claim finalized: %v", err)
	}
}
