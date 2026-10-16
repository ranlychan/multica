package handler

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/multica-ai/multica/server/internal/hatchet"
	"github.com/multica-ai/multica/server/internal/testutil"
	"github.com/multica-ai/multica/server/internal/util/secretbox"
	wr "github.com/multica-ai/multica/server/internal/workflowrunner"
)

// This regression deliberately shares the production SQL provider, encrypted
// mappings and HTTP verifier. A fake provider or direct ack response misses its
// provisioning mutex, which used to block the five-second lease request.
func TestWorkflowSharedTenantProvisioningCannotBlockCheckedPairLeases(t *testing.T) {
	for _, operation := range []string{"mint", "revoke"} {
		t.Run(operation, func(t *testing.T) {
			a := newWorkflowBoundary(t)
			testutil.Call(t, a.h.WorkflowRunnerAcknowledge, a.request("POST", "ack", a.ready)).Want(200)
			var replacement wr.Credentials
			testutil.Call(t, a.h.WorkflowRunnerRotate, a.request("POST", "rotate", nil)).Want(200).JSON(&replacement)
			current := a.ready
			current.Generation = replacement.Generation
			testutil.Call(t, a.h.WorkflowRunnerAcknowledge, a.request("POST", "ack", current)).Want(200)
			otherWorkspace := dbfx.Workspace(t, "shared tenant lease", "shared-tenant-"+uuid.NewString())
			dbfx.Member(t, otherWorkspace, testUserID, "owner")
			b := newWorkflowBoundaryInWorkspace(t, otherWorkspace)
			testutil.Call(t, b.h.WorkflowRunnerAcknowledge, b.request("POST", "ack", b.ready)).Want(200)
			var prepared wr.Credentials
			testutil.Call(t, b.h.WorkflowRunnerRotate, b.request("POST", "rotate", nil)).Want(200).JSON(&prepared)
			candidate := b.ready
			candidate.Generation = prepared.Generation

			box, err := secretbox.New(make([]byte, secretbox.KeySize))
			if err != nil {
				t.Fatal(err)
			}
			evidence := map[string]wr.Registration{}
			for _, boundary := range []*workflowBoundary{a, b} {
				serverToken := "server-only-" + boundary.workspaceID
				sealed, err := box.Seal([]byte(serverToken))
				if err != nil {
					t.Fatal(err)
				}
				dbfx.Exec(t, "UPDATE workflow_hatchet_tenant SET token_encrypted=$2 WHERE workspace_id=$1", boundary.workspaceID, base64.StdEncoding.EncodeToString(sealed))
				evidence[serverToken] = boundary.ready.Registrations[0]
			}
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			var provisioning atomic.Int32
			engine := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.URL.Path == "/api/v1/users/login":
					provisioning.Add(1)
					if r.Header.Get("Cookie") != "" {
						t.Error("explicit login reused private auth cookie")
						w.Header().Set("Location", "/unexpected-redirect")
						w.WriteHeader(302)
						return
					}
					http.SetCookie(w, &http.Cookie{Name: "session", Value: "private", Path: "/"})
					w.Write([]byte(`{}`))
				case r.Method == "POST" && (strings.HasSuffix(r.URL.Path, "/api-tokens") || strings.HasPrefix(r.URL.Path, "/api/v1/api-tokens/")):
					provisioning.Add(1)
					mint := strings.HasSuffix(r.URL.Path, "/api-tokens")
					if (operation == "mint") == mint {
						close(entered)
						select {
						case <-release:
						case <-r.Context().Done():
						}
					}
					if mint {
						claims, _ := json.Marshal(map[string]any{"token_id": uuid.NewString(), "sub": a.cred.TenantID, "exp": time.Now().Add(time.Hour).Unix()})
						json.NewEncoder(w).Encode(map[string]string{"token": "header." + base64.RawURLEncoding.EncodeToString(claims) + ".signature"})
					} else {
						w.WriteHeader(204)
					}
				case r.Method == "GET" && strings.HasSuffix(r.URL.Path, "/versions"):
					ev, ok := evidence[strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")]
					if !ok || r.URL.Query().Get("version") != ev.EngineVersionID {
						t.Error("immutable read used wrong mapped tenant credential")
						w.WriteHeader(401)
						return
					}
					json.NewEncoder(w).Encode(map[string]any{"metadata": map[string]string{"id": ev.EngineVersionID}, "workflow": map[string]any{"metadata": map[string]string{"id": ev.EngineWorkflowID}, "name": ev.EngineName}, "workflowConfig": map[string]any{"Name": ev.EngineName, "Tasks": []map[string]any{{"ReadableId": ev.Actions[0].TaskName, "Action": ev.Actions[0].ActionID, "Parents": []string{}}}, "idempotency": map[string]any{"expression": "input.multica_run_id", "ttlMs": 3600000, "method": "TTL"}}})
				default:
					t.Error("unexpected provisioning/fallback/redirect request")
					w.WriteHeader(500)
				}
			}))
			defer func() { once.Do(func() { close(release) }); engine.Close() }()
			client, err := hatchet.NewClient(engine.URL, engine.Client())
			if err != nil {
				t.Fatal(err)
			}
			shared, err := hatchet.NewSQLTenantService(client, a.h.Queries, testPool, box, "private@test.invalid", "private")
			if err != nil {
				t.Fatal(err)
			}
			for _, boundary := range []*workflowBoundary{a, b} {
				boundary.h.WorkflowControl.Tenants = shared
				boundary.h.WorkflowControl.Verifier = client
			}
			opDone := make(chan error, 1)
			opExited := make(chan struct{})
			go func() {
				defer close(opExited)
				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
				defer cancel()
				if operation == "mint" {
					_, err := shared.WorkerCredentials(ctx, parseUUID(a.workspaceID), "blocked-candidate", "1h")
					opDone <- err
				} else {
					opDone <- shared.RevokeWorkerCredentials(ctx, hatchet.TenantCredentials{TenantID: a.cred.TenantID, TokenID: uuid.NewString()})
				}
			}()
			select {
			case <-entered:
			case <-time.After(3 * time.Second):
				t.Fatal("production provider did not enter blocked engine operation")
			}
			type result struct {
				name string
				code int
			}
			acks := make(chan result, 4)
			for _, item := range []struct {
				boundary *workflowBoundary
				name     string
				message  wr.Message
				prepare  bool
			}{{a, "retiring", a.ready, false}, {a, "current", current, false}, {b, "other-current", b.ready, false}, {b, "other-prepare", candidate, true}} {
				go func(item struct {
					boundary *workflowBoundary
					name     string
					message  wr.Message
					prepare  bool
				}) {
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					req := item.boundary.request("POST", "ack", item.message).WithContext(ctx)
					rec := httptest.NewRecorder()
					if item.prepare {
						item.boundary.h.WorkflowRunnerPrepare(rec, req)
					} else {
						item.boundary.h.WorkflowRunnerAcknowledge(rec, req)
					}
					acks <- result{item.name, rec.Code}
				}(item)
			}
			deadline := time.NewTimer(6 * time.Second)
			timedOut := false
			for i := 0; i < 4; i++ {
				select {
				case got := <-acks:
					if got.code != 200 {
						t.Errorf("%s heartbeat status%d", got.name, got.code)
					}
				case <-deadline.C:
					timedOut = true
					t.Error("real shared tenant mutex starved five-second production ack/prepare")
					once.Do(func() { close(release) })
					for ; i < 4; i++ {
						<-acks
					}
				}
			}
			deadline.Stop()
			if !timedOut {
				select {
				case <-opExited:
					t.Error("engine operation was not still blocked while leases renewed")
				default:
				}
			}
			once.Do(func() { close(release) })
			if err := <-opDone; err != nil {
				t.Fatal(err)
			}
			if timedOut {
				return
			}
			for _, boundary := range []*workflowBoundary{a, b} {
				if !boundary.h.WorkflowRunnerEligible(context.Background(), parseUUID(boundary.workspaceID), boundary.revisionID) {
					t.Error("checked current lost eligibility during unrelated engine operation")
				}
			}
			before := provisioning.Load()
			for _, state := range []string{"deleting", "wrong-tenant", "corrupt", "missing"} {
				switch state {
				case "deleting":
					dbfx.Exec(t, "UPDATE workflow_hatchet_tenant SET deletion_state='pending' WHERE workspace_id=$1", a.workspaceID)
				case "wrong-tenant":
					dbfx.Exec(t, "UPDATE workflow_hatchet_tenant SET deletion_state='active',tenant_id=$2 WHERE workspace_id=$1", a.workspaceID, uuid.NewString())
				case "corrupt":
					dbfx.Exec(t, "UPDATE workflow_hatchet_tenant SET deletion_state='active',token_encrypted='corrupt',tenant_id=$2 WHERE workspace_id=$1", a.workspaceID, a.cred.TenantID)
				case "missing":
					dbfx.Exec(t, "DELETE FROM workflow_hatchet_tenant WHERE workspace_id=$1", a.workspaceID)
				}
				testutil.Call(t, a.h.WorkflowRunnerAcknowledge, a.request("POST", "ack", current)).Want(503)
				testutil.Call(t, b.h.WorkflowRunnerAcknowledge, b.request("POST", "ack", b.ready)).Want(200)
			}
			if provisioning.Load() != before {
				t.Fatal("ack credential lookup provisioned or mutated auth session")
			}
			if _, err := shared.MappedCredentials(context.Background(), parseUUID(uuid.NewString())); err == nil {
				t.Fatal("read-only credentials accepted nonexistent current workspace")
			}
			t.Logf("real shared SQL provider %s blocked; pair/other-workspace ack and prepare stayed authorized; unavailable mappings failed closed", operation)
		})
	}
}
