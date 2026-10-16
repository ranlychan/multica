package hatchet

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http/cookiejar"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/multica-ai/multica/server/internal/util/secretbox"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

// TenantStore is implemented directly by sqlc's *db.Queries. Deletion must be
// queued in the workspace deletion transaction; Cleanup runs asynchronously.
type TenantStore interface {
	GetWorkflowHatchetTenant(context.Context, pgtype.UUID) (db.WorkflowHatchetTenant, error)
	CreateWorkflowHatchetTenant(context.Context, db.CreateWorkflowHatchetTenantParams) (db.WorkflowHatchetTenant, error)
	ClaimWorkflowHatchetTenantDeletion(context.Context, db.ClaimWorkflowHatchetTenantDeletionParams) (db.WorkflowHatchetTenant, error)
	FinishWorkflowHatchetTenantDeletion(context.Context, db.FinishWorkflowHatchetTenantDeletionParams) (db.WorkflowHatchetTenant, error)
}

var _ TenantStore = (*db.Queries)(nil)

type TenantService struct {
	client             *Client
	store              TenantStore
	box                *secretbox.Box
	email, password    string
	mu                 sync.Mutex
	queries            *db.Queries
	tx                 TenantTxStarter
	pendingRevocations map[string]*WorkerTokenRevocationError
}

type TenantTxStarter interface {
	Begin(context.Context) (pgx.Tx, error)
}

// NewSQLTenantService finalizes mappings in a short workspace-lock transaction.
// Engine requests always run outside that transaction.
func NewSQLTenantService(client *Client, queries *db.Queries, tx TenantTxStarter, box *secretbox.Box, email, password string) (*TenantService, error) {
	if queries == nil || tx == nil {
		return nil, errors.New("hatchet: tenant database required")
	}
	s, e := NewTenantService(client, queries, box, email, password)
	if e != nil {
		return nil, e
	}
	s.queries = queries
	s.tx = tx
	s.store = &sqlTenantStore{Queries: queries, tx: tx}
	return s, nil
}

type sqlTenantStore struct {
	*db.Queries
	tx TenantTxStarter
}

func (s *TenantService) workspaceExists(ctx context.Context, workspace pgtype.UUID) error {
	tx, e := s.tx.Begin(ctx)
	if e != nil {
		return errors.New("hatchet: workspace transaction failed")
	}
	defer tx.Rollback(ctx)
	if _, e = s.queries.WithTx(tx).LockWorkspaceForChatSessionCreate(ctx, workspace); e != nil {
		if errors.Is(e, pgx.ErrNoRows) {
			return ErrUnavailable
		}
		return errors.New("hatchet: workspace read failed")
	}
	return tx.Commit(ctx)
}

func (s *sqlTenantStore) CreateWorkflowHatchetTenant(ctx context.Context, p db.CreateWorkflowHatchetTenantParams) (db.WorkflowHatchetTenant, error) {
	tx, e := s.tx.Begin(ctx)
	if e != nil {
		return db.WorkflowHatchetTenant{}, errors.New("hatchet: tenant transaction failed")
	}
	defer tx.Rollback(ctx)
	q := s.Queries.WithTx(tx)
	_, lockErr := q.LockWorkspaceForChatSessionCreate(ctx, p.WorkspaceID)
	if lockErr != nil && !errors.Is(lockErr, pgx.ErrNoRows) {
		return db.WorkflowHatchetTenant{}, lockErr
	}
	row, e := q.CreateWorkflowHatchetTenant(ctx, p)
	if e != nil {
		return row, e
	}
	if errors.Is(lockErr, pgx.ErrNoRows) {
		if e = q.QueueWorkflowHatchetTenantDeletion(ctx, p.WorkspaceID); e != nil {
			return row, e
		}
	}
	if e = tx.Commit(ctx); e != nil {
		return row, errors.New("hatchet: tenant transaction commit failed")
	}
	if errors.Is(lockErr, pgx.ErrNoRows) {
		return row, ErrUnavailable
	}
	return row, nil
}

func NewTenantService(client *Client, store TenantStore, box *secretbox.Box, email, password string) (*TenantService, error) {
	if client == nil || store == nil || box == nil || email == "" || password == "" {
		return nil, errors.New("hatchet: tenant service dependencies required")
	}
	hc := *client.http
	hc.Jar, _ = cookiejar.New(nil)
	return &TenantService{client: &Client{origin: client.origin, http: &hc}, store: store, box: box, email: email, password: password}, nil
}
func (s *TenantService) login(ctx context.Context) error {
	// The pin redirects an already-authenticated login instead of returning 200.
	// All callers hold s.mu, and NewTenantService owns a private Client/http.Client
	// copy: no request can read its jar concurrently with this replacement.
	// Start each explicit credential login with a fresh jar; never follow an
	// authentication redirect or reuse a possibly expired provisioning session.
	s.client.http.Jar, _ = cookiejar.New(nil)
	_, e := s.client.request(ctx, "POST", "/api/v1/users/login", "", map[string]string{"email": s.email, "password": s.password})
	return e
}

type tenantWire struct {
	Metadata struct {
		ID string `json:"id"`
	} `json:"metadata"`
	Slug string `json:"slug"`
}

func (s *TenantService) Ensure(ctx context.Context, workspace pgtype.UUID) (TenantCredentials, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !workspace.Valid {
		return TenantCredentials{}, errors.New("hatchet: workspace ID required")
	}
	if s.queries != nil {
		if e := s.workspaceExists(ctx, workspace); e != nil {
			return TenantCredentials{}, e
		}
	}
	row, e := s.store.GetWorkflowHatchetTenant(ctx, workspace)
	if e == nil {
		return s.credentials(row)
	}
	if !errors.Is(e, pgx.ErrNoRows) {
		return TenantCredentials{}, errors.New("hatchet: tenant mapping read failed")
	}
	if e = s.login(ctx); e != nil {
		return TenantCredentials{}, e
	}
	workspaceID := uuidText(workspace)
	slug := "multica-" + workspaceID
	raw, e := s.client.request(ctx, "GET", "/api/v1/users/memberships", "", nil)
	if e != nil {
		return TenantCredentials{}, e
	}
	var memberships struct {
		Rows []struct {
			Tenant tenantWire `json:"tenant"`
		} `json:"rows"`
	}
	if json.Unmarshal(raw, &memberships) != nil {
		return TenantCredentials{}, errors.New("hatchet: invalid membership response")
	}
	tenant := tenantWire{}
	for _, membership := range memberships.Rows {
		if membership.Tenant.Slug == slug {
			tenant = membership.Tenant
			break
		}
	}
	if tenant.Metadata.ID == "" {
		raw, e = s.client.request(ctx, "POST", "/api/v1/tenants", "", map[string]string{"name": slug, "slug": slug, "engineVersion": "V1"})
		if e != nil {
			return TenantCredentials{}, e
		}
		if json.Unmarshal(raw, &tenant) != nil {
			return TenantCredentials{}, errors.New("hatchet: invalid tenant response")
		}
	}
	if !validID(tenant.Metadata.ID) {
		return TenantCredentials{}, errors.New("hatchet: invalid tenant ID response")
	}
	cred, e := s.mint(ctx, tenant.Metadata.ID, "multica-server", "8760h")
	if e != nil {
		return TenantCredentials{}, e
	}
	sealed, e := s.box.Seal([]byte(cred.Token))
	if e != nil {
		return TenantCredentials{}, errors.New("hatchet: tenant encryption failed")
	}
	row, e = s.store.CreateWorkflowHatchetTenant(ctx, db.CreateWorkflowHatchetTenantParams{WorkspaceID: workspace, TenantID: cred.TenantID, TokenEncrypted: base64.StdEncoding.EncodeToString(sealed)})
	if errors.Is(e, pgx.ErrNoRows) {
		row, e = s.store.GetWorkflowHatchetTenant(ctx, workspace)
	}
	if e != nil {
		if errors.Is(e, ErrUnavailable) {
			return TenantCredentials{}, e
		}
		return TenantCredentials{}, errors.New("hatchet: tenant mapping write failed")
	}
	return s.credentials(row)
}
func uuidText(id pgtype.UUID) string { value, _ := id.Value(); return value.(string) }
func (s *TenantService) credentials(row db.WorkflowHatchetTenant) (TenantCredentials, error) {
	if row.DeletionState != "active" {
		return TenantCredentials{}, ErrUnavailable
	}
	sealed, e := base64.StdEncoding.DecodeString(row.TokenEncrypted)
	if e != nil {
		return TenantCredentials{}, errors.New("hatchet: invalid encrypted mapping")
	}
	plain, e := s.box.Open(sealed)
	if e != nil {
		return TenantCredentials{}, errors.New("hatchet: tenant decryption failed")
	}
	cred := TenantCredentials{TenantID: row.TenantID, Token: string(plain)}
	return cred, checkCredentials(cred)
}
func (s *TenantService) mint(ctx context.Context, tenant, name, expires string) (TenantCredentials, error) {
	raw, e := s.client.request(ctx, "POST", "/api/v1/tenants/"+tenant+"/api-tokens", "", map[string]string{"name": name, "expiresIn": expires})
	if e != nil {
		return TenantCredentials{}, e
	}
	var response struct {
		Token string `json:"token"`
	}
	if json.Unmarshal(raw, &response) != nil {
		return TenantCredentials{}, errors.New("hatchet: invalid token response")
	}
	cred := TenantCredentials{TenantID: tenant, Token: response.Token}
	return cred, checkCredentials(cred)
}

// WorkerTokenRevocationError is a server-only retry receipt for an abandoned
// issuance. It never includes the JWT or provisioning credentials.
type WorkerTokenRevocationError struct{ TenantID, TokenID string }

func (e *WorkerTokenRevocationError) Error() string {
	return fmt.Sprintf("hatchet: worker token revocation failed (tenant=%s token=%s)", e.TenantID, e.TokenID)
}

// RetryWorkerTokenRevocation retries only the token identified by the receipt.
func (s *TenantService) RetryWorkerTokenRevocation(ctx context.Context, receipt *WorkerTokenRevocationError) error {
	if receipt == nil || !validID(receipt.TenantID) || !validID(receipt.TokenID) {
		return errors.New("hatchet: invalid worker revocation receipt")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if e := s.login(ctx); e != nil {
		return receipt
	}
	_, e := s.client.request(ctx, "POST", "/api/v1/api-tokens/"+receipt.TokenID, "", nil)
	if e != nil {
		return receipt
	}
	return nil
}

func workerTokenID(token string) (string, error) {
	// The token came directly from the private mint endpoint; this decodes only
	// its source-proven token_id claim for revocation, never authenticates it.
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return "", errors.New("hatchet: invalid minted worker token")
	}
	raw, e := base64.RawURLEncoding.DecodeString(parts[1])
	if e != nil {
		return "", errors.New("hatchet: invalid minted worker token")
	}
	var claims struct {
		TokenID string `json:"token_id"`
	}
	if json.Unmarshal(raw, &claims) != nil || !validID(claims.TokenID) {
		return "", errors.New("hatchet: invalid worker token ID")
	}
	return claims.TokenID, nil
}
func (s *TenantService) abandonWorker(ctx context.Context, tenant, id string, cause error) error {
	revokeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()
	if _, e := s.client.request(revokeCtx, "POST", "/api/v1/api-tokens/"+id, "", nil); e != nil {
		receipt := &WorkerTokenRevocationError{TenantID: tenant, TokenID: id}
		// Caller holds mu. Best-effort retries survive the request, not a process
		// crash; the undisclosed worker token still has its bounded TTL.
		if s.pendingRevocations == nil {
			s.pendingRevocations = make(map[string]*WorkerTokenRevocationError)
		}
		s.pendingRevocations[id] = receipt
		return errors.Join(cause, receipt)
	}
	return cause
}

// CleanupPending retries undisclosed issuance receipts and persistent workspace
// tombstones. Database failures never erase or advance deletion receipts.
func (s *TenantService) CleanupPending(ctx context.Context) error {
	s.mu.Lock()
	receipts := make([]*WorkerTokenRevocationError, 0, len(s.pendingRevocations))
	for _, receipt := range s.pendingRevocations {
		receipts = append(receipts, receipt)
	}
	s.mu.Unlock()
	var result error
	for _, receipt := range receipts {
		if err := s.RetryWorkerTokenRevocation(ctx, receipt); err != nil {
			result = errors.Join(result, err)
		} else {
			s.mu.Lock()
			delete(s.pendingRevocations, receipt.TokenID)
			s.mu.Unlock()
		}
	}
	if s.queries == nil {
		return result
	}
	workers, err := s.queries.ListWorkflowRunnerEngineCleanup(ctx)
	if err != nil {
		return errors.Join(result, errors.New("hatchet: worker cleanup read failed"))
	}
	for _, worker := range workers {
		if worker.EngineTokenID == "" {
			result = errors.Join(result, ErrUnavailable)
			continue
		}
		if err := s.RevokeWorkerCredentials(ctx, TenantCredentials{TenantID: worker.TenantID, TokenID: worker.EngineTokenID}); err != nil {
			result = errors.Join(result, err)
		} else if err := s.queries.ConfirmWorkflowRunnerEngineRevocation(ctx, db.ConfirmWorkflowRunnerEngineRevocationParams{ID: worker.ID, EngineTokenID: worker.EngineTokenID}); err != nil {
			result = errors.Join(result, err)
		}
	}
	rows, err := s.queries.ListWorkflowHatchetTenantDeletions(ctx, 100)
	if err != nil {
		return errors.Join(result, errors.New("hatchet: pending tenant cleanup read failed"))
	}
	for _, row := range rows {
		if err := s.Cleanup(ctx, row.WorkspaceID, time.Now()); err != nil && !errors.Is(err, ErrTenantDeletionUnsupported) && !errors.Is(err, pgx.ErrNoRows) {
			result = errors.Join(result, err)
		}
	}
	return result
}

// WorkerCredentials mints a separate tenant-only token. The server token and
// provisioning cookie never leave this service. Tokens remain discoverable by
// their reserved name for cleanup even if a caller crashes before delivery.
func (s *TenantService) WorkerCredentials(ctx context.Context, workspace pgtype.UUID, daemonID, expires string) (TenantCredentials, error) {
	if daemonID == "" {
		return TenantCredentials{}, errors.New("hatchet: daemon identity required")
	}
	d, e := time.ParseDuration(expires)
	if e != nil || d <= 0 || d > 24*time.Hour {
		return TenantCredentials{}, errors.New("hatchet: worker token expiration invalid")
	}
	cred, e := s.Ensure(ctx, workspace)
	if e != nil {
		return TenantCredentials{}, e
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if e = s.login(ctx); e != nil {
		return TenantCredentials{}, e
	}
	worker, e := s.mint(ctx, cred.TenantID, "multica-worker-"+daemonID, expires)
	if e != nil {
		return TenantCredentials{}, e
	}
	tokenID, e := workerTokenID(worker.Token)
	if e != nil {
		return TenantCredentials{}, e
	}
	worker.TokenID = tokenID
	parts := strings.Split(worker.Token, ".")
	rawClaims, _ := base64.RawURLEncoding.DecodeString(parts[1])
	var claims struct {
		Exp int64 `json:"exp"`
	}
	if json.Unmarshal(rawClaims, &claims) != nil || claims.Exp <= time.Now().Unix() {
		return TenantCredentials{}, s.abandonWorker(ctx, cred.TenantID, tokenID, errors.New("hatchet: invalid worker token expiry"))
	}
	worker.ExpiresAt = time.Unix(claims.Exp, 0)
	row, readErr := s.store.GetWorkflowHatchetTenant(ctx, workspace)
	if readErr != nil {
		cause := errors.New("hatchet: worker mapping recheck failed")
		if errors.Is(readErr, pgx.ErrNoRows) {
			cause = ErrUnavailable
		}
		return TenantCredentials{}, s.abandonWorker(ctx, cred.TenantID, tokenID, cause)
	}
	if row.DeletionState != "active" {
		return TenantCredentials{}, s.abandonWorker(ctx, cred.TenantID, tokenID, ErrUnavailable)
	}
	if s.queries != nil {
		if e = s.workspaceExists(ctx, workspace); e != nil {
			return TenantCredentials{}, s.abandonWorker(ctx, cred.TenantID, tokenID, e)
		}
	}
	return worker, nil
}

// Cleanup revokes every Multica-issued token using the server cookie. The pin
// has no physical tenant deletion API; retain the fenced encrypted tombstone
// with an operator reason, retrying at most once per day for that gap.
func (s *TenantService) Cleanup(ctx context.Context, workspace pgtype.UUID, now time.Time) error {
	row, e := s.store.ClaimWorkflowHatchetTenantDeletion(ctx, db.ClaimWorkflowHatchetTenantDeletionParams{WorkspaceID: workspace, NextDeletionAttemptAt: pgtype.Timestamptz{Time: now.Add(5 * time.Minute), Valid: true}})
	if e != nil {
		return e
	}
	s.mu.Lock()
	e = s.revoke(ctx, row.TenantID)
	s.mu.Unlock()
	reason := "tenant_deletion_unsupported"
	retry := 24 * time.Hour
	result := ErrTenantDeletionUnsupported
	if e != nil {
		reason = "tenant_token_revocation_failed"
		result = errors.New("hatchet: tenant token revocation failed")
		retry = time.Minute * time.Duration(1<<min(row.DeletionAttempts, 10))
		if retry > 24*time.Hour {
			retry = 24 * time.Hour
		}
	}
	_, finishErr := s.store.FinishWorkflowHatchetTenantDeletion(ctx, db.FinishWorkflowHatchetTenantDeletionParams{WorkspaceID: workspace, DeletionState: "failed", DeletionError: pgtype.Text{String: reason, Valid: true}, NextDeletionAttemptAt: pgtype.Timestamptz{Time: now.Add(retry), Valid: true}, ExpectedAttempt: row.DeletionAttempts})
	if finishErr != nil {
		return finishErr
	}
	return result
}
func (s *TenantService) revoke(ctx context.Context, tenant string) error {
	if !validID(tenant) {
		return errors.New("hatchet: invalid tenant mapping")
	}
	if e := s.login(ctx); e != nil {
		return e
	}
	raw, e := s.client.request(ctx, "GET", "/api/v1/tenants/"+tenant+"/api-tokens", "", nil)
	if e != nil {
		return e
	}
	var response struct {
		Rows []struct {
			Metadata struct {
				ID string `json:"id"`
			} `json:"metadata"`
			Name    string `json:"name"`
			Revoked bool   `json:"revoked"`
		} `json:"rows"`
	}
	if json.Unmarshal(raw, &response) != nil {
		return errors.New("hatchet: invalid token list response")
	}
	for _, token := range response.Rows {
		if token.Revoked || !(token.Name == "multica-server" || strings.HasPrefix(token.Name, "multica-worker-")) {
			continue
		}
		if !validID(token.Metadata.ID) {
			return errors.New("hatchet: invalid token ID response")
		}
		if _, e = s.client.request(ctx, "POST", "/api/v1/api-tokens/"+token.Metadata.ID, "", nil); e != nil {
			return e
		}
	}
	return nil
}
