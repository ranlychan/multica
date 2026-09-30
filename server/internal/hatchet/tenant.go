package hatchet

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
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
	client          *Client
	store           TenantStore
	box             *secretbox.Box
	email, password string
	mu              sync.Mutex
	queries         *db.Queries
	tx              TenantTxStarter
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
	row, readErr := s.store.GetWorkflowHatchetTenant(ctx, workspace)
	if readErr != nil || row.DeletionState != "active" {
		_ = s.revoke(ctx, cred.TenantID)
		return TenantCredentials{}, ErrUnavailable
	}
	if s.queries != nil {
		if e = s.workspaceExists(ctx, workspace); e != nil {
			_ = s.revoke(ctx, cred.TenantID)
			return TenantCredentials{}, ErrUnavailable
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
