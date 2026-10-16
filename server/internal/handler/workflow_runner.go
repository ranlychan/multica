package handler

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/multica-ai/multica/server/internal/auth"
	"github.com/multica-ai/multica/server/internal/hatchet"
	"github.com/multica-ai/multica/server/internal/service"
	"github.com/multica-ai/multica/server/internal/util"
	wr "github.com/multica-ai/multica/server/internal/workflowrunner"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

type WorkflowTenantProvider interface {
	WorkerCredentials(context.Context, pgtype.UUID, string, string) (hatchet.TenantCredentials, error)
	Ensure(context.Context, pgtype.UUID) (hatchet.TenantCredentials, error)
	MappedCredentials(context.Context, pgtype.UUID) (hatchet.TenantCredentials, error)
	RevokeWorkerCredentials(context.Context, hatchet.TenantCredentials) error
}
type WorkflowRegistrationVerifier interface {
	VerifyRegistration(context.Context, hatchet.TenantCredentials, wr.Registration) error
}
type WorkflowAssignmentVerifier interface {
	VerifyAssignment(context.Context, hatchet.Assignment) error
}

// WorkflowControl never executes authored source. Its engine client is private/server-only.
type WorkflowControl struct {
	Ready       func() bool
	Tenants     WorkflowTenantProvider
	Verifier    WorkflowRegistrationVerifier
	Assignments WorkflowAssignmentVerifier
	// Task6 supplies business operations; authentication stays in this handler.
	Action func(context.Context, WorkflowActionAuthority, WorkflowActionRequest) (json.RawMessage, error)
}
type WorkflowActionRequest struct {
	Protocol        int             `json:"protocol"`
	RunID           string          `json:"run_id"`
	StepID          string          `json:"step_id"`
	Attempt         int             `json:"attempt"`
	Operation       string          `json:"operation"`
	OperationKey    string          `json:"operation_key,omitempty"`
	Payload         json.RawMessage `json:"payload"`
	EngineRunID     string          `json:"engine_run_id"`
	EngineTaskID    string          `json:"engine_task_id"`
	InvocationCount int             `json:"invocation_count"`
}
type WorkflowActionAuthority struct {
	Run       db.WorkflowRun
	Revision  db.WorkflowRevision
	Runner    db.WorkflowRunnerToken
	Principal pgtype.UUID
}

func workflowSecret(prefix string) (string, error) {
	var b [32]byte
	_, e := rand.Read(b[:])
	return prefix + hex.EncodeToString(b[:]), e
}
func workflowPrivate(w http.ResponseWriter) { w.Header().Set("Cache-Control", "no-store") }
func workflowDenied(w http.ResponseWriter, status int, code string) {
	workflowPrivate(w)
	writeJSON(w, status, map[string]string{"code": code})
}
func (h *Handler) workflowControlReady() bool {
	return h.WorkflowControl != nil && h.WorkflowControl.Ready != nil && h.WorkflowControl.Ready()
}

func (h *Handler) enrollWorkflowDaemon(r *http.Request, req DaemonRegisterRequest, owner pgtype.UUID) (string, error) {
	if !owner.Valid || isMachineCredentialActor(r) {
		return "", errors.New("workflow_forbidden")
	}
	// Read current membership even if ordinary daemon authentication used a cache.
	if _, e := h.getWorkspaceMember(r.Context(), uuidToString(owner), req.WorkspaceID); e != nil {
		return "", errors.New("workflow_forbidden")
	}
	if req.WorkflowCapable {
		major, _ := strconv.Atoi(strings.Split(req.NodeVersion, ".")[0])
		if !req.WorkflowCodeRiskAccepted || req.RunnerVersion != "0.1.0" || major < 22 {
			return "", errors.New("workflow_invalid_capability")
		}
	}
	token, e := workflowSecret("wdc_")
	if e != nil {
		return "", e
	}
	tx, e := h.TxStarter.Begin(r.Context())
	if e != nil {
		return "", errors.New("workflow_unavailable")
	}
	defer tx.Rollback(r.Context())
	q := h.Queries.WithTx(tx)
	ws := parseUUID(req.WorkspaceID)
	if _, e = q.LockWorkspaceForChatSessionCreate(r.Context(), ws); e != nil {
		return "", errors.New("workflow_forbidden")
	}
	if _, e = q.GetMemberByUserAndWorkspace(r.Context(), db.GetMemberByUserAndWorkspaceParams{WorkspaceID: ws, UserID: owner}); e != nil {
		return "", errors.New("workflow_forbidden")
	}
	// Human-authenticated renewal proves possession of the existing machine
	// capability and extends it without invalidating active/retiring workers.
	if strings.HasPrefix(req.WorkflowControlToken, "wdc_") {
		if existing, err := q.AuthorizeWorkflowControl(r.Context(), db.AuthorizeWorkflowControlParams{WorkspaceID: ws, ControlHash: auth.HashToken(req.WorkflowControlToken)}); err == nil && existing.OwnerID == owner && existing.DaemonID == req.DaemonID {
			token = req.WorkflowControlToken
		}
	}
	_, e = q.EnrollWorkflowDaemon(r.Context(), db.EnrollWorkflowDaemonParams{WorkspaceID: ws, DaemonID: req.DaemonID, OwnerID: owner, ControlHash: auth.HashToken(token), ControlExpiresAt: pgtype.Timestamptz{Time: time.Now().Add(24 * time.Hour), Valid: true}, Capable: req.WorkflowCapable, RunnerVersion: req.RunnerVersion, NodeVersion: req.NodeVersion})
	if e != nil {
		return "", errors.New("workflow_forbidden")
	}
	if token != req.WorkflowControlToken || !req.WorkflowCapable {
		// Replacing authority must fence the previous owner's whole pair in the
		// same transaction. Possession-backed renewal leaves that pair untouched.
		if e = q.RevokeWorkflowRunnerTokens(r.Context(), db.RevokeWorkflowRunnerTokensParams{WorkspaceID: ws, DaemonID: req.DaemonID}); e != nil {
			return "", errors.New("workflow_unavailable")
		}
	}
	if tx.Commit(r.Context()) != nil {
		return "", errors.New("workflow_unavailable")
	}
	_ = h.cleanupWorkflowRunnerCredentials(r.Context(), ws, req.DaemonID)
	return token, nil
}
func (h *Handler) authorizeWorkflowControl(w http.ResponseWriter, r *http.Request) (db.WorkflowDaemon, bool) {
	workflowPrivate(w)
	if !h.workflowControlReady() {
		workflowDenied(w, 503, "workflow_unavailable")
		return db.WorkflowDaemon{}, false
	}
	token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	ws, e := util.ParseUUID(r.Header.Get("X-Workspace-ID"))
	if e != nil || !strings.HasPrefix(token, "wdc_") {
		workflowDenied(w, 401, "workflow_unauthenticated")
		return db.WorkflowDaemon{}, false
	}
	d, e := h.Queries.AuthorizeWorkflowControl(r.Context(), db.AuthorizeWorkflowControlParams{ControlHash: auth.HashToken(token), WorkspaceID: ws})
	if e != nil {
		workflowDenied(w, 401, "workflow_unauthenticated")
		return d, false
	}
	return d, true
}
func wireRevision(r db.WorkflowRevision) wr.Revision {
	var g wr.Graph
	_ = json.Unmarshal(r.DefinitionGraph, &g)
	return wr.Revision{WorkflowID: uuidToString(r.WorkflowID), RevisionID: uuidToString(r.ID), Version: r.Version, Hash: r.DefinitionHash, EngineName: r.HatchetName, Source: r.Definition, Graph: g, GraphDigest: wr.GraphDigest(g)}
}
func (h *Handler) WorkflowRunnerRevisions(w http.ResponseWriter, r *http.Request) {
	d, ok := h.authorizeWorkflowControl(w, r)
	if !ok {
		return
	}
	rows, e := h.Queries.ListWorkflowRunnerRevisions(r.Context(), d.WorkspaceID)
	if e != nil || len(rows) > 256 {
		workflowDenied(w, 503, "workflow_unavailable")
		return
	}
	revisions := make([]wr.Revision, 0, len(rows))
	for _, row := range rows {
		revisions = append(revisions, wireRevision(row))
	}
	writeJSON(w, 200, map[string]any{"revisions": revisions})
}

// This helper never performs engine I/O while holding application row locks.
func (h *Handler) lockWorkflowGeneration(ctx context.Context, d db.WorkflowDaemon) (pgx.Tx, *db.Queries, error) {
	tx, err := h.TxStarter.Begin(ctx)
	if err != nil {
		return nil, nil, err
	}
	q := h.Queries.WithTx(tx)
	if _, err = q.LockWorkspaceForChatSessionCreate(ctx, d.WorkspaceID); err == nil {
		_, err = q.LockWorkflowControl(ctx, db.LockWorkflowControlParams{WorkspaceID: d.WorkspaceID, ControlHash: d.ControlHash})
	}
	if err != nil {
		_ = tx.Rollback(ctx)
		return nil, nil, err
	}
	return tx, q, nil
}
func (h *Handler) cleanupWorkflowRunnerCredentials(ctx context.Context, ws pgtype.UUID, daemon string) error {
	rows, err := h.Queries.ListWorkflowRunnerEngineCleanupForDaemon(ctx, db.ListWorkflowRunnerEngineCleanupForDaemonParams{WorkspaceID: ws, DaemonID: daemon})
	if err != nil {
		return err
	}
	for _, row := range rows {
		if row.EngineTokenID == "" {
			return errors.New("workflow_cleanup_pending")
		}
		if h.WorkflowControl == nil || h.WorkflowControl.Tenants == nil {
			return errors.New("workflow_unavailable")
		}
		if err = h.WorkflowControl.Tenants.RevokeWorkerCredentials(ctx, hatchet.TenantCredentials{TenantID: row.TenantID, TokenID: row.EngineTokenID}); err != nil {
			return err
		}
		if err = h.Queries.ConfirmWorkflowRunnerEngineRevocation(ctx, db.ConfirmWorkflowRunnerEngineRevocationParams{ID: row.ID, EngineTokenID: row.EngineTokenID}); err != nil {
			return err
		}
	}
	return nil
}
func (h *Handler) WorkflowRunnerRotate(w http.ResponseWriter, r *http.Request) {
	d, ok := h.authorizeWorkflowControl(w, r)
	if !ok {
		return
	}
	if h.WorkflowControl.Tenants == nil {
		workflowDenied(w, 503, "workflow_unavailable")
		return
	}
	// Fence a superseded candidate before minting. Concurrent issuance owns a
	// reservation (empty engine ID) and cannot be superseded while mint is in flight.
	tx, q, e := h.lockWorkflowGeneration(r.Context(), d)
	if e != nil {
		workflowDenied(w, 503, "workflow_unavailable")
		return
	}
	defer tx.Rollback(r.Context())
	pending, e := q.WorkflowRunnerRetirementPending(r.Context(), db.WorkflowRunnerRetirementPendingParams{WorkspaceID: d.WorkspaceID, DaemonID: d.DaemonID})
	if e != nil {
		workflowDenied(w, 503, "workflow_unavailable")
		return
	}
	if pending {
		workflowDenied(w, 409, "workflow_retirement_pending")
		return
	}
	issuing, e := q.WorkflowRunnerIssuancePending(r.Context(), db.WorkflowRunnerIssuancePendingParams{WorkspaceID: d.WorkspaceID, DaemonID: d.DaemonID})
	if e != nil || issuing {
		workflowDenied(w, 409, "workflow_cleanup_pending")
		return
	}
	if e = q.RevokeWorkflowCandidateAndExpiredGenerations(r.Context(), db.RevokeWorkflowCandidateAndExpiredGenerationsParams{WorkspaceID: d.WorkspaceID, DaemonID: d.DaemonID}); e != nil {
		workflowDenied(w, 503, "workflow_unavailable")
		return
	}
	if tx.Commit(r.Context()) != nil {
		workflowDenied(w, 503, "workflow_unavailable")
		return
	}
	if h.cleanupWorkflowRunnerCredentials(r.Context(), d.WorkspaceID, d.DaemonID) != nil {
		workflowDenied(w, 409, "workflow_cleanup_pending")
		return
	}
	// Ensure is server-only, outside locks. Recheck ownership/pair before reserving.
	serverCred, e := h.WorkflowControl.Tenants.Ensure(r.Context(), d.WorkspaceID)
	if e != nil {
		workflowDenied(w, 503, "workflow_unavailable")
		return
	}
	tx, q, e = h.lockWorkflowGeneration(r.Context(), d)
	if e != nil {
		workflowDenied(w, 401, "workflow_unauthenticated")
		return
	}
	defer tx.Rollback(r.Context())
	pending, e = q.WorkflowRunnerRetirementPending(r.Context(), db.WorkflowRunnerRetirementPendingParams{WorkspaceID: d.WorkspaceID, DaemonID: d.DaemonID})
	issuing, issueErr := q.WorkflowRunnerIssuancePending(r.Context(), db.WorkflowRunnerIssuancePendingParams{WorkspaceID: d.WorkspaceID, DaemonID: d.DaemonID})
	cleanup, cleanupErr := q.ListWorkflowRunnerEngineCleanupForDaemon(r.Context(), db.ListWorkflowRunnerEngineCleanupForDaemonParams{WorkspaceID: d.WorkspaceID, DaemonID: d.DaemonID})
	if e != nil || issueErr != nil || cleanupErr != nil || pending || issuing || len(cleanup) > 0 {
		workflowDenied(w, 409, "workflow_cleanup_pending")
		return
	}
	token, e := workflowSecret("wrt_")
	if e != nil {
		workflowDenied(w, 503, "workflow_unavailable")
		return
	}
	issuanceDeadline := time.Now().Add(30 * time.Second)
	row, e := q.CreateWorkflowRunnerGeneration(r.Context(), db.CreateWorkflowRunnerGenerationParams{WorkspaceID: d.WorkspaceID, DaemonID: d.DaemonID, TokenHash: auth.HashToken(token), CreatedByID: d.OwnerID, ExpiresAt: pgtype.Timestamptz{Time: issuanceDeadline.Add(time.Hour), Valid: true}, ControlHash: d.ControlHash, TenantID: serverCred.TenantID})
	if e != nil {
		workflowDenied(w, 503, "workflow_unavailable")
		return
	}
	if n, err := q.BeginWorkflowRunnerEngineIssuance(r.Context(), row.ID); err != nil || n != 1 {
		workflowDenied(w, 503, "workflow_unavailable")
		return
	}
	if tx.Commit(r.Context()) != nil {
		workflowDenied(w, 503, "workflow_unavailable")
		return
	}
	mintCtx, mintCancel := context.WithDeadline(r.Context(), issuanceDeadline)
	cred, e := h.WorkflowControl.Tenants.WorkerCredentials(mintCtx, d.WorkspaceID, d.DaemonID, "1h")
	mintCancel()
	var receipt *hatchet.WorkerTokenRevocationError
	if errors.As(e, &receipt) && receipt.TenantID == serverCred.TenantID {
		cred = hatchet.TenantCredentials{TenantID: receipt.TenantID, TokenID: receipt.TokenID, ExpiresAt: row.ExpiresAt.Time}
	}
	delivered := false
	defer func() {
		if delivered {
			return
		}
		ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), 5*time.Second)
		defer cancel()
		if cred.TokenID != "" {
			_ = h.Queries.RememberWorkflowRunnerAbandonedEngineToken(ctx, db.RememberWorkflowRunnerAbandonedEngineTokenParams{ID: row.ID, EngineTokenID: cred.TokenID, ExpiresAt: pgtype.Timestamptz{Time: cred.ExpiresAt, Valid: true}})
		} else {
			_, _ = h.Queries.RetireWorkflowRunnerGeneration(ctx, db.RetireWorkflowRunnerGenerationParams{WorkspaceID: d.WorkspaceID, DaemonID: d.DaemonID, ID: row.ID, ControlHash: d.ControlHash})
		}
		if cred.Token != "" {
			_ = h.WorkflowControl.Tenants.RevokeWorkerCredentials(ctx, cred)
		}
		_ = h.cleanupWorkflowRunnerCredentials(ctx, d.WorkspaceID, d.DaemonID)
	}()
	if e != nil || cred.TokenID == "" || cred.TenantID != serverCred.TenantID || !cred.ExpiresAt.After(time.Now()) {
		workflowDenied(w, 503, "workflow_unavailable")
		return
	}
	tx, q, e = h.lockWorkflowGeneration(r.Context(), d)
	if e != nil {
		workflowDenied(w, 401, "workflow_unauthenticated")
		return
	}
	defer tx.Rollback(r.Context())
	n, e := q.SetWorkflowRunnerEngineToken(r.Context(), db.SetWorkflowRunnerEngineTokenParams{WorkspaceID: d.WorkspaceID, DaemonID: d.DaemonID, ID: row.ID, ControlHash: d.ControlHash, EngineTokenID: cred.TokenID, ExpiresAt: pgtype.Timestamptz{Time: cred.ExpiresAt, Valid: true}})
	if e != nil || n != 1 || tx.Commit(r.Context()) != nil {
		workflowDenied(w, 503, "workflow_unavailable")
		return
	}
	delivered = true
	writeJSON(w, 200, wr.Credentials{WorkspaceID: uuidToString(d.WorkspaceID), TenantID: cred.TenantID, Generation: uuidToString(row.ID), EngineToken: cred.Token, RunnerToken: token, ExpiresAt: cred.ExpiresAt})
}
func (h *Handler) WorkflowRunnerRevoke(w http.ResponseWriter, r *http.Request) {
	d, ok := h.authorizeWorkflowControl(w, r)
	if !ok {
		return
	}
	tx, e := h.TxStarter.Begin(r.Context())
	if e != nil {
		workflowDenied(w, 503, "workflow_unavailable")
		return
	}
	defer tx.Rollback(r.Context())
	q := h.Queries.WithTx(tx)
	if _, e = q.LockWorkspaceForChatSessionCreate(r.Context(), d.WorkspaceID); e != nil {
		workflowDenied(w, 401, "workflow_unauthenticated")
		return
	}
	if _, e = q.LockWorkflowControl(r.Context(), db.LockWorkflowControlParams{WorkspaceID: d.WorkspaceID, ControlHash: d.ControlHash}); e != nil {
		workflowDenied(w, 401, "workflow_unauthenticated")
		return
	}
	if q.RevokeWorkflowRunnerTokens(r.Context(), db.RevokeWorkflowRunnerTokensParams{WorkspaceID: d.WorkspaceID, DaemonID: d.DaemonID}) != nil || tx.Commit(r.Context()) != nil {
		workflowDenied(w, 503, "workflow_unavailable")
		return
	}
	_ = h.cleanupWorkflowRunnerCredentials(r.Context(), d.WorkspaceID, d.DaemonID)
	writeJSON(w, 200, map[string]bool{"revoked": true})
}
func (h *Handler) WorkflowRunnerAcknowledge(w http.ResponseWriter, r *http.Request) {
	h.workflowRunnerAcknowledge(w, r, false)
}
func (h *Handler) WorkflowRunnerPrepare(w http.ResponseWriter, r *http.Request) {
	h.workflowRunnerAcknowledge(w, r, true)
}
func (h *Handler) workflowRunnerAcknowledge(w http.ResponseWriter, r *http.Request, prepare bool) {
	d, ok := h.authorizeWorkflowControl(w, r)
	if !ok {
		return
	}
	var m wr.Message
	if json.NewDecoder(http.MaxBytesReader(w, r.Body, 2<<20)).Decode(&m) != nil || m.Type != "ready" || m.Protocol != 1 || m.WorkspaceID != uuidToString(d.WorkspaceID) || m.RunnerVersion != d.RunnerVersion || m.NodeVersion != d.NodeVersion || len(m.Registrations) > 256 {
		workflowDenied(w, 400, "workflow_validation_mismatch")
		return
	}
	gen, e := util.ParseUUID(m.Generation)
	if e != nil {
		workflowDenied(w, 400, "workflow_validation_mismatch")
		return
	}
	token, e := h.Queries.GetWorkflowRunnerGeneration(r.Context(), db.GetWorkflowRunnerGenerationParams{WorkspaceID: d.WorkspaceID, DaemonID: d.DaemonID, ID: gen, ControlHash: d.ControlHash})
	if e != nil || token.TenantID != m.TenantID {
		workflowDenied(w, 401, "workflow_unauthenticated")
		return
	}
	if h.WorkflowControl.Tenants == nil || h.WorkflowControl.Verifier == nil {
		workflowDenied(w, 503, "workflow_unavailable")
		return
	}
	cred, e := h.WorkflowControl.Tenants.MappedCredentials(r.Context(), d.WorkspaceID)
	if e != nil || cred.TenantID != m.TenantID {
		workflowDenied(w, 503, "workflow_unavailable")
		return
	}
	registrations := map[string]wr.Registration{}
	for _, ev := range m.Registrations {
		id, e := util.ParseUUID(ev.RevisionID)
		if e != nil {
			workflowDenied(w, 400, "workflow_validation_mismatch")
			return
		}
		rev, e := h.Queries.GetWorkflowRevisionInWorkspace(r.Context(), db.GetWorkflowRevisionInWorkspaceParams{ID: id, WorkspaceID: d.WorkspaceID})
		expected := wireRevision(rev)
		graph, _ := json.Marshal(ev.Graph)
		if e != nil || rev.ValidationStatus == "invalid" || ev.Hash != wr.Hash(rev.Definition) || ev.Hash != rev.DefinitionHash || ev.Version != rev.Version || ev.WorkflowID != expected.WorkflowID || ev.EngineName != expected.EngineName || ev.GraphDigest != expected.GraphDigest || wr.GraphDigest(ev.Graph) != expected.GraphDigest || service.ValidateWorkflowGraph(graph) != nil || len(ev.Actions) != len(ev.Graph.Steps) {
			workflowDenied(w, 409, "workflow_validation_mismatch")
			return
		}
		actions := map[string]wr.Action{}
		for _, a := range ev.Actions {
			actions[a.StepID] = a
		}
		for _, step := range ev.Graph.Steps {
			a := actions[step.ID]
			name := service.WorkflowActionName(rev, step.ID)
			if a.TaskName != name || a.ActionID != rev.HatchetName+":"+name {
				workflowDenied(w, 409, "workflow_validation_mismatch")
				return
			}
		}
		if _, exists := registrations[ev.RevisionID]; exists {
			workflowDenied(w, 409, "workflow_validation_mismatch")
			return
		}
		if h.WorkflowControl.Verifier.VerifyRegistration(r.Context(), cred, ev) != nil {
			workflowDenied(w, 409, "workflow_validation_mismatch")
			return
		}
		ev.Source = ""
		ev.SourcePath = ""
		registrations[ev.RevisionID] = ev
	}
	// Serialize the final generation check/activation with rotation, revocation,
	// and re-enrollment. External engine reads run before taking this lock.
	tx, e := h.TxStarter.Begin(r.Context())
	if e != nil {
		workflowDenied(w, 503, "workflow_unavailable")
		return
	}
	defer tx.Rollback(r.Context())
	q := h.Queries.WithTx(tx)
	if _, e = q.LockWorkspaceForChatSessionCreate(r.Context(), d.WorkspaceID); e != nil {
		workflowDenied(w, 401, "workflow_unauthenticated")
		return
	}
	if _, e = q.LockWorkflowControl(r.Context(), db.LockWorkflowControlParams{WorkspaceID: d.WorkspaceID, ControlHash: d.ControlHash}); e != nil {
		workflowDenied(w, 401, "workflow_unauthenticated")
		return
	}
	if token, e = q.GetWorkflowRunnerGeneration(r.Context(), db.GetWorkflowRunnerGenerationParams{WorkspaceID: d.WorkspaceID, DaemonID: d.DaemonID, ID: gen, ControlHash: d.ControlHash}); e != nil {
		workflowDenied(w, 401, "workflow_unauthenticated")
		return
	}
	if prepare && token.Phase != "candidate" {
		workflowDenied(w, 409, "workflow_validation_mismatch")
		return
	}
	if token.Phase == "retiring" {
		var checked map[string]wr.Registration
		if json.Unmarshal(token.Registrations, &checked) != nil || !reflect.DeepEqual(checked, registrations) {
			workflowDenied(w, 409, "workflow_validation_mismatch")
			return
		}
	}
	validationFailed := false
	for id, ev := range registrations {
		if prepare || token.Phase == "retiring" {
			continue
		}
		result, e := h.WorkflowSvc.AcknowledgeRevisionTx(r.Context(), tx, d.WorkspaceID, parseUUID(ev.RevisionID), ev.Hash, true)
		if e != nil {
			workflowDenied(w, 409, "workflow_validation_mismatch")
			return
		}
		if result.RegistrationState != "registered" {
			// Persist the original CAS/authority failure without leasing it.
			delete(registrations, id)
			validationFailed = true
		}
	}
	if !prepare && token.Phase == "candidate" {
		if e = q.DemoteWorkflowRunnerCurrent(r.Context(), db.DemoteWorkflowRunnerCurrentParams{WorkspaceID: d.WorkspaceID, DaemonID: d.DaemonID}); e != nil {
			workflowDenied(w, 503, "workflow_unavailable")
			return
		}
		if n, err := q.PromoteWorkflowRunnerCandidate(r.Context(), db.PromoteWorkflowRunnerCandidateParams{WorkspaceID: d.WorkspaceID, DaemonID: d.DaemonID, ID: gen}); err != nil || n != 1 {
			workflowDenied(w, 409, "workflow_validation_mismatch")
			return
		}
	}
	data, _ := json.Marshal(registrations)
	count, e := q.LeaseWorkflowRunnerGeneration(r.Context(), db.LeaseWorkflowRunnerGenerationParams{WorkspaceID: d.WorkspaceID, DaemonID: d.DaemonID, ID: gen, ControlHash: d.ControlHash, Registrations: data})
	if e != nil || count != 1 || tx.Commit(r.Context()) != nil {
		workflowDenied(w, 401, "workflow_unauthenticated")
		return
	}
	if validationFailed {
		workflowDenied(w, 409, "workflow_validation_mismatch")
		return
	}
	writeJSON(w, 200, map[string]int{"lease_seconds": 45})
}

// Retire exactly a finished/drained generation; global revoke remains separate.
func (h *Handler) WorkflowRunnerRetire(w http.ResponseWriter, r *http.Request) {
	d, ok := h.authorizeWorkflowControl(w, r)
	if !ok {
		return
	}
	var request struct {
		Generation string `json:"generation"`
	}
	if json.NewDecoder(http.MaxBytesReader(w, r.Body, 1024)).Decode(&request) != nil {
		workflowDenied(w, 400, "workflow_invalid_payload")
		return
	}
	id, e := util.ParseUUID(request.Generation)
	if e != nil {
		workflowDenied(w, 400, "workflow_invalid_payload")
		return
	}
	tx, e := h.TxStarter.Begin(r.Context())
	if e != nil {
		workflowDenied(w, 503, "workflow_unavailable")
		return
	}
	defer tx.Rollback(r.Context())
	q := h.Queries.WithTx(tx)
	if _, e = q.LockWorkspaceForChatSessionCreate(r.Context(), d.WorkspaceID); e != nil {
		workflowDenied(w, 401, "workflow_unauthenticated")
		return
	}
	if _, e = q.LockWorkflowControl(r.Context(), db.LockWorkflowControlParams{WorkspaceID: d.WorkspaceID, ControlHash: d.ControlHash}); e != nil {
		workflowDenied(w, 401, "workflow_unauthenticated")
		return
	}
	if _, e = q.RetireWorkflowRunnerGeneration(r.Context(), db.RetireWorkflowRunnerGenerationParams{WorkspaceID: d.WorkspaceID, DaemonID: d.DaemonID, ID: id, ControlHash: d.ControlHash}); e != nil || tx.Commit(r.Context()) != nil {
		workflowDenied(w, 503, "workflow_unavailable")
		return
	}
	_ = h.cleanupWorkflowRunnerCredentials(r.Context(), d.WorkspaceID, d.DaemonID)
	writeJSON(w, 200, map[string]bool{"retired": true})
}
func (h *Handler) WorkflowRunnerValidationFailed(w http.ResponseWriter, r *http.Request) {
	d, ok := h.authorizeWorkflowControl(w, r)
	if !ok {
		return
	}
	var request struct {
		RevisionID string `json:"revision_id"`
		Hash       string `json:"hash"`
	}
	if json.NewDecoder(http.MaxBytesReader(w, r.Body, 1024)).Decode(&request) != nil {
		workflowDenied(w, 400, "workflow_invalid_payload")
		return
	}
	id, e := util.ParseUUID(request.RevisionID)
	if e != nil {
		workflowDenied(w, 400, "workflow_invalid_payload")
		return
	}
	if _, e = h.WorkflowSvc.AcknowledgeRevision(r.Context(), d.WorkspaceID, id, request.Hash, false); e != nil {
		workflowDenied(w, 409, "workflow_validation_mismatch")
		return
	}
	writeJSON(w, 200, map[string]bool{"recorded": true})
}
func (h *Handler) WorkflowRunnerEligible(ctx context.Context, ws, rev pgtype.UUID) bool {
	if !h.workflowControlReady() {
		return false
	}
	yes, e := h.Queries.WorkflowRunnerEligible(ctx, db.WorkflowRunnerEligibleParams{WorkspaceID: ws, RevisionID: uuidToString(rev)})
	return e == nil && yes
}

func (h *Handler) WorkflowRunnerAction(w http.ResponseWriter, r *http.Request) {
	workflowPrivate(w)
	if !h.workflowControlReady() {
		workflowDenied(w, 503, "workflow_unavailable")
		return
	}
	ws, e := util.ParseUUID(r.Header.Get("X-Workspace-ID"))
	if e != nil {
		workflowDenied(w, 401, "workflow_unauthenticated")
		return
	}
	gen, e := util.ParseUUID(r.Header.Get("X-Multica-Workflow-Generation"))
	if e != nil {
		workflowDenied(w, 401, "workflow_unauthenticated")
		return
	}
	secret := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !strings.HasPrefix(secret, "wrt_") {
		workflowDenied(w, 401, "workflow_unauthenticated")
		return
	}
	token, e := h.Queries.AuthorizeWorkflowRunner(r.Context(), db.AuthorizeWorkflowRunnerParams{WorkspaceID: ws, ID: gen, TokenHash: auth.HashToken(secret)})
	if e != nil || !token.LeaseUntil.Valid || !time.Now().Before(token.LeaseUntil.Time) {
		workflowDenied(w, 410, "workflow_capability_revoked")
		return
	}
	body, e := io.ReadAll(http.MaxBytesReader(w, r.Body, (64<<10)+2048))
	if e != nil {
		workflowDenied(w, 400, "workflow_invalid_payload")
		return
	}
	sig, e := hex.DecodeString(r.Header.Get("X-Multica-Workflow-Signature"))
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte("POST\n" + r.URL.EscapedPath() + "\n" + uuidToString(ws) + "\n" + uuidToString(gen) + "\n"))
	mac.Write(body)
	if e != nil || !hmac.Equal(sig, mac.Sum(nil)) {
		workflowDenied(w, 401, "workflow_unauthenticated")
		return
	}
	var req WorkflowActionRequest
	if json.Unmarshal(body, &req) != nil || req.Protocol != 1 || req.RunID != chi.URLParam(r, "id") || req.Attempt < 0 || len(req.OperationKey) > 256 || service.ValidateWorkflowPayload(req.Payload) != nil {
		workflowDenied(w, 400, "workflow_invalid_payload")
		return
	}
	id, e := util.ParseUUID(req.RunID)
	if e != nil {
		workflowDenied(w, 404, "workflow_not_found")
		return
	}
	run, e := h.Queries.GetWorkflowRunInWorkspace(r.Context(), db.GetWorkflowRunInWorkspaceParams{ID: id, WorkspaceID: ws})
	if e != nil {
		workflowDenied(w, 404, "workflow_not_found")
		return
	}
	if (run.Status != "running" && run.Status != "submitting") || run.CancelRequestedAt.Valid {
		workflowDenied(w, 410, "workflow_capability_revoked")
		return
	}
	rev, e := h.Queries.GetWorkflowRevisionInWorkspace(r.Context(), db.GetWorkflowRevisionInWorkspaceParams{ID: run.RevisionID, WorkspaceID: ws})
	var registered map[string]wr.Registration
	_ = json.Unmarshal(token.Registrations, &registered)
	evidence, exists := registered[uuidToString(run.RevisionID)]
	if e != nil || !exists || evidence.Hash != run.DefinitionHash || rev.DefinitionHash != run.DefinitionHash {
		workflowDenied(w, 403, "workflow_forbidden")
		return
	}
	stepOK := false
	for _, step := range evidence.Graph.Steps {
		if step.ID == req.StepID {
			stepOK = true
		}
	}
	if !stepOK {
		workflowDenied(w, 403, "workflow_forbidden")
		return
	}
	workflow, e := h.WorkflowSvc.Load(r.Context(), ws, run.WorkflowID)
	if e != nil || h.WorkflowSvc.Authorize(r.Context(), workflow, service.WorkflowActor{HumanID: run.PrincipalUserID}, false) != nil {
		workflowDenied(w, 403, "workflow_forbidden")
		return
	}
	if h.WorkflowControl.Assignments == nil || h.WorkflowControl.Action == nil {
		workflowDenied(w, 503, "workflow_unavailable")
		return
	}
	if req.Attempt > math.MaxInt32 || req.InvocationCount < 1 || req.InvocationCount > math.MaxInt32 || len(req.StepID) > 128 {
		workflowDenied(w, 403, "workflow_forbidden")
		return
	}
	engineID, idErr := util.ParseUUID(req.EngineRunID)
	_, taskErr := util.ParseUUID(req.EngineTaskID)
	if idErr != nil || taskErr != nil || (run.HatchetRunID.Valid && run.HatchetRunID != engineID) {
		workflowDenied(w, 403, "workflow_forbidden")
		return
	}
	if h.WorkflowControl.Tenants == nil {
		workflowDenied(w, 503, "workflow_unavailable")
		return
	}
	cred, e := h.WorkflowControl.Tenants.MappedCredentials(r.Context(), ws)
	if e != nil || cred.TenantID != token.TenantID {
		workflowDenied(w, 503, "workflow_unavailable")
		return
	}
	assignment := hatchet.Assignment{TenantID: cred.TenantID, EngineRunID: req.EngineRunID, EngineTaskID: req.EngineTaskID, WorkflowID: evidence.EngineWorkflowID, VersionID: evidence.EngineVersionID, WorkerName: "multica-" + uuidToString(token.ID), Retry: req.Attempt, Invocation: req.InvocationCount}
	for _, action := range evidence.Actions {
		if action.StepID == req.StepID {
			assignment.TaskName, assignment.ActionID = action.TaskName, action.ActionID
		}
	}
	if !run.HatchetRunID.Valid {
		if run.Status != "submitting" || run.SubmissionAttempts < 1 || !run.SubmissionStartedAt.Valid || !run.SubmissionDeadline.Valid || !time.Now().Before(run.SubmissionDeadline.Time) {
			workflowDenied(w, 503, "workflow_unavailable")
			return
		}
		assignment.LocalRunID, assignment.SubmissionStartedAt, assignment.SubmissionDeadline = req.RunID, run.SubmissionStartedAt.Time, run.SubmissionDeadline.Time
	}
	if e = h.WorkflowControl.Assignments.VerifyAssignment(r.Context(), assignment); e != nil {
		status, code := 503, "workflow_unavailable"
		if errors.Is(e, hatchet.ErrAssignmentDenied) {
			status, code = 403, "workflow_forbidden"
		}
		workflowDenied(w, status, code)
		return
	}
	// No engine I/O under local locks. The fresh transaction fences local grants
	// before first binding; Task6b must repeat that fence in its receipt transaction.
	authority, status, code := h.bindWorkflowAssignment(r.Context(), ws, gen, auth.HashToken(secret), run, evidence, engineID, assignment.LocalRunID != "")
	if status != 0 {
		workflowDenied(w, status, code)
		return
	}
	result, e := h.WorkflowControl.Action(r.Context(), authority, req)
	if e != nil {
		workflowDenied(w, 409, "workflow_operation_conflict")
		return
	}
	writeJSON(w, 200, map[string]json.RawMessage{"result": result})
}

func (h *Handler) bindWorkflowAssignment(ctx context.Context, ws, gen pgtype.UUID, tokenHash string, observed db.WorkflowRun, evidence wr.Registration, engineID pgtype.UUID, mayBind bool) (WorkflowActionAuthority, int, string) {
	deny := func(status int, code string) (WorkflowActionAuthority, int, string) {
		return WorkflowActionAuthority{}, status, code
	}
	tx, err := h.TxStarter.Begin(ctx)
	if err != nil {
		return deny(503, "workflow_unavailable")
	}
	defer tx.Rollback(ctx)
	q := h.Queries.WithTx(tx)
	if _, err = q.LockWorkspaceForChatSessionCreate(ctx, ws); err != nil {
		return deny(410, "workflow_capability_revoked")
	}
	token, err := q.LockWorkflowRunnerAuthorization(ctx, db.LockWorkflowRunnerAuthorizationParams{WorkspaceID: ws, ID: gen, TokenHash: tokenHash})
	if err != nil {
		return deny(410, "workflow_capability_revoked")
	}
	var registered map[string]wr.Registration
	if json.Unmarshal(token.Registrations, &registered) != nil || !reflect.DeepEqual(registered[uuidToString(observed.RevisionID)], evidence) {
		return deny(403, "workflow_forbidden")
	}
	workflow, err := q.LockWorkflowForUpdate(ctx, db.LockWorkflowForUpdateParams{ID: observed.WorkflowID, WorkspaceID: ws})
	if err != nil {
		return deny(403, "workflow_forbidden")
	}
	run, err := q.LockWorkflowActionRun(ctx, db.LockWorkflowActionRunParams{ID: observed.ID, WorkspaceID: ws})
	if err != nil || run.CancelRequestedAt.Valid || (run.Status != "running" && run.Status != "submitting") || run.RevisionID != observed.RevisionID || run.DefinitionHash != observed.DefinitionHash {
		return deny(410, "workflow_capability_revoked")
	}
	rev, err := q.GetWorkflowRevisionInWorkspace(ctx, db.GetWorkflowRevisionInWorkspaceParams{ID: run.RevisionID, WorkspaceID: ws})
	if err != nil || rev.DefinitionHash != run.DefinitionHash {
		return deny(403, "workflow_forbidden")
	}
	if _, err = q.LockWorkflowActionMembership(ctx, db.LockWorkflowActionMembershipParams{WorkspaceID: ws, UserID: run.PrincipalUserID}); err != nil {
		return deny(403, "workflow_forbidden")
	}
	svc := *h.WorkflowSvc
	svc.Queries = q
	if svc.Authorize(ctx, workflow, service.WorkflowActor{HumanID: run.PrincipalUserID}, false) != nil {
		return deny(403, "workflow_forbidden")
	}
	if !run.HatchetRunID.Valid {
		if !mayBind {
			return deny(410, "workflow_capability_revoked")
		}
		run, err = q.BindWorkflowActionEngineRun(ctx, db.BindWorkflowActionEngineRunParams{ID: run.ID, WorkspaceID: ws, HatchetRunID: engineID})
		if err != nil {
			return deny(410, "workflow_capability_revoked")
		}
	}
	if run.HatchetRunID != engineID {
		return deny(403, "workflow_forbidden")
	}
	if err = tx.Commit(ctx); err != nil {
		return deny(503, "workflow_unavailable")
	}
	return WorkflowActionAuthority{Run: run, Revision: rev, Runner: token, Principal: run.PrincipalUserID}, 0, ""
}
