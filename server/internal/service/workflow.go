package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

const (
	WorkflowMaxSourceBytes  = 256 << 10
	WorkflowMaxPayloadBytes = 64 << 10
	WorkflowMaxGraphBytes   = 128 << 10
	WorkflowMaxSteps        = 256
	WorkflowMaxDepth        = 32
	WorkflowMaxPageSize     = 100
	WorkflowMaxLogBytes     = 64 << 10
	WorkflowMaxLogEntries   = 200
)

type WorkflowError struct{ Code string }

func (e *WorkflowError) Error() string { return e.Code }
func workflowError(code string) error  { return &WorkflowError{Code: code} }

// WorkflowActor preserves the human authority separately from the authenticated audit actor.
type WorkflowActor struct {
	HumanID   pgtype.UUID
	AuditType string
	AuditID   pgtype.UUID
}
type WorkflowService struct {
	Queries   *db.Queries
	TxStarter TxStarter
	Ready     func() bool
	Logs      WorkflowLogReader
	Eligible  func(context.Context, pgtype.UUID, pgtype.UUID) bool
	Engine    WorkflowEngine
}

type WorkflowLogEntry struct {
	StepID    string    `json:"step_id"`
	Level     string    `json:"level"`
	Message   string    `json:"message"`
	CreatedAt time.Time `json:"created_at"`
}

// WorkflowLogReader must return tenant-scoped product entries, never raw engine JSON.
type WorkflowLogReader interface {
	ReadWorkflowLogs(context.Context, db.WorkflowRun, int32, int32) ([]WorkflowLogEntry, error)
}

var workflowLogSecrets = regexp.MustCompile(`(?i)(bearer\s+)[^\s"<>]+|(?:wft_|mul_|mat_)[A-Za-z0-9_-]+|(?:password|secret|token|api[_-]?key)["']?\s*[:=]\s*["']?[^\s,"'<>]+`)

func SanitizeWorkflowLogs(entries []WorkflowLogEntry) []WorkflowLogEntry {
	result := make([]WorkflowLogEntry, 0)
	total := 2
	for _, entry := range entries {
		if len(result) >= WorkflowMaxLogEntries {
			break
		}
		if len(entry.Message) > 4096 {
			entry.Message = entry.Message[:4096]
		}
		entry.Message = workflowLogSecrets.ReplaceAllString(entry.Message, "[redacted]")
		if len(entry.StepID) > 128 {
			entry.StepID = entry.StepID[:128]
		}
		switch entry.Level {
		case "debug", "info", "warn", "error":
		default:
			entry.Level = "info"
		}
		raw, _ := json.Marshal(entry)
		if total+len(raw)+1 > WorkflowMaxLogBytes {
			break
		}
		total += len(raw) + 1
		result = append(result, entry)
	}
	return result
}

// SanitizeWorkflowInput projects bounded product input without known credential
// fields. Oversized/deep historical rows fail closed rather than returning raw data.
func SanitizeWorkflowInput(raw []byte) json.RawMessage {
	if len(raw) == 0 {
		return json.RawMessage(`null`)
	}
	if ValidateWorkflowPayload(raw) != nil {
		return json.RawMessage(`{"_redacted":true}`)
	}
	var value any
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.UseNumber()
	if decoder.Decode(&value) != nil {
		return json.RawMessage(`{"_redacted":true}`)
	}
	var clean func(any) any
	clean = func(v any) any {
		switch x := v.(type) {
		case map[string]any:
			result := map[string]any{}
			for key, child := range x {
				normalized := strings.ToLower(key)
				normalized = strings.NewReplacer("_", "", "-", "", " ", "").Replace(normalized)
				safeKey := workflowLogSecrets.ReplaceAllString(key, "[redacted]")
				sensitive := false
				for _, part := range []string{"password", "secret", "token", "credential", "authorization", "cookie", "privatekey", "apikey", "hatchet", "tenantid"} {
					if strings.Contains(normalized, part) {
						sensitive = true
						break
					}
				}
				if sensitive {
					result[safeKey] = "[redacted]"
				} else {
					result[safeKey] = clean(child)
				}
			}
			return result
		case []any:
			result := make([]any, len(x))
			for i, child := range x {
				result[i] = clean(child)
			}
			return result
		case string:
			return workflowLogSecrets.ReplaceAllString(x, "[redacted]")
		default:
			return v
		}
	}
	encoded, e := json.Marshal(clean(value))
	if e != nil || len(encoded) > WorkflowMaxPayloadBytes {
		return json.RawMessage(`{"_redacted":true}`)
	}
	return json.RawMessage(encoded)
}

func NewWorkflowService(q *db.Queries, tx TxStarter) *WorkflowService {
	return &WorkflowService{Queries: q, TxStarter: tx}
}

func ValidateWorkflowPayload(raw []byte) error {
	if len(raw) > WorkflowMaxPayloadBytes || !json.Valid(raw) {
		return workflowError("workflow_invalid_payload")
	}
	var value any
	if json.Unmarshal(raw, &value) != nil || workflowJSONDepth(value) > WorkflowMaxDepth {
		return workflowError("workflow_invalid_payload")
	}
	return nil
}
func workflowJSONDepth(v any) int {
	n := 0
	switch x := v.(type) {
	case map[string]any:
		for _, c := range x {
			if d := workflowJSONDepth(c); d > n {
				n = d
			}
		}
	case []any:
		for _, c := range x {
			if d := workflowJSONDepth(c); d > n {
				n = d
			}
		}
	}
	return n + 1
}

type WorkflowGraph struct {
	Steps []WorkflowStep `json:"steps"`
}
type WorkflowStep struct {
	ID      string   `json:"id"`
	Parents []string `json:"parents"`
}

// WorkflowActionName namespaces every step by its immutable revision engine name.
func WorkflowActionName(revision db.WorkflowRevision, stepID string) string {
	sum := sha256.Sum256([]byte(stepID))
	return revision.HatchetName + "-step-" + hex.EncodeToString(sum[:])[:16]
}

func WorkflowRegistrationState(revision db.WorkflowRevision) string {
	if revision.PublishedAt.Valid {
		return "registered"
	}
	if revision.ValidationStatus == "invalid" {
		if revision.ValidationError.String == "workflow_version_conflict" {
			return "version_conflict"
		}
		return "validation_failed"
	}
	return "pending_validation"
}

// Static validation rejects malformed DAGs; it is never evidence that TypeScript agrees with the graph.
func ValidateWorkflowGraph(raw json.RawMessage) error {
	if len(raw) > WorkflowMaxGraphBytes {
		return workflowError("workflow_invalid_graph")
	}
	var g WorkflowGraph
	if json.Unmarshal(raw, &g) != nil || len(g.Steps) == 0 || len(g.Steps) > WorkflowMaxSteps {
		return workflowError("workflow_invalid_graph")
	}
	nodes := map[string][]string{}
	for _, s := range g.Steps {
		if s.ID == "" || len(s.ID) > 128 {
			return workflowError("workflow_invalid_graph")
		}
		if _, exists := nodes[s.ID]; exists {
			return workflowError("workflow_invalid_graph")
		}
		nodes[s.ID] = s.Parents
	}
	state := map[string]int{}
	depth := map[string]int{}
	var visit func(string) (int, error)
	visit = func(id string) (int, error) {
		if state[id] == 1 {
			return 0, workflowError("workflow_invalid_graph")
		}
		if state[id] == 2 {
			return depth[id], nil
		}
		parents, ok := nodes[id]
		if !ok {
			return 0, workflowError("workflow_invalid_graph")
		}
		state[id] = 1
		d := 1
		seen := map[string]bool{}
		for _, p := range parents {
			if seen[p] {
				return 0, workflowError("workflow_invalid_graph")
			}
			seen[p] = true
			pd, e := visit(p)
			if e != nil {
				return 0, e
			}
			if pd+1 > d {
				d = pd + 1
			}
		}
		if d > WorkflowMaxDepth {
			return 0, workflowError("workflow_invalid_graph")
		}
		state[id] = 2
		depth[id] = d
		return d, nil
	}
	for id := range nodes {
		if _, e := visit(id); e != nil {
			return e
		}
	}
	return nil
}

func (s *WorkflowService) authorize(ctx context.Context, q *db.Queries, w db.Workflow, a WorkflowActor, manage bool) error {
	m, e := q.GetMemberByUserAndWorkspace(ctx, db.GetMemberByUserAndWorkspaceParams{WorkspaceID: w.WorkspaceID, UserID: a.HumanID})
	if e != nil {
		return workflowError("workflow_forbidden")
	}
	if m.Role == "owner" || m.Role == "admin" || (w.CreatedByType == "member" && w.CreatedByID == a.HumanID) {
		return nil
	}
	if !manage {
		cs, e := q.ListWorkflowCollaborators(ctx, db.ListWorkflowCollaboratorsParams{WorkspaceID: w.WorkspaceID, WorkflowID: w.ID})
		if e != nil {
			return e
		}
		for _, c := range cs {
			if c.UserID == a.HumanID && c.Role == "editor" {
				return nil
			}
		}
	}
	return workflowError("workflow_forbidden")
}
func (s *WorkflowService) Authorize(ctx context.Context, w db.Workflow, a WorkflowActor, manage bool) error {
	return s.authorize(ctx, s.Queries, w, a, manage)
}
func (s *WorkflowService) Load(ctx context.Context, workspace, id pgtype.UUID) (db.Workflow, error) {
	w, e := s.Queries.GetWorkflowInWorkspace(ctx, db.GetWorkflowInWorkspaceParams{ID: id, WorkspaceID: workspace})
	if errors.Is(e, pgx.ErrNoRows) {
		return w, workflowError("workflow_not_found")
	}
	return w, e
}

func (s *WorkflowService) Create(ctx context.Context, ws pgtype.UUID, a WorkflowActor, name, slug, description string) (db.Workflow, error) {
	if strings.TrimSpace(name) == "" || len(name) > 200 || !ValidWorkflowSlug(slug) || len(description) > 4096 {
		return db.Workflow{}, workflowError("workflow_invalid_input")
	}
	if _, e := s.Queries.GetMemberByUserAndWorkspace(ctx, db.GetMemberByUserAndWorkspaceParams{WorkspaceID: ws, UserID: a.HumanID}); e != nil {
		return db.Workflow{}, workflowError("workflow_forbidden")
	}
	return s.Queries.CreateWorkflow(ctx, db.CreateWorkflowParams{WorkspaceID: ws, Name: name, Slug: slug, Description: pgtype.Text{String: description, Valid: true}, HatchetName: "multica-" + uuid.NewString(), CreatedByType: "member", CreatedByID: a.HumanID, AuditActorType: pgtype.Text{String: a.AuditType, Valid: true}, AuditActorID: a.AuditID})
}

// ValidWorkflowSlug is shared by create and update boundaries.
func ValidWorkflowSlug(slug string) bool {
	if len(slug) == 0 || len(slug) > 100 {
		return false
	}
	for _, c := range slug {
		if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-') {
			return false
		}
	}
	return true
}

type WorkflowPublishResult struct {
	Revision          db.WorkflowRevision `json:"revision"`
	RegistrationState string              `json:"registration_state"`
}

// PublishRevision records an immutable proposal. Authored source is never evaluated by this process.
func (s *WorkflowService) PublishRevision(ctx context.Context, ws, id pgtype.UUID, a WorkflowActor, expected int32, source string, graph json.RawMessage) (WorkflowPublishResult, error) {
	var result WorkflowPublishResult
	w, e := s.Load(ctx, ws, id)
	if e != nil {
		return result, e
	}
	if e = s.Authorize(ctx, w, a, false); e != nil {
		return result, e
	}
	if expected < 0 || len(source) == 0 || len(source) > WorkflowMaxSourceBytes {
		return result, workflowError("workflow_invalid_source")
	}
	if e = ValidateWorkflowGraph(graph); e != nil {
		return result, e
	}
	tx, e := s.TxStarter.Begin(ctx)
	if e != nil {
		return result, e
	}
	defer tx.Rollback(ctx)
	q := s.Queries.WithTx(tx)
	w, e = q.LockWorkflowForUpdate(ctx, db.LockWorkflowForUpdateParams{ID: id, WorkspaceID: ws})
	if e != nil {
		return result, e
	}
	if e = s.authorize(ctx, q, w, a, false); e != nil {
		return result, e
	}
	if w.DefinitionVersion != expected {
		return result, workflowError("workflow_version_conflict")
	}
	latest, e := q.LatestWorkflowRevisionVersion(ctx, db.LatestWorkflowRevisionVersionParams{WorkflowID: id, WorkspaceID: ws})
	if e != nil {
		return result, e
	}
	version := latest + 1
	sum := sha256.Sum256([]byte(source))
	hash := hex.EncodeToString(sum[:])
	r, e := q.CreateWorkflowRevision(ctx, db.CreateWorkflowRevisionParams{WorkspaceID: ws, WorkflowID: id, Version: version, Definition: source, DefinitionHash: hash, DefinitionGraph: graph, HatchetName: fmt.Sprintf("multica-%s-v%d-%s", util.UUIDToString(id), version, hash[:12]), CreatedByID: a.HumanID, AuditActorType: pgtype.Text{String: a.AuditType, Valid: true}, AuditActorID: a.AuditID, ExpectedCurrentVersion: pgtype.Int4{Int32: expected, Valid: true}})
	if e != nil {
		return result, e
	}
	if e = tx.Commit(ctx); e != nil {
		return result, e
	}
	return WorkflowPublishResult{Revision: r, RegistrationState: "pending_validation"}, nil
}

// AcknowledgeRevision is a server-internal trusted daemon seam, never an author HTTP endpoint.
// Task5 must authenticate the eligible daemon, bind source hash and extracted graph,
// and confirm engine registration before acknowledging success.
func (s *WorkflowService) AcknowledgeRevision(ctx context.Context, ws, id pgtype.UUID, hash string, valid bool) (WorkflowPublishResult, error) {
	var result WorkflowPublishResult
	r, e := s.Queries.GetWorkflowRevisionInWorkspace(ctx, db.GetWorkflowRevisionInWorkspaceParams{ID: id, WorkspaceID: ws})
	if e != nil {
		return result, workflowError("workflow_not_found")
	}
	tx, e := s.TxStarter.Begin(ctx)
	if e != nil {
		return result, e
	}
	defer tx.Rollback(ctx)
	q := s.Queries.WithTx(tx)
	w, e := q.LockWorkflowForUpdate(ctx, db.LockWorkflowForUpdateParams{ID: r.WorkflowID, WorkspaceID: ws})
	if e != nil {
		return result, e
	}
	r, e = q.GetWorkflowRevisionInWorkspace(ctx, db.GetWorkflowRevisionInWorkspaceParams{ID: id, WorkspaceID: ws})
	if e != nil {
		return result, e
	}
	if hash != r.DefinitionHash || !r.ExpectedCurrentVersion.Valid {
		return result, workflowError("workflow_validation_mismatch")
	}
	if r.ValidationStatus != "pending" {
		if r.PublishedAt.Valid {
			return WorkflowPublishResult{Revision: r, RegistrationState: "registered"}, nil
		}
		return result, workflowError("workflow_validation_final")
	}
	state := "valid"
	reason := pgtype.Text{}
	registration := "registered"
	if !valid {
		state = "invalid"
		reason = pgtype.Text{String: "trusted_validation_failed", Valid: true}
		registration = "validation_failed"
	} else if s.authorize(ctx, q, w, WorkflowActor{HumanID: r.CreatedByID}, false) != nil {
		state = "invalid"
		reason = pgtype.Text{String: "workflow_forbidden", Valid: true}
		registration = "validation_failed"
	} else if w.DefinitionVersion != r.ExpectedCurrentVersion.Int32 {
		state = "invalid"
		reason = pgtype.Text{String: "workflow_version_conflict", Valid: true}
		registration = "version_conflict"
	}
	r, e = q.SetWorkflowRevisionValidation(ctx, db.SetWorkflowRevisionValidationParams{ID: id, WorkspaceID: ws, ValidationStatus: state, ValidationError: reason})
	if e != nil {
		return result, e
	}
	if state == "valid" {
		if _, e = q.PublishWorkflowRevisionCAS(ctx, db.PublishWorkflowRevisionCASParams{ID: w.ID, WorkspaceID: ws, DefinitionVersion: r.ExpectedCurrentVersion.Int32, ID_2: id}); e != nil {
			return result, e
		}
		if e = q.MarkWorkflowRevisionPublished(ctx, db.MarkWorkflowRevisionPublishedParams{ID: id, WorkspaceID: ws}); e != nil {
			return result, e
		}
		r, e = q.GetWorkflowRevisionInWorkspace(ctx, db.GetWorkflowRevisionInWorkspaceParams{ID: id, WorkspaceID: ws})
		if e != nil {
			return result, e
		}
	}
	if e = tx.Commit(ctx); e != nil {
		return result, e
	}
	return WorkflowPublishResult{Revision: r, RegistrationState: registration}, nil
}

// AdmitRun persists the local UUID before Task4 can contact the engine. The default runtime is disabled.
func (s *WorkflowService) AdmitRun(ctx context.Context, w db.Workflow, a WorkflowActor, source string, trigger pgtype.UUID, input json.RawMessage) (db.WorkflowRun, error) {
	if e := s.Authorize(ctx, w, a, false); e != nil {
		return db.WorkflowRun{}, e
	}
	if e := ValidateWorkflowPayload(input); e != nil {
		return db.WorkflowRun{}, e
	}
	if source != "manual" && source != "api" {
		return db.WorkflowRun{}, workflowError("workflow_invalid_input")
	}
	tx, e := s.TxStarter.Begin(ctx)
	if e != nil {
		return db.WorkflowRun{}, e
	}
	defer tx.Rollback(ctx)
	q := s.Queries.WithTx(tx)
	w, e = q.LockWorkflowForUpdate(ctx, db.LockWorkflowForUpdateParams{ID: w.ID, WorkspaceID: w.WorkspaceID})
	if e != nil {
		return db.WorkflowRun{}, e
	}
	if e = s.authorize(ctx, q, w, a, false); e != nil {
		return db.WorkflowRun{}, e
	}
	if w.Status != "active" || !w.CurrentRevisionID.Valid {
		return db.WorkflowRun{}, workflowError("workflow_unrunnable")
	}
	r, e := q.GetWorkflowRevisionInWorkspace(ctx, db.GetWorkflowRevisionInWorkspaceParams{ID: w.CurrentRevisionID, WorkspaceID: w.WorkspaceID})
	if e != nil || r.WorkflowID != w.ID || r.ValidationStatus != "valid" || !r.PublishedAt.Valid {
		return db.WorkflowRun{}, workflowError("workflow_unrunnable")
	}
	state := "queued"
	reason := pgtype.Text{}
	if !s.runnable(ctx, w.WorkspaceID, r.ID) {
		state = "skipped"
		reason = pgtype.Text{String: "workflow_engine_unavailable", Valid: true}
	}
	id, _ := util.ParseUUID(uuid.NewString())
	originatorSource := "direct_human"
	if source == "api" {
		originatorSource = "trigger_owner"
	}
	run, e := q.CreateWorkflowRun(ctx, db.CreateWorkflowRunParams{ID: id, WorkspaceID: w.WorkspaceID, WorkflowID: w.ID, RevisionID: r.ID, DefinitionHash: r.DefinitionHash, PrincipalUserID: a.HumanID, TriggerID: trigger, Source: source, Input: input, Status: state, ReasonCode: reason, OriginatorSource: pgtype.Text{String: originatorSource, Valid: true}})
	if e != nil {
		return run, e
	}
	if e = tx.Commit(ctx); e != nil {
		return run, e
	}
	if state == "skipped" {
		return run, workflowError("workflow_engine_unavailable")
	}
	if s.Engine != nil {
		return s.SubmitRun(ctx, RunIntent{WorkspaceID: run.WorkspaceID, RunID: run.ID})
	}
	return run, nil
}

func (s *WorkflowService) Cancel(ctx context.Context, w db.Workflow, a WorkflowActor, runID pgtype.UUID) (db.WorkflowRun, error) {
	run, e := s.Queries.GetWorkflowRunInWorkspace(ctx, db.GetWorkflowRunInWorkspaceParams{ID: runID, WorkspaceID: w.WorkspaceID})
	if e != nil || run.WorkflowID != w.ID {
		return run, workflowError("workflow_not_found")
	}
	if e = s.Authorize(ctx, w, a, false); e != nil {
		return run, e
	}
	if run.Status == "completed" || run.Status == "failed" || run.Status == "cancelled" || run.Status == "skipped" {
		return run, nil
	}
	// This row is the durable cancellation outbox. Keep it reconcilable, including
	// ambiguous submissions awaiting a late engine ID; Task4 confirms the outcome.
	result, e := s.Queries.RequestWorkflowRunCancellation(ctx, db.RequestWorkflowRunCancellationParams{ID: runID, WorkspaceID: w.WorkspaceID, CancelRequestedByID: a.HumanID})
	if errors.Is(e, pgx.ErrNoRows) {
		return s.Queries.GetWorkflowRunInWorkspace(ctx, db.GetWorkflowRunInWorkspaceParams{ID: runID, WorkspaceID: w.WorkspaceID})
	}
	return result, e
}
