package handler

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/multica-ai/multica/server/internal/middleware"
	"github.com/multica-ai/multica/server/internal/service"
	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
	"github.com/robfig/cron/v3"
)

func workflowAPIError(w http.ResponseWriter, err error) {
	code := "workflow_internal_error"
	status := http.StatusInternalServerError
	var domain *service.WorkflowError
	if errors.As(err, &domain) {
		code = domain.Code
		switch code {
		case "workflow_not_found":
			status = 404
		case "workflow_forbidden", "workflow_no_originator":
			status = 403
		case "workflow_version_conflict", "workflow_validation_final":
			status = 409
		case "workflow_unrunnable", "workflow_engine_unavailable":
			status = 503
		default:
			status = 400
		}
	}
	var pe *pgconn.PgError
	if errors.As(err, &pe) && pe.Code == "23505" {
		code = "workflow_conflict"
		status = 409
	}
	if errors.Is(err, pgx.ErrNoRows) {
		code = "workflow_not_found"
		status = 404
	}
	writeErrorCode(w, status, code, code)
}
func workflowFail(code string) error { return &service.WorkflowError{Code: code} }
func workflowRevisionView(row db.WorkflowRevision) any {
	return struct {
		db.WorkflowRevision
		DefinitionGraph json.RawMessage `json:"definition_graph"`
	}{WorkflowRevision: row, DefinitionGraph: json.RawMessage(row.DefinitionGraph)}
}
func workflowDecode(w http.ResponseWriter, r *http.Request, v any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, service.WorkflowMaxSourceBytes+service.WorkflowMaxGraphBytes+8192)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if dec.Decode(v) != nil || dec.Decode(new(any)) != io.EOF {
		workflowAPIError(w, workflowFail("workflow_invalid_input"))
		return false
	}
	return true
}
func (h *Handler) workflowActor(w http.ResponseWriter, r *http.Request, ws pgtype.UUID) (service.WorkflowActor, bool) {
	var a service.WorkflowActor
	user, ok := requireUserID(w, r)
	if !ok {
		return a, false
	}
	kind, id := h.resolveActor(r, user, uuidToString(ws))
	human := h.invokeOriginatorFromRequest(r, kind, id)
	if human == "" {
		workflowAPIError(w, workflowFail("workflow_no_originator"))
		return a, false
	}
	a.HumanID, _ = util.ParseUUID(human)
	a.AuditID, _ = util.ParseUUID(id)
	a.AuditType = kind
	if _, e := h.getWorkspaceMember(r.Context(), human, uuidToString(ws)); e != nil {
		workflowAPIError(w, workflowFail("workflow_forbidden"))
		return a, false
	}
	return a, true
}

// Load the workspace-bound resource before resolving its human principal.
func (h *Handler) workflowResource(w http.ResponseWriter, r *http.Request, write, manage bool) (db.Workflow, service.WorkflowActor, bool) {
	var entity db.Workflow
	var a service.WorkflowActor
	ws, e := util.ParseUUID(h.resolveWorkspaceID(r))
	if e != nil {
		workflowAPIError(w, workflowFail("workflow_not_found"))
		return entity, a, false
	}
	id, e := util.ParseUUID(chi.URLParam(r, "id"))
	if e != nil {
		workflowAPIError(w, workflowFail("workflow_not_found"))
		return entity, a, false
	}
	entity, e = h.WorkflowSvc.Load(r.Context(), ws, id)
	if e != nil {
		workflowAPIError(w, e)
		return entity, a, false
	}
	// Nested IDs must bind to this workspace and workflow before any principal is read.
	if raw := chi.URLParam(r, "triggerId"); raw != "" {
		child, err := util.ParseUUID(raw)
		if err != nil {
			workflowAPIError(w, workflowFail("workflow_not_found"))
			return entity, a, false
		}
		row, err := h.Queries.GetWorkflowTriggerInWorkspace(r.Context(), db.GetWorkflowTriggerInWorkspaceParams{ID: child, WorkspaceID: ws})
		if err != nil || row.WorkflowID != entity.ID {
			workflowAPIError(w, workflowFail("workflow_not_found"))
			return entity, a, false
		}
	}
	if raw := chi.URLParam(r, "revisionId"); raw != "" {
		child, err := util.ParseUUID(raw)
		if err != nil {
			workflowAPIError(w, workflowFail("workflow_not_found"))
			return entity, a, false
		}
		row, err := h.Queries.GetWorkflowRevisionInWorkspace(r.Context(), db.GetWorkflowRevisionInWorkspaceParams{ID: child, WorkspaceID: ws})
		if err != nil || row.WorkflowID != entity.ID {
			workflowAPIError(w, workflowFail("workflow_not_found"))
			return entity, a, false
		}
	}
	if raw := chi.URLParam(r, "runId"); raw != "" {
		child, err := util.ParseUUID(raw)
		if err != nil {
			workflowAPIError(w, workflowFail("workflow_not_found"))
			return entity, a, false
		}
		row, err := h.Queries.GetWorkflowRunInWorkspace(r.Context(), db.GetWorkflowRunInWorkspaceParams{ID: child, WorkspaceID: ws})
		if err != nil || row.WorkflowID != entity.ID {
			workflowAPIError(w, workflowFail("workflow_not_found"))
			return entity, a, false
		}
	}
	a, ok := h.workflowActor(w, r, ws)
	if !ok {
		return entity, a, false
	}
	if write {
		if e = h.WorkflowSvc.Authorize(r.Context(), entity, a, manage); e != nil {
			workflowAPIError(w, e)
			return entity, a, false
		}
	}
	return entity, a, true
}
func workflowPage(w http.ResponseWriter, r *http.Request) (int32, int32, bool) {
	limit, offset := 50, 0
	var e error
	if x := r.URL.Query().Get("limit"); x != "" {
		limit, e = strconv.Atoi(x)
		if e != nil {
			limit = -1
		}
	}
	if x := r.URL.Query().Get("offset"); x != "" {
		offset, e = strconv.Atoi(x)
		if e != nil {
			offset = -1
		}
	}
	if limit < 1 || limit > service.WorkflowMaxPageSize || offset < 0 || offset > 100000 {
		workflowAPIError(w, workflowFail("workflow_invalid_pagination"))
		return 0, 0, false
	}
	return int32(limit), int32(offset), true
}
func (h *Handler) CreateWorkflow(w http.ResponseWriter, r *http.Request) {
	ws, e := util.ParseUUID(h.resolveWorkspaceID(r))
	if e != nil {
		workflowAPIError(w, workflowFail("workflow_not_found"))
		return
	}
	a, ok := h.workflowActor(w, r, ws)
	if !ok {
		return
	}
	var req struct {
		Name        string `json:"name"`
		Slug        string `json:"slug"`
		Description string `json:"description"`
	}
	if !workflowDecode(w, r, &req) {
		return
	}
	entity, e := h.WorkflowSvc.Create(r.Context(), ws, a, req.Name, req.Slug, req.Description)
	if e != nil {
		workflowAPIError(w, e)
		return
	}
	writeJSON(w, 201, entity)
}
func (h *Handler) ListWorkflows(w http.ResponseWriter, r *http.Request) {
	ws, e := util.ParseUUID(h.resolveWorkspaceID(r))
	if e != nil {
		workflowAPIError(w, workflowFail("workflow_not_found"))
		return
	}
	actor, ok := h.workflowActor(w, r, ws)
	if !ok {
		return
	}
	limit, offset, ok := workflowPage(w, r)
	if !ok {
		return
	}
	entities, e := h.Queries.ListWorkflowsPage(r.Context(), db.ListWorkflowsPageParams{WorkspaceID: ws, Limit: limit, Offset: offset})
	if e != nil {
		workflowAPIError(w, e)
		return
	}
	views := make([]any, 0, len(entities))
	for _, entity := range entities {
		view, err := h.workflowSummary(r, entity, actor)
		if err != nil {
			workflowAPIError(w, err)
			return
		}
		views = append(views, view)
	}
	writeJSON(w, 200, views)
}
func (h *Handler) GetWorkflow(w http.ResponseWriter, r *http.Request) {
	entity, actor, ok := h.workflowResource(w, r, false, false)
	if ok {
		view, err := h.workflowSummary(r, entity, actor)
		if err != nil {
			workflowAPIError(w, err)
			return
		}
		writeJSON(w, 200, view)
	}
}

func (h *Handler) workflowSummary(r *http.Request, entity db.Workflow, actor service.WorkflowActor) (any, error) {
	summary, err := h.Queries.WorkflowTriggerSummary(r.Context(), db.WorkflowTriggerSummaryParams{WorkflowID: entity.ID, WorkspaceID: entity.WorkspaceID})
	if err != nil {
		return nil, err
	}
	state := "unrunnable"
	svc := h.WorkflowSvc
	if svc.Ready == nil || (svc.Enabled != nil && !svc.Enabled()) {
		state = "disabled"
	} else if !svc.Ready() {
		state = "unavailable"
	} else if entity.Status == "active" && entity.CurrentRevisionID.Valid {
		rev, e := h.Queries.GetWorkflowRevisionInWorkspace(r.Context(), db.GetWorkflowRevisionInWorkspaceParams{ID: entity.CurrentRevisionID, WorkspaceID: entity.WorkspaceID})
		if e != nil {
			return nil, e
		}
		if rev.WorkflowID == entity.ID && rev.ValidationStatus == "valid" && rev.PublishedAt.Valid && svc.Eligible != nil && svc.Eligible(r.Context(), entity.WorkspaceID, rev.ID) {
			state = "ready"
		}
	}
	return struct {
		db.Workflow
		CanWrite            bool              `json:"can_write"`
		RuntimeAvailability map[string]string `json:"runtime_availability"`
		TriggerKinds        []string          `json:"trigger_kinds"`
		NextRunAt           any               `json:"next_run_at"`
	}{entity, svc.Authorize(r.Context(), entity, actor, false) == nil, map[string]string{"state": state}, summary.Kinds, summary.NextRunAt}, nil
}
func (h *Handler) UpdateWorkflow(w http.ResponseWriter, r *http.Request) {
	entity, _, ok := h.workflowResource(w, r, true, false)
	if !ok {
		return
	}
	var req struct {
		Name              *string    `json:"name"`
		Slug              *string    `json:"slug"`
		Description       *string    `json:"description"`
		Status            *string    `json:"status"`
		ExpectedUpdatedAt *time.Time `json:"expected_updated_at"`
		ProjectID         *string    `json:"project_id"`
		DefaultAgentID    *string    `json:"default_agent_id"`
	}
	if !workflowDecode(w, r, &req) {
		return
	}
	if req.ExpectedUpdatedAt == nil {
		workflowAPIError(w, workflowFail("workflow_invalid_input"))
		return
	}
	if req.Name != nil {
		entity.Name = *req.Name
	}
	if req.Slug != nil {
		entity.Slug = *req.Slug
	}
	if req.Description != nil {
		entity.Description = pgtype.Text{String: *req.Description, Valid: true}
	}
	if req.Status != nil {
		entity.Status = *req.Status
	}
	if req.ProjectID != nil {
		entity.ProjectID = pgtype.UUID{}
		if *req.ProjectID != "" {
			parsed, ok := parseUUIDOrBadRequest(w, *req.ProjectID, "project_id")
			if !ok {
				return
			}
			if _, err := h.Queries.GetProjectInWorkspace(r.Context(), db.GetProjectInWorkspaceParams{ID: parsed, WorkspaceID: entity.WorkspaceID}); err != nil {
				workflowAPIError(w, workflowFail("workflow_not_found"))
				return
			}
			entity.ProjectID = parsed
		}
	}
	if req.DefaultAgentID != nil {
		entity.DefaultAgentID = pgtype.UUID{}
		if *req.DefaultAgentID != "" {
			parsed, ok := parseUUIDOrBadRequest(w, *req.DefaultAgentID, "default_agent_id")
			if !ok {
				return
			}
			if _, err := h.Queries.GetAgentInWorkspace(r.Context(), db.GetAgentInWorkspaceParams{ID: parsed, WorkspaceID: entity.WorkspaceID}); err != nil {
				workflowAPIError(w, workflowFail("workflow_not_found"))
				return
			}
			entity.DefaultAgentID = parsed
		}
	}
	if strings.TrimSpace(entity.Name) == "" || len(entity.Name) > 200 || !service.ValidWorkflowSlug(entity.Slug) || len(entity.Description.String) > 4096 || (entity.Status != "active" && entity.Status != "paused" && entity.Status != "archived") {
		workflowAPIError(w, workflowFail("workflow_invalid_input"))
		return
	}
	updated, e := h.Queries.UpdateWorkflowCAS(r.Context(), db.UpdateWorkflowCASParams{ID: entity.ID, WorkspaceID: entity.WorkspaceID, Name: entity.Name, Slug: entity.Slug, Description: entity.Description, Status: entity.Status, ProjectID: entity.ProjectID, DefaultAgentID: entity.DefaultAgentID, UpdatedAt: pgtype.Timestamptz{Time: *req.ExpectedUpdatedAt, Valid: true}})
	if errors.Is(e, pgx.ErrNoRows) {
		e = workflowFail("workflow_version_conflict")
	}
	if e != nil {
		workflowAPIError(w, e)
		return
	}
	writeJSON(w, 200, updated)
}
func (h *Handler) DeleteWorkflow(w http.ResponseWriter, r *http.Request) {
	entity, a, ok := h.workflowResource(w, r, true, false)
	if !ok {
		return
	}
	ctx := r.Context()
	tx, e := h.TxStarter.Begin(ctx)
	if e != nil {
		workflowAPIError(w, e)
		return
	}
	defer tx.Rollback(ctx)
	q := h.Queries.WithTx(tx)
	entity, e = q.LockWorkflowForUpdate(ctx, db.LockWorkflowForUpdateParams{ID: entity.ID, WorkspaceID: entity.WorkspaceID})
	if e == nil {
		e = h.WorkflowSvc.Authorize(ctx, entity, a, false)
	}
	if e != nil {
		workflowAPIError(w, e)
		return
	}
	for _, sql := range []string{"DELETE FROM workflow_operation WHERE workspace_id=$1 AND run_id IN (SELECT id FROM workflow_run WHERE workspace_id=$1 AND workflow_id=$2)", "DELETE FROM workflow_run WHERE workspace_id=$1 AND workflow_id=$2", "DELETE FROM workflow_trigger WHERE workspace_id=$1 AND workflow_id=$2", "DELETE FROM workflow_collaborator WHERE workspace_id=$1 AND workflow_id=$2", "DELETE FROM workflow_revision WHERE workspace_id=$1 AND workflow_id=$2", "DELETE FROM workflow WHERE workspace_id=$1 AND id=$2"} {
		if _, e = tx.Exec(ctx, sql, entity.WorkspaceID, entity.ID); e != nil {
			workflowAPIError(w, e)
			return
		}
	}
	if e = tx.Commit(ctx); e != nil {
		workflowAPIError(w, e)
		return
	}
	w.WriteHeader(204)
}
func (h *Handler) PublishWorkflowRevision(w http.ResponseWriter, r *http.Request) {
	entity, a, ok := h.workflowResource(w, r, true, false)
	if !ok {
		return
	}
	var req struct {
		ExpectedVersion *int32          `json:"expected_version"`
		Definition      string          `json:"definition"`
		DefinitionGraph json.RawMessage `json:"definition_graph"`
	}
	if !workflowDecode(w, r, &req) {
		return
	}
	if req.ExpectedVersion == nil {
		workflowAPIError(w, workflowFail("workflow_invalid_input"))
		return
	}
	result, e := h.WorkflowSvc.PublishRevision(r.Context(), entity.WorkspaceID, entity.ID, a, *req.ExpectedVersion, req.Definition, req.DefinitionGraph)
	if e != nil {
		workflowAPIError(w, e)
		return
	}
	writeJSON(w, 202, map[string]any{"revision": workflowRevisionView(result.Revision), "registration_state": result.RegistrationState})
}
func (h *Handler) ListWorkflowRevisions(w http.ResponseWriter, r *http.Request) {
	entity, _, ok := h.workflowResource(w, r, false, false)
	if !ok {
		return
	}
	limit, offset, ok := workflowPage(w, r)
	if !ok {
		return
	}
	rows, e := h.Queries.ListWorkflowRevisionsPage(r.Context(), db.ListWorkflowRevisionsPageParams{WorkspaceID: entity.WorkspaceID, WorkflowID: entity.ID, Limit: limit, Offset: offset})
	if e != nil {
		workflowAPIError(w, e)
		return
	}
	views := []any{}
	for _, row := range rows {
		views = append(views, workflowRevisionView(row))
	}
	writeJSON(w, 200, views)
}
func (h *Handler) GetWorkflowRevision(w http.ResponseWriter, r *http.Request) {
	entity, _, ok := h.workflowResource(w, r, false, false)
	if !ok {
		return
	}
	id, ok := parseUUIDOrBadRequest(w, chi.URLParam(r, "revisionId"), "revisionId")
	if !ok {
		return
	}
	row, e := h.Queries.GetWorkflowRevisionInWorkspace(r.Context(), db.GetWorkflowRevisionInWorkspaceParams{ID: id, WorkspaceID: entity.WorkspaceID})
	if e != nil || row.WorkflowID != entity.ID {
		workflowAPIError(w, workflowFail("workflow_not_found"))
		return
	}
	writeJSON(w, 200, workflowRevisionView(row))
}
func (h *Handler) ValidateWorkflowRevision(w http.ResponseWriter, r *http.Request) {
	entity, _, ok := h.workflowResource(w, r, true, false)
	if !ok {
		return
	}
	id, ok := parseUUIDOrBadRequest(w, chi.URLParam(r, "revisionId"), "revisionId")
	if !ok {
		return
	}
	row, e := h.Queries.GetWorkflowRevisionInWorkspace(r.Context(), db.GetWorkflowRevisionInWorkspaceParams{ID: id, WorkspaceID: entity.WorkspaceID})
	if e != nil || row.WorkflowID != entity.ID {
		workflowAPIError(w, workflowFail("workflow_not_found"))
		return
	}
	writeJSON(w, 202, map[string]any{"revision": workflowRevisionView(row), "registration_state": service.WorkflowRegistrationState(row), "validation_status": row.ValidationStatus})
}

func (h *Handler) ListWorkflowCollaborators(w http.ResponseWriter, r *http.Request) {
	entity, _, ok := h.workflowResource(w, r, false, false)
	if !ok {
		return
	}
	rows, e := h.Queries.ListWorkflowCollaborators(r.Context(), db.ListWorkflowCollaboratorsParams{WorkspaceID: entity.WorkspaceID, WorkflowID: entity.ID})
	if e != nil {
		workflowAPIError(w, e)
		return
	}
	writeJSON(w, 200, rows)
}
func (h *Handler) AddWorkflowCollaborator(w http.ResponseWriter, r *http.Request) {
	entity, a, ok := h.workflowResource(w, r, true, true)
	if !ok {
		return
	}
	var req struct {
		UserID string `json:"user_id"`
		Role   string `json:"role"`
	}
	if !workflowDecode(w, r, &req) {
		return
	}
	id, ok := parseUUIDOrBadRequest(w, req.UserID, "user_id")
	if !ok {
		return
	}
	if req.Role != "editor" && req.Role != "viewer" {
		workflowAPIError(w, workflowFail("workflow_invalid_input"))
		return
	}
	if _, e := h.getWorkspaceMember(r.Context(), req.UserID, uuidToString(entity.WorkspaceID)); e != nil {
		workflowAPIError(w, workflowFail("workflow_forbidden"))
		return
	}
	row, e := h.Queries.AddWorkflowCollaborator(r.Context(), db.AddWorkflowCollaboratorParams{WorkspaceID: entity.WorkspaceID, WorkflowID: entity.ID, UserID: id, Role: req.Role, CreatedByID: a.HumanID})
	if e != nil {
		workflowAPIError(w, e)
		return
	}
	writeJSON(w, 200, row)
}
func (h *Handler) RemoveWorkflowCollaborator(w http.ResponseWriter, r *http.Request) {
	entity, _, ok := h.workflowResource(w, r, true, true)
	if !ok {
		return
	}
	id, ok := parseUUIDOrBadRequest(w, chi.URLParam(r, "userId"), "userId")
	if !ok {
		return
	}
	if e := h.Queries.DeleteWorkflowCollaborator(r.Context(), db.DeleteWorkflowCollaboratorParams{WorkspaceID: entity.WorkspaceID, WorkflowID: entity.ID, UserID: id}); e != nil {
		workflowAPIError(w, e)
		return
	}
	w.WriteHeader(204)
}

type workflowTriggerRequest struct {
	Kind     string          `json:"kind"`
	Enabled  *bool           `json:"enabled"`
	Cron     *string         `json:"cron_expression"`
	Timezone *string         `json:"timezone"`
	Input    json.RawMessage `json:"input_template"`
	Label    *string         `json:"label"`
}

func workflowTriggerConfig(row *db.WorkflowTrigger, req workflowTriggerRequest) error {
	if req.Enabled != nil {
		row.Enabled = *req.Enabled
	}
	if req.Cron != nil {
		row.CronExpression = pgtype.Text{String: *req.Cron, Valid: true}
	}
	if req.Timezone != nil {
		row.Timezone = *req.Timezone
	}
	if req.Label != nil {
		row.Label = pgtype.Text{String: *req.Label, Valid: true}
	}
	if len(row.Label.String) > 200 {
		return workflowFail("workflow_invalid_input")
	}
	if req.Input != nil {
		row.InputTemplate = req.Input
	}
	if len(row.InputTemplate) == 0 {
		row.InputTemplate = []byte(`{}`)
	}
	if e := service.ValidateWorkflowPayload(row.InputTemplate); e != nil {
		return e
	}
	if row.Kind == "schedule" {
		loc, e := time.LoadLocation(row.Timezone)
		if e != nil {
			return workflowFail("workflow_invalid_schedule")
		}
		schedule, e := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow).Parse(row.CronExpression.String)
		if e != nil {
			return workflowFail("workflow_invalid_schedule")
		}
		row.NextRunAt = pgtype.Timestamptz{Time: schedule.Next(time.Now().In(loc)), Valid: true}
	} else if row.Kind != "api" || row.CronExpression.Valid {
		return workflowFail("workflow_invalid_input")
	}
	return nil
}
func workflowToken() (string, string, error) {
	raw := make([]byte, 32)
	if _, e := rand.Read(raw); e != nil {
		return "", "", e
	}
	token := "wft_" + base64.RawURLEncoding.EncodeToString(raw)
	sum := sha256.Sum256([]byte(token))
	return token, hex.EncodeToString(sum[:]), nil
}
func workflowTriggerView(row db.WorkflowTrigger) map[string]any {
	return map[string]any{"id": row.ID, "workspace_id": row.WorkspaceID, "workflow_id": row.WorkflowID, "kind": row.Kind, "principal_user_id": row.PrincipalUserID, "enabled": row.Enabled, "cron_expression": row.CronExpression, "timezone": row.Timezone, "next_run_at": row.NextRunAt, "input_template": service.SanitizeWorkflowInput(row.InputTemplate), "label": row.Label, "created_by_type": row.CreatedByType, "created_by_id": row.CreatedByID}
}
func (h *Handler) ListWorkflowTriggers(w http.ResponseWriter, r *http.Request) {
	entity, _, ok := h.workflowResource(w, r, false, false)
	if !ok {
		return
	}
	rows, e := h.Queries.ListWorkflowTriggers(r.Context(), db.ListWorkflowTriggersParams{WorkspaceID: entity.WorkspaceID, WorkflowID: entity.ID})
	if e != nil {
		workflowAPIError(w, e)
		return
	}
	result := []any{}
	for _, row := range rows {
		result = append(result, workflowTriggerView(row))
	}
	writeJSON(w, 200, result)
}
func (h *Handler) CreateWorkflowTrigger(w http.ResponseWriter, r *http.Request) {
	entity, a, ok := h.workflowResource(w, r, true, false)
	if !ok {
		return
	}
	var req workflowTriggerRequest
	if !workflowDecode(w, r, &req) {
		return
	}
	row := db.WorkflowTrigger{Kind: req.Kind, Enabled: true, Timezone: "UTC"}
	if e := workflowTriggerConfig(&row, req); e != nil {
		workflowAPIError(w, e)
		return
	}
	token := ""
	var e error
	if req.Kind == "api" {
		var hash string
		token, hash, e = workflowToken()
		if e != nil {
			workflowAPIError(w, e)
			return
		}
		row.TokenHash = pgtype.Text{String: hash, Valid: true}
	}
	created, e := h.Queries.CreateWorkflowTrigger(r.Context(), db.CreateWorkflowTriggerParams{WorkspaceID: entity.WorkspaceID, WorkflowID: entity.ID, Kind: row.Kind, PrincipalUserID: a.HumanID, Enabled: row.Enabled, CronExpression: row.CronExpression, Timezone: row.Timezone, NextRunAt: row.NextRunAt, InputTemplate: row.InputTemplate, Label: row.Label, TokenHash: row.TokenHash, CreatedByType: a.AuditType, CreatedByID: a.AuditID})
	if e != nil {
		workflowAPIError(w, e)
		return
	}
	view := workflowTriggerView(created)
	if token != "" {
		view["token"] = token
	}
	writeJSON(w, 201, view)
}
func (h *Handler) workflowTrigger(w http.ResponseWriter, r *http.Request, entity db.Workflow) (db.WorkflowTrigger, bool) {
	id, ok := parseUUIDOrBadRequest(w, chi.URLParam(r, "triggerId"), "triggerId")
	if !ok {
		return db.WorkflowTrigger{}, false
	}
	row, e := h.Queries.GetWorkflowTriggerInWorkspace(r.Context(), db.GetWorkflowTriggerInWorkspaceParams{ID: id, WorkspaceID: entity.WorkspaceID})
	if e != nil || row.WorkflowID != entity.ID {
		workflowAPIError(w, workflowFail("workflow_not_found"))
		return row, false
	}
	return row, true
}
func (h *Handler) UpdateWorkflowTrigger(w http.ResponseWriter, r *http.Request) {
	entity, _, ok := h.workflowResource(w, r, true, false)
	if !ok {
		return
	}
	row, ok := h.workflowTrigger(w, r, entity)
	if !ok {
		return
	}
	var req workflowTriggerRequest
	if !workflowDecode(w, r, &req) {
		return
	}
	if req.Kind != "" && req.Kind != row.Kind {
		workflowAPIError(w, workflowFail("workflow_invalid_input"))
		return
	}
	if e := workflowTriggerConfig(&row, req); e != nil {
		workflowAPIError(w, e)
		return
	}
	updated, e := h.Queries.UpdateWorkflowTrigger(r.Context(), db.UpdateWorkflowTriggerParams{ID: row.ID, WorkspaceID: row.WorkspaceID, Enabled: row.Enabled, CronExpression: row.CronExpression, Timezone: row.Timezone, NextRunAt: row.NextRunAt, InputTemplate: row.InputTemplate, Label: row.Label})
	if e != nil {
		workflowAPIError(w, e)
		return
	}
	writeJSON(w, 200, workflowTriggerView(updated))
}
func (h *Handler) DeleteWorkflowTrigger(w http.ResponseWriter, r *http.Request) {
	entity, _, ok := h.workflowResource(w, r, true, false)
	if !ok {
		return
	}
	row, ok := h.workflowTrigger(w, r, entity)
	if !ok {
		return
	}
	if e := h.Queries.DeleteWorkflowTrigger(r.Context(), db.DeleteWorkflowTriggerParams{ID: row.ID, WorkspaceID: row.WorkspaceID}); e != nil {
		workflowAPIError(w, e)
		return
	}
	w.WriteHeader(204)
}
func (h *Handler) RotateWorkflowTriggerToken(w http.ResponseWriter, r *http.Request) {
	entity, _, ok := h.workflowResource(w, r, true, false)
	if !ok {
		return
	}
	row, ok := h.workflowTrigger(w, r, entity)
	if !ok {
		return
	}
	if row.Kind != "api" {
		workflowAPIError(w, workflowFail("workflow_invalid_input"))
		return
	}
	token, hash, e := workflowToken()
	if e != nil {
		workflowAPIError(w, e)
		return
	}
	if _, e = h.Queries.RotateWorkflowTriggerToken(r.Context(), db.RotateWorkflowTriggerTokenParams{ID: row.ID, WorkspaceID: row.WorkspaceID, TokenHash: pgtype.Text{String: hash, Valid: true}}); e != nil {
		workflowAPIError(w, e)
		return
	}
	writeJSON(w, 200, map[string]string{"token": token})
}

// Run views expose local product state; raw engine JSON and credentials never cross this boundary.
func workflowRunView(run db.WorkflowRun) map[string]any {
	return map[string]any{"id": run.ID, "workspace_id": run.WorkspaceID, "workflow_id": run.WorkflowID, "revision_id": run.RevisionID, "principal_user_id": run.PrincipalUserID, "source": run.Source, "status": run.Status, "reason_code": run.ReasonCode, "triggered_at": run.TriggeredAt, "started_at": run.StartedAt, "finished_at": run.FinishedAt, "cancel_requested_at": run.CancelRequestedAt}
}
func (h *Handler) workflowRun(w http.ResponseWriter, r *http.Request, entity db.Workflow) (db.WorkflowRun, bool) {
	id, ok := parseUUIDOrBadRequest(w, chi.URLParam(r, "runId"), "runId")
	if !ok {
		return db.WorkflowRun{}, false
	}
	run, e := h.Queries.GetWorkflowRunInWorkspace(r.Context(), db.GetWorkflowRunInWorkspaceParams{ID: id, WorkspaceID: entity.WorkspaceID})
	if e != nil || run.WorkflowID != entity.ID {
		workflowAPIError(w, workflowFail("workflow_not_found"))
		return run, false
	}
	return run, true
}
func (h *Handler) ListWorkflowRuns(w http.ResponseWriter, r *http.Request) {
	entity, _, ok := h.workflowResource(w, r, false, false)
	if !ok {
		return
	}
	limit, offset, ok := workflowPage(w, r)
	if !ok {
		return
	}
	rows, e := h.Queries.ListWorkflowRuns(r.Context(), db.ListWorkflowRunsParams{WorkflowID: entity.ID, WorkspaceID: entity.WorkspaceID, Limit: limit, Offset: offset})
	if e != nil {
		workflowAPIError(w, e)
		return
	}
	result := []any{}
	for _, run := range rows {
		result = append(result, workflowRunView(run))
	}
	writeJSON(w, 200, result)
}
func (h *Handler) GetWorkflowRun(w http.ResponseWriter, r *http.Request) {
	entity, _, ok := h.workflowResource(w, r, false, false)
	if !ok {
		return
	}
	run, ok := h.workflowRun(w, r, entity)
	if ok {
		view := workflowRunView(run)
		view["input"] = service.SanitizeWorkflowInput(run.Input)
		view["steps"] = service.WorkflowRunSteps(run.StepProjection)
		writeJSON(w, 200, view)
	}
}
func (h *Handler) WorkflowRunLogs(w http.ResponseWriter, r *http.Request) {
	entity, _, ok := h.workflowResource(w, r, false, false)
	if !ok {
		return
	}
	run, ok := h.workflowRun(w, r, entity)
	if !ok {
		return
	}
	limit, offset, ok := workflowPage(w, r)
	if !ok {
		return
	}
	if h.WorkflowSvc.Logs != nil {
		entries, e := h.WorkflowSvc.Logs.ReadWorkflowLogs(r.Context(), run, limit, offset)
		if e != nil {
			workflowAPIError(w, workflowFail("workflow_engine_unavailable"))
			return
		}
		writeJSON(w, 200, map[string]any{"run_id": run.ID, "entries": service.SanitizeWorkflowLogs(entries), "available": true})
		return
	}
	writeJSON(w, 200, map[string]any{"run_id": run.ID, "entries": []any{}, "available": false, "reason_code": "workflow_log_transport_pending"})
}
func (h *Handler) RunWorkflow(w http.ResponseWriter, r *http.Request) {
	entity, a, ok := h.workflowResource(w, r, true, false)
	if !ok {
		return
	}
	var req struct {
		Input json.RawMessage `json:"input"`
	}
	if !workflowDecode(w, r, &req) {
		return
	}
	if req.Input == nil {
		req.Input = json.RawMessage(`{}`)
	}
	run, e := h.WorkflowSvc.AdmitRun(r.Context(), entity, a, "manual", pgtype.UUID{}, req.Input)
	if e != nil {
		workflowAPIError(w, e)
		return
	}
	writeJSON(w, 202, workflowRunView(run))
}
func (h *Handler) ReplayWorkflowRun(w http.ResponseWriter, r *http.Request) {
	entity, a, ok := h.workflowResource(w, r, true, false)
	if !ok {
		return
	}
	old, ok := h.workflowRun(w, r, entity)
	if !ok {
		return
	}
	input := json.RawMessage(old.Input)
	if len(input) == 0 {
		input = json.RawMessage(`{}`)
	}
	run, e := h.WorkflowSvc.AdmitRun(r.Context(), entity, a, "manual", pgtype.UUID{}, input)
	if e != nil {
		workflowAPIError(w, e)
		return
	}
	writeJSON(w, 202, workflowRunView(run))
}
func (h *Handler) CancelWorkflowRun(w http.ResponseWriter, r *http.Request) {
	entity, a, ok := h.workflowResource(w, r, true, false)
	if !ok {
		return
	}
	old, ok := h.workflowRun(w, r, entity)
	if !ok {
		return
	}
	run, e := h.WorkflowSvc.Cancel(r.Context(), entity, a, old.ID)
	if e != nil {
		workflowAPIError(w, e)
		return
	}
	writeJSON(w, 200, workflowRunView(run))
}
func (h *Handler) HandleWorkflowIngress(w http.ResponseWriter, r *http.Request) {
	token := chi.URLParam(r, "token")
	if len(token) > 128 || !strings.HasPrefix(token, "wft_") {
		workflowAPIError(w, workflowFail("workflow_not_found"))
		return
	}
	sum := sha256.Sum256([]byte(token))
	trigger, e := h.Queries.ResolveWorkflowIngressToken(r.Context(), pgtype.Text{String: hex.EncodeToString(sum[:]), Valid: true})
	if e != nil {
		workflowAPIError(w, workflowFail("workflow_not_found"))
		return
	}
	middleware.SetWebhookTriggerID(r, uuidToString(trigger.ID))
	entity, e := h.WorkflowSvc.Load(r.Context(), trigger.WorkspaceID, trigger.WorkflowID)
	if e != nil {
		workflowAPIError(w, e)
		return
	}
	raw, e := io.ReadAll(http.MaxBytesReader(w, r.Body, service.WorkflowMaxPayloadBytes))
	if e != nil {
		workflowAPIError(w, workflowFail("workflow_invalid_payload"))
		return
	}
	if len(raw) == 0 {
		raw = trigger.InputTemplate
	}
	a := service.WorkflowActor{HumanID: trigger.PrincipalUserID, AuditType: trigger.CreatedByType, AuditID: trigger.CreatedByID}
	run, e := h.WorkflowSvc.AdmitRun(r.Context(), entity, a, "api", trigger.ID, raw)
	if e != nil {
		workflowAPIError(w, e)
		return
	}
	writeJSON(w, 202, workflowRunView(run))
}
