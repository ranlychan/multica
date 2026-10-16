package handler

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/multica-ai/multica/server/internal/issuestatus"
	obsmetrics "github.com/multica-ai/multica/server/internal/metrics"
	"github.com/multica-ai/multica/server/internal/service"
	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

type workflowBusinessPayload struct {
	ID            string          `json:"id,omitempty"`
	AgentID       string          `json:"agent_id,omitempty"`
	IssueID       string          `json:"issue_id,omitempty"`
	AutopilotID   string          `json:"autopilot_id,omitempty"`
	Title         string          `json:"title,omitempty"`
	Description   string          `json:"description,omitempty"`
	Status        string          `json:"status,omitempty"`
	Priority      string          `json:"priority,omitempty"`
	AssigneeType  string          `json:"assignee_type,omitempty"`
	AssigneeID    string          `json:"assignee_id,omitempty"`
	ParentIssueID string          `json:"parent_issue_id,omitempty"`
	ProjectID     string          `json:"project_id,omitempty"`
	Payload       json.RawMessage `json:"payload,omitempty"`
}

func businessError(code string) error { return &service.WorkflowError{Code: code} }

func workflowActionDenied(w http.ResponseWriter, status int, code string) {
	workflowPrivate(w)
	writeJSON(w, status, map[string]any{"error": map[string]string{"code": code}})
}

// WorkflowBusinessAction is entered only after current native assignment proof.
// Every product read/replay repeats local authority and grant fencing in the
// same transaction as its receipt and all durable business writes.
func (h *Handler) WorkflowBusinessAction(ctx context.Context, a WorkflowActionAuthority, req WorkflowActionRequest) (json.RawMessage, error) {
	mutation := req.Operation == "issues.create" || req.Operation == "agents.run" || req.Operation == "autopilots.invoke"
	switch req.Operation {
	case "issues.create", "issues.get", "agents.run", "agents.get", "autopilots.invoke", "autopilots.get":
	default:
		return nil, businessError("workflow_invalid_payload")
	}
	if mutation && req.OperationKey == "" {
		return nil, businessError("workflow_invalid_operation_key")
	}
	var p workflowBusinessPayload
	dec := json.NewDecoder(bytes.NewReader(req.Payload))
	dec.DisallowUnknownFields()
	if dec.Decode(&p) != nil {
		return nil, businessError("workflow_invalid_payload")
	}
	var canonical any
	decoder := json.NewDecoder(bytes.NewReader(req.Payload))
	decoder.UseNumber()
	_ = decoder.Decode(&canonical)
	canonicalBytes, _ := json.Marshal(canonical)
	digest := sha256.Sum256(canonicalBytes)
	requestHash := hex.EncodeToString(digest[:])
	conflictHint := false
	cachedHint := false
	if mutation {
		old, e := h.Queries.GetWorkflowOperation(ctx, db.GetWorkflowOperationParams{WorkspaceID: a.Run.WorkspaceID, RunID: a.Run.ID, StepID: req.StepID, OperationKey: req.OperationKey})
		conflictHint = e == nil && (old.OperationName != req.Operation || old.PayloadHash != requestHash)
		cachedHint = e == nil && old.Status == "completed"
	}
	// Prepare integration I/O outside locks, then verify its binding again under
	// locked current resource rows. Nothing supplied by the author sets ancestry.
	var prepared service.WorkflowTaskEnqueue
	var agentID pgtype.UUID
	var err error
	if !conflictHint {
		agentID, err = h.workflowActionAgent(ctx, h.Queries, a.Run.WorkspaceID, req.Operation, p)
	}
	if err != nil {
		return nil, err
	}
	if agentID.Valid {
		agent, err := h.Queries.GetAgentInWorkspace(ctx, db.GetAgentInWorkspaceParams{ID: agentID, WorkspaceID: a.Run.WorkspaceID})
		if err != nil {
			return nil, businessError("workflow_not_found")
		}
		if !service.CanMemberInvokeAgent(ctx, h.Queries, agent, a.Principal, a.Run.WorkspaceID) {
			return nil, businessError("workflow_forbidden")
		}
		if req.Operation == "autopilots.invoke" {
			ap, e := h.Queries.GetAutopilotInWorkspace(ctx, db.GetAutopilotInWorkspaceParams{ID: mustBusinessUUID(p.AutopilotID), WorkspaceID: a.Run.WorkspaceID})
			if e != nil {
				return nil, businessError("workflow_not_found")
			}
			member, e := h.Queries.GetMemberByUserAndWorkspace(ctx, db.GetMemberByUserAndWorkspaceParams{WorkspaceID: a.Run.WorkspaceID, UserID: a.Principal})
			if e != nil || !h.memberCanWriteAutopilot(ctx, ap, member) {
				return nil, businessError("workflow_forbidden")
			}
		}
		if mutation && !cachedHint {
			prepared = h.TaskService.PrepareWorkflowTask(ctx, a.Run.ID, a.Principal, agent)
		}
	}
	tx, err := h.TxStarter.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	q := h.Queries.WithTx(tx)
	if err = h.lockWorkflowBusinessAuthority(ctx, q, a, req); err != nil {
		return nil, err
	}
	if conflictHint {
		old, e := q.LockWorkflowOperation(ctx, db.LockWorkflowOperationParams{WorkspaceID: a.Run.WorkspaceID, RunID: a.Run.ID, StepID: req.StepID, OperationKey: req.OperationKey})
		if e != nil {
			return nil, e
		}
		if old.OperationName != req.Operation || old.PayloadHash != requestHash {
			return nil, businessError("workflow_operation_conflict")
		}
		return nil, businessError("workflow_unavailable")
	}
	if req.Operation == "issues.create" && p.AssigneeType == "member" {
		if _, e := q.LockWorkflowActionMembership(ctx, db.LockWorkflowActionMembershipParams{WorkspaceID: a.Run.WorkspaceID, UserID: mustBusinessUUID(p.AssigneeID)}); e != nil {
			return nil, businessError("workflow_forbidden")
		}
	}
	if req.Operation == "issues.create" && p.AssigneeType == "squad" {
		if _, e := q.LockSquadForAutopilotAssignment(ctx, db.LockSquadForAutopilotAssignmentParams{ID: mustBusinessUUID(p.AssigneeID), WorkspaceID: a.Run.WorkspaceID}); e != nil {
			return nil, businessError("workflow_forbidden")
		}
	}
	if req.Operation == "autopilots.invoke" || req.Operation == "autopilots.get" {
		apID := mustBusinessUUID(p.AutopilotID)
		if req.Operation == "autopilots.get" {
			run, e := q.GetAutopilotRun(ctx, mustBusinessUUID(p.ID))
			if e != nil {
				return nil, businessError("workflow_not_found")
			}
			apID = run.AutopilotID
		}
		ap, e := q.LockAutopilotForUpdate(ctx, db.LockAutopilotForUpdateParams{ID: apID, WorkspaceID: a.Run.WorkspaceID})
		if e != nil {
			return nil, businessError("workflow_not_found")
		}
		if ap.AssigneeType == "squad" {
			if _, e = q.LockSquadForAutopilotAssignment(ctx, db.LockSquadForAutopilotAssignmentParams{ID: ap.AssigneeID, WorkspaceID: a.Run.WorkspaceID}); e != nil {
				return nil, businessError("workflow_forbidden")
			}
		}
		if _, e = q.LockWorkflowAutopilotCollaborators(ctx, ap.ID); e != nil {
			return nil, e
		}
		member, e := q.GetMemberByUserAndWorkspace(ctx, db.GetMemberByUserAndWorkspaceParams{WorkspaceID: a.Run.WorkspaceID, UserID: a.Principal})
		if e != nil {
			return nil, businessError("workflow_forbidden")
		}
		allowed := autopilotWriteByOwnership(ap, member)
		if !allowed {
			allowed, e = q.IsAutopilotCollaborator(ctx, db.IsAutopilotCollaboratorParams{AutopilotID: ap.ID, UserID: a.Principal})
			if e != nil {
				return nil, e
			}
		}
		if !allowed {
			return nil, businessError("workflow_forbidden")
		}
	}
	// Lock and reauthorize the target before even looking up a cached receipt.
	currentAgent, err := h.workflowActionAgent(ctx, q, a.Run.WorkspaceID, req.Operation, p)
	if err != nil {
		return nil, err
	}
	if currentAgent != agentID {
		return nil, businessError("workflow_unavailable")
	}
	if agentID.Valid {
		agent, err := q.LockAgentForAutopilotAssignment(ctx, db.LockAgentForAutopilotAssignmentParams{ID: agentID, WorkspaceID: a.Run.WorkspaceID})
		if err != nil {
			return nil, businessError("workflow_forbidden")
		}
		if mutation && !cachedHint && !prepared.MatchesAgent(agent) {
			return nil, businessError("workflow_unavailable")
		}
		if _, err = q.LockWorkflowAgentInvocationTargets(ctx, agent.ID); err != nil {
			return nil, err
		}
		if !service.CanMemberInvokeAgent(ctx, q, agent, a.Principal, a.Run.WorkspaceID) {
			return nil, businessError("workflow_forbidden")
		}
		if mutation && !cachedHint {
			verdict, err := service.AgentReadiness(ctx, service.RuntimeLookup{Queries: q, Metrics: h.TaskService.Metrics, Source: obsmetrics.RuntimeLookupSourceTask}, agent)
			if err != nil {
				return nil, err
			}
			if verdict.Blocked() {
				return nil, businessError("workflow_unavailable")
			}
			if req.Operation == "autopilots.invoke" {
				ap, err := q.GetAutopilotInWorkspace(ctx, db.GetAutopilotInWorkspaceParams{ID: mustBusinessUUID(p.AutopilotID), WorkspaceID: a.Run.WorkspaceID})
				if err != nil {
					return nil, err
				}
				if ap.ExecutionMode == "run_only" && !verdict.Ready() {
					return nil, businessError("workflow_unavailable")
				}
			}
		}
	}
	var receipt db.WorkflowOperation
	var payloadHash string
	if mutation {
		payloadHash = requestHash
		receipt, err = q.CreateWorkflowOperation(ctx, db.CreateWorkflowOperationParams{WorkspaceID: a.Run.WorkspaceID, RunID: a.Run.ID, StepID: req.StepID, OperationKey: req.OperationKey, OperationName: req.Operation, PayloadHash: payloadHash, Attempt: int32(req.Attempt) + 1})
		if errors.Is(err, pgx.ErrNoRows) {
			receipt, err = q.LockWorkflowOperation(ctx, db.LockWorkflowOperationParams{WorkspaceID: a.Run.WorkspaceID, RunID: a.Run.ID, StepID: req.StepID, OperationKey: req.OperationKey})
		}
		if err != nil {
			return nil, err
		}
		if receipt.OperationName != req.Operation || receipt.PayloadHash != payloadHash {
			return nil, businessError("workflow_operation_conflict")
		}
		if receipt.Status == "completed" {
			if err = h.workflowBusinessFresh(ctx, q, a); err != nil {
				return nil, err
			}
			if err = tx.Commit(ctx); err != nil {
				return nil, err
			}
			return receipt.Result, nil
		}
	}
	result, after, err := h.performWorkflowBusiness(ctx, tx, q, a, req, p, prepared)
	if err != nil {
		return nil, err
	}
	// This uses wall time after every potentially blocking business/grant lock.
	if err = h.workflowBusinessFresh(ctx, q, a); err != nil {
		return nil, err
	}
	if mutation {
		_, err = q.CompleteWorkflowOperation(ctx, db.CompleteWorkflowOperationParams{WorkspaceID: a.Run.WorkspaceID, RunID: a.Run.ID, StepID: req.StepID, OperationKey: req.OperationKey, PayloadHash: payloadHash, Result: result})
		if err != nil {
			return nil, err
		}
	}
	if err = h.workflowBusinessFresh(ctx, q, a); err != nil {
		return nil, err
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, err
	}
	if after != nil {
		after()
	}
	return result, nil
}

func mustBusinessUUID(value string) pgtype.UUID { id, _ := util.ParseUUID(value); return id }
func businessUUID(value string) (pgtype.UUID, error) {
	id, err := util.ParseUUID(value)
	if err != nil {
		return id, businessError("workflow_invalid_payload")
	}
	return id, nil
}

func (h *Handler) lockWorkflowBusinessAuthority(ctx context.Context, q *db.Queries, a WorkflowActionAuthority, req WorkflowActionRequest) error {
	ws := a.Run.WorkspaceID
	if _, err := q.LockWorkspaceForChatSessionCreate(ctx, ws); err != nil {
		return businessError("workflow_capability_revoked")
	}
	runner, err := q.LockWorkflowRunnerAuthorization(ctx, db.LockWorkflowRunnerAuthorizationParams{WorkspaceID: ws, ID: a.Runner.ID, TokenHash: a.Runner.TokenHash})
	if err != nil || !bytes.Equal(runner.Registrations, a.Runner.Registrations) {
		return businessError("workflow_capability_revoked")
	}
	w, err := q.LockWorkflowForUpdate(ctx, db.LockWorkflowForUpdateParams{ID: a.Run.WorkflowID, WorkspaceID: ws})
	if err != nil {
		return businessError("workflow_forbidden")
	}
	run, err := q.LockWorkflowActionRun(ctx, db.LockWorkflowActionRunParams{ID: a.Run.ID, WorkspaceID: ws})
	if err != nil || run.CancelRequestedAt.Valid || (run.Status != "running" && run.Status != "submitting") || run.RevisionID != a.Revision.ID || run.DefinitionHash != a.Revision.DefinitionHash || run.HatchetRunID != a.Run.HatchetRunID || run.PrincipalUserID != a.Principal {
		return businessError("workflow_capability_revoked")
	}
	if _, err = q.LockWorkflowActionMembership(ctx, db.LockWorkflowActionMembershipParams{WorkspaceID: ws, UserID: a.Principal}); err != nil {
		return businessError("workflow_forbidden")
	}
	if _, err = q.LockWorkflowActionCollaborators(ctx, db.LockWorkflowActionCollaboratorsParams{WorkflowID: w.ID, WorkspaceID: ws}); err != nil {
		return err
	}
	svc := *h.WorkflowSvc
	svc.Queries = q
	if err = svc.Authorize(ctx, w, service.WorkflowActor{HumanID: a.Principal}, false); err != nil {
		return err
	}
	if req.RunID != uuidToString(run.ID) {
		return businessError("workflow_forbidden")
	}
	return nil
}

func (h *Handler) workflowBusinessFresh(ctx context.Context, q *db.Queries, a WorkflowActionAuthority) error {
	fresh, err := q.WorkflowRunnerActionAuthorityFresh(ctx, db.WorkflowRunnerActionAuthorityFreshParams{ID: a.Runner.ID, WorkspaceID: a.Run.WorkspaceID, TokenHash: a.Runner.TokenHash})
	if err != nil {
		return err
	}
	if !fresh {
		return businessError("workflow_capability_revoked")
	}
	return nil
}

func (h *Handler) workflowActionAgent(ctx context.Context, q *db.Queries, ws pgtype.UUID, op string, p workflowBusinessPayload) (pgtype.UUID, error) {
	switch op {
	case "agents.run":
		id, err := businessUUID(p.IssueID)
		if err != nil {
			return id, err
		}
		if _, err = q.GetIssueInWorkspace(ctx, db.GetIssueInWorkspaceParams{ID: id, WorkspaceID: ws}); err != nil {
			return pgtype.UUID{}, businessError("workflow_not_found")
		}
		return businessUUID(p.AgentID)
	case "agents.get":
		id, err := businessUUID(p.ID)
		if err != nil {
			return id, err
		}
		task, err := q.GetAgentTaskInWorkspace(ctx, db.GetAgentTaskInWorkspaceParams{ID: id, WorkspaceID: ws})
		if err != nil {
			return pgtype.UUID{}, businessError("workflow_not_found")
		}
		return task.AgentID, nil
	case "issues.create":
		if p.AssigneeType == "" && p.AssigneeID == "" {
			return pgtype.UUID{}, nil
		}
		id, err := businessUUID(p.AssigneeID)
		if err != nil {
			return id, err
		}
		switch p.AssigneeType {
		case "agent":
			return id, nil
		case "member":
			if _, err = q.GetMemberByUserAndWorkspace(ctx, db.GetMemberByUserAndWorkspaceParams{WorkspaceID: ws, UserID: id}); err != nil {
				return pgtype.UUID{}, businessError("workflow_not_found")
			}
			return pgtype.UUID{}, nil
		case "squad":
			squad, err := q.GetSquadInWorkspace(ctx, db.GetSquadInWorkspaceParams{ID: id, WorkspaceID: ws})
			if err != nil || squad.ArchivedAt.Valid {
				return pgtype.UUID{}, businessError("workflow_not_found")
			}
			return squad.LeaderID, nil
		default:
			return pgtype.UUID{}, businessError("workflow_invalid_payload")
		}
	case "autopilots.invoke", "autopilots.get":
		apID := p.AutopilotID
		if op == "autopilots.get" {
			id, err := businessUUID(p.ID)
			if err != nil {
				return id, err
			}
			run, err := q.GetAutopilotRun(ctx, id)
			if err != nil {
				return pgtype.UUID{}, businessError("workflow_not_found")
			}
			apID = uuidToString(run.AutopilotID)
		}
		id, err := businessUUID(apID)
		if err != nil {
			return id, err
		}
		ap, err := q.GetAutopilotInWorkspace(ctx, db.GetAutopilotInWorkspaceParams{ID: id, WorkspaceID: ws})
		if err != nil {
			return pgtype.UUID{}, businessError("workflow_not_found")
		}
		// Writer gate is applied in the locked action below, before result lookup.
		svc := *h.AutopilotService
		svc.Queries = q
		agent, err := svc.WorkflowAgent(ctx, ap)
		if err != nil {
			return pgtype.UUID{}, businessError("workflow_forbidden")
		}
		return agent.ID, nil
	}
	return pgtype.UUID{}, nil
}

func (h *Handler) performWorkflowBusiness(ctx context.Context, tx pgx.Tx, q *db.Queries, a WorkflowActionAuthority, req WorkflowActionRequest, p workflowBusinessPayload, prepared service.WorkflowTaskEnqueue) (json.RawMessage, func(), error) {
	encode := func(value any, after func(), err error) (json.RawMessage, func(), error) {
		if err != nil {
			return nil, nil, err
		}
		raw, err := json.Marshal(value)
		return raw, after, err
	}
	ws := a.Run.WorkspaceID
	switch req.Operation {
	case "issues.create":
		if strings.TrimSpace(p.Title) == "" {
			return nil, nil, businessError("workflow_invalid_payload")
		}
		if p.Status == "" {
			p.Status = "todo"
		}
		if _, err := issuestatus.Resolve(ctx, q, ws, p.Status); err != nil {
			return nil, nil, businessError("workflow_invalid_status")
		}
		if p.Priority == "" {
			p.Priority = "none"
		}
		switch p.Priority {
		case "none", "low", "medium", "high", "urgent":
		default:
			return nil, nil, businessError("workflow_invalid_payload")
		}
		params := service.IssueCreateParams{WorkspaceID: ws, Title: p.Title, Description: pgtype.Text{String: p.Description, Valid: p.Description != ""}, Status: p.Status, Priority: p.Priority, CreatorType: "member", CreatorID: a.Principal, OriginType: pgtype.Text{String: "workflow", Valid: true}, OriginID: a.Run.ID}
		var err error
		if p.ProjectID != "" {
			params.ProjectID, err = businessUUID(p.ProjectID)
			if err != nil {
				return nil, nil, err
			}
		}
		if p.ParentIssueID != "" {
			params.ParentIssueID, err = businessUUID(p.ParentIssueID)
			if err != nil {
				return nil, nil, err
			}
		}
		if p.AssigneeID != "" {
			params.AssigneeID = mustBusinessUUID(p.AssigneeID)
			params.AssigneeType = pgtype.Text{String: p.AssigneeType, Valid: true}
		}
		opts := service.IssueCreateOpts{Platform: "workflow"}
		created, err := h.IssueService.CreateInTx(ctx, tx, params, opts)
		if err != nil {
			return nil, nil, err
		}
		var task db.AgentTaskQueue
		if prepared.AgentID.Valid && p.Status != "backlog" {
			var squad pgtype.UUID
			if p.AssigneeType == "squad" {
				squad = params.AssigneeID
			}
			task, err = h.TaskService.EnqueueWorkflowIssueTaskInTx(ctx, q, created.Issue, prepared.AgentID, squad, prepared)
			if err != nil {
				return nil, nil, err
			}
		}
		return encode(workflowIssueResult(created.Issue), func() {
			h.IssueService.FinalizeCreateInTx(ctx, created, params, opts)
			if task.ID.Valid {
				h.TaskService.FinalizeWorkflowTask(ctx, task)
			}
		}, nil)
	case "issues.get":
		id, err := businessUUID(p.ID)
		if err != nil {
			return nil, nil, err
		}
		issue, err := q.GetIssueInWorkspace(ctx, db.GetIssueInWorkspaceParams{ID: id, WorkspaceID: ws})
		if err != nil {
			return nil, nil, businessError("workflow_not_found")
		}
		return encode(workflowIssueResult(issue), nil, nil)
	case "agents.run":
		issue, err := q.GetIssueInWorkspace(ctx, db.GetIssueInWorkspaceParams{ID: mustBusinessUUID(p.IssueID), WorkspaceID: ws})
		if err != nil {
			return nil, nil, businessError("workflow_not_found")
		}
		task, err := h.TaskService.EnqueueWorkflowIssueTaskInTx(ctx, q, issue, prepared.AgentID, pgtype.UUID{}, prepared)
		return encode(workflowTaskResult(task), func() { h.TaskService.FinalizeWorkflowTask(ctx, task) }, err)
	case "agents.get":
		task, err := q.GetAgentTaskInWorkspace(ctx, db.GetAgentTaskInWorkspaceParams{ID: mustBusinessUUID(p.ID), WorkspaceID: ws})
		return encode(workflowTaskResult(task), nil, err)
	case "autopilots.invoke", "autopilots.get":
		var run db.AutopilotRun
		id := mustBusinessUUID(p.AutopilotID)
		var err error
		if req.Operation == "autopilots.get" {
			run, err = q.GetAutopilotRun(ctx, mustBusinessUUID(p.ID))
			if err != nil {
				return nil, nil, err
			}
			id = run.AutopilotID
		}
		ap, err := q.LockAutopilotForUpdate(ctx, db.LockAutopilotForUpdateParams{ID: id, WorkspaceID: ws})
		if err != nil {
			return nil, nil, businessError("workflow_not_found")
		}
		if req.Operation == "autopilots.get" {
			return encode(workflowAutopilotResult(run), nil, nil)
		}
		if ap.Status != "active" {
			return nil, nil, businessError("workflow_unavailable")
		}
		payload := p.Payload
		if len(payload) == 0 {
			payload = json.RawMessage(`{}`)
		}
		run, after, err := h.AutopilotService.DispatchWorkflowInTx(ctx, tx, ap, a.Principal, a.Run.ID, req.OperationKey, payload, prepared)
		return encode(workflowAutopilotResult(run), after, err)
	}
	return nil, nil, businessError("workflow_invalid_payload")
}

func workflowIssueResult(issue db.Issue) map[string]any {
	return map[string]any{"id": uuidToString(issue.ID), "status": issue.Status}
}
func workflowTaskResult(task db.AgentTaskQueue) map[string]any {
	return map[string]any{"id": uuidToString(task.ID), "status": task.Status, "agent_id": uuidToString(task.AgentID), "issue_id": util.UUIDToPtr(task.IssueID)}
}
func workflowAutopilotResult(run db.AutopilotRun) map[string]any {
	return map[string]any{"id": uuidToString(run.ID), "status": run.Status, "autopilot_id": uuidToString(run.AutopilotID), "issue_id": util.UUIDToPtr(run.IssueID), "task_id": util.UUIDToPtr(run.TaskID)}
}

func workflowBusinessFailure(err error) (int, string) {
	var e *service.WorkflowError
	if errors.As(err, &e) {
		switch e.Code {
		case "workflow_forbidden":
			return 403, e.Code
		case "workflow_not_found":
			return 404, e.Code
		case "workflow_capability_revoked":
			return 410, e.Code
		case "workflow_operation_conflict":
			return 409, e.Code
		case "workflow_invalid_payload", "workflow_invalid_status", "workflow_invalid_operation_key":
			return 400, e.Code
		}
	}
	var quota *service.AutopilotQuotaExceededError
	var issueQuota *service.IssueLimitReachedError
	if errors.As(err, &quota) || errors.As(err, &issueQuota) {
		return 429, "workflow_rate_limited"
	}
	if errors.Is(err, service.ErrDuplicatePendingTask) || errors.Is(err, service.ErrActiveDuplicate) {
		return 409, "workflow_operation_conflict"
	}
	return 503, "workflow_unavailable"
}
