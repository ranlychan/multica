package service

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
	"time"
)

// WorkflowEngine exchanges tenant-scoped data with the engine. Raw transport
// responses and credentials never cross this product boundary.
type WorkflowEngine interface {
	Submit(context.Context, db.WorkflowRun, db.WorkflowRevision) (string, error)
	Discover(context.Context, db.WorkflowRun) (string, error)
	Inspect(context.Context, db.WorkflowRun) (WorkflowEngineState, error)
	Cancel(context.Context, db.WorkflowRun) error
}
type WorkflowEngineState struct {
	Status string
	Output json.RawMessage
	Logs   []WorkflowLogEntry
	Steps  json.RawMessage
}
type RunIntent struct {
	WorkspaceID pgtype.UUID
	RunID       pgtype.UUID
}

// Task5 must register this TTL on every immutable workflow name. The persisted
// dispatch deadline leaves five minutes for clock/request latency inside it.
const WorkflowEngineIdempotencyTTL = time.Hour
const workflowSubmissionWindow = 55 * time.Minute
const workflowRequestTimeout = 30 * time.Second

func (s *WorkflowService) runnable(ctx context.Context, ws, rev pgtype.UUID) bool {
	return s.Ready != nil && s.Ready() && s.Eligible != nil && s.Eligible(ctx, ws, rev)
}

// ClaimScheduledRuns commits the local ledger and watermark together. Engine
// submission is deliberately outside this transaction and recovered by scans.
func (s *WorkflowService) ClaimScheduledRuns(ctx context.Context, ws pgtype.UUID, now time.Time, limit int32) ([]db.WorkflowRun, error) {
	if limit < 1 || limit > WorkflowMaxPageSize {
		limit = WorkflowMaxPageSize
	}
	tx, e := s.TxStarter.Begin(ctx)
	if e != nil {
		return nil, e
	}
	defer tx.Rollback(ctx)
	q := s.Queries.WithTx(tx)
	triggers, e := q.LockDueWorkflowTriggers(ctx, db.LockDueWorkflowTriggersParams{WorkspaceID: ws, NextRunAt: pgtype.Timestamptz{Time: now, Valid: true}, Limit: limit})
	if e != nil {
		return nil, e
	}
	runs := make([]db.WorkflowRun, 0, len(triggers))
	for _, tr := range triggers {
		w, e := q.LockWorkflowForUpdate(ctx, db.LockWorkflowForUpdateParams{ID: tr.WorkflowID, WorkspaceID: ws})
		if e != nil {
			return nil, e
		}
		if w.Status != "active" {
			continue
		}
		if !w.CurrentRevisionID.Valid {
			continue
		}
		r, e := q.GetWorkflowRevisionInWorkspace(ctx, db.GetWorkflowRevisionInWorkspaceParams{ID: w.CurrentRevisionID, WorkspaceID: ws})
		if errors.Is(e, pgx.ErrNoRows) {
			continue
		}
		if e != nil {
			return nil, e
		}
		if r.WorkflowID != w.ID || r.ValidationStatus != "valid" || !r.PublishedAt.Valid {
			continue
		}
		next, e := NextOccurrenceAfterUTC(tr.CronExpression.String, tr.Timezone, now)
		if e != nil || next.IsZero() {
			return nil, workflowError("workflow_invalid_schedule")
		}
		state, reason := "queued", ""
		if _, e = q.GetMemberByUserAndWorkspace(ctx, db.GetMemberByUserAndWorkspaceParams{WorkspaceID: ws, UserID: tr.PrincipalUserID}); errors.Is(e, pgx.ErrNoRows) {
			state, reason = "skipped", "workflow_principal_departed"
		} else if e != nil {
			return nil, e
		} else if s.authorize(ctx, q, w, WorkflowActor{HumanID: tr.PrincipalUserID}, false) != nil {
			state, reason = "skipped", "workflow_forbidden"
		} else if r.WorkflowID != w.ID || r.ValidationStatus != "valid" || !r.PublishedAt.Valid {
			state, reason = "skipped", "workflow_unrunnable"
		} else if !s.runnable(ctx, ws, r.ID) {
			state, reason = "skipped", "workflow_engine_unavailable"
		}
		existing, e := q.GetScheduledWorkflowRun(ctx, db.GetScheduledWorkflowRunParams{TriggerID: tr.ID, WorkspaceID: ws, PlannedAt: tr.NextRunAt})
		if errors.Is(e, pgx.ErrNoRows) {
			existing, e = q.CreateWorkflowRun(ctx, db.CreateWorkflowRunParams{ID: pgtype.UUID{Bytes: uuid.New(), Valid: true}, WorkspaceID: ws, WorkflowID: w.ID, RevisionID: r.ID, DefinitionHash: r.DefinitionHash, PrincipalUserID: tr.PrincipalUserID, TriggerID: tr.ID, Source: "schedule", PlannedAt: tr.NextRunAt, Input: tr.InputTemplate, Status: state, ReasonCode: pgtype.Text{String: reason, Valid: reason != ""}, OriginatorSource: pgtype.Text{String: "trigger_owner", Valid: true}})
		}
		if e != nil {
			return nil, e
		}
		if _, e = q.AdvanceWorkflowTriggerCAS(ctx, db.AdvanceWorkflowTriggerCASParams{ID: tr.ID, WorkspaceID: ws, NextRunAt: pgtype.Timestamptz{Time: next, Valid: true}, LastFiredAt: tr.NextRunAt, NextRunAt_2: tr.NextRunAt}); e != nil {
			return nil, e
		}
		runs = append(runs, existing)
	}
	if e = tx.Commit(ctx); e != nil {
		return nil, e
	}
	return runs, nil
}

// SubmitRun accepts only a previously committed local intent, never user code.
// A lost response leaves the row submitting; discovery precedes every retry.
func (s *WorkflowService) SubmitRun(ctx context.Context, intent RunIntent) (db.WorkflowRun, error) {
	r, e := s.Queries.GetWorkflowRunInWorkspace(ctx, db.GetWorkflowRunInWorkspaceParams{ID: intent.RunID, WorkspaceID: intent.WorkspaceID})
	if e != nil {
		return r, e
	}
	if r.Status != "queued" && r.Status != "submitting" {
		return r, nil
	}
	if r.CancelRequestedAt.Valid {
		if r.Status == "queued" && !r.SubmissionStartedAt.Valid {
			result, e := s.Queries.CancelUnsubmittedWorkflowRun(ctx, db.CancelUnsubmittedWorkflowRunParams{ID: r.ID, WorkspaceID: r.WorkspaceID})
			if errors.Is(e, pgx.ErrNoRows) {
				return r, nil
			}
			return result, e
		}
		return r, nil
	}
	rev, e := s.Queries.GetWorkflowRevisionInWorkspace(ctx, db.GetWorkflowRevisionInWorkspaceParams{ID: r.RevisionID, WorkspaceID: r.WorkspaceID})
	if e != nil {
		return r, e
	}
	if s.Engine == nil || !s.runnable(ctx, r.WorkspaceID, rev.ID) {
		return r, nil
	}
	if r.Status == "queued" {
		now, e := s.Queries.WorkflowDatabaseTime(ctx)
		if e != nil {
			return r, e
		}
		r, e = s.Queries.BeginWorkflowRunSubmission(ctx, db.BeginWorkflowRunSubmissionParams{ID: r.ID, WorkspaceID: r.WorkspaceID, SubmissionStartedAt: now, SubmissionDeadline: pgtype.Timestamptz{Time: now.Time.Add(workflowSubmissionWindow), Valid: true}})
		if errors.Is(e, pgx.ErrNoRows) {
			return s.Queries.GetWorkflowRunInWorkspace(ctx, db.GetWorkflowRunInWorkspaceParams{ID: intent.RunID, WorkspaceID: intent.WorkspaceID})
		}
		if e != nil {
			return r, e
		}
	} else {
		requestCtx, cancel := context.WithTimeout(ctx, workflowRequestTimeout)
		engineID, e := s.Engine.Discover(requestCtx, r)
		cancel()
		if e != nil {
			return r, nil
		}
		if engineID != "" {
			return s.acknowledgeEngineRun(ctx, r, engineID)
		}
	}
	// Start the network budget before claiming the lease: a suspended process
	// cannot awaken after expiry and initiate a fresh request with a new budget.
	requestCtx, cancel := context.WithTimeout(ctx, workflowRequestTimeout)
	defer cancel()
	claimed, e := s.Queries.ClaimWorkflowRunSubmissionAttempt(requestCtx, db.ClaimWorkflowRunSubmissionAttemptParams{ID: r.ID, WorkspaceID: r.WorkspaceID})
	if errors.Is(e, pgx.ErrNoRows) {
		return r, nil
	}
	if e != nil {
		return r, e
	}
	engineID, e := s.Engine.Submit(requestCtx, claimed, rev)
	cancel()
	if e != nil || engineID == "" {
		return claimed, nil
	}
	parsed, e := uuid.Parse(engineID)
	if e != nil {
		return claimed, workflowError("workflow_invalid_engine_response")
	}
	result, e := s.Queries.AcknowledgeWorkflowSubmissionAttempt(ctx, db.AcknowledgeWorkflowSubmissionAttemptParams{ID: claimed.ID, WorkspaceID: claimed.WorkspaceID, HatchetRunID: pgtype.UUID{Bytes: parsed, Valid: true}, SubmissionAttempts: claimed.SubmissionAttempts})
	if errors.Is(e, pgx.ErrNoRows) {
		return s.Queries.GetWorkflowRunInWorkspace(ctx, db.GetWorkflowRunInWorkspaceParams{ID: claimed.ID, WorkspaceID: claimed.WorkspaceID})
	}
	return result, e
}

func (s *WorkflowService) acknowledgeEngineRun(ctx context.Context, r db.WorkflowRun, id string) (db.WorkflowRun, error) {
	parsed, e := uuid.Parse(id)
	if e != nil {
		return r, workflowError("workflow_invalid_engine_response")
	}
	result, e := s.Queries.AcknowledgeWorkflowRun(ctx, db.AcknowledgeWorkflowRunParams{ID: r.ID, WorkspaceID: r.WorkspaceID, HatchetRunID: pgtype.UUID{Bytes: parsed, Valid: true}})
	if errors.Is(e, pgx.ErrNoRows) {
		return s.Queries.GetWorkflowRunInWorkspace(ctx, db.GetWorkflowRunInWorkspaceParams{ID: r.ID, WorkspaceID: r.WorkspaceID})
	}
	return result, e
}

func (s *WorkflowService) ReconcileRuns(ctx context.Context, ws pgtype.UUID, limit int32) error {
	if limit < 1 || limit > WorkflowMaxPageSize {
		limit = WorkflowMaxPageSize
	}
	// Advance a durable oldest-attempted marker before external requests. Crashes,
	// unavailable engines and long-lived runs yield their turn to later rows.
	runs, e := s.Queries.ClaimWorkflowRunsToReconcile(ctx, db.ClaimWorkflowRunsToReconcileParams{WorkspaceID: ws, Limit: limit})
	if e != nil {
		return e
	}
	for _, r := range runs {
		if e = ctx.Err(); e != nil {
			return e
		}
		if r.Status == "queued" {
			if _, e = s.SubmitRun(ctx, RunIntent{WorkspaceID: ws, RunID: r.ID}); e != nil {
				return e
			}
			continue
		}
		if s.Engine == nil || s.Ready == nil || !s.Ready() {
			continue
		}
		if !r.HatchetRunID.Valid {
			requestCtx, cancel := context.WithTimeout(ctx, workflowRequestTimeout)
			id, err := s.Engine.Discover(requestCtx, r)
			cancel()
			if err != nil {
				continue
			}
			if id == "" {
				if !r.CancelRequestedAt.Valid {
					if _, e = s.SubmitRun(ctx, RunIntent{WorkspaceID: ws, RunID: r.ID}); e != nil {
						return e
					}
				}
				continue
			}
			r, e = s.acknowledgeEngineRun(ctx, r, id)
			if e != nil {
				return e
			}
		}
		if r.Status != "running" {
			continue
		}
		requestCtx, cancel := context.WithTimeout(ctx, workflowRequestTimeout)
		if r.CancelRequestedAt.Valid {
			_ = s.Engine.Cancel(requestCtx, r)
		}
		state, err := s.Engine.Inspect(requestCtx, r)
		cancel()
		if err != nil {
			continue
		}
		logs, _ := json.Marshal(SanitizeWorkflowLogs(state.Logs))
		steps := sanitizeWorkflowSteps(state.Steps)
		if _, e = s.Queries.ProjectWorkflowRun(ctx, db.ProjectWorkflowRunParams{ID: r.ID, WorkspaceID: ws, LogProjection: logs, StepProjection: steps}); e != nil && !errors.Is(e, pgx.ErrNoRows) {
			return e
		}
		terminal := map[string]string{"COMPLETED": "completed", "FAILED": "failed", "CANCELLED": "cancelled"}[state.Status]
		if terminal == "" {
			continue
		}
		if _, e = s.Queries.FinishWorkflowRunCAS(ctx, db.FinishWorkflowRunCASParams{ID: r.ID, WorkspaceID: ws, Status: terminal, Status_2: "running", Output: SanitizeWorkflowInput(state.Output)}); e != nil && !errors.Is(e, pgx.ErrNoRows) {
			return e
		}
	}
	return nil
}

// WorkflowRunStep is the public product projection; IDs are authored graph
// IDs, never engine task/step UUIDs or action names.
type WorkflowRunStep struct {
	ID         string     `json:"id"`
	Status     string     `json:"status"`
	Attempt    int32      `json:"attempt"`
	StartedAt  *time.Time `json:"started_at,omitempty"`
	FinishedAt *time.Time `json:"finished_at,omitempty"`
}

// WorkflowRunSteps also bounds historical stored projections at read time.
func WorkflowRunSteps(raw json.RawMessage) []WorkflowRunStep {
	if len(raw) > WorkflowMaxGraphBytes {
		return []WorkflowRunStep{}
	}
	var steps []WorkflowRunStep
	if json.Unmarshal(raw, &steps) != nil || len(steps) > WorkflowMaxSteps {
		return []WorkflowRunStep{}
	}
	for i := range steps {
		if len(steps[i].ID) > 128 || steps[i].ID == "" {
			return []WorkflowRunStep{}
		}
		steps[i].ID = workflowLogSecrets.ReplaceAllString(steps[i].ID, "[redacted]")
		if steps[i].Attempt < 0 {
			steps[i].Attempt = 0
		}
		switch steps[i].Status {
		case "QUEUED", "RUNNING", "COMPLETED", "FAILED", "CANCELLED":
		default:
			steps[i].Status = "QUEUED"
		}
	}
	if steps == nil {
		return []WorkflowRunStep{}
	}
	return steps
}

func sanitizeWorkflowSteps(raw json.RawMessage) []byte {
	b, _ := json.Marshal(WorkflowRunSteps(raw))
	return b
}

// ReadWorkflowLogs reads the bounded persisted projection during an outage.
func (s *WorkflowService) ReadWorkflowLogs(ctx context.Context, r db.WorkflowRun, limit, offset int32) ([]WorkflowLogEntry, error) {
	var entries []WorkflowLogEntry
	if json.Unmarshal(r.LogProjection, &entries) != nil {
		return []WorkflowLogEntry{}, nil
	}
	entries = SanitizeWorkflowLogs(entries)
	if offset < 0 {
		offset = 0
	}
	if int(offset) >= len(entries) {
		return []WorkflowLogEntry{}, nil
	}
	if limit < 1 || limit > WorkflowMaxLogEntries {
		limit = WorkflowMaxLogEntries
	}
	end := int(offset + limit)
	if end > len(entries) {
		end = len(entries)
	}
	return entries[offset:end], nil
}
