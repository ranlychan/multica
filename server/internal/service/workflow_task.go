package service

import (
	"context"
	"github.com/jackc/pgx/v5/pgtype"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
	"github.com/multica-ai/multica/server/pkg/protocol"
	"reflect"
)

// WorkflowTaskEnqueue carries a prepared integration overlay into the shared
// task producer. Resource grants/readiness belong to the caller's transaction.
type WorkflowTaskEnqueue struct {
	Queries        *db.Queries
	RunID          pgtype.UUID
	Principal      pgtype.UUID
	AgentID        pgtype.UUID
	RuntimeID      pgtype.UUID
	AutopilotRunID pgtype.UUID
	overlay        runtimeMCPOverlayData
	agent          db.Agent
}

// Preparation may perform integration I/O, and must precede application locks.
func (s *TaskService) PrepareWorkflowTask(ctx context.Context, run, principal pgtype.UUID, agent db.Agent) WorkflowTaskEnqueue {
	return WorkflowTaskEnqueue{RunID: run, Principal: principal, AgentID: agent.ID, RuntimeID: agent.RuntimeID, agent: agent, overlay: s.buildRuntimeMCPOverlay(ctx, principal, agent)}
}

// A changed owner or integration configuration cannot reuse an overlay prepared
// from the previous agent snapshot. Retry prepares from the new locked binding.
func (p WorkflowTaskEnqueue) MatchesAgent(agent db.Agent) bool {
	return reflect.DeepEqual(p.agent, agent)
}

func (s *TaskService) EnqueueWorkflowIssueTaskInTx(ctx context.Context, q *db.Queries, issue db.Issue, agent pgtype.UUID, squad pgtype.UUID, prepared WorkflowTaskEnqueue) (db.AgentTaskQueue, error) {
	prepared.Queries = q
	return s.enqueueMentionTaskWithCommentPlan(ctx, issue, agent, pgtype.UUID{}, nil, squad.Valid, squad, false, "", prepared.Principal, pgtype.UUID{}, OriginDerived, prepared)
}

func (s *TaskService) FinalizeWorkflowTask(ctx context.Context, task db.AgentTaskQueue) {
	s.broadcastTaskEvent(ctx, protocol.EventTaskQueued, task)
	s.NotifyTaskEnqueued(ctx, task)
}

// Derived assignment from a workflow issue preserves the original frozen human.
// Autopilot-origin issues keep their own origin and reach workflow ancestry via
// the actual linked Autopilot run instead.
func (s *TaskService) workflowRunForIssue(ctx context.Context, q *db.Queries, issue db.Issue) (db.WorkflowRun, bool) {
	if s == nil || q == nil {
		return db.WorkflowRun{}, false
	}
	var id pgtype.UUID
	if issue.OriginType.String == "workflow" {
		id = issue.OriginID
	}
	if issue.OriginType.String == "autopilot" {
		if apRun, err := q.GetAutopilotRunByIssue(ctx, issue.ID); err == nil {
			id = apRun.WorkflowRunID
		}
	}
	if !id.Valid {
		return db.WorkflowRun{}, false
	}
	run, err := q.GetWorkflowRunInWorkspace(ctx, db.GetWorkflowRunInWorkspaceParams{ID: id, WorkspaceID: issue.WorkspaceID})
	return run, err == nil && run.PrincipalUserID.Valid
}
