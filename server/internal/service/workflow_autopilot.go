package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/multica-ai/multica/server/internal/events"
	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
	"github.com/multica-ai/multica/server/pkg/dbid"
	"github.com/multica-ai/multica/server/pkg/protocol"
)

type workflowAutopilotDispatch struct {
	tx       pgx.Tx
	prepared WorkflowTaskEnqueue
	tasks    []db.AgentTaskQueue
	after    []func()
	service  *AutopilotService
}

func (s *AutopilotService) WorkflowAgent(ctx context.Context, ap db.Autopilot) (db.Agent, error) {
	if ap.AssigneeType == "squad" {
		if _, err := s.Queries.GetSquadInWorkspace(ctx, db.GetSquadInWorkspaceParams{ID: ap.AssigneeID, WorkspaceID: ap.WorkspaceID}); err != nil {
			return db.Agent{}, err
		}
	}
	agent, _, err := s.resolveAutopilotLeader(ctx, ap)
	if err == nil && agent.WorkspaceID != ap.WorkspaceID {
		return db.Agent{}, workflowError("workflow_forbidden")
	}
	return agent, err
}

// DispatchWorkflowInTx uses the existing admission, quota and downstream
// producers, with full business ownership in the caller's receipt transaction.
// Current writer/invocation grants must already be locked by that caller.
func (s *AutopilotService) DispatchWorkflowInTx(ctx context.Context, tx pgx.Tx, ap db.Autopilot, principal, workflowRun pgtype.UUID, stepID, key string, payload []byte, prepared WorkflowTaskEnqueue) (db.AutopilotRun, func(), error) {
	scoped := *s
	scoped.Queries = s.Queries.WithTx(tx)
	state := &workflowAutopilotDispatch{tx: tx, prepared: prepared, service: s}
	if _, _, skip := scoped.shouldSkipDispatch(ctx, ap, principal, pgtype.UUID{}); skip {
		return db.AutopilotRun{}, nil, workflowError("workflow_unavailable")
	}
	status := "issue_created"
	if ap.ExecutionMode == "run_only" {
		status = "running"
	}
	// The caller key is arbitrary signed author input, not a globally trusted
	// SDK hash. Scope quota admission to the verified local receipt identity.
	identity, _ := json.Marshal([]string{util.UUIDToString(workflowRun), stepID, key})
	digest := sha256.Sum256(identity)
	quotaKey := "workflow:" + hex.EncodeToString(digest[:])
	run, reused, err := scoped.createAutopilotRunWithQuota(ctx, ap.WorkspaceID, "manual", quotaKey, db.CreateAutopilotRunParams{ID: dbid.NewV7(), AutopilotID: ap.ID, Source: "manual", Status: status, TriggerPayload: payload, SquadID: autopilotSquadAttribution(ap), WorkflowRunID: workflowRun, PrincipalUserID: principal}, tx)
	if err != nil {
		return run, nil, err
	}
	if reused {
		// Ordinary replay was already answered by the locked completed receipt.
		// Missing/inconsistent receipt ownership cannot authorize redispatching
		// or replacing downstream links on an existing quota run.
		return run, nil, workflowError("workflow_operation_conflict")
	}
	state.prepared.AutopilotRunID = run.ID
	switch ap.ExecutionMode {
	case "create_issue":
		err = scoped.dispatchCreateIssue(ctx, ap, &run, DefaultAutopilotTriggerTimezone, principal, state)
	case "run_only":
		err = scoped.dispatchRunOnly(ctx, ap, &run, principal, state)
	default:
		err = fmt.Errorf("unknown execution mode")
	}
	if err != nil {
		return run, nil, err
	}
	if err = scoped.Queries.UpdateAutopilotLastRunAt(ctx, ap.ID); err != nil {
		return run, nil, err
	}
	return run, func() {
		for _, after := range state.after {
			after()
		}
		for _, task := range state.tasks {
			s.TaskSvc.FinalizeWorkflowTask(ctx, task)
		}
		s.captureAutopilotRunStarted(ap, run, "manual")
		s.Bus.Publish(events.Event{Type: protocol.EventAutopilotRunStart, WorkspaceID: util.UUIDToString(ap.WorkspaceID), ActorType: "member", ActorID: util.UUIDToString(principal), Payload: map[string]any{"run_id": util.UUIDToString(run.ID), "autopilot_id": util.UUIDToString(ap.ID), "source": "manual", "status": run.Status}})
	}, nil
}
