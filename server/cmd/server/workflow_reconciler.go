package main

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/multica-ai/multica/server/internal/hatchet"
	"github.com/multica-ai/multica/server/internal/scheduler"
	"github.com/multica-ai/multica/server/internal/service"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
	"strings"
	"time"
)

type workflowTenantProvider interface {
	Ensure(context.Context, pgtype.UUID) (hatchet.TenantCredentials, error)
}
type workflowHatchetEngine struct {
	client  *hatchet.Client
	tenants workflowTenantProvider
	queries *db.Queries
}

// Task5 injects Eligible only after checking workspace worker capability and
// immutable registration. Engine health alone never grants execution authority.
func wireWorkflowRuntime(s *service.WorkflowService, supervisor *hatchet.Supervisor, tenants *hatchet.TenantService, apiURL string) error {
	s.Ready = supervisor.Ready
	s.Logs = s
	if tenants == nil {
		return nil
	}
	client, e := hatchet.NewClient(apiURL, nil)
	if e != nil {
		return e
	}
	s.Engine = &workflowHatchetEngine{client: client, tenants: tenants, queries: s.Queries}
	return nil
}

func (e *workflowHatchetEngine) Submit(ctx context.Context, r db.WorkflowRun, v db.WorkflowRevision) (string, error) {
	cred, err := e.tenants.Ensure(ctx, r.WorkspaceID)
	if err != nil {
		return "", err
	}
	raw, err := e.client.Submit(ctx, cred, hatchet.SubmitRequest{RunID: uuid.UUID(r.ID.Bytes).String(), WorkflowName: v.HatchetName, Input: r.Input})
	if err != nil {
		return "", err
	}
	var response struct {
		Run struct {
			Metadata struct {
				ID string `json:"id"`
			} `json:"metadata"`
		} `json:"run"`
	}
	if json.Unmarshal(raw, &response) != nil {
		return "", errors.New("workflow_invalid_engine_response")
	}
	if _, err = uuid.Parse(response.Run.Metadata.ID); err != nil {
		return "", errors.New("workflow_invalid_engine_response")
	}
	return response.Run.Metadata.ID, nil
}

func (e *workflowHatchetEngine) Discover(ctx context.Context, r db.WorkflowRun) (string, error) {
	cred, err := e.tenants.Ensure(ctx, r.WorkspaceID)
	if err != nil {
		return "", err
	}
	localID := uuid.UUID(r.ID.Bytes).String()
	since := r.TriggeredAt.Time.Add(-time.Minute)
	if r.SubmissionStartedAt.Valid {
		since = r.SubmissionStartedAt.Time.Add(-time.Minute)
	}
	raw, err := e.client.Reconcile(ctx, cred, localID, since)
	if err != nil {
		return "", err
	}
	var response struct {
		Rows []struct {
			ID     string `json:"workflowRunExternalId"`
			Key    string `json:"idempotencyKey"`
			Tenant string `json:"tenantId"`
		} `json:"rows"`
	}
	if json.Unmarshal(raw, &response) != nil {
		return "", errors.New("workflow_invalid_engine_response")
	}
	id := ""
	for _, row := range response.Rows {
		if row.Key != localID || row.Tenant != cred.TenantID {
			continue
		}
		if _, err = uuid.Parse(row.ID); err != nil {
			return "", errors.New("workflow_invalid_engine_response")
		}
		if id != "" && id != row.ID {
			return "", errors.New("workflow_engine_identity_conflict")
		}
		id = row.ID
	}
	return id, nil
}

func (e *workflowHatchetEngine) Inspect(ctx context.Context, r db.WorkflowRun) (service.WorkflowEngineState, error) {
	state := service.WorkflowEngineState{}
	cred, err := e.tenants.Ensure(ctx, r.WorkspaceID)
	if err != nil {
		return state, err
	}
	id := uuid.UUID(r.HatchetRunID.Bytes).String()
	state.Status, err = e.client.Status(ctx, cred, id)
	if err != nil {
		return state, err
	}
	raw, err := e.client.Detail(ctx, cred, id)
	if err != nil {
		return state, err
	}
	var detail struct {
		Run struct {
			Output json.RawMessage `json:"output"`
		} `json:"run"`
		Tasks []struct {
			StepID     string     `json:"stepId"`
			Status     string     `json:"status"`
			TaskID     string     `json:"taskExternalId"`
			ActionID   string     `json:"actionId"`
			Attempt    int32      `json:"attempt"`
			StartedAt  *time.Time `json:"startedAt"`
			FinishedAt *time.Time `json:"finishedAt"`
		} `json:"tasks"`
	}
	if json.Unmarshal(raw, &detail) != nil || len(detail.Tasks) > service.WorkflowMaxSteps {
		return state, errors.New("workflow_invalid_engine_response")
	}
	state.Output = detail.Run.Output
	revision, err := e.queries.GetWorkflowRevisionInWorkspace(ctx, db.GetWorkflowRevisionInWorkspaceParams{ID: r.RevisionID, WorkspaceID: r.WorkspaceID})
	if err != nil {
		return state, err
	}
	var graph service.WorkflowGraph
	if json.Unmarshal(revision.DefinitionGraph, &graph) != nil {
		return state, errors.New("workflow_invalid_graph")
	}
	actions := map[string]string{}
	for _, step := range graph.Steps {
		actions[revision.HatchetName+":"+service.WorkflowActionName(revision, step.ID)] = step.ID
	}
	steps := []service.WorkflowRunStep{}
	for _, task := range detail.Tasks {
		stepID, ok := actions[task.ActionID]
		if !ok {
			continue
		}
		steps = append(steps, service.WorkflowRunStep{ID: stepID, Status: task.Status, Attempt: task.Attempt, StartedAt: task.StartedAt, FinishedAt: task.FinishedAt})
		if len(state.Logs) >= service.WorkflowMaxLogEntries {
			continue
		}
		logsRaw, logErr := e.client.Logs(ctx, cred, task.TaskID, service.WorkflowMaxLogEntries)
		if logErr != nil {
			continue
		}
		var logs struct {
			Rows []struct {
				Message   string    `json:"message"`
				Level     string    `json:"level"`
				CreatedAt time.Time `json:"createdAt"`
			} `json:"rows"`
		}
		if json.Unmarshal(logsRaw, &logs) != nil {
			continue
		}
		for _, log := range logs.Rows {
			state.Logs = append(state.Logs, service.WorkflowLogEntry{StepID: stepID, Level: strings.ToLower(log.Level), Message: log.Message, CreatedAt: log.CreatedAt})
			if len(state.Logs) >= service.WorkflowMaxLogEntries {
				break
			}
		}
	}
	state.Logs = service.SanitizeWorkflowLogs(state.Logs)
	state.Steps, _ = json.Marshal(steps)
	return state, nil
}

func (e *workflowHatchetEngine) Cancel(ctx context.Context, r db.WorkflowRun) error {
	cred, err := e.tenants.Ensure(ctx, r.WorkspaceID)
	if err != nil {
		return err
	}
	return e.client.Cancel(ctx, cred, uuid.UUID(r.HatchetRunID.Bytes).String())
}

func workflowReconcilerJob(s *service.WorkflowService) scheduler.JobSpec {
	return workflowRuntimeJob(s, "workflow_run_reconcile", func(ctx context.Context, in scheduler.HandlerInput) (scheduler.HandlerResult, error) {
		ws, e := uuid.Parse(in.Scope.ID)
		if e != nil {
			return scheduler.HandlerResult{}, e
		}
		return scheduler.HandlerResult{}, s.ReconcileRuns(ctx, pgtype.UUID{Bytes: ws, Valid: true}, service.WorkflowMaxPageSize)
	})
}
