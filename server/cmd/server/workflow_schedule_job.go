package main

import (
	"context"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/multica-ai/multica/server/internal/scheduler"
	"github.com/multica-ai/multica/server/internal/service"
	"time"
)

func workflowRuntimeJob(s *service.WorkflowService, name string, handler scheduler.Handler) scheduler.JobSpec {
	return scheduler.JobSpec{Name: name, Cadence: 30 * time.Second, RunTimeout: 2 * time.Minute, StaleTimeout: 3 * time.Minute, HeartbeatInterval: 30 * time.Second, AllowStaleReentry: true, MaxAttempts: 3, RetryBackoff: []time.Duration{time.Minute}, Scopes: func(ctx context.Context, _ time.Time) ([]scheduler.Scope, error) {
		workspaces, e := s.Queries.ListWorkflowRuntimeWorkspaces(ctx)
		if e != nil {
			return nil, e
		}
		out := make([]scheduler.Scope, 0, len(workspaces))
		for _, ws := range workspaces {
			out = append(out, scheduler.Scope{Kind: "workflow_workspace", ID: uuid.UUID(ws.Bytes).String()})
		}
		return out, nil
	}, Handler: handler}
}

func workflowScheduleJob(s *service.WorkflowService) scheduler.JobSpec {
	return workflowRuntimeJob(s, "workflow_schedule_dispatch", func(ctx context.Context, in scheduler.HandlerInput) (scheduler.HandlerResult, error) {
		ws, e := uuid.Parse(in.Scope.ID)
		if e != nil {
			return scheduler.HandlerResult{}, e
		}
		now, e := s.Queries.WorkflowDatabaseTime(ctx)
		if e != nil {
			return scheduler.HandlerResult{}, e
		}
		runs, e := s.ClaimScheduledRuns(ctx, pgtype.UUID{Bytes: ws, Valid: true}, now.Time, service.WorkflowMaxPageSize)
		return scheduler.HandlerResult{RowsAffected: int64(len(runs))}, e
	})
}
