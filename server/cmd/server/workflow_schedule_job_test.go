package main

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/multica-ai/multica/server/internal/service"
	"github.com/multica-ai/multica/server/internal/testutil"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

func workflowRuntimeFixture(t *testing.T) (*service.WorkflowService, db.Workflow, db.WorkflowTrigger) {
	t.Helper()
	ctx := context.Background()
	fx := testutil.New(testPool, "", "")
	human := fx.Insert(t, "user", testutil.Cols{"name": "workflow scheduler", "email": uuid.NewString() + "@test.invalid"})
	ws := fx.Insert(t, "workspace", testutil.Cols{"name": "workflow scheduler", "slug": uuid.NewString()})
	fx.Insert(t, "member", testutil.Cols{"workspace_id": ws, "user_id": human, "role": "member"})
	s := service.NewWorkflowService(db.New(testPool), testPool)
	s.Ready = func() bool { return true }
	s.Eligible = func(context.Context, pgtype.UUID, pgtype.UUID) bool { return true }
	a := service.WorkflowActor{HumanID: parseUUID(human), AuditID: parseUUID(human), AuditType: "member"}
	w, e := s.Create(ctx, parseUUID(ws), a, "dispatch", "dispatch", "")
	if e != nil {
		t.Fatal(e)
	}
	fx.Cleanup(t, "DELETE FROM workflow WHERE id=$1", w.ID)
	fx.Cleanup(t, "DELETE FROM workflow_revision WHERE workflow_id=$1", w.ID)
	fx.Cleanup(t, "DELETE FROM workflow_trigger WHERE workflow_id=$1", w.ID)
	fx.Cleanup(t, "DELETE FROM workflow_run WHERE workflow_id=$1", w.ID)
	rev, e := s.PublishRevision(ctx, w.WorkspaceID, w.ID, a, 0, "export default fixture", json.RawMessage(`{"steps":[{"id":"a"}]}`))
	if e != nil {
		t.Fatal(e)
	}
	if _, e = s.AcknowledgeRevision(ctx, w.WorkspaceID, rev.Revision.ID, rev.Revision.DefinitionHash, true); e != nil {
		t.Fatal(e)
	}
	w, e = s.Load(ctx, w.WorkspaceID, w.ID)
	if e != nil {
		t.Fatal(e)
	}
	tr, e := s.Queries.CreateWorkflowTrigger(ctx, db.CreateWorkflowTriggerParams{WorkspaceID: w.WorkspaceID, WorkflowID: w.ID, Kind: "schedule", PrincipalUserID: a.HumanID, Enabled: true, CronExpression: pgtype.Text{String: "* * * * *", Valid: true}, Timezone: "UTC", NextRunAt: pgtype.Timestamptz{Time: time.Now().Add(-time.Minute).Truncate(time.Minute), Valid: true}, InputTemplate: []byte(`{"x":1}`), CreatedByType: "member", CreatedByID: a.HumanID})
	if e != nil {
		t.Fatal(e)
	}
	return s, w, tr
}

// Removing the transactional advance/insert permits duplicate delivery after lease theft.
func TestWorkflowScheduleDuplicateDeliveryAndDepartedPrincipal(t *testing.T) {
	for _, departed := range []bool{false, true} {
		t.Run(map[bool]string{false: "duplicate", true: "departed"}[departed], func(t *testing.T) {
			s, w, tr := workflowRuntimeFixture(t)
			ctx := context.Background()
			if departed {
				if _, e := testPool.Exec(ctx, "DELETE FROM member WHERE workspace_id=$1 AND user_id=$2", w.WorkspaceID, tr.PrincipalUserID); e != nil {
					t.Fatal(e)
				}
			}
			var wg sync.WaitGroup
			errs := make(chan error, 2)
			for i := 0; i < 2; i++ {
				wg.Add(1)
				go func() { defer wg.Done(); _, e := s.ClaimScheduledRuns(ctx, w.WorkspaceID, time.Now(), 100); errs <- e }()
			}
			wg.Wait()
			close(errs)
			for e := range errs {
				if e != nil {
					t.Fatal(e)
				}
			}
			runs, e := s.Queries.ListWorkflowRuns(ctx, db.ListWorkflowRunsParams{WorkflowID: w.ID, WorkspaceID: w.WorkspaceID, Limit: 100})
			if e != nil || len(runs) != 1 {
				t.Fatalf("exactly one committed run: %d %v", len(runs), e)
			}
			r := runs[0]
			if r.RevisionID != w.CurrentRevisionID || r.PrincipalUserID != tr.PrincipalUserID || !r.PlannedAt.Time.Equal(tr.NextRunAt.Time) {
				t.Fatal("admission snapshot changed")
			}
			if departed && (r.Status != "skipped" || r.ReasonCode.String != "workflow_principal_departed") {
				t.Fatalf("departed principal must be visible: %+v", r)
			}
			advanced, e := s.Queries.GetWorkflowTriggerInWorkspace(ctx, db.GetWorkflowTriggerInWorkspaceParams{ID: tr.ID, WorkspaceID: w.WorkspaceID})
			if e != nil || !advanced.NextRunAt.Time.After(time.Now()) {
				t.Fatalf("schedule advance: %+v %v", advanced, e)
			}
		})
	}
}

func TestWorkflowScheduleDisabledAndPaused(t *testing.T) {
	for _, kind := range []string{"paused_trigger", "archived_workflow", "disabled_engine", "no_daemon"} {
		t.Run(kind, func(t *testing.T) {
			s, w, tr := workflowRuntimeFixture(t)
			ctx := context.Background()
			switch kind {
			case "paused_trigger":
				_, _ = testPool.Exec(ctx, "UPDATE workflow_trigger SET enabled=false WHERE id=$1", tr.ID)
			case "archived_workflow":
				_, _ = testPool.Exec(ctx, "UPDATE workflow SET status='archived' WHERE id=$1", w.ID)
			case "disabled_engine":
				s.Ready = func() bool { return false }
			case "no_daemon":
				s.Eligible = nil
			}
			_, e := s.ClaimScheduledRuns(ctx, w.WorkspaceID, time.Now(), 100)
			if e != nil {
				t.Fatal(e)
			}
			runs, e := s.Queries.ListWorkflowRuns(ctx, db.ListWorkflowRunsParams{WorkflowID: w.ID, WorkspaceID: w.WorkspaceID, Limit: 100})
			if e != nil {
				t.Fatal(e)
			}
			if kind == "paused_trigger" || kind == "archived_workflow" {
				if len(runs) != 0 {
					t.Fatal("inactive schedule fired")
				}
			} else if len(runs) != 1 || runs[0].Status != "skipped" {
				t.Fatal("unavailable schedule not visible")
			}
		})
	}
}

func TestWorkflowScheduleManualRequiresWorkspaceEligibility(t *testing.T) {
	s, w, _ := workflowRuntimeFixture(t)
	s.Eligible = nil
	run, e := s.AdmitRun(context.Background(), w, service.WorkflowActor{HumanID: w.CreatedByID, AuditType: "member", AuditID: w.CreatedByID}, "manual", pgtype.UUID{}, json.RawMessage(`{}`))
	if e == nil || run.Status != "skipped" || run.ReasonCode.String != "workflow_engine_unavailable" {
		t.Fatalf("unverified daemon admitted: %+v %v", run, e)
	}
}
