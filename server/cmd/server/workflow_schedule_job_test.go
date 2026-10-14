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

func TestWorkflowScheduleDepartureAfterCommittedIntentSkipsBeforeSubmission(t *testing.T) {
	s, w, _ := workflowRuntimeFixture(t)
	ctx := context.Background()
	engine := &workflowEngineFixture{accepted: map[string]string{}, status: "RUNNING"}
	s.Engine = engine
	runs, e := s.ClaimScheduledRuns(ctx, w.WorkspaceID, time.Now(), 100)
	if e != nil || len(runs) != 1 || runs[0].Status != "queued" {
		t.Fatalf("committed intent: %+v %v", runs, e)
	}
	if _, e = testPool.Exec(ctx, "DELETE FROM member WHERE workspace_id=$1 AND user_id=$2", w.WorkspaceID, runs[0].PrincipalUserID); e != nil {
		t.Fatal(e)
	}
	if e = s.ReconcileRuns(ctx, w.WorkspaceID, 100); e != nil {
		t.Fatal(e)
	}
	run, e := s.Queries.GetWorkflowRunInWorkspace(ctx, db.GetWorkflowRunInWorkspaceParams{ID: runs[0].ID, WorkspaceID: w.WorkspaceID})
	if e != nil || run.Status != "skipped" || run.ReasonCode.String != "workflow_principal_departed" || run.SubmissionStartedAt.Valid || engine.submissions != 0 || engine.discoveries != 0 {
		t.Fatalf("departed principal reached engine: %+v submissions=%d discoveries=%d %v", run, engine.submissions, engine.discoveries, e)
	}
}

func TestWorkflowScheduleDepartureDuringSubmissionClaimSkipsAtomically(t *testing.T) {
	s, w, _ := workflowRuntimeFixture(t)
	ctx := context.Background()
	engine := &workflowEngineFixture{accepted: map[string]string{}, status: "RUNNING"}
	s.Engine = engine
	runs, e := s.ClaimScheduledRuns(ctx, w.WorkspaceID, time.Now(), 100)
	if e != nil || len(runs) != 1 {
		t.Fatal(e)
	}
	removed := false
	eligibleCalled := false
	s.Ready = func() bool {
		if !removed {
			removed = true
			if _, removeErr := testPool.Exec(ctx, "DELETE FROM member WHERE workspace_id=$1 AND user_id=$2", w.WorkspaceID, runs[0].PrincipalUserID); removeErr != nil {
				t.Fatal(removeErr)
			}
		}
		return true
	}
	s.Eligible = func(context.Context, pgtype.UUID, pgtype.UUID) bool { eligibleCalled = true; return true }
	run, e := s.SubmitRun(ctx, service.RunIntent{WorkspaceID: w.WorkspaceID, RunID: runs[0].ID})
	if e != nil || !removed || run.Status != "skipped" || run.ReasonCode.String != "workflow_principal_departed" || run.SubmissionStartedAt.Valid || engine.submissions != 0 || engine.discoveries != 0 {
		t.Fatalf("principal left between precheck and atomic submission claim: %+v submissions=%d discoveries=%d eligible_called=%v %v", run, engine.submissions, engine.discoveries, eligibleCalled, e)
	}
}

func TestWorkflowScheduleDepartureAfterBeginBeforeFirstAttemptSkipsWithoutEngineContact(t *testing.T) {
	for _, restart := range []bool{false, true} {
		name := "direct_submit"
		if restart {
			name = "restarted_reconciler"
		}
		t.Run(name, func(t *testing.T) {
			s, w, _ := workflowRuntimeFixture(t)
			ctx := context.Background()
			engine := &workflowEngineFixture{accepted: map[string]string{}, status: "RUNNING"}
			s.Engine = engine
			runs, e := s.ClaimScheduledRuns(ctx, w.WorkspaceID, time.Now(), 100)
			if e != nil || len(runs) != 1 {
				t.Fatalf("committed intent: %+v %v", runs, e)
			}
			now, e := s.Queries.WorkflowDatabaseTime(ctx)
			if e != nil {
				t.Fatal(e)
			}
			started, e := s.Queries.BeginWorkflowRunSubmission(ctx, db.BeginWorkflowRunSubmissionParams{ID: runs[0].ID, WorkspaceID: w.WorkspaceID, SubmissionStartedAt: now, SubmissionDeadline: pgtype.Timestamptz{Time: now.Time.Add(55 * time.Minute), Valid: true}})
			if e != nil || started.Status != "submitting" || started.SubmissionAttempts != 0 {
				t.Fatalf("durable pre-attempt gap: %+v %v", started, e)
			}
			if _, e = testPool.Exec(ctx, "DELETE FROM member WHERE workspace_id=$1 AND user_id=$2", w.WorkspaceID, started.PrincipalUserID); e != nil {
				t.Fatal(e)
			}
			if restart {
				replica := service.NewWorkflowService(s.Queries, testPool)
				replica.Ready, replica.Eligible, replica.Engine = s.Ready, s.Eligible, engine
				e = replica.ReconcileRuns(ctx, w.WorkspaceID, 100)
			} else {
				_, e = s.SubmitRun(ctx, service.RunIntent{WorkspaceID: w.WorkspaceID, RunID: started.ID})
			}
			if e != nil {
				t.Fatal(e)
			}
			run, e := s.Queries.GetWorkflowRunInWorkspace(ctx, db.GetWorkflowRunInWorkspaceParams{ID: started.ID, WorkspaceID: w.WorkspaceID})
			if e != nil || run.Status != "skipped" || run.ReasonCode.String != "workflow_principal_departed" || run.SubmissionAttempts != 0 || !run.SubmissionStartedAt.Time.Equal(started.SubmissionStartedAt.Time) || !run.SubmissionDeadline.Time.Equal(started.SubmissionDeadline.Time) || engine.discoveries != 0 || engine.submissions != 0 {
				t.Fatalf("post-Begin departed intent reached orchestration or lost immutable window: status=%s reason=%s attempts=%d discoveries=%d submissions=%d %v", run.Status, run.ReasonCode.String, run.SubmissionAttempts, engine.discoveries, engine.submissions, e)
			}
		})
	}
}

func TestWorkflowScheduleDepartureDuringReadinessAfterBeginSkipsWithoutDiscovery(t *testing.T) {
	for _, restart := range []bool{false, true} {
		name := "direct_submit"
		if restart {
			name = "restarted_reconciler"
		}
		t.Run(name, func(t *testing.T) {
			s, w, _ := workflowRuntimeFixture(t)
			ctx := context.Background()
			engine := &workflowEngineFixture{accepted: map[string]string{}, status: "RUNNING"}
			s.Engine = engine
			runs, e := s.ClaimScheduledRuns(ctx, w.WorkspaceID, time.Now(), 100)
			if e != nil || len(runs) != 1 {
				t.Fatalf("committed intent: %+v %v", runs, e)
			}
			now, e := s.Queries.WorkflowDatabaseTime(ctx)
			if e != nil {
				t.Fatal(e)
			}
			started, e := s.Queries.BeginWorkflowRunSubmission(ctx, db.BeginWorkflowRunSubmissionParams{ID: runs[0].ID, WorkspaceID: w.WorkspaceID, SubmissionStartedAt: now, SubmissionDeadline: pgtype.Timestamptz{Time: now.Time.Add(55 * time.Minute), Valid: true}})
			if e != nil || started.Status != "submitting" || started.SubmissionAttempts != 0 {
				t.Fatalf("durable pre-attempt gap: %+v %v", started, e)
			}
			removed := false
			ready := func() bool {
				if !removed {
					removed = true
					if _, removeErr := testPool.Exec(ctx, "DELETE FROM member WHERE workspace_id=$1 AND user_id=$2", w.WorkspaceID, started.PrincipalUserID); removeErr != nil {
						t.Fatal(removeErr)
					}
				}
				return true
			}
			if restart {
				replica := service.NewWorkflowService(s.Queries, testPool)
				replica.Ready, replica.Eligible, replica.Engine = ready, s.Eligible, engine
				e = replica.ReconcileRuns(ctx, w.WorkspaceID, 100)
			} else {
				s.Ready = ready
				_, e = s.SubmitRun(ctx, service.RunIntent{WorkspaceID: w.WorkspaceID, RunID: started.ID})
			}
			if e != nil {
				t.Fatal(e)
			}
			run, e := s.Queries.GetWorkflowRunInWorkspace(ctx, db.GetWorkflowRunInWorkspaceParams{ID: started.ID, WorkspaceID: w.WorkspaceID})
			if e != nil || !removed || run.Status != "skipped" || run.ReasonCode.String != "workflow_principal_departed" || run.SubmissionAttempts != 0 || !run.SubmissionStartedAt.Time.Equal(started.SubmissionStartedAt.Time) || !run.SubmissionDeadline.Time.Equal(started.SubmissionDeadline.Time) || engine.discoveries != 0 || engine.submissions != 0 {
				t.Fatalf("departure during Ready contacted engine or lost immutable window: removed=%v status=%s reason=%s attempts=%d discoveries=%d submissions=%d %v", removed, run.Status, run.ReasonCode.String, run.SubmissionAttempts, engine.discoveries, engine.submissions, e)
			}
		})
	}
}

func TestWorkflowScheduleDefersUnpublishedWithoutPoisoningPublishedTrigger(t *testing.T) {
	s, w, tr := workflowRuntimeFixture(t)
	ctx := context.Background()
	a := service.WorkflowActor{HumanID: w.CreatedByID, AuditID: w.CreatedByID, AuditType: "member"}
	unpublished, e := s.Create(ctx, w.WorkspaceID, a, "pending", "pending", "")
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		_, _ = testPool.Exec(context.Background(), "DELETE FROM workflow_run WHERE workflow_id=$1", unpublished.ID)
		_, _ = testPool.Exec(context.Background(), "DELETE FROM workflow_trigger WHERE workflow_id=$1", unpublished.ID)
		_, _ = testPool.Exec(context.Background(), "DELETE FROM workflow_revision WHERE workflow_id=$1", unpublished.ID)
		_, _ = testPool.Exec(context.Background(), "DELETE FROM workflow WHERE id=$1", unpublished.ID)
	})
	deferred, e := s.Queries.CreateWorkflowTrigger(ctx, db.CreateWorkflowTriggerParams{WorkspaceID: w.WorkspaceID, WorkflowID: unpublished.ID, Kind: "schedule", PrincipalUserID: a.HumanID, Enabled: true, CronExpression: tr.CronExpression, Timezone: tr.Timezone, NextRunAt: pgtype.Timestamptz{Time: tr.NextRunAt.Time.Add(-time.Minute), Valid: true}, InputTemplate: []byte(`{}`), CreatedByType: "member", CreatedByID: a.HumanID})
	if e != nil {
		t.Fatal(e)
	}
	runs, e := s.ClaimScheduledRuns(ctx, w.WorkspaceID, time.Now(), 100)
	if e != nil || len(runs) != 1 || runs[0].WorkflowID != w.ID {
		t.Fatalf("unpublished schedule poisoned valid delivery: rows=%d %v", len(runs), e)
	}
	after, e := s.Queries.GetWorkflowTriggerInWorkspace(ctx, db.GetWorkflowTriggerInWorkspaceParams{ID: deferred.ID, WorkspaceID: w.WorkspaceID})
	if e != nil || after.LastFiredAt.Valid || after.NextRunAt != deferred.NextRunAt {
		t.Fatal("unpublished schedule advanced without revision")
	}
	history, e := s.Queries.ListWorkflowRuns(ctx, db.ListWorkflowRunsParams{WorkflowID: unpublished.ID, WorkspaceID: w.WorkspaceID, Limit: 100})
	if e != nil || len(history) != 0 {
		t.Fatal("unpublished schedule invented a snapshot")
	}
	// The saved overdue occurrence is eligible after publication. Its original
	// planned time remains the deduplication key and no earlier run is invented.
	publication, e := s.PublishRevision(ctx, w.WorkspaceID, unpublished.ID, a, 0, "export default pending", json.RawMessage(`{"steps":[{"id":"a"}]}`))
	if e != nil {
		t.Fatal(e)
	}
	if _, e = s.AcknowledgeRevision(ctx, w.WorkspaceID, publication.Revision.ID, publication.Revision.DefinitionHash, true); e != nil {
		t.Fatal(e)
	}
	runs, e = s.ClaimScheduledRuns(ctx, w.WorkspaceID, time.Now(), 100)
	if e != nil || len(runs) != 1 || runs[0].WorkflowID != unpublished.ID || runs[0].RevisionID != publication.Revision.ID || !runs[0].PlannedAt.Time.Equal(deferred.NextRunAt.Time) {
		t.Fatalf("overdue occurrence after publication: rows=%+v %v", runs, e)
	}
	after, e = s.Queries.GetWorkflowTriggerInWorkspace(ctx, db.GetWorkflowTriggerInWorkspaceParams{ID: deferred.ID, WorkspaceID: w.WorkspaceID})
	if e != nil || !after.LastFiredAt.Time.Equal(deferred.NextRunAt.Time) || !after.NextRunAt.Time.After(time.Now()) {
		t.Fatalf("published schedule did not advance: %+v %v", after, e)
	}
}
