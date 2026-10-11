package service

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/multica-ai/multica/server/internal/testutil"
	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

func TestWorkflowGraphRejectsCycleAndPayloadLimit(t *testing.T) {
	if err := ValidateWorkflowGraph(json.RawMessage(`{"steps":[{"id":"a","parents":["b"]},{"id":"b","parents":["a"]}]}`)); err == nil {
		t.Fatal("cycle accepted")
	}
	if err := ValidateWorkflowGraph(json.RawMessage(`{"steps":[{"id":"a"},{"id":"b","parents":["a"]}]}`)); err != nil {
		t.Fatal(err)
	}
	if err := ValidateWorkflowPayload(make([]byte, WorkflowMaxPayloadBytes+1)); err == nil {
		t.Fatal("oversized payload accepted")
	}
}

func TestWorkflowLogsBoundedAndRedacted(t *testing.T) {
	entries := make([]WorkflowLogEntry, WorkflowMaxLogEntries+1)
	for i := range entries {
		entries[i] = WorkflowLogEntry{Message: "Authorization: Bearer secret123 token=wft_secret password=hunter2"}
	}
	safe := SanitizeWorkflowLogs(entries)
	if len(safe) > WorkflowMaxLogEntries {
		t.Fatal("unbounded logs")
	}
	raw, _ := json.Marshal(safe)
	if strings.Contains(string(raw), "secret123") || strings.Contains(string(raw), "hunter2") || strings.Contains(string(raw), "wft_secret") {
		t.Fatal("log secret leaked")
	}
}

func TestWorkflowDeferredPublishCASAndAuthority(t *testing.T) {
	pool := sharedTestPool(t)
	ctx := context.Background()
	fx := testutil.New(pool, "", "")
	human := fx.Insert(t, "user", testutil.Cols{"name": "workflow creator", "email": uuid.NewString() + "@test.invalid"})
	ws := fx.Insert(t, "workspace", testutil.Cols{"name": "workflow test", "slug": uuid.NewString()})
	fx.Insert(t, "member", testutil.Cols{"workspace_id": ws, "user_id": human, "role": "member"})
	wsID, _ := util.ParseUUID(ws)
	humanID, _ := util.ParseUUID(human)
	agentID, _ := util.ParseUUID(uuid.NewString())
	a := WorkflowActor{HumanID: humanID, AuditType: "agent", AuditID: agentID}
	svc := NewWorkflowService(db.New(pool), pool)
	w, e := svc.Create(ctx, wsID, a, "test", "test", "")
	if e != nil {
		t.Fatal(e)
	}
	fx.Cleanup(t, "DELETE FROM workflow WHERE id=$1", w.ID)
	fx.Cleanup(t, "DELETE FROM workflow_revision WHERE workflow_id=$1", w.ID)
	if w.CreatedByID != humanID || w.AuditActorID != agentID {
		t.Fatal("human and agent audit identities conflated")
	}
	first, e := svc.PublishRevision(ctx, wsID, w.ID, a, 0, "export default first", json.RawMessage(`{"steps":[{"id":"a"}]}`))
	if e != nil {
		t.Fatal(e)
	}
	second, e := svc.PublishRevision(ctx, wsID, w.ID, a, 0, "export default second", json.RawMessage(`{"steps":[{"id":"b"}]}`))
	if e != nil {
		t.Fatal(e)
	}
	if first.Revision.HatchetName == second.Revision.HatchetName || WorkflowActionName(first.Revision, "same") == WorkflowActionName(second.Revision, "same") {
		t.Fatal("revision engine names collided")
	}
	current, e := svc.Load(ctx, wsID, w.ID)
	if e != nil || current.CurrentRevisionID.Valid {
		t.Fatal("untrusted source became current")
	}
	// A new service instance acknowledges using only durable original CAS metadata.
	restarted := NewWorkflowService(db.New(pool), pool)
	result, e := restarted.AcknowledgeRevision(ctx, wsID, second.Revision.ID, second.Revision.DefinitionHash, true)
	if e != nil || result.RegistrationState != "registered" {
		t.Fatalf("ack=%+v err=%v", result, e)
	}
	stale, e := restarted.AcknowledgeRevision(ctx, wsID, first.Revision.ID, first.Revision.DefinitionHash, true)
	if e != nil || stale.RegistrationState != "version_conflict" || stale.Revision.ValidationStatus != "invalid" {
		t.Fatalf("stale=%+v err=%v", stale, e)
	}
	current, e = svc.Load(ctx, wsID, w.ID)
	if e != nil || current.CurrentRevisionID != second.Revision.ID {
		t.Fatal("stale ack replaced current")
	}
	t.Run("competing acknowledgements have one winner", func(t *testing.T) {
		third, e := svc.PublishRevision(ctx, wsID, w.ID, a, current.DefinitionVersion, "third", json.RawMessage(`{"steps":[{"id":"a"}]}`))
		if e != nil {
			t.Fatal(e)
		}
		fourth, e := svc.PublishRevision(ctx, wsID, w.ID, a, current.DefinitionVersion, "fourth", json.RawMessage(`{"steps":[{"id":"b"}]}`))
		if e != nil {
			t.Fatal(e)
		}
		results := make(chan WorkflowPublishResult, 2)
		failures := make(chan error, 2)
		var wg sync.WaitGroup
		for _, candidate := range []WorkflowPublishResult{third, fourth} {
			wg.Add(1)
			go func(c WorkflowPublishResult) {
				defer wg.Done()
				result, e := NewWorkflowService(db.New(pool), pool).AcknowledgeRevision(ctx, wsID, c.Revision.ID, c.Revision.DefinitionHash, true)
				if e != nil {
					failures <- e
				} else {
					results <- result
				}
			}(candidate)
		}
		wg.Wait()
		close(results)
		close(failures)
		for err := range failures {
			t.Fatal(err)
		}
		states := map[string]int{}
		for result := range results {
			states[result.RegistrationState]++
		}
		if states["registered"] != 1 || states["version_conflict"] != 1 {
			t.Fatal(states)
		}
	})
	if _, e = svc.PublishRevision(ctx, wsID, w.ID, a, 0, "stale", json.RawMessage(`{"steps":[{"id":"a"}]}`)); e == nil {
		t.Fatal("stale publish accepted")
	}
	outsider := fx.Insert(t, "user", testutil.Cols{"name": "viewer", "email": uuid.NewString() + "@test.invalid"})
	fx.Insert(t, "member", testutil.Cols{"workspace_id": ws, "user_id": outsider, "role": "member"})
	viewer, _ := util.ParseUUID(outsider)
	fx.Insert(t, "workflow_collaborator", testutil.Cols{"workspace_id": ws, "workflow_id": w.ID, "user_id": viewer, "role": "viewer", "created_by_id": humanID})
	if svc.Authorize(ctx, current, WorkflowActor{HumanID: viewer}, false) == nil {
		t.Fatal("viewer may write")
	}
	fx.Exec(t, "UPDATE workflow_collaborator SET role='editor' WHERE workflow_id=$1 AND user_id=$2", w.ID, viewer)
	if e = svc.Authorize(ctx, current, WorkflowActor{HumanID: viewer}, false); e != nil {
		t.Fatal(e)
	}
	if svc.Authorize(ctx, current, WorkflowActor{HumanID: viewer}, true) == nil {
		t.Fatal("editor may grant access")
	}
	run, e := svc.AdmitRun(ctx, current, a, "manual", pgtype.UUID{}, json.RawMessage(`{"secret":"value"}`))
	var unavailable *WorkflowError
	if !errors.As(e, &unavailable) || unavailable.Code != "workflow_engine_unavailable" {
		t.Fatalf("disabled runtime must return unavailable, got %v", e)
	}
	fx.Cleanup(t, "DELETE FROM workflow_run WHERE id=$1", run.ID)
	if run.Status != "skipped" || run.ReasonCode.String != "workflow_engine_unavailable" {
		t.Fatal("disabled engine admission not durable")
	}
	cancelled, e := svc.Cancel(ctx, current, a, run.ID)
	if e != nil || cancelled.Status != "skipped" {
		t.Fatal("terminal cancel not idempotent")
	}
}

func TestWorkflowCancelRetainsReconcilableStateAndLateAcknowledgement(t *testing.T) {
	pool := sharedTestPool(t)
	ctx := context.Background()
	fx := testutil.New(pool, "", "")
	human := fx.Insert(t, "user", testutil.Cols{"name": "cancel human", "email": uuid.NewString() + "@test.invalid"})
	ws := fx.Insert(t, "workspace", testutil.Cols{"name": "cancel test", "slug": uuid.NewString()})
	fx.Insert(t, "member", testutil.Cols{"workspace_id": ws, "user_id": human, "role": "member"})
	wsID, _ := util.ParseUUID(ws)
	humanID, _ := util.ParseUUID(human)
	a := WorkflowActor{HumanID: humanID, AuditType: "member", AuditID: humanID}
	svc := NewWorkflowService(db.New(pool), pool)
	w, e := svc.Create(ctx, wsID, a, "cancel", "cancel", "")
	if e != nil {
		t.Fatal(e)
	}
	fx.Cleanup(t, "DELETE FROM workflow WHERE id=$1", w.ID)
	for _, state := range []string{"queued", "submitting", "running"} {
		t.Run(state, func(t *testing.T) {
			id := uuid.NewString()
			cols := testutil.Cols{"id": id, "workspace_id": ws, "workflow_id": w.ID, "revision_id": uuid.NewString(), "definition_hash": "hash", "principal_user_id": human, "source": "manual", "status": state, "idempotency_key": id, "input": "{}"}
			if state != "queued" {
				cols["submission_started_at"] = time.Now().Add(-time.Minute)
				cols["submission_deadline"] = time.Now().Add(time.Minute)
			}
			if state == "running" {
				cols["hatchet_run_id"] = uuid.NewString()
			}
			fx.Insert(t, "workflow_run", cols)
			runID, _ := util.ParseUUID(id)
			first, e := svc.Cancel(ctx, w, a, runID)
			if e != nil {
				t.Fatal(e)
			}
			if first.Status != state || first.FinishedAt.Valid {
				t.Fatalf("cancel froze reconcilable %s run: %+v", state, first)
			}
			var requested time.Time
			fx.QueryRow(t, "SELECT cancel_requested_at FROM workflow_run WHERE id=$1", runID).Scan(&requested)
			if _, e = svc.Cancel(ctx, w, a, runID); e != nil {
				t.Fatal(e)
			}
			var again time.Time
			fx.QueryRow(t, "SELECT cancel_requested_at FROM workflow_run WHERE id=$1", runID).Scan(&again)
			if !requested.Equal(again) {
				t.Fatal("cancel request was rewritten")
			}
			rows, e := svc.Queries.ListWorkflowRunsToReconcile(ctx, db.ListWorkflowRunsToReconcileParams{WorkspaceID: wsID, Limit: 100})
			if e != nil {
				t.Fatal(e)
			}
			found := false
			for _, row := range rows {
				if row.ID == runID {
					found = true
				}
			}
			if !found {
				t.Fatal("cancel request lost from reconciliation")
			}
			if state == "queued" {
				_, e = svc.Queries.BeginWorkflowRunSubmission(ctx, db.BeginWorkflowRunSubmissionParams{ID: runID, WorkspaceID: wsID, SubmissionStartedAt: pgtype.Timestamptz{Time: time.Now(), Valid: true}, SubmissionDeadline: pgtype.Timestamptz{Time: time.Now().Add(time.Minute), Valid: true}})
				if !errors.Is(e, pgx.ErrNoRows) {
					t.Fatalf("cancelled queue could submit: %v", e)
				}
			}
			if state == "submitting" {
				engineID, _ := util.ParseUUID(uuid.NewString())
				late, e := svc.Queries.AcknowledgeWorkflowRun(ctx, db.AcknowledgeWorkflowRunParams{ID: runID, WorkspaceID: wsID, HatchetRunID: engineID})
				if e != nil || late.Status != "running" || late.HatchetRunID != engineID {
					t.Fatalf("ambiguous submission cannot attach late ID: %+v %v", late, e)
				}
				if !late.CancelRequestedAt.Valid || late.CancelRequestedByID != humanID {
					t.Fatal("late acknowledgment lost cancellation request")
				}
			}
			if _, e = pool.Exec(ctx, "UPDATE workflow_run SET cancel_requested_at=NULL,cancel_requested_by_id=NULL WHERE id=$1", runID); e == nil {
				t.Fatal("durable cancellation request could be cleared")
			}
			active, e := svc.Queries.GetWorkflowRunInWorkspace(ctx, db.GetWorkflowRunInWorkspaceParams{ID: runID, WorkspaceID: wsID})
			if e != nil {
				t.Fatal(e)
			}
			// Represents Task4's confirmed engine outcome (or confirmed no submission
			// for queued runs), distinct from the author's cancellation request.
			terminal, e := svc.Queries.FinishWorkflowRunCAS(ctx, db.FinishWorkflowRunCASParams{ID: runID, WorkspaceID: wsID, Status: "cancelled", Status_2: active.Status})
			if e != nil || !terminal.FinishedAt.Valid || !terminal.CancelRequestedAt.Valid {
				t.Fatalf("confirmed cancellation cannot finalize: %+v %v", terminal, e)
			}
			same, e := svc.Cancel(ctx, w, a, runID)
			if e != nil || same.Status != "cancelled" || !same.FinishedAt.Time.Equal(terminal.FinishedAt.Time) {
				t.Fatal("terminal cancel was rewritten")
			}
		})
	}
}
