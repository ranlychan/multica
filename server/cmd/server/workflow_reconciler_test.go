package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/multica-ai/multica/server/internal/hatchet"
	"github.com/multica-ai/multica/server/internal/scheduler"
	"github.com/multica-ai/multica/server/internal/service"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

type workflowCredentialFixture struct{ cred hatchet.TenantCredentials }

func workflowHandlerInput(ws pgtype.UUID) scheduler.HandlerInput {
	return scheduler.HandlerInput{Scope: scheduler.Scope{Kind: "workflow_workspace", ID: uuid.UUID(ws.Bytes).String()}}
}
func (c workflowCredentialFixture) Ensure(context.Context, pgtype.UUID) (hatchet.TenantCredentials, error) {
	return c.cred, nil
}

// Real transport plus pinned captured responses catches invented endpoints and
// wrong engine identity extraction that typed fake engines cannot exercise.
func TestWorkflowReconcilePinnedTransportAndJobWiring(t *testing.T) {
	raw, e := os.ReadFile("../../internal/hatchet/testdata/live-responses.json")
	if e != nil {
		t.Fatal(e)
	}
	var captures map[string]struct {
		Body json.RawMessage `json:"body"`
	}
	if e = json.Unmarshal(raw, &captures); e != nil {
		t.Fatal(e)
	}
	s, w, _ := workflowRuntimeFixture(t)
	ctx := context.Background()
	revision, e := s.Queries.GetWorkflowRevisionInWorkspace(ctx, db.GetWorkflowRevisionInWorkspaceParams{ID: w.CurrentRevisionID, WorkspaceID: w.WorkspaceID})
	if e != nil {
		t.Fatal(e)
	}
	action := revision.HatchetName + ":" + revision.HatchetName + "-step-ca978112ca1bbdca"
	detail := bytes.Replace(captures["recovered-run"].Body, []byte("mc-contract-r1:mc-contract-r1"), []byte(action), 1)
	if bytes.Equal(detail, captures["recovered-run"].Body) {
		t.Fatal("pinned detail did not contain the expected action")
	}
	cred := hatchet.TenantCredentials{TenantID: uuid.NewString(), Token: "tenant-only"}
	requests := 0
	api := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		requests++
		if r.Header.Get("Authorization") != "Bearer tenant-only" {
			t.Error("tenant scope missing")
		}
		rw.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == "POST" && strings.HasSuffix(r.URL.Path, "/workflow-runs/trigger"):
			var body struct {
				WorkflowName string `json:"workflowName"`
				Input        struct {
					RunID string         `json:"multica_run_id"`
					Input map[string]int `json:"input"`
				} `json:"input"`
			}
			if json.NewDecoder(r.Body).Decode(&body) != nil || body.WorkflowName != revision.HatchetName || body.Input.RunID == "" || body.Input.Input["x"] != 1 {
				t.Errorf("wrong immutable submission: %+v", body)
			}
			localID, err := uuid.Parse(body.Input.RunID)
			if err != nil {
				t.Error(err)
			} else {
				local, err := s.Queries.GetWorkflowRunInWorkspace(ctx, db.GetWorkflowRunInWorkspaceParams{ID: pgtype.UUID{Bytes: localID, Valid: true}, WorkspaceID: w.WorkspaceID})
				if err != nil || local.Status != "submitting" || local.IdempotencyKey != local.ID {
					t.Errorf("network preceded committed immutable intent: %+v %v", local, err)
				}
			}
			_, _ = rw.Write(captures["submit"].Body)
		case strings.HasSuffix(r.URL.Path, "/status"):
			_, _ = rw.Write([]byte(`"COMPLETED"`))
		case strings.HasSuffix(r.URL.Path, "/logs"):
			_, _ = rw.Write([]byte(`{"rows":[]}`))
		case r.Method == "GET":
			_, _ = rw.Write(detail)
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			rw.WriteHeader(404)
		}
	}))
	defer api.Close()
	client, e := hatchet.NewClient(api.URL, nil)
	if e != nil {
		t.Fatal(e)
	}
	s.Engine = &workflowHatchetEngine{client: client, tenants: workflowCredentialFixture{cred: cred}, queries: s.Queries}
	s.Logs = s
	schedule := workflowScheduleJob(s)
	if _, e = schedule.Handler(ctx, workflowHandlerInput(w.WorkspaceID)); e != nil {
		t.Fatal(e)
	}
	reconcile := workflowReconcilerJob(s)
	if _, e = reconcile.Handler(ctx, workflowHandlerInput(w.WorkspaceID)); e != nil {
		t.Fatal(e)
	}
	if _, e = reconcile.Handler(ctx, workflowHandlerInput(w.WorkspaceID)); e != nil {
		t.Fatal(e)
	}
	runs, e := s.Queries.ListWorkflowRuns(ctx, db.ListWorkflowRunsParams{WorkspaceID: w.WorkspaceID, WorkflowID: w.ID, Limit: 100})
	if e != nil || len(runs) != 1 || runs[0].Status != "completed" || requests < 3 {
		t.Fatalf("production jobs/transport did not complete: %+v %v requests=%d", runs, e, requests)
	}
	var steps []service.WorkflowRunStep
	if e = json.Unmarshal(runs[0].StepProjection, &steps); e != nil || len(steps) != 1 || steps[0].ID != "a" || steps[0].Status != "COMPLETED" || steps[0].Attempt != 2 || steps[0].StartedAt == nil || steps[0].FinishedAt == nil {
		t.Fatalf("pinned detail step did not become a bounded product projection: %+v %v", steps, e)
	}
	if bytes.Contains(runs[0].StepProjection, []byte("taskExternalId")) || bytes.Contains(runs[0].StepProjection, []byte("actionId")) || bytes.Contains(runs[0].StepProjection, []byte("663acdbf-5da2-412c-a095-d8dc814fa7ad")) {
		t.Fatal("raw engine identity leaked into persisted steps")
	}
}

func TestWorkflowReconcileTerminalLogOutagePreservesAndRecoversProjection(t *testing.T) {
	s, w, trigger := workflowRuntimeFixture(t)
	ctx := context.Background()
	runs, e := s.ClaimScheduledRuns(ctx, w.WorkspaceID, time.Now(), 100)
	if e != nil || len(runs) != 1 {
		t.Fatal(e)
	}
	engineID := uuid.NewString()
	if _, e = testPool.Exec(ctx, "UPDATE workflow_run SET status='running',submission_started_at=now(),submission_deadline=now()+interval '55 minutes',hatchet_run_id=$2 WHERE id=$1", runs[0].ID, engineID); e != nil {
		t.Fatal(e)
	}
	var capture map[string]struct {
		Body json.RawMessage `json:"body"`
	}
	raw, e := os.ReadFile("../../internal/hatchet/testdata/live-responses.json")
	if e != nil || json.Unmarshal(raw, &capture) != nil {
		t.Fatal(e)
	}
	revision, e := s.Queries.GetWorkflowRevisionInWorkspace(ctx, db.GetWorkflowRevisionInWorkspaceParams{ID: w.CurrentRevisionID, WorkspaceID: w.WorkspaceID})
	if e != nil {
		t.Fatal(e)
	}
	action := revision.HatchetName + ":" + revision.HatchetName + "-step-ca978112ca1bbdca"
	detail := bytes.Replace(capture["recovered-run"].Body, []byte("mc-contract-r1:mc-contract-r1"), []byte(action), 1)
	var mode atomic.Int32 // 0: running/healthy, 1: terminal/log outage, 2: terminal/recovered, 3: engine outage
	api := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		rw.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "/status"):
			if mode.Load() == 3 {
				rw.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			if mode.Load() == 0 {
				_, _ = rw.Write([]byte(`"RUNNING"`))
			} else {
				_, _ = rw.Write([]byte(`"COMPLETED"`))
			}
		case strings.HasSuffix(r.URL.Path, "/logs"):
			if mode.Load() == 1 {
				rw.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			message := "known-before-outage"
			if mode.Load() == 2 {
				message = "recovered-final-log"
			}
			_, _ = rw.Write([]byte(`{"rows":[{"message":"` + message + `","level":"INFO","createdAt":"2026-09-30T00:00:00Z"}]}`))
		default:
			_, _ = rw.Write(detail)
		}
	}))
	defer api.Close()
	client, e := hatchet.NewClient(api.URL, nil)
	if e != nil {
		t.Fatal(e)
	}
	engine := &workflowHatchetEngine{client: client, tenants: workflowCredentialFixture{cred: hatchet.TenantCredentials{TenantID: uuid.NewString(), Token: "tenant-only"}}, queries: s.Queries}
	s.Engine = engine
	if e = s.ReconcileRuns(ctx, w.WorkspaceID, 100); e != nil {
		t.Fatal(e)
	}
	mode.Store(1)
	if e = s.ReconcileRuns(ctx, w.WorkspaceID, 100); e != nil {
		t.Fatal(e)
	}
	run, e := s.Queries.GetWorkflowRunInWorkspace(ctx, db.GetWorkflowRunInWorkspaceParams{ID: runs[0].ID, WorkspaceID: w.WorkspaceID})
	if e != nil || run.Status != "running" || !bytes.Contains(run.LogProjection, []byte("known-before-outage")) {
		t.Fatalf("failed terminal log read erased prior logs or finalized early: status=%s logs=%s %v", run.Status, run.LogProjection, e)
	}
	restarted := service.NewWorkflowService(s.Queries, testPool)
	restarted.Ready, restarted.Eligible, restarted.Engine = s.Ready, s.Eligible, engine
	mode.Store(2)
	if e = restarted.ReconcileRuns(ctx, w.WorkspaceID, 100); e != nil {
		t.Fatal(e)
	}
	run, e = s.Queries.GetWorkflowRunInWorkspace(ctx, db.GetWorkflowRunInWorkspaceParams{ID: runs[0].ID, WorkspaceID: w.WorkspaceID})
	if e != nil || run.Status != "completed" || !bytes.Contains(run.LogProjection, []byte("recovered-final-log")) || run.ReasonCode.Valid {
		t.Fatalf("final logs did not recover before immutable terminal transition: status=%s logs=%s reason=%s %v", run.Status, run.LogProjection, run.ReasonCode.String, e)
	}
	// A permanently unavailable log endpoint gets a finite retry budget. The
	// run still records the observed terminal result and flags incomplete logs.
	if _, e = testPool.Exec(ctx, "UPDATE workflow_trigger SET next_run_at=now()-interval '2 minutes' WHERE id=$1", trigger.ID); e != nil {
		t.Fatal(e)
	}
	runs, e = s.ClaimScheduledRuns(ctx, w.WorkspaceID, time.Now(), 100)
	if e != nil || len(runs) != 1 {
		t.Fatalf("second committed run: %+v %v", runs, e)
	}
	if _, e = testPool.Exec(ctx, "UPDATE workflow_run SET status='running',submission_started_at=now(),submission_deadline=now()+interval '55 minutes',hatchet_run_id=$2 WHERE id=$1", runs[0].ID, uuid.NewString()); e != nil {
		t.Fatal(e)
	}
	mode.Store(0)
	if e = s.ReconcileRuns(ctx, w.WorkspaceID, 100); e != nil {
		t.Fatal(e)
	}
	mode.Store(1)
	for attempt := 1; attempt <= 3; attempt++ {
		if attempt > 1 {
			mode.Store(3)
		}
		replica := service.NewWorkflowService(s.Queries, testPool)
		replica.Ready, replica.Eligible, replica.Engine = s.Ready, s.Eligible, engine
		if e = replica.ReconcileRuns(ctx, w.WorkspaceID, 100); e != nil {
			t.Fatal(e)
		}
		run, e = s.Queries.GetWorkflowRunInWorkspace(ctx, db.GetWorkflowRunInWorkspaceParams{ID: runs[0].ID, WorkspaceID: w.WorkspaceID})
		wantStatus := "running"
		if attempt == 3 {
			wantStatus = "completed"
		}
		if e != nil || run.Status != wantStatus || run.FinalProjectionAttempts != int32(attempt) || !bytes.Contains(run.LogProjection, []byte("known-before-outage")) {
			t.Fatalf("attempt %d lost known logs or terminal state: status=%s logs=%s retries=%d %v", attempt, run.Status, run.LogProjection, run.FinalProjectionAttempts, e)
		}
		if attempt == 1 {
			var output map[string]any
			if json.Unmarshal(run.PendingTerminalOutput, &output) != nil || run.PendingTerminalStatus.String != "completed" || !run.PendingTerminalStatus.Valid || output["revision"] != "r1" {
				t.Fatalf("terminal result was not durably staged before restart: status=%+v output=%s", run.PendingTerminalStatus, run.PendingTerminalOutput)
			}
		}
	}
	var finalOutput map[string]any
	if json.Unmarshal(run.Output, &finalOutput) != nil || run.ReasonCode.String != "workflow_final_logs_incomplete" || !run.ReasonCode.Valid || finalOutput["revision"] != "r1" {
		t.Fatalf("exhausted final logs not visible or output unsafe: reason=%+v output=%s", run.ReasonCode, run.Output)
	}
	if _, e = testPool.Exec(ctx, "UPDATE workflow_run SET log_projection='[]' WHERE id=$1", run.ID); e == nil {
		t.Fatal("terminal log projection changed after finalization")
	}
}

func TestWorkflowReconcileOverlappingOldRunningPollCannotPublishTerminalLogs(t *testing.T) {
	s, w, _ := workflowRuntimeFixture(t)
	ctx := context.Background()
	runs, e := s.ClaimScheduledRuns(ctx, w.WorkspaceID, time.Now(), 100)
	if e != nil || len(runs) != 1 {
		t.Fatal(e)
	}
	if _, e = testPool.Exec(ctx, "UPDATE workflow_run SET status='running',submission_started_at=now(),submission_deadline=now()+interval '55 minutes',hatchet_run_id=$2 WHERE id=$1", runs[0].ID, uuid.NewString()); e != nil {
		t.Fatal(e)
	}
	var capture map[string]struct {
		Body json.RawMessage `json:"body"`
	}
	raw, e := os.ReadFile("../../internal/hatchet/testdata/live-responses.json")
	if e != nil || json.Unmarshal(raw, &capture) != nil {
		t.Fatal(e)
	}
	revision, e := s.Queries.GetWorkflowRevisionInWorkspace(ctx, db.GetWorkflowRevisionInWorkspaceParams{ID: w.CurrentRevisionID, WorkspaceID: w.WorkspaceID})
	if e != nil {
		t.Fatal(e)
	}
	action := revision.HatchetName + ":" + revision.HatchetName + "-step-ca978112ca1bbdca"
	detail := bytes.Replace(capture["recovered-run"].Body, []byte("mc-contract-r1:mc-contract-r1"), []byte(action), 1)
	var mode atomic.Int32 // 0: prior healthy, 1: old running poll blocks, 2: terminal logs fail, 3: final logs recover
	oldPollAtLogs := make(chan struct{})
	releaseOldPoll := make(chan struct{})
	var release sync.Once
	releasePoll := func() { release.Do(func() { close(releaseOldPoll) }) }
	defer releasePoll()
	api := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		rw.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "/status"):
			if mode.Load() < 2 {
				_, _ = rw.Write([]byte(`"RUNNING"`))
			} else {
				_, _ = rw.Write([]byte(`"COMPLETED"`))
			}
		case strings.HasSuffix(r.URL.Path, "/logs"):
			switch mode.Load() {
			case 1:
				close(oldPollAtLogs)
				<-releaseOldPoll
				_, _ = rw.Write([]byte(`{"rows":[]}`))
			case 2:
				rw.WriteHeader(http.StatusServiceUnavailable)
			case 3:
				_, _ = rw.Write([]byte(`{"rows":[{"message":"final-log","level":"INFO","createdAt":"2026-09-30T00:00:00Z"}]}`))
			default:
				_, _ = rw.Write([]byte(`{"rows":[{"message":"known-log","level":"INFO","createdAt":"2026-09-30T00:00:00Z"}]}`))
			}
		default:
			_, _ = rw.Write(detail)
		}
	}))
	defer api.Close()
	client, e := hatchet.NewClient(api.URL, nil)
	if e != nil {
		t.Fatal(e)
	}
	engine := &workflowHatchetEngine{client: client, tenants: workflowCredentialFixture{cred: hatchet.TenantCredentials{TenantID: uuid.NewString(), Token: "tenant-only"}}, queries: s.Queries}
	s.Engine = engine
	if e = s.ReconcileRuns(ctx, w.WorkspaceID, 100); e != nil {
		t.Fatal(e)
	}
	replica := service.NewWorkflowService(s.Queries, testPool)
	replica.Ready, replica.Eligible, replica.Engine = s.Ready, s.Eligible, engine
	mode.Store(1)
	oldPollDone := make(chan error, 1)
	go func() { oldPollDone <- replica.ReconcileRuns(ctx, w.WorkspaceID, 100) }()
	select {
	case <-oldPollAtLogs:
	case <-time.After(5 * time.Second):
		t.Fatal("old running poll did not reach blocked log response")
	}
	mode.Store(2)
	if e = s.ReconcileRuns(ctx, w.WorkspaceID, 100); e != nil {
		t.Fatal(e)
	}
	staged, e := s.Queries.GetWorkflowRunInWorkspace(ctx, db.GetWorkflowRunInWorkspaceParams{ID: runs[0].ID, WorkspaceID: w.WorkspaceID})
	if e != nil || staged.Status != "running" || staged.PendingTerminalStatus.String != "completed" || !bytes.Contains(staged.LogProjection, []byte("known-log")) {
		t.Fatalf("terminal poll did not retain prior logs and stage outcome: status=%s pending=%s logs=%s %v", staged.Status, staged.PendingTerminalStatus.String, staged.LogProjection, e)
	}
	releasePoll()
	if e = <-oldPollDone; e != nil {
		t.Fatal(e)
	}
	staged, e = s.Queries.GetWorkflowRunInWorkspace(ctx, db.GetWorkflowRunInWorkspaceParams{ID: runs[0].ID, WorkspaceID: w.WorkspaceID})
	if e != nil || staged.Status != "running" || staged.PendingTerminalStatus.String != "completed" || !bytes.Contains(staged.LogProjection, []byte("known-log")) || staged.ReasonCode.Valid {
		t.Fatalf("old running poll overwrote terminal recovery: status=%s pending=%s logs=%s reason=%s %v", staged.Status, staged.PendingTerminalStatus.String, staged.LogProjection, staged.ReasonCode.String, e)
	}
	mode.Store(3)
	if e = s.ReconcileRuns(ctx, w.WorkspaceID, 100); e != nil {
		t.Fatal(e)
	}
	final, e := s.Queries.GetWorkflowRunInWorkspace(ctx, db.GetWorkflowRunInWorkspaceParams{ID: runs[0].ID, WorkspaceID: w.WorkspaceID})
	if e != nil || final.Status != "completed" || !bytes.Contains(final.LogProjection, []byte("final-log")) || final.ReasonCode.Valid {
		t.Fatalf("matching terminal poll failed to publish final logs: status=%s logs=%s reason=%s %v", final.Status, final.LogProjection, final.ReasonCode.String, e)
	}
}

type workflowEngineFixture struct {
	mu          sync.Mutex
	accepted    map[string]string
	ambiguous   bool
	unavailable bool
	status      string
	submissions int
	discoveries int
	cancelled   bool
	logs        []service.WorkflowLogEntry
	steps       json.RawMessage
	invisible   bool
}

type delayedWorkflowEngine struct {
	*workflowEngineFixture
	entered chan struct{}
	release chan struct{}
}

type cancellingInspectEngine struct {
	*workflowEngineFixture
	cancel      context.CancelFunc
	inspections int
}

func (e *cancellingInspectEngine) Inspect(ctx context.Context, r db.WorkflowRun) (service.WorkflowEngineState, error) {
	e.inspections++
	if e.inspections == 3 {
		e.cancel()
	}
	return e.workflowEngineFixture.Inspect(ctx, r)
}

func (e *delayedWorkflowEngine) Submit(ctx context.Context, r db.WorkflowRun, v db.WorkflowRevision) (string, error) {
	close(e.entered)
	<-e.release
	return e.workflowEngineFixture.Submit(ctx, r, v)
}

func TestWorkflowReconcileStolenSubmissionLeaseFencesLateResponse(t *testing.T) {
	s, w, _ := workflowRuntimeFixture(t)
	ctx := context.Background()
	engine := &delayedWorkflowEngine{workflowEngineFixture: &workflowEngineFixture{accepted: map[string]string{}, status: "RUNNING"}, entered: make(chan struct{}), release: make(chan struct{})}
	s.Engine = engine
	runs, e := s.ClaimScheduledRuns(ctx, w.WorkspaceID, time.Now(), 100)
	if e != nil || len(runs) != 1 {
		t.Fatal(e)
	}
	r := runs[0]
	done := make(chan error, 1)
	go func() {
		_, e := s.SubmitRun(ctx, service.RunIntent{WorkspaceID: w.WorkspaceID, RunID: r.ID})
		done <- e
	}()
	<-engine.entered
	if _, e = testPool.Exec(ctx, "UPDATE workflow_run SET submission_lease_until=now()-interval '1 second' WHERE id=$1", r.ID); e != nil {
		t.Fatal(e)
	}
	second, e := s.Queries.ClaimWorkflowRunSubmissionAttempt(ctx, db.ClaimWorkflowRunSubmissionAttemptParams{ID: r.ID, WorkspaceID: w.WorkspaceID})
	if e != nil || second.SubmissionAttempts != 2 {
		t.Fatalf("steal: %+v %v", second, e)
	}
	close(engine.release)
	if e = <-done; e != nil {
		t.Fatal(e)
	}
	r, e = s.Queries.GetWorkflowRunInWorkspace(ctx, db.GetWorkflowRunInWorkspaceParams{ID: r.ID, WorkspaceID: w.WorkspaceID})
	if e != nil || r.Status != "submitting" {
		t.Fatalf("stolen owner attached late response: %s %v", r.Status, e)
	}
	if e = s.ReconcileRuns(ctx, w.WorkspaceID, 100); e != nil {
		t.Fatal(e)
	}
	r, _ = s.Queries.GetWorkflowRunInWorkspace(ctx, db.GetWorkflowRunInWorkspaceParams{ID: r.ID, WorkspaceID: w.WorkspaceID})
	if r.Status != "running" {
		t.Fatal("late accepted identity was not discovered")
	}
}

func TestWorkflowReconcileAmbiguousSubmissionDiscoversAfterPrincipalDeparture(t *testing.T) {
	s, w, _ := workflowRuntimeFixture(t)
	ctx := context.Background()
	engine := &workflowEngineFixture{accepted: map[string]string{}, ambiguous: true, status: "RUNNING"}
	s.Engine = engine
	runs, e := s.ClaimScheduledRuns(ctx, w.WorkspaceID, time.Now(), 100)
	if e != nil || len(runs) != 1 {
		t.Fatal(e)
	}
	if _, e = s.SubmitRun(ctx, service.RunIntent{WorkspaceID: w.WorkspaceID, RunID: runs[0].ID}); e != nil {
		t.Fatal(e)
	}
	engine.invisible = true
	if _, e = testPool.Exec(ctx, "UPDATE workflow_run SET submission_lease_until=now()-interval '1 second' WHERE id=$1", runs[0].ID); e != nil {
		t.Fatal(e)
	}
	if _, e = testPool.Exec(ctx, "DELETE FROM member WHERE workspace_id=$1 AND user_id=$2", w.WorkspaceID, runs[0].PrincipalUserID); e != nil {
		t.Fatal(e)
	}
	if e = s.ReconcileRuns(ctx, w.WorkspaceID, 100); e != nil {
		t.Fatal(e)
	}
	run, e := s.Queries.GetWorkflowRunInWorkspace(ctx, db.GetWorkflowRunInWorkspaceParams{ID: runs[0].ID, WorkspaceID: w.WorkspaceID})
	if e != nil || run.Status != "submitting" || engine.submissions != 1 {
		t.Fatalf("departed principal retried or skipped ambiguous submission: status=%s submissions=%d %v", run.Status, engine.submissions, e)
	}
	engine.invisible = false
	if e = s.ReconcileRuns(ctx, w.WorkspaceID, 100); e != nil {
		t.Fatal(e)
	}
	run, e = s.Queries.GetWorkflowRunInWorkspace(ctx, db.GetWorkflowRunInWorkspaceParams{ID: runs[0].ID, WorkspaceID: w.WorkspaceID})
	if e != nil || run.Status != "running" || !run.HatchetRunID.Valid || engine.submissions != 1 {
		t.Fatalf("accepted ambiguous submission was lost or repeated after departure: %+v submissions=%d %v", run, engine.submissions, e)
	}
}

func (e *workflowEngineFixture) Submit(ctx context.Context, r db.WorkflowRun, v db.WorkflowRevision) (string, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.submissions++
	if e.unavailable {
		return "", errors.New("offline")
	}
	id := uuid.UUID(r.ID.Bytes).String()
	engine := e.accepted[id]
	if engine == "" {
		engine = uuid.NewString()
		e.accepted[id] = engine
	}
	if e.ambiguous {
		return "", errors.New("response lost")
	}
	return engine, nil
}
func (e *workflowEngineFixture) Discover(ctx context.Context, r db.WorkflowRun) (string, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.discoveries++
	if e.unavailable {
		return "", errors.New("offline")
	}
	if e.invisible {
		return "", nil
	}
	return e.accepted[uuid.UUID(r.ID.Bytes).String()], nil
}
func (e *workflowEngineFixture) Inspect(ctx context.Context, r db.WorkflowRun) (service.WorkflowEngineState, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.unavailable {
		return service.WorkflowEngineState{}, errors.New("offline")
	}
	return service.WorkflowEngineState{Status: e.status, Output: json.RawMessage(`{"token":"secret","answer":42}`), Logs: e.logs, Steps: e.steps}, nil
}
func (e *workflowEngineFixture) Cancel(ctx context.Context, r db.WorkflowRun) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.cancelled = true
	return nil
}

func TestWorkflowReconcileCommitCrashAmbiguousAndTerminalMonotonicity(t *testing.T) {
	s, w, _ := workflowRuntimeFixture(t)
	ctx := context.Background()
	engine := &workflowEngineFixture{accepted: map[string]string{}, ambiguous: true, status: "RUNNING"}
	s.Engine = engine
	runs, err := s.ClaimScheduledRuns(ctx, w.WorkspaceID, time.Now(), 100)
	if err != nil || len(runs) != 1 {
		t.Fatalf("claim %v", err)
	}
	r := runs[0]
	// A restart after ledger commit discovers this row; accepted response loss remains active.
	if e := s.ReconcileRuns(ctx, w.WorkspaceID, 100); e != nil {
		t.Fatal(e)
	}
	r, _ = s.Queries.GetWorkflowRunInWorkspace(ctx, db.GetWorkflowRunInWorkspaceParams{ID: r.ID, WorkspaceID: w.WorkspaceID})
	if r.Status != "submitting" {
		t.Fatalf("lost response marked terminal: %s", r.Status)
	}
	start, deadline := r.SubmissionStartedAt, r.SubmissionDeadline
	if e := s.ReconcileRuns(ctx, w.WorkspaceID, 100); e != nil {
		t.Fatal(e)
	}
	r, _ = s.Queries.GetWorkflowRunInWorkspace(ctx, db.GetWorkflowRunInWorkspaceParams{ID: r.ID, WorkspaceID: w.WorkspaceID})
	if r.Status != "running" || !r.HatchetRunID.Valid || engine.submissions != 1 {
		t.Fatalf("discovery did not recover accepted run: %+v", r)
	}
	if r.SubmissionStartedAt != start || r.SubmissionDeadline != deadline {
		t.Fatal("retry renewed bounded window")
	}
	engine.status = "COMPLETED"
	if e := s.ReconcileRuns(ctx, w.WorkspaceID, 100); e != nil {
		t.Fatal(e)
	}
	r, _ = s.Queries.GetWorkflowRunInWorkspace(ctx, db.GetWorkflowRunInWorkspaceParams{ID: r.ID, WorkspaceID: w.WorkspaceID})
	var productOutput map[string]any
	if json.Unmarshal(r.Output, &productOutput) != nil || r.Status != "completed" || productOutput["token"] != "[redacted]" || productOutput["answer"] != float64(42) {
		t.Fatalf("terminal projection unsafe: %+v", r)
	}
	engine.status = "FAILED"
	if e := s.ReconcileRuns(ctx, w.WorkspaceID, 100); e != nil {
		t.Fatal(e)
	}
	after, _ := s.Queries.GetWorkflowRunInWorkspace(ctx, db.GetWorkflowRunInWorkspaceParams{ID: r.ID, WorkspaceID: w.WorkspaceID})
	if after.Status != "completed" {
		t.Fatal("terminal regressed")
	}
}

func TestWorkflowReconcileRetryBudgetAndConcurrentReplicas(t *testing.T) {
	s, w, _ := workflowRuntimeFixture(t)
	ctx := context.Background()
	engine := &workflowEngineFixture{accepted: map[string]string{}, unavailable: true}
	s.Engine = engine
	runs, e := s.ClaimScheduledRuns(ctx, w.WorkspaceID, time.Now(), 100)
	if e != nil || len(runs) != 1 {
		t.Fatal(e)
	}
	r := runs[0]
	if _, e = s.SubmitRun(ctx, service.RunIntent{WorkspaceID: w.WorkspaceID, RunID: r.ID}); e != nil {
		t.Fatal(e)
	}
	r, _ = s.Queries.GetWorkflowRunInWorkspace(ctx, db.GetWorkflowRunInWorkspaceParams{ID: r.ID, WorkspaceID: w.WorkspaceID})
	start, deadline := r.SubmissionStartedAt, r.SubmissionDeadline
	engine.unavailable = false
	engine.ambiguous = true
	engine.invisible = true
	// Discovery remains eventually absent; bounded idempotent retries may occur.
	for round := 0; round < 6; round++ {
		if _, e = testPool.Exec(ctx, "UPDATE workflow_run SET submission_lease_until=now()-interval '1 second' WHERE id=$1", r.ID); e != nil {
			t.Fatal(e)
		}
		var wg sync.WaitGroup
		errs := make(chan error, 2)
		for i := 0; i < 2; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, e := s.SubmitRun(ctx, service.RunIntent{WorkspaceID: w.WorkspaceID, RunID: r.ID})
				errs <- e
			}()
		}
		wg.Wait()
		close(errs)
		for e := range errs {
			if e != nil {
				t.Fatal(e)
			}
		}
	}
	r, _ = s.Queries.GetWorkflowRunInWorkspace(ctx, db.GetWorkflowRunInWorkspaceParams{ID: r.ID, WorkspaceID: w.WorkspaceID})
	if r.SubmissionAttempts != 5 || engine.submissions != 5 || r.SubmissionStartedAt != start || r.SubmissionDeadline != deadline || r.Status != "submitting" {
		t.Fatalf("retry budget or immutable window broken: attempts=%d calls=%d status=%s", r.SubmissionAttempts, engine.submissions, r.Status)
	}
}

func TestWorkflowReconcileCancellationDuringSubmission(t *testing.T) {
	s, w, _ := workflowRuntimeFixture(t)
	ctx := context.Background()
	engine := &delayedWorkflowEngine{workflowEngineFixture: &workflowEngineFixture{accepted: map[string]string{}, status: "RUNNING"}, entered: make(chan struct{}), release: make(chan struct{})}
	s.Engine = engine
	runs, e := s.ClaimScheduledRuns(ctx, w.WorkspaceID, time.Now(), 100)
	if e != nil || len(runs) != 1 {
		t.Fatal(e)
	}
	r := runs[0]
	done := make(chan error, 1)
	go func() {
		_, e := s.SubmitRun(ctx, service.RunIntent{WorkspaceID: w.WorkspaceID, RunID: r.ID})
		done <- e
	}()
	<-engine.entered
	if _, e = s.Queries.RequestWorkflowRunCancellation(ctx, db.RequestWorkflowRunCancellationParams{ID: r.ID, WorkspaceID: w.WorkspaceID, CancelRequestedByID: r.PrincipalUserID}); e != nil {
		t.Fatal(e)
	}
	close(engine.release)
	if e = <-done; e != nil {
		t.Fatal(e)
	}
	if e = s.ReconcileRuns(ctx, w.WorkspaceID, 100); e != nil {
		t.Fatal(e)
	}
	r, _ = s.Queries.GetWorkflowRunInWorkspace(ctx, db.GetWorkflowRunInWorkspaceParams{ID: r.ID, WorkspaceID: w.WorkspaceID})
	if !engine.cancelled || r.Status != "running" || !r.CancelRequestedAt.Valid {
		t.Fatal("cancel race prematurely terminal or lost engine cancel")
	}
}

func TestWorkflowReconcileProjectionBounds(t *testing.T) {
	s, w, _ := workflowRuntimeFixture(t)
	ctx := context.Background()
	engine := &workflowEngineFixture{accepted: map[string]string{}, status: "RUNNING"}
	s.Engine = engine
	runs, e := s.ClaimScheduledRuns(ctx, w.WorkspaceID, time.Now(), 100)
	if e != nil || len(runs) != 1 {
		t.Fatal(e)
	}
	r := runs[0]
	_, _ = s.SubmitRun(ctx, service.RunIntent{WorkspaceID: w.WorkspaceID, RunID: r.ID})
	r, _ = s.Queries.GetWorkflowRunInWorkspace(ctx, db.GetWorkflowRunInWorkspaceParams{ID: r.ID, WorkspaceID: w.WorkspaceID})
	entries := make([]service.WorkflowLogEntry, 210)
	for i := range entries {
		entries[i] = service.WorkflowLogEntry{Message: "token=wft_secret " + strings.Repeat("x", 4090)}
	}
	// Only bounded product fields are persisted, independent of raw engine JSON.
	engine.logs = entries
	engine.steps = bytes.Repeat([]byte("x"), service.WorkflowMaxGraphBytes+1)
	if e = s.ReconcileRuns(ctx, w.WorkspaceID, 100); e != nil {
		t.Fatal(e)
	}
	r, _ = s.Queries.GetWorkflowRunInWorkspace(ctx, db.GetWorkflowRunInWorkspaceParams{ID: r.ID, WorkspaceID: w.WorkspaceID})
	logs, e := s.ReadWorkflowLogs(ctx, r, 200, 0)
	if e != nil || len(r.LogProjection) > 65536 || len(logs) > 200 || bytes.Contains(r.LogProjection, []byte("wft_secret")) {
		t.Fatalf("projection unsafe bytes=%d entries=%d %v", len(r.LogProjection), len(logs), e)
	}
}

func TestWorkflowReconcileFairProgressBeyondOnePageAcrossRestartAndReplicas(t *testing.T) {
	s, w, _ := workflowRuntimeFixture(t)
	ctx := context.Background()
	engine := &workflowEngineFixture{accepted: map[string]string{}, status: "RUNNING", logs: []service.WorkflowLogEntry{{StepID: "a", Message: "fair-scan", Level: "info"}}}
	s.Engine = engine
	_, e := testPool.Exec(ctx, `WITH ids AS (SELECT gen_random_uuid() id FROM generate_series(1,105))
 INSERT INTO workflow_run(id,workspace_id,workflow_id,revision_id,definition_hash,principal_user_id,source,status,idempotency_key,input,submission_started_at,submission_deadline,hatchet_run_id,triggered_at)
 SELECT id,$1,$2,$3,'hash',$4,'manual','running',id,'{}',now()-interval '1 hour',now()+interval '1 hour',gen_random_uuid(),now()-interval '1 hour' FROM ids`, w.WorkspaceID, w.ID, w.CurrentRevisionID, w.CreatedByID)
	if e != nil {
		t.Fatal(e)
	}
	runs, e := s.ClaimScheduledRuns(ctx, w.WorkspaceID, time.Now(), 100)
	if e != nil || len(runs) != 1 {
		t.Fatal(e)
	}
	later := runs[0]
	if e = s.ReconcileRuns(ctx, w.WorkspaceID, 100); e != nil {
		t.Fatal(e)
	}
	restarted := service.NewWorkflowService(s.Queries, testPool)
	restarted.Ready = s.Ready
	restarted.Eligible = s.Eligible
	restarted.Engine = engine
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for _, replica := range []*service.WorkflowService{s, restarted} {
		wg.Add(1)
		go func(replica *service.WorkflowService) {
			defer wg.Done()
			errs <- replica.ReconcileRuns(ctx, w.WorkspaceID, 100)
		}(replica)
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		if e != nil {
			t.Fatal(e)
		}
	}
	run, e := s.Queries.GetWorkflowRunInWorkspace(ctx, db.GetWorkflowRunInWorkspaceParams{ID: later.ID, WorkspaceID: w.WorkspaceID})
	if e != nil || run.Status != "running" {
		t.Fatalf("later committed intent starved behind old active page: status=%s %v", run.Status, e)
	}
	var reconciled int
	if e = testPool.QueryRow(ctx, "SELECT count(*) FROM workflow_run WHERE workspace_id=$1 AND id<>$2 AND log_projection::text LIKE '%fair-scan%'", w.WorkspaceID, later.ID).Scan(&reconciled); e != nil || reconciled != 105 {
		t.Fatalf("old active runs lost reconciliation progress: %d %v", reconciled, e)
	}
}

func TestWorkflowReconcileMarksOnlyAttemptedRowsAcrossTimeoutRestarts(t *testing.T) {
	s, w, _ := workflowRuntimeFixture(t)
	ctx := context.Background()
	_, e := testPool.Exec(ctx, `WITH ids AS (SELECT gen_random_uuid() id FROM generate_series(1,105))
 INSERT INTO workflow_run(id,workspace_id,workflow_id,revision_id,definition_hash,principal_user_id,source,status,idempotency_key,input,submission_started_at,submission_deadline,hatchet_run_id,triggered_at)
 SELECT id,$1,$2,$3,'hash',$4,'manual','running',id,'{}',now()-interval '1 hour',now()+interval '1 hour',gen_random_uuid(),now()-interval '1 hour' FROM ids`, w.WorkspaceID, w.ID, w.CurrentRevisionID, w.CreatedByID)
	if e != nil {
		t.Fatal(e)
	}
	runs, e := s.ClaimScheduledRuns(ctx, w.WorkspaceID, time.Now(), 100)
	if e != nil || len(runs) != 1 {
		t.Fatal(e)
	}
	later := runs[0]
	cutoff, e := s.Queries.WorkflowDatabaseTime(ctx)
	if e != nil {
		t.Fatal(e)
	}
	for restart := 1; restart <= 3; restart++ {
		attemptCtx, cancel := context.WithCancel(ctx)
		replica := service.NewWorkflowService(s.Queries, testPool)
		replica.Ready, replica.Eligible = s.Ready, s.Eligible
		engine := &cancellingInspectEngine{workflowEngineFixture: &workflowEngineFixture{accepted: map[string]string{}, status: "RUNNING"}, cancel: cancel}
		replica.Engine = engine
		_ = replica.ReconcileRuns(attemptCtx, w.WorkspaceID, 100)
		cancel()
		if engine.inspections != 3 {
			t.Fatalf("restart %d attempted %d rows, want 3", restart, engine.inspections)
		}
		var marked int
		if e = testPool.QueryRow(ctx, "SELECT count(*) FROM workflow_run WHERE workspace_id=$1 AND last_reconciled_at>$2", w.WorkspaceID, cutoff.Time).Scan(&marked); e != nil || marked != restart*3 {
			t.Fatalf("restart %d marked %d rows after %d attempts: %v", restart, marked, restart*3, e)
		}
	}
	engine := &workflowEngineFixture{accepted: map[string]string{}, status: "RUNNING"}
	s.Engine = engine
	if e = s.ReconcileRuns(ctx, w.WorkspaceID, 100); e != nil {
		t.Fatal(e)
	}
	run, e := s.Queries.GetWorkflowRunInWorkspace(ctx, db.GetWorkflowRunInWorkspaceParams{ID: later.ID, WorkspaceID: w.WorkspaceID})
	if e != nil || run.Status != "running" {
		t.Fatalf("later queued intent remained behind unattempted rows: %s %v", run.Status, e)
	}
}

func TestWorkflowCancelAfterBeginBeforeAttempt(t *testing.T) {
	for _, direct := range []bool{false, true} {
		t.Run(map[bool]string{false: "reconcile", true: "submit"}[direct], func(t *testing.T) {
			s, w, _ := workflowRuntimeFixture(t)
			ctx := context.Background()
			engine := &workflowEngineFixture{accepted: map[string]string{}, status: "RUNNING"}
			s.Engine = engine
			runs, err := s.ClaimScheduledRuns(ctx, w.WorkspaceID, time.Now(), 100)
			if err != nil || len(runs) != 1 {
				t.Fatalf("claim: %d %v", len(runs), err)
			}
			now, err := s.Queries.WorkflowDatabaseTime(ctx)
			if err != nil {
				t.Fatal(err)
			}
			begun, err := s.Queries.BeginWorkflowRunSubmission(ctx, db.BeginWorkflowRunSubmissionParams{ID: runs[0].ID, WorkspaceID: w.WorkspaceID, SubmissionStartedAt: now, SubmissionDeadline: pgtype.Timestamptz{Time: now.Time.Add(time.Hour), Valid: true}})
			if err != nil || begun.SubmissionAttempts != 0 || begun.HatchetRunID.Valid {
				t.Fatalf("begin: %+v %v", begun, err)
			}
			cancelled, err := s.Cancel(ctx, w, service.WorkflowActor{HumanID: begun.PrincipalUserID}, begun.ID)
			if err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 2; i++ {
				if direct {
					_, err = s.SubmitRun(ctx, service.RunIntent{RunID: begun.ID, WorkspaceID: w.WorkspaceID})
				} else {
					err = s.ReconcileRuns(ctx, w.WorkspaceID, 100)
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			got, err := s.Queries.GetWorkflowRunInWorkspace(ctx, db.GetWorkflowRunInWorkspaceParams{ID: begun.ID, WorkspaceID: w.WorkspaceID})
			if err != nil || got.Status != "cancelled" || !got.FinishedAt.Valid || got.SubmissionAttempts != 0 || got.HatchetRunID.Valid || got.CancelRequestedByID != cancelled.CancelRequestedByID || !got.CancelRequestedAt.Time.Equal(cancelled.CancelRequestedAt.Time) || !got.SubmissionStartedAt.Time.Equal(begun.SubmissionStartedAt.Time) || !got.SubmissionDeadline.Time.Equal(begun.SubmissionDeadline.Time) {
				t.Fatalf("zero-attempt cancellation failed or changed admission evidence: status=%s attempts=%d err=%v", got.Status, got.SubmissionAttempts, err)
			}
			if engine.submissions != 0 || engine.discoveries != 0 || engine.cancelled {
				t.Fatal("unsubmitted cancellation contacted engine")
			}
		})
	}
}

func TestWorkflowReconcileOutageExpiryAndCancellation(t *testing.T) {
	for _, kind := range []string{"outage", "expired", "cancel_queued", "cancel_ambiguous"} {
		t.Run(kind, func(t *testing.T) {
			s, w, _ := workflowRuntimeFixture(t)
			ctx := context.Background()
			engine := &workflowEngineFixture{accepted: map[string]string{}, ambiguous: true, status: "RUNNING"}
			s.Engine = engine
			runs, e := s.ClaimScheduledRuns(ctx, w.WorkspaceID, time.Now(), 100)
			if e != nil || len(runs) != 1 {
				t.Fatalf("expected one committed run, got %d: %v", len(runs), e)
			}
			r := runs[0]
			if kind == "expired" {
				_, e = testPool.Exec(ctx, "UPDATE workflow_run SET status='submitting',submission_started_at=now()-interval '2 hours',submission_deadline=now()-interval '1 hour' WHERE id=$1", r.ID)
				if e != nil {
					t.Fatal(e)
				}
			}
			if kind == "cancel_ambiguous" {
				if e = s.ReconcileRuns(ctx, w.WorkspaceID, 100); e != nil {
					t.Fatal(e)
				}
			}
			if kind == "cancel_queued" || kind == "cancel_ambiguous" {
				_, e = s.Queries.RequestWorkflowRunCancellation(ctx, db.RequestWorkflowRunCancellationParams{ID: r.ID, WorkspaceID: w.WorkspaceID, CancelRequestedByID: r.PrincipalUserID})
				if e != nil {
					t.Fatal(e)
				}
			}
			if kind == "outage" {
				engine.unavailable = true
			}
			if e = s.ReconcileRuns(ctx, w.WorkspaceID, 100); e != nil {
				t.Fatal(e)
			}
			r, _ = s.Queries.GetWorkflowRunInWorkspace(ctx, db.GetWorkflowRunInWorkspaceParams{ID: r.ID, WorkspaceID: w.WorkspaceID})
			switch kind {
			case "outage":
				if r.Status != "submitting" {
					t.Fatal("outage failed run")
				}
			case "expired":
				if engine.submissions != 0 || r.Status != "submitting" {
					t.Fatal("expired window resubmitted")
				}
			case "cancel_queued":
				if r.Status != "cancelled" || engine.submissions != 0 {
					t.Fatal("pre-submit cancellation sent work")
				}
			case "cancel_ambiguous":
				if !engine.cancelled || r.Status != "running" {
					t.Fatal("late cancel not reconciled")
				}
				engine.status = "CANCELLED"
				_ = s.ReconcileRuns(ctx, w.WorkspaceID, 100)
				r, _ = s.Queries.GetWorkflowRunInWorkspace(ctx, db.GetWorkflowRunInWorkspaceParams{ID: r.ID, WorkspaceID: w.WorkspaceID})
				if r.Status != "cancelled" {
					t.Fatal("confirmed cancellation not terminal")
				}
			}
		})
	}
}
