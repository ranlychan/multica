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
			_, _ = rw.Write(captures["recovered-run"].Body)
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
}

type workflowEngineFixture struct {
	mu          sync.Mutex
	accepted    map[string]string
	ambiguous   bool
	unavailable bool
	status      string
	submissions int
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
