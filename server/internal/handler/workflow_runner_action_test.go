package handler

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/multica-ai/multica/server/internal/auth"
	"github.com/multica-ai/multica/server/internal/entitlement"
	"github.com/multica-ai/multica/server/internal/entitlement/entitlementtest"
	"github.com/multica-ai/multica/server/internal/featureflags"
	"github.com/multica-ai/multica/server/internal/hatchet"
	"github.com/multica-ai/multica/server/internal/runtimeapps"
	"github.com/multica-ai/multica/server/internal/service"
	"github.com/multica-ai/multica/server/internal/testutil"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
	"github.com/multica-ai/multica/server/pkg/featureflag"
)

type workflowBusinessFixture struct {
	*workflowBoundary
	runID, engineID, taskID string
}

func newWorkflowBusinessFixture(t *testing.T, principals ...string) *workflowBusinessFixture {
	return newWorkflowBusinessFixtureWithSteps(t, []string{"a"}, principals...)
}
func newWorkflowBusinessFixtureWithSteps(t *testing.T, steps []string, principals ...string) *workflowBusinessFixture {
	t.Helper()
	b := newWorkflowBoundaryInWorkspace(t, testWorkspaceID, steps...)
	testutil.Call(t, b.h.WorkflowRunnerAcknowledge, b.request("POST", "ack", b.ready)).Want(200)
	w, err := b.h.WorkflowSvc.Load(context.Background(), parseUUID(b.workspaceID), b.workflowID)
	if err != nil {
		t.Fatal(err)
	}
	principal := testUserID
	if len(principals) > 0 {
		principal = principals[0]
		dbfx.Insert(t, "workflow_collaborator", testutil.Cols{"workspace_id": testWorkspaceID, "workflow_id": b.workflowID, "user_id": principal, "role": "editor", "created_by_id": testUserID})
	}
	run, err := b.h.WorkflowSvc.AdmitRun(context.Background(), w, service.WorkflowActor{HumanID: parseUUID(principal)}, "manual", pgtype.UUID{}, json.RawMessage(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	f := &workflowBusinessFixture{b, uuidToString(run.ID), uuid.NewString(), uuid.NewString()}
	dbfx.Exec(t, "UPDATE workflow_run SET status='running',hatchet_run_id=$2,submission_started_at=now(),submission_deadline=now()+interval '55 minutes' WHERE id=$1", run.ID, f.engineID)
	dbfx.Cleanup(t, "DELETE FROM workflow_operation WHERE run_id=$1", run.ID)
	dbfx.Cleanup(t, "DELETE FROM issue WHERE origin_type='workflow' AND origin_id=$1", run.ID)
	dbfx.Cleanup(t, "DELETE FROM autopilot_run WHERE workflow_run_id=$1", run.ID)
	dbfx.Cleanup(t, "DELETE FROM issue WHERE id IN (SELECT issue_id FROM autopilot_run WHERE workflow_run_id=$1)", run.ID)
	dbfx.Cleanup(t, "DELETE FROM agent_task_queue WHERE workflow_run_id=$1", run.ID)
	b.h.WorkflowControl.Action = b.h.WorkflowBusinessAction
	b.h.WorkflowControl.Assignments = workflowAssignmentCheck(func(context.Context, hatchet.Assignment) error { return nil })
	return f
}

func (f *workflowBusinessFixture) action(operation, key string, payload any) *http.Request {
	return f.actionInStep("a", operation, key, payload)
}
func (f *workflowBusinessFixture) actionInStep(step, operation, key string, payload any) *http.Request {
	raw, _ := json.Marshal(payload)
	path := "/api/workflow-runs/" + f.runID + "/actions"
	body, _ := json.Marshal(WorkflowActionRequest{Protocol: 1, RunID: f.runID, StepID: step, Operation: operation, OperationKey: key, Payload: raw, EngineRunID: f.engineID, EngineTaskID: f.taskID, InvocationCount: 1})
	r := withURLParam(newRequest("POST", path, nil), "id", f.runID)
	r.Body = io.NopCloser(strings.NewReader(string(body)))
	r.Header.Set("Authorization", "Bearer "+f.cred.RunnerToken)
	r.Header.Set("X-Workspace-ID", f.workspaceID)
	r.Header.Set("X-Multica-Workflow-Generation", f.cred.Generation)
	mac := hmac.New(sha256.New, []byte(f.cred.RunnerToken))
	mac.Write([]byte("POST\n" + path + "\n" + f.workspaceID + "\n" + f.cred.Generation + "\n"))
	mac.Write(body)
	r.Header.Set("X-Multica-Workflow-Signature", hex.EncodeToString(mac.Sum(nil)))
	return r
}

// A receipt written separately from issue creation duplicates work after retry;
// these assertions observe actual issue rows and durable receipts.
func TestWorkflowActionIssueAtomicReplayConflictAndConcurrency(t *testing.T) {
	f := newWorkflowBusinessFixture(t)
	payload := map[string]any{"title": "workflow receipt " + uuid.NewString(), "status": "todo"}
	first := testutil.Call(t, f.h.WorkflowRunnerAction, f.action("issues.create", "create-one", payload)).Want(200).Map()
	second := testutil.Call(t, f.h.WorkflowRunnerAction, f.action("issues.create", "create-one", payload)).Want(200).Map()
	if string(workflowMustJSON(first)) != string(workflowMustJSON(second)) {
		t.Fatal("replay changed result", first, second)
	}
	var count int
	dbfx.QueryRow(t, "SELECT count(*) FROM issue WHERE origin_type='workflow' AND origin_id=$1", f.runID).Scan(&count)
	if count != 1 {
		t.Fatalf("replay created %d issues", count)
	}
	changed := map[string]any{"title": "different"}
	testutil.Call(t, f.h.WorkflowRunnerAction, f.action("issues.create", "create-one", changed)).Want(409)
	testutil.Call(t, f.h.WorkflowRunnerAction, f.action("agents.run", "create-one", payload)).Want(409)
	result := first["result"].(map[string]any)
	testutil.Call(t, f.h.WorkflowRunnerAction, f.action("issues.get", "", map[string]any{"id": result["id"]})).Want(200)
	payload["title"] = "concurrent " + uuid.NewString()
	var wg sync.WaitGroup
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			testutil.Call(t, f.h.WorkflowRunnerAction, f.action("issues.create", "concurrent", payload)).Want(200)
		}()
	}
	wg.Wait()
	dbfx.QueryRow(t, "SELECT count(*) FROM issue WHERE origin_type='workflow' AND origin_id=$1", f.runID).Scan(&count)
	if count != 2 {
		t.Fatalf("concurrent replay created %d issues", count)
	}
}

func workflowMustJSON(v any) []byte { raw, _ := json.Marshal(v); return raw }

func workflowAgentFixture(t *testing.T) (string, string) {
	t.Helper()
	runtime := dbfx.Runtime(t, "workflow-business-"+uuid.NewString())
	return dbfx.Agent(t, "workflow-business-"+uuid.NewString(), runtime), runtime
}

func workflowAutopilotFixture(t *testing.T, agent, mode string) string {
	t.Helper()
	return dbfx.Insert(t, "autopilot", testutil.Cols{"workspace_id": testWorkspaceID, "title": "Workflow AP " + uuid.NewString(), "assignee_type": "agent", "assignee_id": agent, "status": "active", "execution_mode": mode, "created_by_type": "member", "created_by_id": testUserID})
}

func TestWorkflowActionAgentAttributionOfflineReadAndGrantReplay(t *testing.T) {
	f := newWorkflowBusinessFixture(t)
	agent, runtime := workflowAgentFixture(t)
	issue := dbfx.Issue(t, "Workflow dispatch "+uuid.NewString())
	payload := map[string]any{"agent_id": agent, "issue_id": issue}
	dbfx.Exec(t, "UPDATE agent_runtime SET status='offline' WHERE id=$1", runtime)
	first := testutil.Call(t, f.h.WorkflowRunnerAction, f.action("agents.run", "dispatch", payload)).Want(200).Map()["result"].(map[string]any)
	var origin, accountable, source, workflow string
	dbfx.QueryRow(t, "SELECT originator_user_id::text,accountable_user_id::text,originator_source,workflow_run_id::text FROM agent_task_queue WHERE id=$1", first["id"]).Scan(&origin, &accountable, &source, &workflow)
	if origin != testUserID || accountable != testUserID || source != "workflow" || workflow != f.runID {
		t.Fatal("wrong frozen workflow attribution", origin, accountable, source, workflow)
	}
	testutil.Call(t, f.h.WorkflowRunnerAction, f.action("agents.get", "", map[string]any{"id": first["id"]})).Want(200)
	other := plainMember(t, "private-workflow-owner")
	dbfx.Exec(t, "UPDATE agent SET owner_id=$2 WHERE id=$1", agent, other)
	testutil.Call(t, f.h.WorkflowRunnerAction, f.action("agents.run", "dispatch", payload)).Want(403)
	testutil.Call(t, f.h.WorkflowRunnerAction, f.action("agents.get", "", map[string]any{"id": first["id"]})).Want(403)
	dbfx.Exec(t, "UPDATE agent SET permission_mode='public_to' WHERE id=$1", agent)
	dbfx.Insert(t, "agent_invocation_target", testutil.Cols{"agent_id": agent, "target_type": "member", "target_id": testUserID, "created_by": other})
	testutil.Call(t, f.h.WorkflowRunnerAction, f.action("agents.run", "dispatch", payload)).Want(200)
	dbfx.Exec(t, "DELETE FROM agent_invocation_target WHERE agent_id=$1", agent)
	testutil.Call(t, f.h.WorkflowRunnerAction, f.action("agents.run", "dispatch", payload)).Want(403)
	denied := testutil.Call(t, f.h.WorkflowRunnerAction, f.action("agents.run", "standalone", map[string]any{"agent_id": agent, "prompt": "do work"})).Want(400).Map()
	if e, ok := denied["error"].(map[string]any); !ok || e["code"] != "workflow_invalid_payload" {
		t.Fatal("private action error contract changed", denied)
	}
}

func TestWorkflowActionAutopilotAtomicAncestryQuotaReplayAndOfflinePolicy(t *testing.T) {
	for _, mode := range []string{"run_only", "create_issue"} {
		t.Run(mode, func(t *testing.T) {
			f := newWorkflowBusinessFixture(t)
			agent, runtime := workflowAgentFixture(t)
			ap := workflowAutopilotFixture(t, agent, mode)
			start := time.Now().UTC().Add(-time.Hour)
			end := start.Add(24 * time.Hour)
			limit := 1
			stub := entitlementtest.New()
			stub.Set(uuid.MustParse(testWorkspaceID), entitlement.GateAutopilotRuns, entitlement.Decision{Gate: entitlement.Gate{Action: entitlement.ActionEnforce, Limit: &limit, PeriodStart: &start, PeriodEnd: &end, ResetAt: &end}, PolicyRevision: 7, SubscriptionVersion: 1})
			svc := *f.h.AutopilotService
			svc.Entitlements = stub
			f.h.AutopilotService = &svc
			dbfx.Cleanup(t, "DELETE FROM autopilot_quota_period WHERE workspace_id=$1 AND period_start=$2", testWorkspaceID, start)
			dbfx.Cleanup(t, "DELETE FROM autopilot_quota_reservation WHERE workspace_id=$1 AND period_start=$2", testWorkspaceID, start)
			payload := map[string]any{"autopilot_id": ap, "payload": map[string]any{"secret": "not returned"}}
			dbfx.Exec(t, "UPDATE agent_runtime SET status='offline' WHERE id=$1", runtime)
			if mode == "run_only" {
				testutil.Call(t, f.h.WorkflowRunnerAction, f.action("autopilots.invoke", "invoke", payload)).Want(503)
				dbfx.Exec(t, "UPDATE agent_runtime SET status='online' WHERE id=$1", runtime)
			}
			first := testutil.Call(t, f.h.WorkflowRunnerAction, f.action("autopilots.invoke", "invoke", payload)).Want(200).Map()["result"].(map[string]any)
			testutil.Call(t, f.h.WorkflowRunnerAction, f.action("autopilots.invoke", "invoke", payload)).Want(200)
			testutil.Call(t, f.h.WorkflowRunnerAction, f.action("autopilots.invoke", "another", payload)).Want(429)
			testutil.Call(t, f.h.WorkflowRunnerAction, f.action("autopilots.get", "", map[string]any{"id": first["id"]})).Want(200)
			if strings.Contains(string(workflowMustJSON(first)), "secret") {
				t.Fatal("unsafe AP result", first)
			}
			var workflow, principal string
			var n int
			dbfx.QueryRow(t, "SELECT workflow_run_id::text,principal_user_id::text FROM autopilot_run WHERE id=$1", first["id"]).Scan(&workflow, &principal)
			if workflow != f.runID || principal != testUserID {
				t.Fatal("AP ancestry lost", workflow, principal)
			}
			if _, err := testPool.Exec(context.Background(), "UPDATE autopilot_run SET principal_user_id=$2 WHERE id=$1", first["id"], uuid.NewString()); err == nil {
				t.Fatal("frozen Autopilot workflow principal was mutable")
			}
			dbfx.QueryRow(t, "SELECT count(*) FROM agent_task_queue WHERE workflow_run_id=$1 AND originator_source='workflow' AND originator_user_id=$2 AND accountable_user_id=$2", f.runID, testUserID).Scan(&n)
			if n != 1 {
				t.Fatalf("AP created %d correctly attributed tasks", n)
			}
			if mode == "create_issue" {
				var origin, originID string
				dbfx.QueryRow(t, "SELECT origin_type,origin_id::text FROM issue WHERE id=$1", first["issue_id"]).Scan(&origin, &originID)
				if origin != "autopilot" || originID != ap {
					t.Fatal("AP issue origin replaced", origin, originID)
				}
			}
			var taskID string
			dbfx.QueryRow(t, "SELECT id::text FROM agent_task_queue WHERE workflow_run_id=$1", f.runID).Scan(&taskID)
			dbfx.Exec(t, "UPDATE agent_task_queue SET status='completed',completed_at=now() WHERE id=$1", taskID)
			task, err := f.h.Queries.GetAgentTask(context.Background(), parseUUID(taskID))
			if err != nil {
				t.Fatal(err)
			}
			if task.TriggerEvidenceKind.String != "autopilot_run" || task.TriggerEvidenceRefID != parseUUID(first["id"].(string)) {
				t.Fatal("AP evidence lost at task insert", task.TriggerEvidenceKind, task.TriggerEvidenceRefID)
			}
			f.h.AutopilotService.SyncRunFromTask(context.Background(), task)
			current, err := f.h.Queries.GetAutopilotRun(context.Background(), parseUUID(first["id"].(string)))
			if err != nil {
				t.Fatal(err)
			}
			if mode == "create_issue" {
				if task.AutopilotRunID.Valid || current.Status != "issue_created" {
					t.Fatal("issue task completion terminalized AP run", current.Status)
				}
				dbfx.Exec(t, "UPDATE issue SET status='done' WHERE id=$1", first["issue_id"])
				issue, err := f.h.Queries.GetIssueInWorkspace(context.Background(), db.GetIssueInWorkspaceParams{ID: parseUUID(first["issue_id"].(string)), WorkspaceID: parseUUID(testWorkspaceID)})
				if err != nil {
					t.Fatal(err)
				}
				f.h.AutopilotService.SyncRunFromIssue(context.Background(), issue)
				current, err = f.h.Queries.GetAutopilotRun(context.Background(), current.ID)
				if err != nil {
					t.Fatal(err)
				}
			}
			if current.Status != "completed" {
				t.Fatal("canonical AP completion consumer failed", mode, current.Status)
			}
			other := plainMember(t, "workflow AP owner")
			dbfx.Exec(t, "UPDATE autopilot SET created_by_id=$2 WHERE id=$1", ap, other)
			// Owner/admin implicitly writes; use a current ordinary member principal.
			dbfx.Exec(t, "UPDATE member SET role='member' WHERE workspace_id=$1 AND user_id=$2", testWorkspaceID, testUserID)
			t.Cleanup(func() {
				dbfx.Exec(t, "UPDATE member SET role='owner' WHERE workspace_id=$1 AND user_id=$2", testWorkspaceID, testUserID)
			})
			testutil.Call(t, f.h.WorkflowRunnerAction, f.action("autopilots.invoke", "invoke", payload)).Want(403)
			testutil.Call(t, f.h.WorkflowRunnerAction, f.action("autopilots.get", "", map[string]any{"id": first["id"]})).Want(403)
		})
	}
}

func TestWorkflowActionReceiptFailureRollsBackAllBusinessRows(t *testing.T) {
	for _, op := range []string{"issues.create", "agents.run", "autopilots.invoke/run_only", "autopilots.invoke/create_issue"} {
		t.Run(op, func(t *testing.T) {
			f := newWorkflowBusinessFixture(t)
			agent, _ := workflowAgentFixture(t)
			payload := map[string]any{"title": "atomic rollback " + uuid.NewString(), "assignee_type": "agent", "assignee_id": agent}
			operation := op
			var quotaStart time.Time
			if op == "agents.run" {
				payload = map[string]any{"agent_id": agent, "issue_id": dbfx.Issue(t, "rollback dispatch "+uuid.NewString())}
			}
			if strings.HasPrefix(op, "autopilots.invoke/") {
				operation = "autopilots.invoke"
				payload = map[string]any{"autopilot_id": workflowAutopilotFixture(t, agent, strings.TrimPrefix(op, "autopilots.invoke/"))}
				quotaStart = time.Now().UTC().Add(-time.Hour).Truncate(time.Microsecond)
				end := quotaStart.Add(24 * time.Hour)
				limit := 1
				stub := entitlementtest.New()
				stub.Set(uuid.MustParse(testWorkspaceID), entitlement.GateAutopilotRuns, entitlement.Decision{Gate: entitlement.Gate{Action: entitlement.ActionEnforce, Limit: &limit, PeriodStart: &quotaStart, PeriodEnd: &end, ResetAt: &end}})
				svc := *f.h.AutopilotService
				svc.Entitlements = stub
				f.h.AutopilotService = &svc
				dbfx.Cleanup(t, "DELETE FROM autopilot_quota_period WHERE workspace_id=$1 AND period_start=$2", testWorkspaceID, quotaStart)
				dbfx.Cleanup(t, "DELETE FROM autopilot_quota_reservation WHERE workspace_id=$1 AND period_start=$2", testWorkspaceID, quotaStart)
			}
			name := "wf_receipt_" + strings.ReplaceAll(uuid.NewString(), "-", "")
			dbfx.Exec(t, fmt.Sprintf(`CREATE FUNCTION %s() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.run_id='%s'::uuid THEN RAISE EXCEPTION 'owned receipt rollback'; END IF; RETURN NEW; END $$`, name, f.runID))
			dbfx.Cleanup(t, "DROP FUNCTION IF EXISTS "+name+"() CASCADE")
			dbfx.Exec(t, "CREATE TRIGGER "+name+" BEFORE UPDATE ON workflow_operation FOR EACH ROW EXECUTE FUNCTION "+name+"()")
			testutil.Call(t, f.h.WorkflowRunnerAction, f.action(operation, "rollback", payload)).Want(503)
			for _, query := range []string{"SELECT count(*) FROM workflow_operation WHERE run_id=$1", "SELECT count(*) FROM agent_task_queue WHERE workflow_run_id=$1", "SELECT count(*) FROM autopilot_run WHERE workflow_run_id=$1", "SELECT count(*) FROM issue WHERE origin_type='workflow' AND origin_id=$1"} {
				var n int
				dbfx.QueryRow(t, query, f.runID).Scan(&n)
				if n != 0 {
					t.Fatal("business/receipt escaped rollback", query, n)
				}
			}
			if !quotaStart.IsZero() {
				for _, table := range []string{"autopilot_quota_period", "autopilot_quota_reservation"} {
					var n int
					dbfx.QueryRow(t, "SELECT count(*) FROM "+table+" WHERE workspace_id=$1 AND period_start=$2", testWorkspaceID, quotaStart).Scan(&n)
					if n != 0 {
						t.Fatal("quota escaped receipt rollback", table, n)
					}
				}
				var n int
				dbfx.QueryRow(t, "SELECT count(*) FROM issue WHERE origin_type='autopilot' AND origin_id=$1", payload["autopilot_id"]).Scan(&n)
				if n != 0 {
					t.Fatal("Autopilot issue escaped rollback", n)
				}
			}
			dbfx.Exec(t, "DROP TRIGGER "+name+" ON workflow_operation")
			testutil.Call(t, f.h.WorkflowRunnerAction, f.action(operation, "rollback", payload)).Want(200)
		})
	}
}

func businessAuthority(t *testing.T, f *workflowBusinessFixture) WorkflowActionAuthority {
	t.Helper()
	ctx := context.Background()
	q := f.h.Queries
	run, err := q.GetWorkflowRunInWorkspace(ctx, db.GetWorkflowRunInWorkspaceParams{ID: parseUUID(f.runID), WorkspaceID: parseUUID(f.workspaceID)})
	if err != nil {
		t.Fatal(err)
	}
	rev, err := q.GetWorkflowRevisionInWorkspace(ctx, db.GetWorkflowRevisionInWorkspaceParams{ID: run.RevisionID, WorkspaceID: run.WorkspaceID})
	if err != nil {
		t.Fatal(err)
	}
	runner, err := q.AuthorizeWorkflowRunner(ctx, db.AuthorizeWorkflowRunnerParams{WorkspaceID: run.WorkspaceID, ID: parseUUID(f.cred.Generation), TokenHash: auth.HashToken(f.cred.RunnerToken)})
	if err != nil {
		t.Fatal(err)
	}
	return WorkflowActionAuthority{Run: run, Revision: rev, Runner: runner, Principal: run.PrincipalUserID}
}

func waitWorkflowBlocked(t *testing.T, ctx context.Context, pid int32) {
	t.Helper()
	for {
		var blocked bool
		if err := testPool.QueryRow(ctx, "SELECT cardinality(pg_blocking_pids($1))>0", pid).Scan(&blocked); err != nil {
			t.Fatal(err)
		}
		if blocked {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("expected business grant lock wait")
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// A revocation already holding the grant row wins over a waiting business
// transaction. A stale preflight permission must never escape this fence.
func TestWorkflowActionConcurrentGrantRevocationWinsBeforeBusiness(t *testing.T) {
	for _, grant := range []string{"workflow", "agent", "autopilot"} {
		t.Run(grant, func(t *testing.T) {
			other := plainMember(t, "workflow grant owner")
			var f *workflowBusinessFixture
			if grant == "workflow" {
				f = newWorkflowBusinessFixture(t, other)
			} else {
				f = newWorkflowBusinessFixture(t)
			}
			agent, _ := workflowAgentFixture(t)
			operation := "issues.create"
			payload := map[string]any{"title": "revoked " + uuid.NewString()}
			var deletion string
			var target any
			switch grant {
			case "workflow":
				deletion = "DELETE FROM workflow_collaborator WHERE workflow_id=$1"
				target = f.workflowID
			case "agent":
				dbfx.Exec(t, "UPDATE agent SET owner_id=$2,permission_mode='public_to' WHERE id=$1", agent, other)
				dbfx.Exec(t, "UPDATE agent_runtime SET visibility='public' WHERE id=(SELECT runtime_id FROM agent WHERE id=$1)", agent)
				dbfx.Insert(t, "agent_invocation_target", testutil.Cols{"agent_id": agent, "target_type": "member", "target_id": testUserID, "created_by": other})
				operation = "agents.run"
				payload = map[string]any{"agent_id": agent, "issue_id": dbfx.Issue(t, "revoke issue "+uuid.NewString())}
				deletion = "DELETE FROM agent_invocation_target WHERE agent_id=$1"
				target = agent
			case "autopilot":
				ap := workflowAutopilotFixture(t, agent, "run_only")
				dbfx.Exec(t, "UPDATE autopilot SET created_by_id=$2 WHERE id=$1", ap, other)
				dbfx.Exec(t, "UPDATE member SET role='member' WHERE workspace_id=$1 AND user_id=$2", testWorkspaceID, testUserID)
				t.Cleanup(func() {
					dbfx.Exec(t, "UPDATE member SET role='owner' WHERE workspace_id=$1 AND user_id=$2", testWorkspaceID, testUserID)
				})
				dbfx.Exec(t, "INSERT INTO autopilot_collaborator(autopilot_id,user_type,user_id,granted_by) VALUES($1,'member',$2,$3)", ap, testUserID, other)
				dbfx.Cleanup(t, "DELETE FROM autopilot_collaborator WHERE autopilot_id=$1", ap)
				operation = "autopilots.invoke"
				payload = map[string]any{"autopilot_id": ap}
				deletion = "DELETE FROM autopilot_collaborator WHERE autopilot_id=$1"
				target = ap
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			a := businessAuthority(t, f)
			blocker, err := testPool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer blocker.Rollback(context.Background())
			if _, err = blocker.Exec(ctx, deletion, target); err != nil {
				t.Fatal(err)
			}
			started := make(chan int32, 1)
			f.h.TxStarter = workflowActionTransactionStarted{txStarter: testPool, started: started}
			raw, _ := json.Marshal(payload)
			done := make(chan error, 1)
			go func() {
				_, e := f.h.WorkflowBusinessAction(ctx, a, WorkflowActionRequest{RunID: f.runID, StepID: "a", Operation: operation, OperationKey: "revoked", Payload: raw})
				done <- e
			}()
			pid := <-started
			waitWorkflowBlocked(t, ctx, pid)
			if err = blocker.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			e := <-done
			status, _ := workflowBusinessFailure(e)
			if status != 403 {
				t.Fatalf("revoked grant accepted: %v", e)
			}
			var n int
			dbfx.QueryRow(t, "SELECT count(*) FROM workflow_operation WHERE run_id=$1", f.runID).Scan(&n)
			if n != 0 {
				t.Fatal("receipt persisted after revocation", n)
			}
		})
	}
}

func TestWorkflowActionBusinessPostLockWallClockExpiry(t *testing.T) {
	f := newWorkflowBusinessFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	a := businessAuthority(t, f)
	var expires time.Time
	if err := testPool.QueryRow(ctx, "UPDATE workflow_runner_token SET lease_until=clock_timestamp()+interval '1500 milliseconds' WHERE id=$1 RETURNING lease_until", f.cred.Generation).Scan(&expires); err != nil {
		t.Fatal(err)
	}
	blocker, err := testPool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Rollback(context.Background())
	if _, err = blocker.Exec(ctx, "SELECT id FROM workflow WHERE id=$1 FOR UPDATE", f.workflowID); err != nil {
		t.Fatal(err)
	}
	started := make(chan int32, 1)
	f.h.TxStarter = workflowActionTransactionStarted{txStarter: testPool, started: started}
	done := make(chan error, 1)
	go func() {
		_, e := f.h.WorkflowBusinessAction(ctx, a, WorkflowActionRequest{RunID: f.runID, StepID: "a", Operation: "issues.create", OperationKey: "expired", Payload: json.RawMessage(`{"title":"must roll back"}`)})
		done <- e
	}()
	pid := <-started
	waitWorkflowBlocked(t, ctx, pid)
	time.Sleep(time.Until(expires) + 30*time.Millisecond)
	if err = blocker.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	e := <-done
	status, _ := workflowBusinessFailure(e)
	if status != 410 {
		t.Fatalf("expired business authority accepted: %v", e)
	}
	var n int
	dbfx.QueryRow(t, "SELECT count(*) FROM issue WHERE origin_type='workflow' AND origin_id=$1", f.runID).Scan(&n)
	if n != 0 {
		t.Fatal("expired action committed issue", n)
	}
}

func TestWorkflowActionNoBusinessAfterWrongEngineSelectorsOrDeparture(t *testing.T) {
	principal := plainMember(t, "departed workflow principal")
	f := newWorkflowBusinessFixture(t, principal)
	payload := map[string]any{"title": "never created"}
	f.h.WorkflowControl.Assignments = workflowAssignmentCheck(func(context.Context, hatchet.Assignment) error { return hatchet.ErrAssignmentDenied })
	testutil.Call(t, f.h.WorkflowRunnerAction, f.action("issues.create", "wrong", payload)).Want(403)
	f.h.WorkflowControl.Assignments = workflowAssignmentCheck(func(context.Context, hatchet.Assignment) error { return nil })
	req := f.action("issues.create", "wrong", payload)
	req = withURLParam(req, "id", uuid.NewString())
	testutil.Call(t, f.h.WorkflowRunnerAction, req).Want(400)
	// A business transaction must also reject a principal removed after the
	// initial assignment proof, using its own fresh membership read.
	a := businessAuthority(t, f)
	dbfx.Exec(t, "DELETE FROM member WHERE workspace_id=$1 AND user_id=$2", testWorkspaceID, principal)
	_, err := f.h.WorkflowBusinessAction(context.Background(), a, WorkflowActionRequest{RunID: f.runID, StepID: "a", Operation: "issues.create", OperationKey: "departed", Payload: json.RawMessage(`{"title":"departed"}`)})
	status, _ := workflowBusinessFailure(err)
	if status != 403 {
		t.Fatal("departed frozen human authorized", err)
	}
	var n int
	dbfx.QueryRow(t, "SELECT count(*) FROM workflow_operation WHERE run_id=$1", f.runID).Scan(&n)
	if n != 0 {
		t.Fatal("denied proof reached business", n)
	}
}

type workflowOverlayCounter struct{ calls atomic.Int32 }

func (c *workflowOverlayCounter) BuildTaskOverlay(context.Context, pgtype.UUID, db.Agent) (runtimeapps.MCPOverlayResult, error) {
	c.calls.Add(1)
	return runtimeapps.MCPOverlayResult{}, nil
}

func TestWorkflowActionReadAndCachedReplayDoNotPrepareIntegrations(t *testing.T) {
	f := newWorkflowBusinessFixture(t)
	agent, _ := workflowAgentFixture(t)
	issue := dbfx.Issue(t, "overlay "+uuid.NewString())
	tasks := service.NewTaskService(f.h.Queries, testPool, nil, f.h.Bus)
	counter := &workflowOverlayCounter{}
	tasks.Composio = counter
	flags := featureflag.NewStaticProvider()
	flags.Set(featureflags.ComposioMCPApps, featureflag.Rule{Default: true})
	tasks.FeatureFlags = featureflag.NewService(flags)
	f.h.TaskService = tasks
	payload := map[string]any{"agent_id": agent, "issue_id": issue}
	first := testutil.Call(t, f.h.WorkflowRunnerAction, f.action("agents.run", "overlay", payload)).Want(200).Map()["result"].(map[string]any)
	if counter.calls.Load() != 1 {
		t.Fatal("new dispatch did not prepare once")
	}
	testutil.Call(t, f.h.WorkflowRunnerAction, f.action("agents.run", "overlay", payload)).Want(200)
	testutil.Call(t, f.h.WorkflowRunnerAction, f.action("agents.get", "", map[string]any{"id": first["id"]})).Want(200)
	if counter.calls.Load() != 1 {
		t.Fatal("read/replay created a new integration session")
	}
	other := plainMember(t, "integration owner")
	dbfx.Exec(t, "UPDATE agent SET owner_id=$2 WHERE id=$1", agent, other)
	testutil.Call(t, f.h.WorkflowRunnerAction, f.action("agents.run", "denied", payload)).Want(403)
	if counter.calls.Load() != 1 {
		t.Fatal("private denial prepared integration credentials")
	}
}

func TestWorkflowActionAutopilotQuotaOffConcurrencyUsesFrozenHuman(t *testing.T) {
	for _, mode := range []string{"run_only", "create_issue"} {
		t.Run(mode, func(t *testing.T) {
			principal := plainMember(t, "workflow frozen invoker")
			f := newWorkflowBusinessFixture(t, principal)
			agent, runtime := workflowAgentFixture(t)
			dbfx.Exec(t, "UPDATE agent SET owner_id=$2 WHERE id=$1", agent, principal)
			dbfx.Exec(t, "UPDATE agent_runtime SET owner_id=$2 WHERE id=$1", runtime, principal)
			ap := workflowAutopilotFixture(t, agent, mode)
			dbfx.Exec(t, "INSERT INTO autopilot_collaborator(autopilot_id,user_type,user_id,granted_by) VALUES($1,'member',$2,$3)", ap, principal, testUserID)
			dbfx.Cleanup(t, "DELETE FROM autopilot_collaborator WHERE autopilot_id=$1", ap)
			payload := map[string]any{"autopilot_id": ap}
			var wg sync.WaitGroup
			for i := 0; i < 6; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					testutil.Call(t, f.h.WorkflowRunnerAction, f.action("autopilots.invoke", "quota-off", payload)).Want(200)
				}()
			}
			wg.Wait()
			var runs, tasks int
			dbfx.QueryRow(t, "SELECT count(*) FROM autopilot_run WHERE workflow_run_id=$1 AND principal_user_id=$2", f.runID, principal).Scan(&runs)
			dbfx.QueryRow(t, "SELECT count(*) FROM agent_task_queue WHERE workflow_run_id=$1 AND originator_user_id=$2 AND accountable_user_id=$2 AND originator_source='workflow'", f.runID, principal).Scan(&tasks)
			if runs != 1 || tasks != 1 {
				t.Fatal("quota-off replay duplicated or borrowed daemon/AP creator", runs, tasks)
			}
		})
	}
}

func TestWorkflowActionDerivedAssignmentKeepsWorkflowHuman(t *testing.T) {
	principal := plainMember(t, "workflow standing principal")
	f := newWorkflowBusinessFixture(t, principal)
	agent, runtime := workflowAgentFixture(t)
	dbfx.Exec(t, "UPDATE agent SET owner_id=$2 WHERE id=$1", agent, principal)
	dbfx.Exec(t, "UPDATE agent_runtime SET owner_id=$2 WHERE id=$1", runtime, principal)
	created := testutil.Call(t, f.h.WorkflowRunnerAction, f.action("issues.create", "derived", map[string]any{"title": "derived " + uuid.NewString()})).Want(200).Map()["result"].(map[string]any)
	dbfx.Exec(t, "UPDATE issue SET assignee_type='agent',assignee_id=$2 WHERE id=$1", created["id"], agent)
	issue, err := f.h.Queries.GetIssueInWorkspace(context.Background(), db.GetIssueInWorkspaceParams{ID: parseUUID(created["id"].(string)), WorkspaceID: parseUUID(testWorkspaceID)})
	if err != nil {
		t.Fatal(err)
	}
	task, err := f.h.TaskService.EnqueueTaskForIssue(context.Background(), issue)
	if err != nil {
		t.Fatal(err)
	}
	if task.WorkflowRunID != parseUUID(f.runID) || task.OriginatorUserID != parseUUID(principal) || task.AccountableUserID != parseUUID(principal) || task.OriginatorSource.String != "workflow" {
		t.Fatal("standing workflow origin lost at task insert", task.ID)
	}
	dbfx.Exec(t, "UPDATE agent_task_queue SET status='failed',completed_at=now() WHERE id=$1", task.ID)
	retry, err := f.h.Queries.CreateRetryTask(context.Background(), db.CreateRetryTaskParams{ID: task.ID})
	if err != nil {
		t.Fatal(err)
	}
	if retry.WorkflowRunID != parseUUID(f.runID) || retry.OriginatorUserID != parseUUID(principal) || retry.OriginatorSource.String != "workflow" {
		t.Fatal("automatic retry lost workflow ancestry", retry.ID)
	}
}

func TestWorkflowActionCrossWorkspaceAndUnusableReadinessDenial(t *testing.T) {
	f := newWorkflowBusinessFixture(t)
	agent, runtime := workflowAgentFixture(t)
	foreignWS := dbfx.Insert(t, "workspace", testutil.Cols{"name": "workflow other", "slug": uuid.NewString()})
	foreignIssue := dbfx.Issue(t, "foreign issue", testutil.Cols{"workspace_id": foreignWS})
	testutil.Call(t, f.h.WorkflowRunnerAction, f.action("issues.get", "", map[string]any{"id": foreignIssue})).Want(404)
	testutil.Call(t, f.h.WorkflowRunnerAction, f.action("agents.run", "foreign", map[string]any{"agent_id": agent, "issue_id": foreignIssue})).Want(404)
	issue := dbfx.Issue(t, "unusable "+uuid.NewString())
	dbfx.Exec(t, `UPDATE agent_runtime SET status='offline',metadata='{"offline_reason":{"code":"not_executable","detail":"sensitive path /private/key"}}' WHERE id=$1`, runtime)
	testutil.Call(t, f.h.WorkflowRunnerAction, f.action("agents.run", "blocked", map[string]any{"agent_id": agent, "issue_id": issue})).Want(503)
	var n int
	dbfx.QueryRow(t, "SELECT count(*) FROM workflow_operation WHERE run_id=$1", f.runID).Scan(&n)
	if n != 0 {
		t.Fatal("denied target inserted receipt", n)
	}
}

func TestWorkflowActionCustomStatusIsExactWorkspaceKey(t *testing.T) {
	f := newWorkflowBusinessFixture(t)
	key := "wf_custom_" + strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	createTestCustomStatus(t, key, "unstarted")
	result := testutil.Call(t, f.h.WorkflowRunnerAction, f.action("issues.create", "custom", map[string]any{"title": "exact key", "status": key})).Want(200).Map()["result"].(map[string]any)
	if result["status"] != key {
		t.Fatal("custom key collapsed", result)
	}
	read := testutil.Call(t, f.h.WorkflowRunnerAction, f.action("issues.get", "", map[string]any{"id": result["id"]})).Want(200).Map()["result"].(map[string]any)
	if read["status"] != key {
		t.Fatal("wait read does not return exact status key", read)
	}
}

func TestWorkflowActionIssueAssigneeUsesExistingMemberAndSquadPolicy(t *testing.T) {
	f := newWorkflowBusinessFixture(t)
	agent, _ := workflowAgentFixture(t)
	squad := dbfx.Insert(t, "squad", testutil.Cols{"workspace_id": testWorkspaceID, "name": "workflow squad " + uuid.NewString(), "leader_id": agent, "creator_id": testUserID})
	for _, target := range []struct{ kind, id string }{{"member", testUserID}, {"squad", squad}} {
		result := testutil.Call(t, f.h.WorkflowRunnerAction, f.action("issues.create", target.kind, map[string]any{"title": "workflow " + target.kind, "assignee_type": target.kind, "assignee_id": target.id})).Want(200).Map()["result"].(map[string]any)
		var actualType, actualID string
		dbfx.QueryRow(t, "SELECT assignee_type,assignee_id::text FROM issue WHERE id=$1", result["id"]).Scan(&actualType, &actualID)
		if actualType != target.kind || actualID != target.id {
			t.Fatal("issue assignment changed", actualType, actualID)
		}
	}
	var n int
	dbfx.QueryRow(t, "SELECT count(*) FROM agent_task_queue WHERE workflow_run_id=$1 AND is_leader_task AND squad_id=$2", f.runID, squad).Scan(&n)
	if n != 1 {
		t.Fatal("squad leader task missing", n)
	}
}

func TestWorkflowActionInvocationGrantHeldThroughBusinessCommit(t *testing.T) {
	f := newWorkflowBusinessFixture(t)
	agent, runtime := workflowAgentFixture(t)
	other := plainMember(t, "grant row owner")
	dbfx.Exec(t, "UPDATE agent SET owner_id=$2,permission_mode='public_to' WHERE id=$1", agent, other)
	dbfx.Exec(t, "UPDATE agent_runtime SET visibility='public' WHERE id=$1", runtime)
	dbfx.Insert(t, "agent_invocation_target", testutil.Cols{"agent_id": agent, "target_type": "member", "target_id": testUserID, "created_by": other})
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	key := time.Now().UnixNano()
	name := "wf_hold_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	blocker, err := testPool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Rollback(context.Background())
	if _, err = blocker.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", key); err != nil {
		t.Fatal(err)
	}
	dbfx.Exec(t, fmt.Sprintf(`CREATE FUNCTION %s() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.run_id='%s'::uuid THEN PERFORM pg_advisory_xact_lock(%d); END IF; RETURN NEW; END $$`, name, f.runID, key))
	dbfx.Cleanup(t, "DROP FUNCTION IF EXISTS "+name+"() CASCADE")
	dbfx.Exec(t, "CREATE TRIGGER "+name+" BEFORE UPDATE ON workflow_operation FOR EACH ROW EXECUTE FUNCTION "+name+"()")
	a := businessAuthority(t, f)
	started := make(chan int32, 1)
	f.h.TxStarter = workflowActionTransactionStarted{txStarter: testPool, started: started}
	issue := dbfx.Issue(t, "grant held "+uuid.NewString())
	payload := map[string]any{"agent_id": agent, "issue_id": issue}
	raw, _ := json.Marshal(payload)
	done := make(chan error, 1)
	go func() {
		_, e := f.h.WorkflowBusinessAction(ctx, a, WorkflowActionRequest{RunID: f.runID, StepID: "a", Operation: "agents.run", OperationKey: "held", Payload: raw})
		done <- e
	}()
	pid := <-started
	waitWorkflowBlocked(t, ctx, pid)
	deleting, err := testPool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer deleting.Release()
	var deletePID int32
	if err = deleting.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&deletePID); err != nil {
		t.Fatal(err)
	}
	deleted := make(chan error, 1)
	go func() {
		_, e := deleting.Exec(ctx, "DELETE FROM agent_invocation_target WHERE agent_id=$1", agent)
		deleted <- e
	}()
	waitWorkflowBlocked(t, ctx, deletePID)
	if err = blocker.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err = <-done; err != nil {
		t.Fatal("business lost already-held grant", err)
	}
	if err = <-deleted; err != nil {
		t.Fatal(err)
	}
	f.h.TxStarter = testPool
	testutil.Call(t, f.h.WorkflowRunnerAction, f.action("agents.run", "held", payload)).Want(403)
}
