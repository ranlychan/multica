package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/multica-ai/multica/server/internal/entitlement"
	"github.com/multica-ai/multica/server/internal/entitlement/entitlementtest"
	"github.com/multica-ai/multica/server/internal/service"
	"github.com/multica-ai/multica/server/internal/testutil"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

func workflowReviewQuota(t *testing.T, limit int, fixtures ...*workflowBusinessFixture) time.Time {
	t.Helper()
	start := time.Now().UTC().Add(-time.Hour).Truncate(time.Microsecond)
	end := start.Add(24 * time.Hour)
	stub := entitlementtest.New()
	stub.Set(uuid.MustParse(testWorkspaceID), entitlement.GateAutopilotRuns, entitlement.Decision{Gate: entitlement.Gate{Action: entitlement.ActionEnforce, Limit: &limit, PeriodStart: &start, PeriodEnd: &end, ResetAt: &end}})
	for _, f := range fixtures {
		svc := *f.h.AutopilotService
		svc.Entitlements = stub
		f.h.AutopilotService = &svc
	}
	dbfx.Cleanup(t, "DELETE FROM autopilot_quota_period WHERE workspace_id=$1 AND period_start=$2", testWorkspaceID, start)
	dbfx.Cleanup(t, "DELETE FROM autopilot_quota_reservation WHERE workspace_id=$1 AND period_start=$2", testWorkspaceID, start)
	return start
}

// The signed caller key is not a global quota namespace. Distinct verified
// run/step identities must each own their real AP run and charge.
func TestWorkflowActionQuotaIdentityIncludesVerifiedRunAndStep(t *testing.T) {
	for _, mode := range []string{"run_only", "create_issue"} {
		t.Run(mode, func(t *testing.T) {
			other := plainMember(t, "quota scope principal")
			f1 := newWorkflowBusinessFixtureWithSteps(t, []string{"a", "b"})
			f2 := newWorkflowBusinessFixtureWithSteps(t, []string{"a", "b"}, other)
			agent, _ := workflowAgentFixture(t)
			dbfx.Exec(t, "UPDATE agent SET permission_mode='public_to' WHERE id=$1", agent)
			dbfx.Insert(t, "agent_invocation_target", testutil.Cols{"agent_id": agent, "target_type": "workspace", "target_id": testWorkspaceID, "created_by": testUserID})
			ap1, ap2 := workflowAutopilotFixture(t, agent, mode), workflowAutopilotFixture(t, agent, mode)
			for _, ap := range []string{ap1, ap2} {
				dbfx.Exec(t, "INSERT INTO autopilot_collaborator(autopilot_id,user_type,user_id,granted_by) VALUES($1,'member',$2,$3)", ap, other, testUserID)
				dbfx.Cleanup(t, "DELETE FROM autopilot_collaborator WHERE autopilot_id=$1", ap)
			}
			start := workflowReviewQuota(t, 4, f1, f2)
			seen := map[string]bool{}
			seenLinks := map[string]bool{}
			scopes := []struct {
				f                   *workflowBusinessFixture
				step, ap, principal string
			}{{f1, "a", ap1, testUserID}, {f1, "b", ap1, testUserID}, {f2, "a", ap1, other}, {f2, "b", ap2, other}}
			for i, s := range scopes {
				// Existing recent-duplicate policy is title-scoped; each intended new
				// create_issue request uses a deliberately distinct current rule title.
				dbfx.Exec(t, "UPDATE autopilot SET issue_title_template=$2 WHERE id=$1", s.ap, fmt.Sprintf("quota scope %d %s", i, uuid.NewString()))
				payload := map[string]any{"autopilot_id": s.ap}
				first := testutil.Call(t, s.f.h.WorkflowRunnerAction, s.f.actionInStep(s.step, "autopilots.invoke", "same untrusted raw key", payload)).Want(200).Map()["result"].(map[string]any)
				id := first["id"].(string)
				if seen[id] {
					t.Fatalf("different receipt scopes reused AP run %s", id)
				}
				seen[id] = true
				var workflow, principal, ap, link string
				dbfx.QueryRow(t, "SELECT workflow_run_id::text,principal_user_id::text,autopilot_id::text,COALESCE(task_id,issue_id)::text FROM autopilot_run WHERE id=$1", id).Scan(&workflow, &principal, &ap, &link)
				if workflow != s.f.runID || principal != s.principal || ap != s.ap || link == "" {
					t.Fatal("quota identity borrowed another operation's business ownership", workflow, principal, ap, link)
				}
				if seenLinks[link] {
					t.Fatal("distinct quota scopes replaced a shared downstream link", link)
				}
				seenLinks[link] = true
				replay := testutil.Call(t, s.f.h.WorkflowRunnerAction, s.f.actionInStep(s.step, "autopilots.invoke", "same untrusted raw key", payload)).Want(200).Map()["result"].(map[string]any)
				if replay["id"] != id {
					t.Fatal("same receipt scope changed run", first, replay)
				}
			}
			var used, reserved, runs, tasks int
			dbfx.QueryRow(t, "SELECT used_count,reserved_count FROM autopilot_quota_period WHERE workspace_id=$1 AND period_start=$2", testWorkspaceID, start).Scan(&used, &reserved)
			dbfx.QueryRow(t, "SELECT count(*) FROM autopilot_run WHERE workflow_run_id=ANY($1::uuid[])", []string{f1.runID, f2.runID}).Scan(&runs)
			dbfx.QueryRow(t, "SELECT count(*) FROM agent_task_queue WHERE workflow_run_id=ANY($1::uuid[])", []string{f1.runID, f2.runID}).Scan(&tasks)
			if used+reserved != 4 || runs != 4 || tasks != 4 {
				t.Fatal("distinct scope quota/business counts wrong", used, reserved, runs, tasks)
			}
			testutil.Call(t, f2.h.WorkflowRunnerAction, f2.actionInStep("b", "autopilots.invoke", "next independent operation", map[string]any{"autopilot_id": ap2})).Want(429)
		})
	}
}

func TestWorkflowActionUnexpectedQuotaReuseCannotRedispatch(t *testing.T) {
	f := newWorkflowBusinessFixture(t)
	agent, _ := workflowAgentFixture(t)
	ap := workflowAutopilotFixture(t, agent, "run_only")
	start := workflowReviewQuota(t, 2, f)
	payload := map[string]any{"autopilot_id": ap}
	first := testutil.Call(t, f.h.WorkflowRunnerAction, f.action("autopilots.invoke", "missing-receipt", payload)).Want(200).Map()["result"].(map[string]any)
	dbfx.Exec(t, "DELETE FROM workflow_operation WHERE run_id=$1 AND operation_key='missing-receipt'", f.runID)
	testutil.Call(t, f.h.WorkflowRunnerAction, f.action("autopilots.invoke", "missing-receipt", payload)).Want(409)
	var count, quota int
	var taskID string
	dbfx.QueryRow(t, "SELECT count(*) FROM agent_task_queue WHERE workflow_run_id=$1", f.runID).Scan(&count)
	dbfx.QueryRow(t, "SELECT used_count+reserved_count FROM autopilot_quota_period WHERE workspace_id=$1 AND period_start=$2", testWorkspaceID, start).Scan(&quota)
	dbfx.QueryRow(t, "SELECT task_id::text FROM autopilot_run WHERE id=$1", first["id"]).Scan(&taskID)
	if count != 1 || quota != 1 || taskID != first["task_id"] {
		t.Fatal("unexpected quota reuse changed durable work", count, quota, taskID, first)
	}
	var receipts int
	dbfx.QueryRow(t, "SELECT count(*) FROM workflow_operation WHERE run_id=$1", f.runID).Scan(&receipts)
	if receipts != 0 {
		t.Fatal("unexpected reuse committed a replacement receipt", receipts)
	}
}

// A completed privacy revocation must win even when the new action was
// already waiting inside its business transaction on an unrelated receipt lock.
func TestWorkflowActionRuntimeRevocationWinsBlockedAdmission(t *testing.T) {
	for _, kind := range []string{"agents.run", "issues.create", "autopilots.invoke/run_only", "autopilots.invoke/create_issue"} {
		t.Run(kind, func(t *testing.T) {
			operation := kind
			f := newWorkflowBusinessFixture(t)
			agent, runtime := workflowAgentFixture(t)
			runtimeOwner := plainMember(t, "borrowed runtime owner")
			dbfx.Exec(t, "UPDATE agent_runtime SET owner_id=$2,visibility='public' WHERE id=$1", runtime, runtimeOwner)
			payload := map[string]any{}
			if operation == "agents.run" {
				payload = map[string]any{"agent_id": agent, "issue_id": dbfx.Issue(t, "borrowed runtime "+uuid.NewString())}
			}
			if operation == "issues.create" {
				payload = map[string]any{"title": "privacy revocation " + uuid.NewString(), "assignee_type": "agent", "assignee_id": agent}
			}
			if strings.HasPrefix(operation, "autopilots.invoke/") {
				payload = map[string]any{"autopilot_id": workflowAutopilotFixture(t, agent, strings.TrimPrefix(operation, "autopilots.invoke/"))}
				operation = "autopilots.invoke"
			}
			ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
			defer cancel()
			a := businessAuthority(t, f)
			blocker, err := testPool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer blocker.Rollback(context.Background())
			// On the old implementation this lookup occurs after readiness, so
			// blocking receipt INSERT exposes its unfenced consent snapshot.
			key := time.Now().UnixNano()
			name := "wf_privacy_" + strings.ReplaceAll(uuid.NewString(), "-", "")
			if _, err = blocker.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", key); err != nil {
				t.Fatal(err)
			}
			dbfx.Exec(t, fmt.Sprintf(`CREATE FUNCTION %s() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.run_id='%s'::uuid THEN PERFORM pg_advisory_xact_lock(%d); END IF; RETURN NEW; END $$`, name, f.runID, key))
			dbfx.Cleanup(t, "DROP FUNCTION IF EXISTS "+name+"() CASCADE")
			dbfx.Exec(t, "CREATE TRIGGER "+name+" BEFORE INSERT ON workflow_operation FOR EACH ROW EXECUTE FUNCTION "+name+"()")
			started := make(chan int32, 1)
			f.h.TxStarter = workflowActionTransactionStarted{txStarter: testPool, started: started}
			raw, _ := json.Marshal(payload)
			done := make(chan error, 1)
			go func() {
				_, e := f.h.WorkflowBusinessAction(ctx, a, WorkflowActionRequest{RunID: f.runID, StepID: "a", Operation: operation, OperationKey: "privacy", Payload: raw})
				done <- e
			}()
			pid := <-started
			waitWorkflowBlocked(t, ctx, pid)
			revokeCtx, revokeCancel := context.WithTimeout(ctx, 150*time.Millisecond)
			_, revoked := testPool.Exec(revokeCtx, "UPDATE agent_runtime SET visibility='private' WHERE id=$1", runtime)
			revokeCancel()
			if revoked != nil {
				if !errors.Is(revoked, context.DeadlineExceeded) {
					t.Fatal("privacy update failed without demonstrating lock wait", revoked)
				}
				// With a held consent lock, revoke has not committed. Finish the admitted
				// action first; a requested-but-blocked revoke is not a prior revocation.
				if err = blocker.Commit(ctx); err != nil {
					t.Fatal(err)
				}
				if err = <-done; err != nil {
					t.Fatal("valid held consent admission failed", err)
				}
				var visibility string
				dbfx.QueryRow(t, "SELECT visibility FROM agent_runtime WHERE id=$1", runtime).Scan(&visibility)
				if visibility != "public" {
					t.Fatal("revoke committed while admission held consent", visibility)
				}
				dbfx.Exec(t, "UPDATE agent_runtime SET visibility='private' WHERE id=$1", runtime)
				t.Log("privacy revoke could not commit before held-consent admission; committed after action")
				return
			}
			if err = blocker.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			err = <-done
			status, _ := workflowBusinessFailure(err)
			if err == nil || status < 400 {
				t.Fatal("new business committed after successful prior runtime consent revocation")
			}
			var n int
			dbfx.QueryRow(t, "SELECT count(*) FROM agent_task_queue WHERE workflow_run_id=$1", f.runID).Scan(&n)
			if n != 0 {
				t.Fatal("revoked borrowed runtime kept task", n)
			}
			dbfx.QueryRow(t, "SELECT count(*) FROM workflow_operation WHERE run_id=$1", f.runID).Scan(&n)
			if n != 0 {
				t.Fatal("revoked borrowed runtime kept receipt", n)
			}
		})
	}
}

func TestWorkflowActionRuntimeConsentRevocationOrTeardownWins(t *testing.T) {
	for _, change := range []string{"privacy", "teardown", "rebind"} {
		t.Run(change, func(t *testing.T) {
			f := newWorkflowBusinessFixture(t)
			agent, runtime := workflowAgentFixture(t)
			owner := plainMember(t, "runtime lender")
			dbfx.Exec(t, "UPDATE agent_runtime SET owner_id=$2,visibility='public' WHERE id=$1", runtime, owner)
			ap := workflowAutopilotFixture(t, agent, "run_only")
			var replacement string
			if change == "rebind" {
				replacement = dbfx.Runtime(t, "replacement workflow runtime "+uuid.NewString())
			}
			ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
			defer cancel()
			a := businessAuthority(t, f)
			blocker, err := testPool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer blocker.Rollback(context.Background())
			q := db.New(testPool).WithTx(blocker)
			if change == "privacy" {
				_, err = blocker.Exec(ctx, "UPDATE agent_runtime SET visibility='private' WHERE id=$1", runtime)
			} else {
				_, err = q.LockAgentRuntime(ctx, parseUUID(runtime))
			}
			if err != nil {
				t.Fatal(err)
			}
			started := make(chan int32, 1)
			f.h.TxStarter = workflowActionTransactionStarted{txStarter: testPool, started: started}
			done := make(chan error, 1)
			raw, _ := json.Marshal(map[string]any{"autopilot_id": ap})
			go func() {
				_, e := f.h.WorkflowBusinessAction(ctx, a, WorkflowActionRequest{RunID: f.runID, StepID: "a", Operation: "autopilots.invoke", OperationKey: "consent-wins", Payload: raw})
				done <- e
			}()
			pid := <-started
			waitWorkflowBlocked(t, ctx, pid)
			if change == "teardown" {
				if _, err = service.TeardownRuntime(ctx, q, parseUUID(runtime), service.RuntimeTeardownOptions{CancelNonTerminalTasks: true}); err != nil {
					t.Fatal("runtime→agent→AP teardown deadlocked behind waiting admission", err)
				}
			}
			if change == "rebind" {
				if _, err = blocker.Exec(ctx, "UPDATE agent SET runtime_id=$2 WHERE id=$1", agent, replacement); err != nil {
					t.Fatal("rebind deadlocked behind optimistic prepared binding", err)
				}
			}
			if err = blocker.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			if err = <-done; err == nil {
				t.Fatal("admission committed after consent/binding revoked")
			}
			for _, query := range []string{"SELECT count(*) FROM workflow_operation WHERE run_id=$1", "SELECT count(*) FROM agent_task_queue WHERE workflow_run_id=$1", "SELECT count(*) FROM autopilot_run WHERE workflow_run_id=$1"} {
				var n int
				dbfx.QueryRow(t, query, f.runID).Scan(&n)
				if n != 0 {
					t.Fatal("revocation winner left admitted business", query, n)
				}
			}
		})
	}
}

func TestWorkflowActionPrivateRuntimePolicyUsesAgentOwner(t *testing.T) {
	human := plainMember(t, "shared-agent invoking human")
	f := newWorkflowBusinessFixture(t, human)
	agent, _ := workflowAgentFixture(t)
	dbfx.Exec(t, "UPDATE agent SET permission_mode='public_to' WHERE id=$1", agent)
	dbfx.Insert(t, "agent_invocation_target", testutil.Cols{"agent_id": agent, "target_type": "member", "target_id": human, "created_by": testUserID})
	// Runtime and agent share testUserID as owner; the invoking human differs.
	// Existing invocation permission, not ownership of the machine, admits them.
	issue := dbfx.Issue(t, "private runtime legitimate shared agent "+uuid.NewString())
	payload := map[string]any{"agent_id": agent, "issue_id": issue}
	testutil.Call(t, f.h.WorkflowRunnerAction, f.action("agents.run", "agent-owner-consent", payload)).Want(200)
}

func TestWorkflowActionCachedResultsDoNotReadmitRuntimeConsent(t *testing.T) {
	f := newWorkflowBusinessFixture(t)
	agent, runtime := workflowAgentFixture(t)
	owner := plainMember(t, "cached runtime lender")
	dbfx.Exec(t, "UPDATE agent_runtime SET owner_id=$2,visibility='public' WHERE id=$1", runtime, owner)
	issue := dbfx.Issue(t, "cached borrowed runtime "+uuid.NewString())
	payload := map[string]any{"agent_id": agent, "issue_id": issue}
	result := testutil.Call(t, f.h.WorkflowRunnerAction, f.action("agents.run", "cached-runtime", payload)).Want(200).Map()["result"].(map[string]any)
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	blocker, err := testPool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Rollback(context.Background())
	if _, err = blocker.Exec(ctx, "UPDATE agent_runtime SET visibility='private',status='offline' WHERE id=$1", runtime); err != nil {
		t.Fatal(err)
	}
	// The completed receipt and its safe task read require current invocation
	// grants, but neither new execution consent nor liveness admission.
	replay := f.action("agents.run", "cached-runtime", payload)
	replayCtx, replayCancel := context.WithTimeout(replay.Context(), time.Second)
	testutil.Call(t, f.h.WorkflowRunnerAction, replay.WithContext(replayCtx)).Want(200)
	replayCancel()
	read := f.action("agents.get", "", map[string]any{"id": result["id"]})
	readCtx, readCancel := context.WithTimeout(read.Context(), time.Second)
	testutil.Call(t, f.h.WorkflowRunnerAction, read.WithContext(readCtx)).Want(200)
	readCancel()
	if err = blocker.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	testutil.Call(t, f.h.WorkflowRunnerAction, f.action("agents.run", "cached-runtime", payload)).Want(200)
	testutil.Call(t, f.h.WorkflowRunnerAction, f.action("agents.run", "new-private-runtime", payload)).Want(503)
}
