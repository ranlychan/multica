package handler

import (
	"context"
	"encoding/base64"
	"net/http"
	"testing"
	"time"

	"github.com/multica-ai/multica/server/internal/testutil"
	"github.com/multica-ai/multica/server/internal/util/secretbox"
)

type workspaceWorkflowFixture struct {
	workspaceID, runID, issueID, taskID, encryptedToken string
	box                                                 *secretbox.Box
}

// The run deliberately references an old revision while the workflow points to
// a newer revision: teardown must remove historical runs and their receipts too.
func seedWorkspaceWorkflowDeletion(t *testing.T, slug string) workspaceWorkflowFixture {
	t.Helper()
	wsID := dbfx.Insert(t, "workspace", testutil.Cols{"name": "Workflow deletion", "slug": slug})
	fx := testutil.New(testPool, wsID, testUserID)
	fx.Insert(t, "member", testutil.Cols{"workspace_id": wsID, "user_id": testUserID, "role": "owner"})
	workflowID := fx.Insert(t, "workflow", testutil.Cols{
		"workspace_id": wsID, "name": "Deletion history", "slug": "history",
		"created_by_type": "member", "created_by_id": testUserID,
	})
	revisionID := fx.Insert(t, "workflow_revision", testutil.Cols{
		"workspace_id": wsID, "workflow_id": workflowID, "version": 1,
		"definition": "export default oldWorkflow", "definition_hash": "old-definition",
		"definition_graph": testutil.Raw("'{}'::jsonb"), "hatchet_name": slug + "-v1", "created_by_id": testUserID,
	})
	currentRevisionID := fx.Insert(t, "workflow_revision", testutil.Cols{
		"workspace_id": wsID, "workflow_id": workflowID, "version": 2,
		"definition": "export default newWorkflow", "definition_hash": "new-definition",
		"definition_graph": testutil.Raw("'{}'::jsonb"), "hatchet_name": slug + "-v2", "created_by_id": testUserID,
	})
	fx.Exec(t, `UPDATE workflow SET current_revision_id=$2, definition_version=2 WHERE id=$1`, workflowID, currentRevisionID)
	triggerID := fx.Insert(t, "workflow_trigger", testutil.Cols{
		"workspace_id": wsID, "workflow_id": workflowID, "kind": "api", "token_hash": slug + "-trigger-hash",
		"principal_user_id": testUserID, "created_by_type": "member", "created_by_id": testUserID,
	})
	var runID string
	fx.QueryRow(t, `SELECT gen_random_uuid()`).Scan(&runID)
	fx.Insert(t, "workflow_run", testutil.Cols{
		"id": runID, "workspace_id": wsID, "workflow_id": workflowID, "revision_id": revisionID,
		"definition_hash": "old-definition", "principal_user_id": testUserID, "trigger_id": triggerID,
		"source": "api", "idempotency_key": runID, "status": "completed", "finished_at": testutil.Raw("now()"),
	})
	fx.Insert(t, "workflow_operation", testutil.Cols{
		"workspace_id": wsID, "run_id": runID, "step_id": "issue", "operation_key": "create",
		"payload_hash": "payload", "attempt": 1, "status": "completed", "result": testutil.Raw("'{\"issue\":\"created\"}'::jsonb"),
	})
	fx.Insert(t, "workflow_runner_token", testutil.Cols{
		"workspace_id": wsID, "daemon_id": slug, "token_hash": slug + "-runner-hash",
		"created_by_id": testUserID, "expires_at": testutil.Raw("now() + interval '1 hour'"),
	})
	fx.Insert(t, "workflow_collaborator", testutil.Cols{
		"workspace_id": wsID, "workflow_id": workflowID, "user_id": testUserID, "role": "editor", "created_by_id": testUserID,
	})
	box, err := secretbox.New(make([]byte, secretbox.KeySize))
	if err != nil {
		t.Fatal(err)
	}
	ciphertext, err := box.Seal([]byte("tenant-credential"))
	if err != nil {
		t.Fatal(err)
	}
	encryptedToken := base64.StdEncoding.EncodeToString(ciphertext)
	fx.InsertNoID(t, "workflow_hatchet_tenant", testutil.Cols{
		"workspace_id": wsID, "tenant_id": slug + "-tenant", "token_encrypted": encryptedToken,
	}, "workspace_id=$1", wsID)
	runtimeID := fx.Runtime(t, "Workflow deletion runtime")
	agentID := fx.Agent(t, "Workflow deletion agent", runtimeID)
	issueID := fx.Issue(t, "Workflow deletion issue", testutil.Cols{"origin_type": "workflow", "origin_id": runID})
	taskID := fx.Task(t, agentID, testutil.Cols{
		"runtime_id": runtimeID, "issue_id": issueID, "workflow_run_id": runID, "status": "completed",
	})
	return workspaceWorkflowFixture{workspaceID: wsID, runID: runID, issueID: issueID, taskID: taskID, encryptedToken: encryptedToken, box: box}
}

func TestDeleteWorkspace_CleansWorkflowHistoryAndQueuesTenant(t *testing.T) {
	if testPool == nil {
		t.Skip("database not available")
	}
	target := seedWorkspaceWorkflowDeletion(t, "workflow-delete-target")
	other := seedWorkspaceWorkflowDeletion(t, "workflow-delete-other")
	// A workflow association alone must not override ordinary task ownership.
	// This task belongs to the surviving workspace despite referring to an old run.
	var otherAgentID, otherRuntimeID string
	dbfx.QueryRow(t, `SELECT agent_id,runtime_id FROM agent_task_queue WHERE id=$1`, other.taskID).Scan(&otherAgentID, &otherRuntimeID)
	crossTaskID := dbfx.Task(t, otherAgentID, testutil.Cols{"runtime_id": otherRuntimeID, "workflow_run_id": target.runID, "status": "completed"})
	req := withURLParam(newRequest(http.MethodDelete, "/api/workspaces/"+target.workspaceID, nil), "id", target.workspaceID)
	testutil.Call(t, testHandler.DeleteWorkspace, req).Want(http.StatusNoContent)

	for _, table := range []string{"workflow_operation", "workflow_run", "workflow_trigger", "workflow_collaborator", "workflow_revision", "workflow_runner_token", "workflow"} {
		var targetCount, otherCount int
		dbfx.QueryRow(t, `SELECT count(*) FROM `+table+` WHERE workspace_id=$1`, target.workspaceID).Scan(&targetCount)
		dbfx.QueryRow(t, `SELECT count(*) FROM `+table+` WHERE workspace_id=$1`, other.workspaceID).Scan(&otherCount)
		if targetCount != 0 || otherCount == 0 {
			t.Errorf("%s rows: deleted workspace=%d, surviving workspace=%d", table, targetCount, otherCount)
		}
	}
	var tenantID, token, state string
	var requested, due bool
	var attempts int
	dbfx.QueryRow(t, `SELECT tenant_id,token_encrypted,deletion_state,deletion_requested_at IS NOT NULL,COALESCE(next_deletion_attempt_at<=now(),false),deletion_attempts FROM workflow_hatchet_tenant WHERE workspace_id=$1`, target.workspaceID).Scan(&tenantID, &token, &state, &requested, &due, &attempts)
	if tenantID != "workflow-delete-target-tenant" || token != target.encryptedToken || state != "pending" || !requested || !due || attempts != 0 {
		t.Fatalf("tenant cleanup ledger lost or not queued: tenant=%q state=%q requested=%v due=%v attempts=%d credentialPreserved=%v", tenantID, state, requested, due, attempts, token == target.encryptedToken)
	}
	ciphertext, err := base64.StdEncoding.DecodeString(token)
	if err != nil {
		t.Fatal(err)
	}
	plaintext, err := target.box.Open(ciphertext)
	if err != nil || string(plaintext) != "tenant-credential" {
		t.Fatalf("retained credential cannot be decrypted: %v", err)
	}
	for _, row := range []struct{ table, id string }{{"workspace", target.workspaceID}, {"issue", target.issueID}, {"agent_task_queue", target.taskID}} {
		var exists bool
		dbfx.QueryRow(t, `SELECT EXISTS(SELECT 1 FROM `+row.table+` WHERE id=$1)`, row.id).Scan(&exists)
		if exists {
			t.Errorf("%s survives ordinary workspace deletion", row.table)
		}
	}
	var otherState string
	dbfx.QueryRow(t, `SELECT deletion_state FROM workflow_hatchet_tenant WHERE workspace_id=$1`, other.workspaceID).Scan(&otherState)
	if otherState != "active" {
		t.Fatalf("other workspace tenant queued: %q", otherState)
	}
	var crossTaskExists bool
	dbfx.QueryRow(t, `SELECT EXISTS(SELECT 1 FROM agent_task_queue WHERE id=$1)`, crossTaskID).Scan(&crossTaskExists)
	if !crossTaskExists {
		t.Fatal("workflow association overrode surviving task ownership")
	}
}

func TestDeleteWorkspace_RollsBackWorkflowCleanupAndTenantQueue(t *testing.T) {
	if testPool == nil {
		t.Skip("database not available")
	}
	ctx := context.Background()
	lockRollupSingleton(t)
	setWorkspaceDeleteLockTimeoutForTest(t, 50*time.Millisecond)
	target := seedWorkspaceWorkflowDeletion(t, "workflow-delete-rollback")
	blocker, err := testPool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = blocker.Rollback(context.Background()) })
	if _, err := blocker.Exec(ctx, `SELECT id FROM member WHERE workspace_id=$1 FOR UPDATE`, target.workspaceID); err != nil {
		t.Fatal(err)
	}
	req := withURLParam(newRequest(http.MethodDelete, "/api/workspaces/"+target.workspaceID, nil), "id", target.workspaceID)
	testutil.Call(t, testHandler.DeleteWorkspace, req).Want(http.StatusServiceUnavailable)
	if err := blocker.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	for _, item := range []struct {
		table string
		count int
	}{
		{"workflow_operation", 1}, {"workflow_run", 1}, {"workflow_trigger", 1}, {"workflow_collaborator", 1}, {"workflow_revision", 2}, {"workflow_runner_token", 1}, {"workflow", 1},
	} {
		var count int
		dbfx.QueryRow(t, `SELECT count(*) FROM `+item.table+` WHERE workspace_id=$1`, target.workspaceID).Scan(&count)
		if count != item.count {
			t.Errorf("%s after rollback=%d, want %d", item.table, count, item.count)
		}
	}
	var token, state string
	var noRequest, noAttempt bool
	dbfx.QueryRow(t, `SELECT token_encrypted,deletion_state,deletion_requested_at IS NULL,next_deletion_attempt_at IS NULL FROM workflow_hatchet_tenant WHERE workspace_id=$1`, target.workspaceID).Scan(&token, &state, &noRequest, &noAttempt)
	if token != target.encryptedToken || state != "active" || !noRequest || !noAttempt {
		t.Fatalf("tenant queue did not roll back: state=%q noRequest=%v noAttempt=%v", state, noRequest, noAttempt)
	}
	for _, row := range []struct{ table, id string }{{"workspace", target.workspaceID}, {"issue", target.issueID}, {"agent_task_queue", target.taskID}} {
		var exists bool
		dbfx.QueryRow(t, `SELECT EXISTS(SELECT 1 FROM `+row.table+` WHERE id=$1)`, row.id).Scan(&exists)
		if !exists {
			t.Errorf("%s missing after rollback", row.table)
		}
	}
}
