package handler

import (
	"context"
	"encoding/json"
	"github.com/golang-jwt/jwt/v5"
	"github.com/multica-ai/multica/server/internal/auth"
	"github.com/multica-ai/multica/server/internal/middleware"
	"github.com/multica-ai/multica/server/internal/service"
	"github.com/multica-ai/multica/server/internal/testutil"
	"github.com/multica-ai/multica/server/internal/util"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestWorkflowHTTPMiddlewareRejectsForgedAgentIdentity(t *testing.T) {
	human := plainMember(t, "workflow-jwt-human")
	token, e := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{"sub": human, "email": "workflow@test.invalid", "exp": time.Now().Add(time.Hour).Unix()}).SignedString(auth.JWTSecret())
	if e != nil {
		t.Fatal(e)
	}
	req := newRequest("POST", "/api/workflows", map[string]any{"name": "jwt workflow", "slug": "workflow-jwt"})
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("X-Agent-ID", testUserID)
	req.Header.Set("X-Task-ID", testUserID)
	req.Header.Set("X-Actor-Source", "task_token")
	route := middleware.Auth(testHandler.Queries, nil, nil, nil)(middleware.RequireWorkspaceMember(testHandler.Queries)(http.HandlerFunc(testHandler.CreateWorkflow)))
	var result struct {
		ID          string `json:"id"`
		CreatedByID string `json:"created_by_id"`
		AuditType   string `json:"audit_actor_type"`
	}
	testutil.Call(t, route.ServeHTTP, req).Want(201).JSON(&result)
	dbfx.Cleanup(t, "DELETE FROM workflow WHERE id=$1", result.ID)
	if result.CreatedByID != human || result.AuditType != "member" {
		t.Fatal("forged agent headers accepted")
	}
}

func TestWorkflowHTTPAgentAuthorityTriggerIngressReplayAndRedaction(t *testing.T) {
	human := plainMember(t, "workflow-human")
	machine := plainMember(t, "workflow-machine")
	var agent string
	dbfx.QueryRow(t, "SELECT id FROM agent WHERE workspace_id=$1 LIMIT 1", testWorkspaceID).Scan(&agent)
	task := callerTask(t, agent, human)
	caller := actingCaller{authUserID: machine, agentID: agent, taskID: task}
	var created struct {
		ID           string `json:"id"`
		CreatedByID  string `json:"created_by_id"`
		AuditActorID string `json:"audit_actor_id"`
	}
	testutil.Call(t, testHandler.CreateWorkflow, caller.request("POST", "/api/workflows", map[string]any{"name": "agent workflow", "slug": "workflow-agent"})).Want(201).JSON(&created)
	dbfx.Cleanup(t, "DELETE FROM workflow WHERE id=$1", created.ID)
	dbfx.Cleanup(t, "DELETE FROM workflow_revision WHERE workflow_id=$1", created.ID)
	dbfx.Cleanup(t, "DELETE FROM workflow_run WHERE workflow_id=$1", created.ID)
	dbfx.Cleanup(t, "DELETE FROM workflow_trigger WHERE workflow_id=$1", created.ID)
	if created.CreatedByID != human || created.AuditActorID != agent {
		t.Fatal("human and agent identity lost")
	}
	denied := actingCaller{authUserID: testUserID, agentID: agent, taskID: callerTask(t, agent, plainMember(t, "workflow-outsider"))}
	req := withURLParam(denied.request("POST", "/api/workflows/"+created.ID+"/run", map[string]any{}), "id", created.ID)
	testutil.Call(t, testHandler.RunWorkflow, req).Want(403)
	ws, _ := util.ParseUUID(testWorkspaceID)
	wid, _ := util.ParseUUID(created.ID)
	hid, _ := util.ParseUUID(human)
	aid, _ := util.ParseUUID(agent)
	published, e := testHandler.WorkflowSvc.PublishRevision(context.Background(), ws, wid, service.WorkflowActor{HumanID: hid, AuditType: "agent", AuditID: aid}, 0, "export default {}", json.RawMessage(`{"steps":[{"id":"one"}]}`))
	if e != nil {
		t.Fatal(e)
	}
	if _, e = testHandler.WorkflowSvc.AcknowledgeRevision(context.Background(), ws, published.Revision.ID, published.Revision.DefinitionHash, true); e != nil {
		t.Fatal(e)
	}
	var trigger struct {
		ID        string `json:"id"`
		Token     string `json:"token"`
		Principal string `json:"principal_user_id"`
	}
	req = withURLParam(caller.request("POST", "/api/workflows/"+created.ID+"/triggers", map[string]any{"kind": "api", "input_template": map[string]any{"secret": "dont leak"}}), "id", created.ID)
	testutil.Call(t, testHandler.CreateWorkflowTrigger, req).Want(201).JSON(&trigger)
	if trigger.Principal != human || trigger.Token == "" {
		t.Fatal("missing frozen principal/token")
	}
	req = withURLParams(newRequest("POST", "/api/workflow-triggers/"+trigger.Token, map[string]any{"password": "dont leak"}), "token", trigger.Token)
	req.Header.Set("X-Workspace-ID", "invalid-header-is-ignored")
	req.Header.Set("X-User-ID", "invalid-user-is-ignored")
	var run struct {
		ID        string `json:"id"`
		Principal string `json:"principal_user_id"`
		Status    string `json:"status"`
	}
	response := testutil.Call(t, testHandler.HandleWorkflowIngress, req).Want(202)
	response.JSON(&run)
	if run.Principal != human || run.Status != "skipped" || strings.Contains(response.Text(), "dont leak") {
		t.Fatal("ingress identity, admission or run redaction failed")
	}
	req = withURLParams(caller.request("POST", "/api/workflows/"+created.ID+"/runs/"+run.ID+"/replay", nil), "id", created.ID, "runId", run.ID)
	var replay struct {
		ID string `json:"id"`
	}
	testutil.Call(t, testHandler.ReplayWorkflowRun, req).Want(202).JSON(&replay)
	if replay.ID == run.ID {
		t.Fatal("replay reused run")
	}
	for i := 0; i < 2; i++ {
		req = withURLParams(caller.request("POST", "/api/workflows/"+created.ID+"/runs/"+run.ID+"/cancel", nil), "id", created.ID, "runId", run.ID)
		testutil.Call(t, testHandler.CancelWorkflowRun, req).Want(200)
	}
	req = withURLParam(caller.request("GET", "/api/workflows/"+created.ID+"/triggers", nil), "id", created.ID)
	raw := testutil.Call(t, testHandler.ListWorkflowTriggers, req).Want(200).Text()
	if strings.Contains(raw, "token_hash") || strings.Contains(raw, trigger.Token) {
		t.Fatal("credential leaked")
	}
	req = withURLParam(caller.request("GET", "/api/workflows/"+created.ID+"/runs?limit=101", nil), "id", created.ID)
	testutil.Call(t, testHandler.ListWorkflowRuns, req).Want(400)
	req = withURLParam(caller.request("POST", "/api/workflows/"+created.ID+"/run", map[string]any{"input": strings.Repeat("a", service.WorkflowMaxPayloadBytes)}), "id", created.ID)
	testutil.Call(t, testHandler.RunWorkflow, req).Want(400)
	dbfx.Exec(t, "UPDATE agent_task_queue SET status='completed',completed_at=now() WHERE id=$1", task)
	req = withURLParam(caller.request("POST", "/api/workflows/"+created.ID+"/triggers", map[string]any{"kind": "api"}), "id", created.ID)
	testutil.Call(t, testHandler.CreateWorkflowTrigger, req).Want(403)
}

func TestWorkflowHTTPCreatePendingAndCrossWorkspace(t *testing.T) {
	var created struct {
		ID        string    `json:"id"`
		UpdatedAt time.Time `json:"updated_at"`
	}
	testutil.Call(t, testHandler.CreateWorkflow, newRequest("POST", "/api/workflows?workspace_id="+testWorkspaceID, map[string]any{"name": "Workflow", "slug": "workflow-http"})).Want(http.StatusCreated).JSON(&created)
	dbfx.Cleanup(t, "DELETE FROM workflow WHERE id=$1", created.ID)
	dbfx.Cleanup(t, "DELETE FROM workflow_revision WHERE workflow_id=$1", created.ID)
	var agent string
	dbfx.QueryRow(t, "SELECT id FROM agent WHERE workspace_id=$1 LIMIT 1", testWorkspaceID).Scan(&agent)
	patch := withURLParam(newRequest("PATCH", "/api/workflows/"+created.ID, map[string]any{"name": "Updated Workflow", "expected_updated_at": created.UpdatedAt, "default_agent_id": agent}), "id", created.ID)
	testutil.Call(t, testHandler.UpdateWorkflow, patch).Want(200)
	req := newRequest("POST", "/api/workflows/"+created.ID+"/revisions?workspace_id="+testWorkspaceID, map[string]any{"expected_version": 0, "definition": "throw new Error('server must not evaluate')", "definition_graph": map[string]any{"steps": []any{map[string]any{"id": "one"}}}, "validated": true})
	req = withURLParam(req, "id", created.ID)
	testutil.Call(t, testHandler.PublishWorkflowRevision, req).Want(http.StatusBadRequest)
	req = newRequest("POST", "/api/workflows/"+created.ID+"/revisions?workspace_id="+testWorkspaceID, map[string]any{"expected_version": 0, "definition": "throw new Error('server must not evaluate')", "definition_graph": map[string]any{"steps": []any{map[string]any{"id": "one"}}}})
	req = withURLParam(req, "id", created.ID)
	var published struct {
		RegistrationState string `json:"registration_state"`
		Revision          struct {
			DefinitionGraph service.WorkflowGraph `json:"definition_graph"`
		} `json:"revision"`
	}
	testutil.Call(t, testHandler.PublishWorkflowRevision, req).Want(http.StatusAccepted).JSON(&published)
	if published.RegistrationState != "pending_validation" {
		t.Fatal(published)
	}
	if len(published.Revision.DefinitionGraph.Steps) != 1 {
		t.Fatal("graph was not projected as JSON")
	}
	other := dbfx.Insert(t, "workspace", testutil.Cols{"name": "other", "slug": "other-workflow-http"})
	req = withURLParam(newRequest("GET", "/api/workflows/"+created.ID+"?workspace_id="+other, nil), "id", created.ID)
	req.Header.Set("X-Workspace-ID", other)
	testutil.Call(t, testHandler.GetWorkflow, req).Want(http.StatusNotFound)
	req = withURLParam(newRequest("DELETE", "/api/workflows/"+created.ID, nil), "id", created.ID)
	testutil.Call(t, testHandler.DeleteWorkflow, req).Want(204)
	req = withURLParam(newRequest("GET", "/api/workflows/"+created.ID, nil), "id", created.ID)
	testutil.Call(t, testHandler.GetWorkflow, req).Want(404)
}
