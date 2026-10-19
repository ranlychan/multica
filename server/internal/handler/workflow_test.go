package handler

import (
	"context"
	"encoding/json"
	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
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

func TestWorkflowHTTPObservabilityAuthorityAndAvailability(t *testing.T) {
	human := plainMember(t, "workflow-observer-author")
	reader := plainMember(t, "workflow-observer-reader")
	caller := actingCaller{authUserID: human}
	var created struct {
		ID string `json:"id"`
	}
	testutil.Call(t, testHandler.CreateWorkflow, caller.request("POST", "/api/workflows", map[string]any{"name": "observable", "slug": "observable"})).Want(201).JSON(&created)
	dbfx.Cleanup(t, "DELETE FROM workflow WHERE id=$1", created.ID)
	dbfx.Cleanup(t, "DELETE FROM workflow_revision WHERE workflow_id=$1", created.ID)
	ws, _ := util.ParseUUID(testWorkspaceID)
	wid, _ := util.ParseUUID(created.ID)
	hid, _ := util.ParseUUID(human)
	pub, err := testHandler.WorkflowSvc.PublishRevision(context.Background(), ws, wid, service.WorkflowActor{HumanID: hid}, 0, "export default {}", json.RawMessage(`{"steps":[{"id":"one"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = testHandler.WorkflowSvc.AcknowledgeRevision(context.Background(), ws, pub.Revision.ID, pub.Revision.DefinitionHash, true); err != nil {
		t.Fatal(err)
	}
	svc := testHandler.WorkflowSvc
	oldReady, oldEligible := svc.Ready, svc.Eligible
	t.Cleanup(func() { svc.Ready, svc.Eligible = oldReady, oldEligible })
	for _, tc := range []struct {
		name, user, state      string
		ready, eligible, write bool
	}{
		{"disabled", human, "disabled", false, false, true},
		{"unrunnable", human, "unrunnable", true, false, true},
		{"author", human, "ready", true, true, true},
		{"reader", reader, "ready", true, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			svc.Ready = nil
			if tc.ready {
				svc.Ready = func() bool { return true }
			}
			svc.Eligible = func(context.Context, pgtype.UUID, pgtype.UUID) bool { return tc.eligible }
			req := withURLParam((actingCaller{authUserID: tc.user}).request("GET", "/api/workflows/"+created.ID, nil), "id", created.ID)
			got := testutil.Call(t, testHandler.GetWorkflow, req).Want(200).Map()
			availability, ok := got["runtime_availability"].(map[string]any)
			if !ok || availability["state"] != tc.state || got["can_write"] != tc.write {
				t.Fatalf("projection: %+v", got)
			}
		})
	}
}

func TestWorkflowHTTPTaskTokenMiddlewareBindsActorWorkspaceAndCurrentOriginator(t *testing.T) {
	human := plainMember(t, "workflow-token-human")
	var agent string
	dbfx.QueryRow(t, "SELECT id FROM agent WHERE workspace_id=$1 LIMIT 1", testWorkspaceID).Scan(&agent)
	task := callerTask(t, agent, human)
	token := "mat_" + uuid.NewString()
	dbfx.Insert(t, "task_token", testutil.Cols{"token_hash": auth.HashToken(token), "task_id": task, "agent_id": agent, "workspace_id": testWorkspaceID, "user_id": testUserID, "expires_at": testutil.Raw("now()+interval '1 hour'")})
	route := middleware.Auth(testHandler.Queries, nil, nil, nil)(middleware.RequireWorkspaceMember(testHandler.Queries)(http.HandlerFunc(testHandler.CreateWorkflow)))
	request := func(slug string) *http.Request {
		r := newRequest("POST", "/api/workflows?workspace_slug=forged", map[string]any{"name": "token workflow", "slug": slug})
		r.Header.Set("Authorization", "Bearer "+token)
		r.Header.Set("X-Workspace-ID", uuid.NewString())
		r.Header.Set("X-Workspace-Slug", "forged")
		r.Header.Set("X-Agent-ID", human)
		r.Header.Set("X-Task-ID", uuid.NewString())
		r.Header.Set("X-Actor-Source", "member")
		return r
	}
	var result struct {
		ID        string `json:"id"`
		Creator   string `json:"created_by_id"`
		Actor     string `json:"audit_actor_id"`
		Workspace string `json:"workspace_id"`
	}
	testutil.Call(t, route.ServeHTTP, request("workflow-real-token")).Want(201).JSON(&result)
	dbfx.Cleanup(t, "DELETE FROM workflow WHERE id=$1", result.ID)
	if result.Creator != human || result.Actor != agent || result.Workspace != testWorkspaceID {
		t.Fatal("task token binding or human resolution failed")
	}
	dbfx.Exec(t, "UPDATE agent_task_queue SET status='completed',completed_at=now() WHERE id=$1", task)
	response := testutil.Call(t, route.ServeHTTP, request("workflow-real-token-terminal")).Want(403)
	if response.Map()["code"] != "workflow_no_originator" {
		t.Fatal(response.Text())
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
	response := testutil.Call(t, testHandler.HandleWorkflowIngress, req).Want(503)
	if response.Map()["code"] != "workflow_engine_unavailable" {
		t.Fatal(response.Text())
	}
	var storedID string
	dbfx.QueryRow(t, "SELECT id FROM workflow_run WHERE workflow_id=$1 ORDER BY triggered_at DESC LIMIT 1", created.ID).Scan(&storedID)
	req = withURLParams(caller.request("GET", "/api/workflows/"+created.ID+"/runs/"+storedID, nil), "id", created.ID, "runId", storedID)
	testutil.Call(t, testHandler.GetWorkflowRun, req).Want(200).JSON(&run)
	if run.Principal != human || run.Status != "skipped" || strings.Contains(response.Text(), "dont leak") {
		t.Fatal("ingress identity, admission or run redaction failed")
	}
	req = withURLParams(caller.request("POST", "/api/workflows/"+created.ID+"/runs/"+run.ID+"/replay", nil), "id", created.ID, "runId", run.ID)
	var replay struct {
		ID string `json:"id"`
	}
	testutil.Call(t, testHandler.ReplayWorkflowRun, req).Want(503)
	dbfx.QueryRow(t, "SELECT id FROM workflow_run WHERE workflow_id=$1 ORDER BY triggered_at DESC LIMIT 1", created.ID).Scan(&replay.ID)
	if replay.ID == run.ID {
		t.Fatal("replay reused run")
	}
	req = withURLParam(caller.request("POST", "/api/workflows/"+created.ID+"/run", map[string]any{"input": map[string]any{"password": "manual-secret", "ticket": "ABC-42"}}), "id", created.ID)
	manual := testutil.Call(t, testHandler.RunWorkflow, req).Want(503)
	if manual.Map()["code"] != "workflow_engine_unavailable" {
		t.Fatal(manual.Text())
	}
	dbfx.QueryRow(t, "SELECT id FROM workflow_run WHERE workflow_id=$1 ORDER BY triggered_at DESC LIMIT 1", created.ID).Scan(&storedID)
	req = withURLParams(caller.request("GET", "/api/workflows/"+created.ID+"/runs/"+storedID, nil), "id", created.ID, "runId", storedID)
	detail := testutil.Call(t, testHandler.GetWorkflowRun, req).Want(200).Map()
	input, ok := detail["input"].(map[string]any)
	if !ok || input["ticket"] != "ABC-42" || input["password"] != "[redacted]" {
		t.Fatalf("missing/redaction failure input view: %+v", detail)
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
	var updated struct {
		UpdatedAt time.Time `json:"updated_at"`
	}
	testutil.Call(t, testHandler.GetWorkflow, withURLParam(newRequest("GET", "/api/workflows/"+created.ID, nil), "id", created.ID)).Want(200).JSON(&updated)
	for _, slug := range []string{"Upper", "with space", "has/slash"} {
		patch = withURLParam(newRequest("PATCH", "/api/workflows/"+created.ID, map[string]any{"slug": slug, "expected_updated_at": updated.UpdatedAt}), "id", created.ID)
		testutil.Call(t, testHandler.UpdateWorkflow, patch).Want(400)
	}
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

func TestWorkflowHTTPDetailProjectsBoundedRedactedInput(t *testing.T) {
	wf := dbfx.Insert(t, "workflow", testutil.Cols{"workspace_id": testWorkspaceID, "name": "input", "slug": "workflow-detail-input", "created_by_type": "member", "created_by_id": testUserID})
	id := uuid.NewString()
	dbfx.Insert(t, "workflow_run", testutil.Cols{"id": id, "workspace_id": testWorkspaceID, "workflow_id": wf, "revision_id": testUserID, "definition_hash": "hash", "principal_user_id": testUserID, "source": "manual", "idempotency_key": id, "input": `{"ticket":"ABC-42","password":"hidden","nested":[{"api_key":"key-hidden","count":3}],"note":"Bearer bearer-hidden"}`})
	req := withURLParams(newRequest("GET", "/api/workflows/"+wf+"/runs/"+id, nil), "id", wf, "runId", id)
	response := testutil.Call(t, testHandler.GetWorkflowRun, req).Want(200)
	detail := response.Map()
	input, ok := detail["input"].(map[string]any)
	if !ok || input["ticket"] != "ABC-42" || input["password"] != "[redacted]" {
		t.Fatalf("input missing or invalid: %+v", detail)
	}
	if strings.Contains(response.Text(), "hidden") {
		t.Fatal("input secret leaked")
	}
	bigID := uuid.NewString()
	dbfx.Insert(t, "workflow_run", testutil.Cols{"id": bigID, "workspace_id": testWorkspaceID, "workflow_id": wf, "revision_id": testUserID, "definition_hash": "hash", "principal_user_id": testUserID, "source": "manual", "idempotency_key": bigID, "input": `{"large":"` + strings.Repeat("x", service.WorkflowMaxPayloadBytes) + `"}`})
	req = withURLParams(newRequest("GET", "/api/workflows/"+wf+"/runs/"+bigID, nil), "id", wf, "runId", bigID)
	response = testutil.Call(t, testHandler.GetWorkflowRun, req).Want(200)
	if len(response.Text()) > service.WorkflowMaxPayloadBytes+4096 {
		t.Fatal("detail input unbounded")
	}
}

func TestWorkflowHTTPDetailExposesBoundedProductSteps(t *testing.T) {
	wf := dbfx.Insert(t, "workflow", testutil.Cols{"workspace_id": testWorkspaceID, "name": "steps", "slug": "workflow-detail-steps", "created_by_type": "member", "created_by_id": testUserID})
	id := uuid.NewString()
	internalID := uuid.NewString()
	dbfx.Insert(t, "workflow_run", testutil.Cols{"id": id, "workspace_id": testWorkspaceID, "workflow_id": wf, "revision_id": testUserID, "definition_hash": "hash", "principal_user_id": testUserID, "source": "manual", "idempotency_key": id, "step_projection": `[{"id":"authored-a","status":"RUNNING","attempt":2,"started_at":"2026-10-12T00:00:00Z","taskExternalId":"` + internalID + `","token":"wft_hidden","path":"/Users/hidden"}]`})
	req := withURLParams(newRequest("GET", "/api/workflows/"+wf+"/runs/"+id, nil), "id", wf, "runId", id)
	response := testutil.Call(t, testHandler.GetWorkflowRun, req).Want(200)
	steps, ok := response.Map()["steps"].([]any)
	if !ok || len(steps) != 1 {
		t.Fatalf("product steps missing: %s", response.Text())
	}
	step := steps[0].(map[string]any)
	if len(step) != 4 || step["id"] != "authored-a" || step["status"] != "RUNNING" || step["attempt"] != float64(2) || step["started_at"] != "2026-10-12T00:00:00Z" {
		t.Fatalf("step fields changed: %+v", step)
	}
	if strings.Contains(response.Text(), internalID) || strings.Contains(response.Text(), "hidden") {
		t.Fatal("raw engine identity/path/credential leaked")
	}
	summary := testutil.Call(t, testHandler.ListWorkflowRuns, withURLParams(newRequest("GET", "/api/workflows/"+wf+"/runs", nil), "id", wf)).Want(200)
	if strings.Contains(summary.Text(), "authored-a") {
		t.Fatal("list returned step details")
	}
	oversized := strings.Repeat(`{"id":"a","status":"RUNNING"},`, service.WorkflowMaxSteps) + `{"id":"b","status":"RUNNING"}`
	dbfx.Exec(t, "UPDATE workflow_run SET step_projection=$2 WHERE id=$1", id, "["+oversized+"]")
	response = testutil.Call(t, testHandler.GetWorkflowRun, req).Want(200)
	steps, ok = response.Map()["steps"].([]any)
	if !ok || len(steps) != 0 {
		t.Fatal("historical step projection unbounded")
	}
}
