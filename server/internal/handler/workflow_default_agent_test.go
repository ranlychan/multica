package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/multica-ai/multica/server/internal/auth"
	"github.com/multica-ai/multica/server/internal/middleware"
	"github.com/multica-ai/multica/server/internal/testutil"
	"github.com/multica-ai/multica/server/internal/util"
)

// The ordinary-agent contract is capability, not a grant to the machine owner:
// a task token acts for its human even without custom instructions or skills.
func TestWorkflowDefaultAgentCanManageExistingSurface(t *testing.T) {
	human := plainMember(t, "workflow-default-human")
	peer := plainMember(t, "workflow-default-peer")
	agent := dbfx.Agent(t, "ordinary workflow operator", testRuntimeID, testutil.Cols{"instructions": ""})
	task := callerTask(t, agent, human)
	token := "mat_" + uuid.NewString()
	dbfx.Insert(t, "task_token", testutil.Cols{"token_hash": auth.HashToken(token), "task_id": task, "agent_id": agent, "workspace_id": testWorkspaceID, "user_id": testUserID, "expires_at": testutil.Raw("now()+interval '1 hour'")})
	agentID, err := util.ParseUUID(agent)
	if err != nil {
		t.Fatal(err)
	}
	skills, refs, err := testHandler.TaskService.LoadAgentSkillBundles(context.Background(), agentID, "", false)
	if err != nil {
		t.Fatal(err)
	}
	if len(refs) == 0 {
		t.Fatal("ordinary agent did not receive built-in skill refs")
	}
	found := false
	for _, skill := range skills {
		if skill.Name != "multica-platform" {
			continue
		}
		for _, file := range skill.Files {
			if file.Path == "references/workflows.md" && file.Content != "" {
				found = true
			}
		}
	}
	if !found {
		t.Fatal("unbound ordinary agent lacks default workflow reference")
	}

	router := chi.NewRouter()
	router.Use(middleware.Auth(testHandler.Queries, nil, nil, nil))
	router.Use(middleware.RequireWorkspaceMember(testHandler.Queries))
	router.Post("/api/workflows", testHandler.CreateWorkflow)
	router.Get("/api/workflows", testHandler.ListWorkflows)
	router.Get("/api/workflows/{id}", testHandler.GetWorkflow)
	router.Patch("/api/workflows/{id}", testHandler.UpdateWorkflow)
	router.Delete("/api/workflows/{id}", testHandler.DeleteWorkflow)
	router.Post("/api/workflows/{id}/revisions", testHandler.PublishWorkflowRevision)
	router.Get("/api/workflows/{id}/revisions", testHandler.ListWorkflowRevisions)
	router.Get("/api/workflows/{id}/revisions/{revisionId}", testHandler.GetWorkflowRevision)
	router.Post("/api/workflows/{id}/triggers", testHandler.CreateWorkflowTrigger)
	router.Get("/api/workflows/{id}/triggers", testHandler.ListWorkflowTriggers)
	router.Patch("/api/workflows/{id}/triggers/{triggerId}", testHandler.UpdateWorkflowTrigger)
	router.Delete("/api/workflows/{id}/triggers/{triggerId}", testHandler.DeleteWorkflowTrigger)
	router.Post("/api/workflows/{id}/collaborators", testHandler.AddWorkflowCollaborator)
	router.Get("/api/workflows/{id}/collaborators", testHandler.ListWorkflowCollaborators)
	router.Delete("/api/workflows/{id}/collaborators/{userId}", testHandler.RemoveWorkflowCollaborator)
	router.Post("/api/workflows/{id}/run", testHandler.RunWorkflow)
	router.Get("/api/workflows/{id}/runs", testHandler.ListWorkflowRuns)
	router.Get("/api/workflows/{id}/runs/{runId}", testHandler.GetWorkflowRun)
	router.Get("/api/workflows/{id}/runs/{runId}/logs", testHandler.WorkflowRunLogs)
	router.Post("/api/workflows/{id}/runs/{runId}/cancel", testHandler.CancelWorkflowRun)
	router.Post("/api/workflows/{id}/runs/{runId}/replay", testHandler.ReplayWorkflowRun)
	call := func(method, path string, body any, status int) *testutil.Response {
		r := newRequest(method, path, body)
		r.Header.Set("Authorization", "Bearer "+token)
		return testutil.Call(t, router.ServeHTTP, r).Want(status)
	}
	created := call("POST", "/api/workflows", map[string]any{"name": "ordinary agent workflow", "slug": "ordinary-" + uuid.NewString()}, 201).Map()
	id := created["id"].(string)
	base := "/api/workflows/" + id
	for _, table := range []string{"workflow", "workflow_revision", "workflow_trigger", "workflow_collaborator", "workflow_run"} {
		query := "DELETE FROM " + table + " WHERE workflow_id=$1"
		if table == "workflow" {
			query = "DELETE FROM workflow WHERE id=$1"
		}
		dbfx.Cleanup(t, query, id)
	}
	if created["created_by_id"] != human || created["audit_actor_id"] != agent {
		t.Fatal("default agent lost human ownership or audit actor")
	}
	call("GET", "/api/workflows", nil, 200)
	call("GET", base, nil, 200)
	call("PATCH", base, map[string]any{"name": "ordinary agent edited", "expected_updated_at": created["updated_at"]}, 200)
	published := call("POST", base+"/revisions", map[string]any{"expected_version": 0, "definition": "export default {}", "definition_graph": map[string]any{"steps": []any{map[string]any{"id": "first", "parents": []any{}}}}}, 202).Map()
	revision := published["revision"].(map[string]any)
	ws, _ := util.ParseUUID(testWorkspaceID)
	rid, _ := util.ParseUUID(revision["id"].(string))
	if _, err := testHandler.WorkflowSvc.AcknowledgeRevision(context.Background(), ws, rid, revision["definition_hash"].(string), true); err != nil {
		t.Fatal(err)
	}
	call("GET", base+"/revisions", nil, 200)
	call("GET", base+"/revisions/"+revision["id"].(string), nil, 200)
	trigger := call("POST", base+"/triggers", map[string]any{"kind": "api"}, 201).Map()
	tid := trigger["id"].(string)
	call("GET", base+"/triggers", nil, 200)
	call("PATCH", base+"/triggers/"+tid, map[string]any{"enabled": false, "label": "edited by ordinary agent"}, 200)
	call("POST", base+"/collaborators", map[string]any{"user_id": peer, "role": "editor"}, 200)
	call("GET", base+"/collaborators", nil, 200)
	call("DELETE", base+"/collaborators/"+peer, nil, 204)

	// Infrastructure may be disabled, but that must not become an agent ACL failure.
	ready := testHandler.WorkflowSvc.Ready
	testHandler.WorkflowSvc.Ready = nil
	t.Cleanup(func() { testHandler.WorkflowSvc.Ready = ready })
	refused := call("POST", base+"/run", map[string]any{"input": map[string]any{"message": "ordinary agent"}}, http.StatusServiceUnavailable)
	if refused.Map()["code"] != "workflow_engine_unavailable" {
		t.Fatal("manual run did not reach infrastructure admission")
	}
	var runs []struct {
		ID string `json:"id"`
	}
	call("GET", base+"/runs", nil, 200).JSON(&runs)
	if len(runs) != 1 {
		t.Fatalf("run ledger count %d, want 1", len(runs))
	}
	runPath := base + "/runs/" + runs[0].ID
	call("GET", runPath, nil, 200)
	call("GET", runPath+"/logs", nil, 200)
	call("POST", runPath+"/cancel", nil, 200)
	call("POST", runPath+"/replay", json.RawMessage(`{}`), 503)
	call("DELETE", base+"/triggers/"+tid, nil, 204)
	call("DELETE", base, nil, 204)
	call("GET", base, nil, 404)
}
