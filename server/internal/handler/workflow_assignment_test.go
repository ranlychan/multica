package handler

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/multica-ai/multica/server/internal/auth"
	"github.com/multica-ai/multica/server/internal/hatchet"
	"github.com/multica-ai/multica/server/internal/service"
	"github.com/multica-ai/multica/server/internal/testutil"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

func TestWorkflowAssignmentFirstBindingRechecksAuthorityBeforeCAS(t *testing.T) {
	for _, change := range []string{"cancel", "generation", "principal", "control"} {
		t.Run(change, func(t *testing.T) {
			b := newWorkflowBoundary(t)
			testutil.Call(t, b.h.WorkflowRunnerAcknowledge, b.request("POST", "ack", b.ready)).Want(200)
			principal := plainMember(t, "assignment-principal")
			dbfx.Exec(t, "INSERT INTO workflow_collaborator(workspace_id,workflow_id,user_id,role,created_by_id) VALUES($1,$2,$3,'editor',$4)", testWorkspaceID, b.workflowID, principal, testUserID)
			dbfx.Cleanup(t, "DELETE FROM workflow_collaborator WHERE workflow_id=$1", b.workflowID)
			w, err := b.h.WorkflowSvc.Load(context.Background(), parseUUID(testWorkspaceID), b.workflowID)
			if err != nil {
				t.Fatal(err)
			}
			run, err := b.h.WorkflowSvc.AdmitRun(context.Background(), w, service.WorkflowActor{HumanID: parseUUID(principal), AuditID: parseUUID(principal), AuditType: "member"}, "manual", pgtype.UUID{}, json.RawMessage(`{}`))
			if err != nil {
				t.Fatal(err)
			}
			dbfx.Exec(t, "UPDATE workflow_run SET status='submitting',submission_attempts=1,submission_started_at=now(),submission_deadline=now()+interval '55 minutes' WHERE id=$1", run.ID)
			called := false
			b.h.WorkflowControl.Action = func(context.Context, WorkflowActionAuthority, WorkflowActionRequest) (json.RawMessage, error) {
				called = true
				return json.RawMessage(`{}`), nil
			}
			b.h.WorkflowControl.Assignments = workflowAssignmentCheck(func(_ context.Context, a hatchet.Assignment) error {
				if a.LocalRunID != uuidToString(run.ID) {
					t.Fatal("missing pre-ACK input proof")
				}
				switch change {
				case "cancel":
					dbfx.Exec(t, "UPDATE workflow_run SET cancel_requested_at=now(),cancel_requested_by_id=$2 WHERE id=$1", run.ID, testUserID)
				case "generation":
					dbfx.Exec(t, "UPDATE workflow_runner_token SET revoked_at=now() WHERE id=$1", b.cred.Generation)
				case "control":
					dbfx.Exec(t, "UPDATE workflow_daemon SET capable=false WHERE workspace_id=$1 AND control_hash=$2", testWorkspaceID, auth.HashToken(b.control))
				case "principal":
					dbfx.Exec(t, "DELETE FROM member WHERE workspace_id=$1 AND user_id=$2", testWorkspaceID, principal)
				}
				return nil
			})
			req := WorkflowActionRequest{Protocol: 1, RunID: uuidToString(run.ID), StepID: "a", EngineRunID: uuid.NewString(), EngineTaskID: uuid.NewString(), InvocationCount: 1, Operation: "issues.get", Payload: json.RawMessage(`{}`)}
			response := testutil.Call(t, b.h.WorkflowRunnerAction, assignmentRequest(b, req))
			if response.Code == 200 {
				t.Fatal("revoked authority executed action")
			}
			current, err := b.h.Queries.GetWorkflowRunInWorkspace(context.Background(), db.GetWorkflowRunInWorkspaceParams{ID: run.ID, WorkspaceID: parseUUID(testWorkspaceID)})
			if err != nil {
				t.Fatal(err)
			}
			if current.HatchetRunID.Valid || called {
				t.Fatalf("revoked authority bound engine ID or ran hook: bound=%t hook=%t", current.HatchetRunID.Valid, called)
			}
		})
	}
}

func assignmentRequest(b *workflowBoundary, req WorkflowActionRequest) *http.Request {
	path := "/api/workflow-runs/" + req.RunID + "/actions"
	body, _ := json.Marshal(req)
	r := withURLParam(newRequest("POST", path, nil), "id", req.RunID)
	r.Body = io.NopCloser(strings.NewReader(string(body)))
	r.Header.Set("Authorization", "Bearer "+b.cred.RunnerToken)
	r.Header.Set("X-Workspace-ID", testWorkspaceID)
	r.Header.Set("X-Multica-Workflow-Generation", b.cred.Generation)
	mac := hmac.New(sha256.New, []byte(b.cred.RunnerToken))
	mac.Write([]byte("POST\n" + path + "\n" + testWorkspaceID + "\n" + b.cred.Generation + "\n"))
	mac.Write(body)
	r.Header.Set("X-Multica-Workflow-Signature", hex.EncodeToString(mac.Sum(nil)))
	return r
}

func TestWorkflowAssignmentSubmissionWindowAppliesOnlyToFirstBinding(t *testing.T) {
	for _, state := range []string{"expired", "unattempted", "bound"} {
		t.Run(state, func(t *testing.T) {
			b := newWorkflowBoundary(t)
			testutil.Call(t, b.h.WorkflowRunnerAcknowledge, b.request("POST", "ack", b.ready)).Want(200)
			w, err := b.h.WorkflowSvc.Load(context.Background(), parseUUID(testWorkspaceID), b.workflowID)
			if err != nil {
				t.Fatal(err)
			}
			run, err := b.h.WorkflowSvc.AdmitRun(context.Background(), w, service.WorkflowActor{HumanID: parseUUID(testUserID), AuditID: parseUUID(testUserID), AuditType: "member"}, "manual", pgtype.UUID{}, json.RawMessage(`{}`))
			if err != nil {
				t.Fatal(err)
			}
			engineID := uuid.NewString()
			switch state {
			case "expired":
				dbfx.Exec(t, "UPDATE workflow_run SET status='submitting',submission_attempts=1,submission_started_at=now()-interval '56 minutes',submission_deadline=now()-interval '1 minute' WHERE id=$1", run.ID)
			case "unattempted":
				dbfx.Exec(t, "UPDATE workflow_run SET status='submitting',submission_started_at=now(),submission_deadline=now()+interval '55 minutes' WHERE id=$1", run.ID)
			case "bound":
				dbfx.Exec(t, "UPDATE workflow_run SET status='running',hatchet_run_id=$2,submission_attempts=1,submission_started_at=now()-interval '2 days',submission_deadline=now()-interval '2 days'+interval '55 minutes' WHERE id=$1", run.ID, engineID)
			}
			called := false
			b.h.WorkflowControl.Assignments = workflowAssignmentCheck(func(_ context.Context, a hatchet.Assignment) error {
				if state != "bound" {
					t.Fatal("unbound outside permitted first-bind window reached verifier")
				}
				if a.LocalRunID != "" || !a.SubmissionStartedAt.IsZero() {
					t.Fatal("bound descendant constrained by first-bind window")
				}
				return nil
			})
			b.h.WorkflowControl.Action = func(context.Context, WorkflowActionAuthority, WorkflowActionRequest) (json.RawMessage, error) {
				called = true
				return json.RawMessage(`{}`), nil
			}
			req := WorkflowActionRequest{Protocol: 1, RunID: uuidToString(run.ID), StepID: "a", EngineRunID: engineID, EngineTaskID: uuid.NewString(), InvocationCount: 1, Operation: "issues.get", Payload: json.RawMessage(`{}`)}
			want := 503
			if state == "bound" {
				want = 200
			}
			testutil.Call(t, b.h.WorkflowRunnerAction, assignmentRequest(b, req)).Want(want)
			if called != (state == "bound") {
				t.Fatal("wrong action execution", called)
			}
		})
	}
}
