package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/multica-ai/multica/server/internal/auth"
	"github.com/multica-ai/multica/server/internal/hatchet"
	"github.com/multica-ai/multica/server/internal/service"
	wr "github.com/multica-ai/multica/server/internal/workflowrunner"
	db "github.com/multica-ai/multica/server/pkg/db/generated"

	"github.com/multica-ai/multica/server/internal/testutil"
)

func TestWorkflowTriggerPublicTemplateRedactionPreservesStoredInput(t *testing.T) {
	b := newWorkflowBoundary(t)
	reader := actingCaller{authUserID: plainMember(t, "trigger-template-reader")}
	template := json.RawMessage(`{"ticket":"ABC-42","password":"private-password","nested":{"token":"wft_private","items":[{"authorization":"Bearer private-bearer"},"Bearer private-inline"]}}`)
	var trigger struct {
		ID string `json:"id"`
	}
	create := withURLParam(newRequest("POST", "/api/workflows/"+uuidToString(b.workflowID)+"/triggers", map[string]any{"kind": "api", "input_template": template}), "id", uuidToString(b.workflowID))
	created := testutil.Call(t, b.h.CreateWorkflowTrigger, create).Want(201)
	created.JSON(&trigger)
	dbfx.Cleanup(t, "DELETE FROM workflow_trigger WHERE id=$1", trigger.ID)
	responses := map[string]string{"create": created.Text()}
	for _, operation := range []string{"update", "rotate", "member-list"} {
		var request *http.Request
		var handler http.HandlerFunc
		switch operation {
		case "update":
			request = withURLParams(newRequest("PATCH", "/api/workflows/"+uuidToString(b.workflowID)+"/triggers/"+trigger.ID, map[string]any{"input_template": template}), "id", uuidToString(b.workflowID), "triggerId", trigger.ID)
			handler = b.h.UpdateWorkflowTrigger
		case "rotate":
			request = withURLParams(newRequest("POST", "/api/workflows/"+uuidToString(b.workflowID)+"/triggers/"+trigger.ID+"/rotate-token", nil), "id", uuidToString(b.workflowID), "triggerId", trigger.ID)
			handler = b.h.RotateWorkflowTriggerToken
		case "member-list":
			request = withURLParam(reader.request("GET", "/api/workflows/"+uuidToString(b.workflowID)+"/triggers", nil), "id", uuidToString(b.workflowID))
			handler = b.h.ListWorkflowTriggers
		}
		responses[operation] = testutil.Call(t, handler, request).Want(200).Text()
	}
	for operation, response := range responses {
		t.Run(operation, func(t *testing.T) {
			for _, secret := range []string{"private-password", "wft_private", "private-bearer", "private-inline"} {
				if strings.Contains(response, secret) {
					t.Errorf("%s leaked stored credential %s: %s", operation, secret, response)
				}
			}
			if operation != "rotate" && (!strings.Contains(response, "ABC-42") || !strings.Contains(response, "[redacted]")) {
				t.Errorf("missing sanitized product input: %s", response)
			}
		})
	}
	var stored string
	if err := testPool.QueryRow(context.Background(), "SELECT input_template::text FROM workflow_trigger WHERE id=$1", trigger.ID).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	var got, want any
	if err := json.Unmarshal([]byte(stored), &got); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(template, &want); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("public projection changed private execution input: %s", stored)
	}
}

func finalFixRegistration(revision db.WorkflowRevision) wr.Registration {
	r := wr.Registration{Revision: wireRevision(revision), EngineWorkflowID: "20000000-0000-0000-0000-000000000002", EngineVersionID: "30000000-0000-0000-0000-000000000003"}
	for _, step := range r.Graph.Steps {
		name := service.WorkflowActionName(revision, step.ID)
		r.Actions = append(r.Actions, wr.Action{StepID: step.ID, TaskName: name, ActionID: revision.HatchetName + ":" + name})
	}
	return r
}

func TestWorkflowFailedFinalAckPreservesHealthyCurrentAndAllowsReplacement(t *testing.T) {
	for _, failure := range []string{"competing_proposals", "publisher_authority"} {
		t.Run(failure, func(t *testing.T) {
			ctx := context.Background()
			b := newWorkflowBoundary(t)
			testutil.Call(t, b.h.WorkflowRunnerAcknowledge, b.request("POST", "ack", b.ready)).Want(200)
			daemon, err := b.h.Queries.AuthorizeWorkflowControl(ctx, db.AuthorizeWorkflowControlParams{WorkspaceID: parseUUID(b.workspaceID), ControlHash: auth.HashToken(b.control)})
			if err != nil {
				t.Fatal(err)
			}
			current, err := b.h.Queries.GetWorkflowRunnerGeneration(ctx, db.GetWorkflowRunnerGenerationParams{WorkspaceID: parseUUID(b.workspaceID), DaemonID: daemon.DaemonID, ID: parseUUID(b.cred.Generation), ControlHash: daemon.ControlHash})
			if err != nil {
				t.Fatal(err)
			}
			actor := service.WorkflowActor{HumanID: parseUUID(testUserID), AuditID: parseUUID(testUserID), AuditType: "member"}
			publisher := actor
			if failure == "publisher_authority" {
				editor := plainMember(t, "final-ack-editor")
				dbfx.Exec(t, "INSERT INTO workflow_collaborator(workspace_id,workflow_id,user_id,role,created_by_id) VALUES($1,$2,$3,'editor',$4)", b.workspaceID, b.workflowID, editor, testUserID)
				dbfx.Cleanup(t, "DELETE FROM workflow_collaborator WHERE workflow_id=$1", b.workflowID)
				publisher.HumanID, publisher.AuditID = parseUUID(editor), parseUUID(editor)
			}
			candidateReady := b.ready
			candidateReady.Registrations = append([]wr.Registration(nil), b.ready.Registrations...)
			for i := 0; i < map[bool]int{true: 2, false: 1}[failure == "competing_proposals"]; i++ {
				proposal, err := b.h.WorkflowSvc.PublishRevision(ctx, parseUUID(b.workspaceID), b.workflowID, publisher, 1, fmt.Sprintf("proposal-%d", i), json.RawMessage(`{"steps":[{"id":"a","parents":[]}]}`))
				if err != nil {
					t.Fatal(err)
				}
				candidateReady.Registrations = append(candidateReady.Registrations, finalFixRegistration(proposal.Revision))
			}
			if failure == "publisher_authority" {
				dbfx.Exec(t, "DELETE FROM workflow_collaborator WHERE workflow_id=$1", b.workflowID)
			}
			var candidate wr.Credentials
			testutil.Call(t, b.h.WorkflowRunnerRotate, b.request("POST", "rotate", nil)).Want(200).JSON(&candidate)
			candidateReady.Generation = candidate.Generation
			testutil.Call(t, b.h.WorkflowRunnerPrepare, b.request("POST", "prepare", candidateReady)).Want(200)
			var prepared, failed string
			dbfx.QueryRow(t, "SELECT to_jsonb(t)::text FROM workflow_runner_token t WHERE id=$1", candidate.Generation).Scan(&prepared)
			testutil.Call(t, b.h.WorkflowRunnerAcknowledge, b.request("POST", "ack", candidateReady)).Want(409)
			dbfx.QueryRow(t, "SELECT to_jsonb(t)::text FROM workflow_runner_token t WHERE id=$1", candidate.Generation).Scan(&failed)
			if prepared != failed {
				t.Error("failed final ACK changed prepared candidate lease/registrations/phase")
			}
			var phase string
			var lease time.Time
			var registrations []byte
			dbfx.QueryRow(t, "SELECT phase,lease_until,registrations FROM workflow_runner_token WHERE id=$1", b.cred.Generation).Scan(&phase, &lease, &registrations)
			if phase != "current" || !lease.Equal(current.LeaseUntil.Time) || !reflect.DeepEqual(registrations, current.Registrations) {
				t.Errorf("failed ACK changed healthy current: phase=%s lease=%s", phase, lease)
			}
			dbfx.QueryRow(t, "SELECT phase FROM workflow_runner_token WHERE id=$1", candidate.Generation).Scan(&phase)
			if phase != "candidate" {
				t.Errorf("failed ACK promoted candidate to %s", phase)
			}
			var invalid, published int
			dbfx.QueryRow(t, "SELECT count(*) FILTER(WHERE validation_status='invalid'),count(*) FILTER(WHERE published_at IS NOT NULL) FROM workflow_revision WHERE workflow_id=$1", b.workflowID).Scan(&invalid, &published)
			wantPublished := 1
			if failure == "competing_proposals" {
				wantPublished = 2
			}
			if invalid != 1 || published != wantPublished {
				t.Fatalf("partial publication evidence: invalid=%d published=%d", invalid, published)
			}
			// The daemon owns the failed candidate cleanup, while its existing process
			// keeps renewing the old generation. Mirror that precise HTTP sequence.
			testutil.Call(t, b.h.WorkflowRunnerRetire, b.request("POST", "retire", map[string]string{"generation": candidate.Generation})).Want(200)
			testutil.Call(t, b.h.WorkflowRunnerAcknowledge, b.request("POST", "ack", b.ready)).Want(200)
			var replacement wr.Credentials
			testutil.Call(t, b.h.WorkflowRunnerRotate, b.request("POST", "rotate", nil)).Want(200).JSON(&replacement)
			next := b.ready
			next.Generation = replacement.Generation
			next.Registrations = nil
			rows, err := b.h.Queries.ListWorkflowRunnerRevisions(ctx, parseUUID(b.workspaceID))
			if err != nil {
				t.Fatal(err)
			}
			for _, revision := range rows {
				if revision.WorkflowID == b.workflowID {
					next.Registrations = append(next.Registrations, finalFixRegistration(revision))
				}
			}
			testutil.Call(t, b.h.WorkflowRunnerAcknowledge, b.request("POST", "ack", next)).Want(200)
			workflow, err := b.h.WorkflowSvc.Load(ctx, parseUUID(b.workspaceID), b.workflowID)
			if err != nil {
				t.Fatal(err)
			}
			run, err := b.h.WorkflowSvc.AdmitRun(ctx, workflow, actor, "manual", pgtype.UUID{}, json.RawMessage(`{}`))
			if err != nil || run.Status != "queued" || run.RevisionID != workflow.CurrentRevisionID {
				t.Fatalf("replacement did not restore admission: status=%s err=%v", run.Status, err)
			}
		})
	}
}

// Observe the exact request transaction waiting on an unchanged row, and let
// PostgreSQL's wall clock establish expiry before releasing that row.
func finalFixUnlockAfterExpiry(t *testing.T, ctx context.Context, blocker pgx.Tx, started <-chan int32, finished <-chan struct{}, expires time.Time) {
	t.Helper()
	var blockerPID, requestPID int32
	if err := blocker.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&blockerPID); err != nil {
		t.Fatal(err)
	}
	select {
	case requestPID = <-started:
	case <-ctx.Done():
		t.Fatal("request never began final transaction", ctx.Err())
	}
	for {
		var waiting bool
		if err := testPool.QueryRow(ctx, "SELECT $2::integer=ANY(pg_blocking_pids($1::integer))", requestPID, blockerPID).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		select {
		case <-finished:
			t.Fatal("request skipped unchanged held lock")
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(5 * time.Millisecond):
		}
	}
	for {
		var expired bool
		if err := testPool.QueryRow(ctx, "SELECT clock_timestamp()>$1::timestamptz", expires).Scan(&expired); err != nil {
			t.Fatal(err)
		}
		if expired {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
	if err := blocker.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-finished:
	case <-ctx.Done():
		t.Fatal("request did not finish after unlock", ctx.Err())
	}
}

func TestWorkflowControlFinalAckAndPreparePostLockExpiry(t *testing.T) {
	for _, operation := range []string{"ack", "prepare", "failed-ack"} {
		for _, expiry := range []string{"control", "generation"} {
			for _, lock := range []string{"workspace", "control", "generation", "publication", "current-generation"} {
				if operation == "prepare" && lock == "publication" {
					continue
				}
				if lock == "current-generation" && operation != "ack" {
					continue
				}
				t.Run(operation+"/"+expiry+"/"+lock, func(t *testing.T) {
					b := newWorkflowBoundary(t)
					// Keep a healthy current to detect accidental demotion on expiry.
					testutil.Call(t, b.h.WorkflowRunnerAcknowledge, b.request("POST", "ack", b.ready)).Want(200)
					ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
					defer cancel()
					actor := service.WorkflowActor{HumanID: parseUUID(testUserID), AuditID: parseUUID(testUserID), AuditType: "member"}
					proposal, err := b.h.WorkflowSvc.PublishRevision(ctx, parseUUID(b.workspaceID), b.workflowID, actor, 1, "post-lock-proposal", json.RawMessage(`{"steps":[{"id":"a","parents":[]}]}`))
					if err != nil {
						t.Fatal(err)
					}
					if operation == "failed-ack" {
						winner, err := b.h.WorkflowSvc.PublishRevision(ctx, parseUUID(b.workspaceID), b.workflowID, actor, 1, "preexisting-winner", json.RawMessage(`{"steps":[{"id":"a","parents":[]}]}`))
						if err != nil {
							t.Fatal(err)
						}
						if _, err = b.h.WorkflowSvc.AcknowledgeRevision(ctx, parseUUID(b.workspaceID), winner.Revision.ID, winner.Revision.DefinitionHash, true); err != nil {
							t.Fatal(err)
						}
					}
					var candidate wr.Credentials
					testutil.Call(t, b.h.WorkflowRunnerRotate, b.request("POST", "rotate", nil)).Want(200).JSON(&candidate)
					ready := b.ready
					ready.Generation = candidate.Generation
					ready.Registrations = []wr.Registration{finalFixRegistration(proposal.Revision)}
					var expires time.Time
					if expiry == "control" {
						err = testPool.QueryRow(ctx, "UPDATE workflow_daemon SET control_expires_at=clock_timestamp()+interval '1500 milliseconds' WHERE control_hash=$1 RETURNING control_expires_at", auth.HashToken(b.control)).Scan(&expires)
					} else {
						err = testPool.QueryRow(ctx, "UPDATE workflow_runner_token SET expires_at=clock_timestamp()+interval '1500 milliseconds' WHERE id=$1 RETURNING expires_at", candidate.Generation).Scan(&expires)
					}
					if err != nil {
						t.Fatal(err)
					}
					var before string
					dbfx.QueryRow(t, "SELECT jsonb_agg(to_jsonb(t) ORDER BY id)::text FROM workflow_runner_token t WHERE id IN ($1,$2)", b.cred.Generation, candidate.Generation).Scan(&before)
					blocker, err := testPool.Begin(ctx)
					if err != nil {
						t.Fatal(err)
					}
					defer blocker.Rollback(context.Background())
					switch lock {
					case "workspace":
						_, err = blocker.Exec(ctx, "SELECT id FROM workspace WHERE id=$1 FOR UPDATE", b.workspaceID)
					case "control":
						_, err = blocker.Exec(ctx, "SELECT daemon_id FROM workflow_daemon WHERE control_hash=$1 FOR UPDATE", auth.HashToken(b.control))
					case "generation":
						_, err = blocker.Exec(ctx, "SELECT id FROM workflow_runner_token WHERE id=$1 FOR UPDATE", candidate.Generation)
					case "publication":
						_, err = blocker.Exec(ctx, "SELECT id FROM workflow WHERE id=$1 FOR UPDATE", b.workflowID)
					case "current-generation":
						_, err = blocker.Exec(ctx, "SELECT id FROM workflow_runner_token WHERE id=$1 FOR UPDATE", b.cred.Generation)
					}
					if err != nil {
						t.Fatal(err)
					}
					started := make(chan int32, 1)
					b.h.TxStarter = workflowActionTransactionStarted{txStarter: b.h.TxStarter, started: started}
					recorder := httptest.NewRecorder()
					finished := make(chan struct{})
					request := b.request("POST", operation, ready).WithContext(ctx)
					go func() {
						defer close(finished)
						if operation == "prepare" {
							b.h.WorkflowRunnerPrepare(recorder, request)
						} else {
							b.h.WorkflowRunnerAcknowledge(recorder, request)
						}
					}()
					defer func() { _ = blocker.Rollback(context.Background()); cancel(); <-finished }()
					finalFixUnlockAfterExpiry(t, ctx, blocker, started, finished, expires)
					if recorder.Code != 401 {
						t.Errorf("expired %s %s authorized after %s lock: %d %s", expiry, operation, lock, recorder.Code, recorder.Body)
					}
					var after, status string
					var published bool
					dbfx.QueryRow(t, "SELECT jsonb_agg(to_jsonb(t) ORDER BY id)::text FROM workflow_runner_token t WHERE id IN ($1,$2)", b.cred.Generation, candidate.Generation).Scan(&after)
					if before != after {
						t.Error("expired request changed generation phase, lease, or registrations")
					}
					dbfx.QueryRow(t, "SELECT validation_status,published_at IS NOT NULL FROM workflow_revision WHERE id=$1", proposal.Revision.ID).Scan(&status, &published)
					if status != "pending" || published {
						t.Errorf("expired request committed publication evidence: %s", status)
					}
				})
			}
		}
	}
}

func TestWorkflowControlRotationPostLockExpiryRevokesOnlyUndeliveredJWT(t *testing.T) {
	for _, expiry := range []string{"control", "generation"} {
		for _, lock := range []string{"workspace", "control", "generation"} {
			t.Run(expiry+"/"+lock, func(t *testing.T) {
				b := newWorkflowBoundary(t)
				testutil.Call(t, b.h.WorkflowRunnerAcknowledge, b.request("POST", "ack", b.ready)).Want(200)
				ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
				defer cancel()
				minted, release := make(chan struct{}), make(chan struct{})
				gate := &workflowBoundaryIssuance{WorkflowTenantProvider: b.h.WorkflowControl.Tenants, issue: func() { close(minted); <-release }}
				provider := &workflowDeliveredCleanup{WorkflowTenantProvider: gate}
				b.h.WorkflowControl.Tenants = provider
				recorder := httptest.NewRecorder()
				finished := make(chan struct{})
				go func() {
					defer close(finished)
					b.h.WorkflowRunnerRotate(recorder, b.request("POST", "rotate", nil).WithContext(ctx))
				}()
				defer func() {
					select {
					case <-release:
					default:
						close(release)
					}
					cancel()
					<-finished
				}()
				select {
				case <-minted:
				case <-ctx.Done():
					t.Fatal("rotation did not mint", ctx.Err())
				}
				var candidate string
				dbfx.QueryRow(t, "SELECT id FROM workflow_runner_token WHERE control_hash=$1 AND phase='candidate' AND revoked_at IS NULL", auth.HashToken(b.control)).Scan(&candidate)
				var expires time.Time
				var err error
				if expiry == "control" {
					err = testPool.QueryRow(ctx, "UPDATE workflow_daemon SET control_expires_at=clock_timestamp()+interval '1500 milliseconds' WHERE control_hash=$1 RETURNING control_expires_at", auth.HashToken(b.control)).Scan(&expires)
				} else {
					err = testPool.QueryRow(ctx, "UPDATE workflow_runner_token SET expires_at=clock_timestamp()+interval '1500 milliseconds' WHERE id=$1 RETURNING expires_at", candidate).Scan(&expires)
				}
				if err != nil {
					t.Fatal(err)
				}
				blocker, err := testPool.Begin(ctx)
				if err != nil {
					t.Fatal(err)
				}
				defer blocker.Rollback(context.Background())
				switch lock {
				case "workspace":
					_, err = blocker.Exec(ctx, "SELECT id FROM workspace WHERE id=$1 FOR UPDATE", b.workspaceID)
				case "control":
					_, err = blocker.Exec(ctx, "SELECT daemon_id FROM workflow_daemon WHERE control_hash=$1 FOR UPDATE", auth.HashToken(b.control))
				case "generation":
					_, err = blocker.Exec(ctx, "SELECT id FROM workflow_runner_token WHERE id=$1 FOR UPDATE", candidate)
				}
				if err != nil {
					t.Fatal(err)
				}
				started := make(chan int32, 1)
				b.h.TxStarter = workflowActionTransactionStarted{txStarter: b.h.TxStarter, started: started}
				close(release)
				finalFixUnlockAfterExpiry(t, ctx, blocker, started, finished, expires)
				if recorder.Code != 401 || strings.Contains(recorder.Body.String(), "tenant-worker-only") {
					t.Errorf("expired controller received JWT: %d %s", recorder.Code, recorder.Body)
				}
				if len(provider.revoked) == 0 {
					t.Error("undelivered JWT was not targeted for cleanup")
				}
				var abandoned, oldRevoked bool
				var abandonedID, oldID string
				dbfx.QueryRow(t, "SELECT revoked_at IS NOT NULL AND engine_revoked_at IS NOT NULL,engine_token_id FROM workflow_runner_token WHERE id=$1", candidate).Scan(&abandoned, &abandonedID)
				dbfx.QueryRow(t, "SELECT revoked_at IS NOT NULL,engine_token_id FROM workflow_runner_token WHERE id=$1", b.cred.Generation).Scan(&oldRevoked, &oldID)
				for _, receipt := range provider.revoked {
					if receipt.TokenID != abandonedID || receipt.TokenID == oldID || receipt.TenantID != b.cred.TenantID {
						t.Errorf("cleanup revoked another token: %+v", receipt)
					}
				}
				if !abandoned || oldRevoked {
					t.Errorf("cleanup ownership: abandoned=%t oldRevoked=%t", abandoned, oldRevoked)
				}
			})
		}
	}
}

type finalFixEnsureGate struct {
	WorkflowTenantProvider
	ensured func()
}

func (p finalFixEnsureGate) Ensure(ctx context.Context, ws pgtype.UUID) (hatchet.TenantCredentials, error) {
	cred, err := p.WorkflowTenantProvider.Ensure(ctx, ws)
	p.ensured()
	return cred, err
}

func TestWorkflowControlRotationInitialStagesPostLockExpiry(t *testing.T) {
	for _, stage := range []string{"fence", "reserve"} {
		for _, lock := range []string{"workspace", "control", "generation"} {
			if stage == "reserve" && lock == "generation" {
				continue
			} // Reservation inserts a new token.
			t.Run(stage+"/"+lock, func(t *testing.T) {
				b := newWorkflowBoundary(t)
				ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
				defer cancel()
				provider := &workflowDeliveredCleanup{WorkflowTenantProvider: b.h.WorkflowControl.Tenants}
				b.h.WorkflowControl.Tenants = provider
				entered, release := make(chan struct{}), make(chan struct{})
				if stage == "reserve" {
					b.h.WorkflowControl.Tenants = finalFixEnsureGate{WorkflowTenantProvider: provider, ensured: func() { close(entered); <-release }}
				}
				recorder := httptest.NewRecorder()
				finished := make(chan struct{})
				if stage == "reserve" {
					go func() {
						defer close(finished)
						b.h.WorkflowRunnerRotate(recorder, b.request("POST", "rotate", nil).WithContext(ctx))
					}()
					defer func() {
						select {
						case <-release:
						default:
							close(release)
						}
						cancel()
						<-finished
					}()
					select {
					case <-entered:
					case <-ctx.Done():
						t.Fatal("rotation never ensured tenant", ctx.Err())
					}
				}
				var expires time.Time
				if err := testPool.QueryRow(ctx, "UPDATE workflow_daemon SET control_expires_at=clock_timestamp()+interval '1500 milliseconds' WHERE control_hash=$1 RETURNING control_expires_at", auth.HashToken(b.control)).Scan(&expires); err != nil {
					t.Fatal(err)
				}
				var before string
				dbfx.QueryRow(t, "SELECT to_jsonb(t)::text FROM workflow_runner_token t WHERE id=$1", b.cred.Generation).Scan(&before)
				blocker, err := testPool.Begin(ctx)
				if err != nil {
					t.Fatal(err)
				}
				defer blocker.Rollback(context.Background())
				switch lock {
				case "workspace":
					_, err = blocker.Exec(ctx, "SELECT id FROM workspace WHERE id=$1 FOR UPDATE", b.workspaceID)
				case "control":
					_, err = blocker.Exec(ctx, "SELECT daemon_id FROM workflow_daemon WHERE control_hash=$1 FOR UPDATE", auth.HashToken(b.control))
				case "generation":
					_, err = blocker.Exec(ctx, "SELECT id FROM workflow_runner_token WHERE id=$1 FOR UPDATE", b.cred.Generation)
				}
				if err != nil {
					t.Fatal(err)
				}
				started := make(chan int32, 3)
				b.h.TxStarter = workflowActionTransactionStarted{txStarter: b.h.TxStarter, started: started}
				if stage == "reserve" {
					close(release)
				} else {
					go func() {
						defer close(finished)
						b.h.WorkflowRunnerRotate(recorder, b.request("POST", "rotate", nil).WithContext(ctx))
					}()
					defer func() { _ = blocker.Rollback(context.Background()); cancel(); <-finished }()
				}
				finalFixUnlockAfterExpiry(t, ctx, blocker, started, finished, expires)
				if recorder.Code != 401 || provider.minted != 0 {
					t.Errorf("expired %s changed authority or minted JWT: status=%d minted=%d", stage, recorder.Code, provider.minted)
				}
				var after string
				var count int
				dbfx.QueryRow(t, "SELECT to_jsonb(t)::text FROM workflow_runner_token t WHERE id=$1", b.cred.Generation).Scan(&after)
				dbfx.QueryRow(t, "SELECT count(*) FROM workflow_runner_token WHERE control_hash=$1", auth.HashToken(b.control)).Scan(&count)
				if before != after || count != 1 {
					t.Errorf("expired %s committed token fencing/reservation: count=%d", stage, count)
				}
			})
		}
	}
}
