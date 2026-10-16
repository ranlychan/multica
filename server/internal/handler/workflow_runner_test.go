package handler

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/multica-ai/multica/server/internal/auth"
	"github.com/multica-ai/multica/server/internal/hatchet"
	"github.com/multica-ai/multica/server/internal/service"
	"github.com/multica-ai/multica/server/internal/testutil"
	wr "github.com/multica-ai/multica/server/internal/workflowrunner"
)

type workflowBoundaryTenant struct{ tenant string }

func (workflowBoundaryTenant) RevokeWorkerCredentials(context.Context, hatchet.TenantCredentials) error {
	return nil
}

type workflowBoundaryIssuance struct {
	WorkflowTenantProvider
	issue   func()
	revoked bool
}

type workflowDeliveredCleanup struct {
	WorkflowTenantProvider
	revoked []hatchet.TenantCredentials
	minted  int
	fail    bool
	mintErr error
}

func (p *workflowDeliveredCleanup) WorkerCredentials(ctx context.Context, ws pgtype.UUID, daemon, expires string) (hatchet.TenantCredentials, error) {
	p.minted++
	if p.mintErr != nil {
		return hatchet.TenantCredentials{}, p.mintErr
	}
	return p.WorkflowTenantProvider.WorkerCredentials(ctx, ws, daemon, expires)
}
func (p *workflowDeliveredCleanup) RevokeWorkerCredentials(_ context.Context, cred hatchet.TenantCredentials) error {
	p.revoked = append(p.revoked, cred)
	if p.fail {
		return errors.New("engine temporarily unavailable")
	}
	return nil
}

func TestWorkflowDeliveredCandidateCleanupIsTargetedDurableAndBackpressured(t *testing.T) {
	b := newWorkflowBoundary(t)
	var originalID string
	dbfx.QueryRow(t, "SELECT engine_token_id FROM workflow_runner_token WHERE id=$1", b.cred.Generation).Scan(&originalID)
	p := &workflowDeliveredCleanup{WorkflowTenantProvider: b.h.WorkflowControl.Tenants}
	b.h.WorkflowControl.Tenants = p
	var next wr.Credentials
	testutil.Call(t, b.h.WorkflowRunnerRotate, b.request("POST", "rotate", nil)).Want(200).JSON(&next)
	if len(p.revoked) != 1 {
		t.Fatal("superseded delivered candidate worker token not revoked")
	}
	if p.revoked[0].TokenID != originalID || p.revoked[0].TenantID != b.cred.TenantID || p.revoked[0].Token != "" {
		t.Fatal("delivered cleanup did not use exact non-secret token receipt")
	}
	p.fail = true
	testutil.Call(t, b.h.WorkflowRunnerRotate, b.request("POST", "rotate", nil)).Want(409)
	if p.minted != 1 {
		t.Fatal("pending delivered cleanup minted another worker credential")
	}
	// A fresh handler/provider must discover the receipt from DB, not old memory.
	fresh := &workflowDeliveredCleanup{WorkflowTenantProvider: p.WorkflowTenantProvider}
	b.h.WorkflowControl.Tenants = fresh
	testutil.Call(t, b.h.WorkflowRunnerRotate, b.request("POST", "rotate", nil)).Want(200).JSON(&next)
	if len(fresh.revoked) != 1 || fresh.minted != 1 {
		t.Fatal("persistent pending cleanup was lost across handler restart")
	}
	current := b.ready
	current.Generation = next.Generation
	testutil.Call(t, b.h.WorkflowRunnerAcknowledge, b.request("POST", "ack", current)).Want(200)
	var replacement wr.Credentials
	testutil.Call(t, b.h.WorkflowRunnerRotate, b.request("POST", "rotate", nil)).Want(200).JSON(&replacement)
	current.Generation = replacement.Generation
	testutil.Call(t, b.h.WorkflowRunnerAcknowledge, b.request("POST", "ack", current)).Want(200)
	before := len(fresh.revoked)
	var retiringTokenID string
	dbfx.QueryRow(t, "SELECT engine_token_id FROM workflow_runner_token WHERE id=$1", next.Generation).Scan(&retiringTokenID)
	testutil.Call(t, b.h.WorkflowRunnerRetire, b.request("POST", "retire", map[string]string{"generation": next.Generation})).Want(200)
	if len(fresh.revoked) != before+1 {
		t.Fatal("completed retiring worker token not revoked")
	}
	if fresh.revoked[before].TokenID != retiringTokenID {
		t.Fatal("retirement revoked wrong engine generation")
	}
	testutil.Call(t, b.h.WorkflowRunnerAcknowledge, b.request("POST", "ack", current)).Want(200)
}

func TestWorkflowDeliveredRevokeAndFreshControlCleanupFenceAndRetryOnlyOwnedPair(t *testing.T) {
	for _, freshControl := range []bool{false, true} {
		t.Run(fmt.Sprintf("fresh=%t", freshControl), func(t *testing.T) {
			b := newWorkflowBoundary(t)
			other := newWorkflowBoundary(t)
			testutil.Call(t, other.h.WorkflowRunnerAcknowledge, other.request("POST", "ack", other.ready)).Want(200)
			testutil.Call(t, b.h.WorkflowRunnerAcknowledge, b.request("POST", "ack", b.ready)).Want(200)
			var next wr.Credentials
			testutil.Call(t, b.h.WorkflowRunnerRotate, b.request("POST", "rotate", nil)).Want(200).JSON(&next)
			current := b.ready
			current.Generation = next.Generation
			testutil.Call(t, b.h.WorkflowRunnerAcknowledge, b.request("POST", "ack", current)).Want(200)
			p := &workflowDeliveredCleanup{WorkflowTenantProvider: b.h.WorkflowControl.Tenants, fail: true}
			b.h.WorkflowControl.Tenants = p
			if freshControl {
				var daemon string
				dbfx.QueryRow(t, "SELECT daemon_id FROM workflow_daemon WHERE control_hash=$1", auth.HashToken(b.control)).Scan(&daemon)
				var response struct {
					Token string `json:"workflow_control_token"`
				}
				req := map[string]any{"workspace_id": testWorkspaceID, "daemon_id": daemon, "workflow_capable": true, "workflow_code_risk_accepted": true, "runner_version": "0.1.0", "node_version": "22.23.3"}
				testutil.Call(t, b.h.DaemonRegister, newRequest("POST", "/api/daemon/register", req)).Want(200).JSON(&response)
				b.control = response.Token
			} else {
				testutil.Call(t, b.h.WorkflowRunnerRevoke, b.request("POST", "revoke", nil)).Want(200)
			}
			testutil.Call(t, b.h.WorkflowRunnerAcknowledge, b.request("POST", "ack", current)).Want(401)
			testutil.Call(t, b.h.WorkflowRunnerRotate, b.request("POST", "rotate", nil)).Want(409)
			if p.minted != 0 {
				t.Fatal("cleanup failure minted a third engine credential")
			}
			p.fail = false
			p.revoked = nil
			testutil.Call(t, b.h.WorkflowRunnerRotate, b.request("POST", "rotate", nil)).Want(200)
			if len(p.revoked) != 2 {
				t.Fatalf("owned pair cleanup count%d", len(p.revoked))
			}
			testutil.Call(t, other.h.WorkflowRunnerAcknowledge, other.request("POST", "ack", other.ready)).Want(200)
		})
	}
}

func TestWorkflowUnknownPriorIssuanceBlocksMintUntilOriginalExpiry(t *testing.T) {
	b := newWorkflowBoundary(t)
	p := &workflowDeliveredCleanup{WorkflowTenantProvider: b.h.WorkflowControl.Tenants}
	b.h.WorkflowControl.Tenants = p
	dbfx.Exec(t, "UPDATE workflow_runner_token SET engine_token_id='',engine_issuance_unknown=TRUE WHERE id=$1", b.cred.Generation)
	testutil.Call(t, b.h.WorkflowRunnerRevoke, b.request("POST", "revoke", nil)).Want(200)
	testutil.Call(t, b.h.WorkflowRunnerRotate, b.request("POST", "rotate", nil)).Want(409)
	if p.minted != 0 || len(p.revoked) != 0 {
		t.Fatal("unknown prior engine issuance was guessed or ignored")
	}
	dbfx.Exec(t, "UPDATE workflow_runner_token SET created_at=now()-interval '2 hours',expires_at=now()-interval '1 second' WHERE id=$1", b.cred.Generation)
	testutil.Call(t, b.h.WorkflowRunnerRotate, b.request("POST", "rotate", nil)).Want(200)
	var confirmed bool
	dbfx.QueryRow(t, "SELECT engine_revoked_at IS NOT NULL FROM workflow_runner_token WHERE id=$1", b.cred.Generation).Scan(&confirmed)
	if confirmed {
		t.Fatal("expiry falsely declared unknown engine credential revoked")
	}
}

func TestWorkflowConcurrentRotationReservesBeforeMint(t *testing.T) {
	b := newWorkflowBoundary(t)
	testutil.Call(t, b.h.WorkflowRunnerAcknowledge, b.request("POST", "ack", b.ready)).Want(200)
	entered, release := make(chan struct{}), make(chan struct{})
	p := &workflowBoundaryIssuance{WorkflowTenantProvider: b.h.WorkflowControl.Tenants, issue: func() { close(entered); <-release }}
	b.h.WorkflowControl.Tenants = p
	done := make(chan int, 1)
	go func() {
		rec := httptest.NewRecorder()
		b.h.WorkflowRunnerRotate(rec, b.request("POST", "rotate", nil))
		done <- rec.Code
	}()
	<-entered
	testutil.Call(t, b.h.WorkflowRunnerRotate, b.request("POST", "rotate", nil)).Want(409)
	close(release)
	if status := <-done; status != 200 {
		t.Fatalf("reserved mint status%d", status)
	}
}

func TestWorkflowAmbiguousMintPersistsUnknownOrExactReceiptBeforeRetry(t *testing.T) {
	for _, known := range []bool{false, true} {
		t.Run(fmt.Sprintf("receipt=%t", known), func(t *testing.T) {
			b := newWorkflowBoundary(t)
			testutil.Call(t, b.h.WorkflowRunnerAcknowledge, b.request("POST", "ack", b.ready)).Want(200)
			p := &workflowDeliveredCleanup{WorkflowTenantProvider: b.h.WorkflowControl.Tenants, fail: true, mintErr: errors.New("ambiguous mint response")}
			id := uuid.NewString()
			if known {
				p.mintErr = &hatchet.WorkerTokenRevocationError{TenantID: b.cred.TenantID, TokenID: id}
			}
			b.h.WorkflowControl.Tenants = p
			testutil.Call(t, b.h.WorkflowRunnerRotate, b.request("POST", "rotate", nil)).Want(503)
			var expiry time.Time
			dbfx.QueryRow(t, "SELECT expires_at FROM workflow_runner_token WHERE control_hash=$1 AND revoked_at IS NOT NULL ORDER BY created_at DESC LIMIT 1", auth.HashToken(b.control)).Scan(&expiry)
			if !expiry.After(time.Now().Add(time.Hour + 20*time.Second)) {
				t.Fatal("unknown expiry does not cover bounded issuance deadline plus token TTL")
			}
			testutil.Call(t, b.h.WorkflowRunnerRotate, b.request("POST", "rotate", nil)).Want(409)
			if p.minted != 1 {
				t.Fatal("ambiguous mint accumulated another credential")
			}
			if known {
				for _, cred := range p.revoked {
					if cred.TokenID != id {
						t.Fatal("mint receipt targeted a healthy credential")
					}
				}
			} else {
				if len(p.revoked) != 0 {
					t.Fatal("unknown issuance guessed a token identity")
				}
				dbfx.Exec(t, "UPDATE workflow_runner_token SET created_at=now()-interval '2 hours',expires_at=now()-interval '1 second' WHERE control_hash=$1 AND revoked_at IS NOT NULL AND engine_issuance_unknown AND engine_token_id=''", auth.HashToken(b.control))
			}
			p.mintErr = nil
			p.fail = false
			testutil.Call(t, b.h.WorkflowRunnerRotate, b.request("POST", "rotate", nil)).Want(200)
			testutil.Call(t, b.h.WorkflowRunnerAcknowledge, b.request("POST", "ack", b.ready)).Want(200)
		})
	}
}

func (p *workflowBoundaryIssuance) WorkerCredentials(ctx context.Context, ws pgtype.UUID, daemonID, expires string) (hatchet.TenantCredentials, error) {
	cred, err := p.WorkflowTenantProvider.WorkerCredentials(ctx, ws, daemonID, expires)
	p.issue()
	return cred, err
}
func (p *workflowBoundaryIssuance) RevokeWorkerCredentials(_ context.Context, cred hatchet.TenantCredentials) error {
	p.revoked = p.revoked || cred.Token == "tenant-worker-only"
	return nil
}

func TestWorkflowRotationRechecksControlAfterMintAndTargetsUndeliveredToken(t *testing.T) {
	b := newWorkflowBoundary(t)
	p := &workflowBoundaryIssuance{WorkflowTenantProvider: b.h.WorkflowControl.Tenants}
	p.issue = func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if _, err := testPool.Exec(ctx, "UPDATE workflow_daemon SET control_hash=$2 WHERE control_hash=$1", auth.HashToken(b.control), auth.HashToken("wdc_reenrolled")); err != nil {
			t.Error("mint held daemon SQL lock", err)
		}
	}
	b.h.WorkflowControl.Tenants = p
	testutil.Call(t, b.h.WorkflowRunnerRotate, b.request("POST", "rotate", nil)).Want(401)
	if !p.revoked {
		t.Fatal("undelivered worker token not targeted for revocation")
	}
}

func TestWorkflowRotationStagesCandidateWithoutRevokingCheckedOldGeneration(t *testing.T) {
	b := newWorkflowBoundary(t)
	testutil.Call(t, b.h.WorkflowRunnerAcknowledge, b.request("POST", "ack", b.ready)).Want(200)
	var next wr.Credentials
	testutil.Call(t, b.h.WorkflowRunnerRotate, b.request("POST", "rotate", nil)).Want(200).JSON(&next)
	testutil.Call(t, b.h.WorkflowRunnerAcknowledge, b.request("POST", "ack", b.ready)).Want(200)
	if !b.h.WorkflowRunnerEligible(context.Background(), parseUUID(testWorkspaceID), b.revisionID) {
		t.Fatal("candidate revoked old checked eligibility")
	}
	m := b.ready
	m.Generation = next.Generation
	testutil.Call(t, b.h.WorkflowRunnerAcknowledge, b.request("POST", "ack", m)).Want(200)
	testutil.Call(t, b.h.WorkflowRunnerAcknowledge, b.request("POST", "ack", b.ready)).Want(200)
	testutil.Call(t, b.h.WorkflowRunnerRotate, b.request("POST", "rotate", nil)).Want(409)
	testutil.Call(t, b.h.WorkflowRunnerRetire, b.request("POST", "retire", map[string]string{"generation": b.cred.Generation})).Want(200)
	testutil.Call(t, b.h.WorkflowRunnerAcknowledge, b.request("POST", "ack", b.ready)).Want(401)
	testutil.Call(t, b.h.WorkflowRunnerAcknowledge, b.request("POST", "ack", m)).Want(200)
}

func TestWorkflowPreparedCandidateDoesNotPublishOrBecomeEligible(t *testing.T) {
	b := newWorkflowBoundary(t)
	testutil.Call(t, b.h.WorkflowRunnerPrepare, b.request("POST", "prepare", b.ready)).Want(200)
	if b.h.WorkflowRunnerEligible(context.Background(), parseUUID(testWorkspaceID), b.revisionID) {
		t.Fatal("prepared candidate published eligibility before activation")
	}
	w, err := b.h.WorkflowSvc.Load(context.Background(), parseUUID(testWorkspaceID), b.workflowID)
	if err != nil || w.CurrentRevisionID.Valid {
		t.Fatal("prepared candidate activated revision", err)
	}
	var phase string
	var leased bool
	dbfx.QueryRow(t, "SELECT phase,lease_until>now() FROM workflow_runner_token WHERE id=$1", b.cred.Generation).Scan(&phase, &leased)
	if phase != "candidate" || !leased {
		t.Fatal("candidate lacks checked action lease")
	}
	testutil.Call(t, b.h.WorkflowRunnerAcknowledge, b.request("POST", "ack", b.ready)).Want(200)
}

func TestWorkflowRetiringGenerationCannotPublishNewRevision(t *testing.T) {
	b := newWorkflowBoundary(t)
	testutil.Call(t, b.h.WorkflowRunnerAcknowledge, b.request("POST", "ack", b.ready)).Want(200)
	var next wr.Credentials
	testutil.Call(t, b.h.WorkflowRunnerRotate, b.request("POST", "rotate", nil)).Want(200).JSON(&next)
	current := b.ready
	current.Generation = next.Generation
	testutil.Call(t, b.h.WorkflowRunnerAcknowledge, b.request("POST", "ack", current)).Want(200)
	actor := service.WorkflowActor{HumanID: parseUUID(testUserID), AuditID: parseUUID(testUserID), AuditType: "member"}
	pub, err := b.h.WorkflowSvc.PublishRevision(context.Background(), parseUUID(testWorkspaceID), b.workflowID, actor, 1, "new-source", json.RawMessage(`{"steps":[{"id":"a","parents":[]}]}`))
	if err != nil {
		t.Fatal(err)
	}
	forged := b.ready
	action := service.WorkflowActionName(pub.Revision, "a")
	forged.Registrations = append(append([]wr.Registration{}, forged.Registrations...), wr.Registration{Revision: wireRevision(pub.Revision), EngineWorkflowID: "20000000-0000-0000-0000-000000000002", EngineVersionID: "30000000-0000-0000-0000-000000000003", Actions: []wr.Action{{StepID: "a", TaskName: action, ActionID: pub.Revision.HatchetName + ":" + action}}})
	testutil.Call(t, b.h.WorkflowRunnerAcknowledge, b.request("POST", "ack", forged)).Want(409)
	var status string
	dbfx.QueryRow(t, "SELECT validation_status FROM workflow_revision WHERE id=$1", pub.Revision.ID).Scan(&status)
	if status != "pending" {
		t.Fatal("retiring generation changed pending publication")
	}
	testutil.Call(t, b.h.WorkflowRunnerRevoke, b.request("POST", "revoke", nil)).Want(200)
	testutil.Call(t, b.h.WorkflowRunnerAcknowledge, b.request("POST", "ack", b.ready)).Want(401)
	testutil.Call(t, b.h.WorkflowRunnerAcknowledge, b.request("POST", "ack", current)).Want(401)
}

func TestWorkflowFailedCurrentRecoveryWaitsForRetiringGeneration(t *testing.T) {
	b := newWorkflowBoundary(t)
	testutil.Call(t, b.h.WorkflowRunnerAcknowledge, b.request("POST", "ack", b.ready)).Want(200)
	var next wr.Credentials
	testutil.Call(t, b.h.WorkflowRunnerRotate, b.request("POST", "rotate", nil)).Want(200).JSON(&next)
	current := b.ready
	current.Generation = next.Generation
	testutil.Call(t, b.h.WorkflowRunnerAcknowledge, b.request("POST", "ack", current)).Want(200)
	testutil.Call(t, b.h.WorkflowRunnerRetire, b.request("POST", "retire", map[string]string{"generation": next.Generation})).Want(200)
	testutil.Call(t, b.h.WorkflowRunnerAcknowledge, b.request("POST", "ack", b.ready)).Want(200)
	testutil.Call(t, b.h.WorkflowRunnerRotate, b.request("POST", "rotate", nil)).Want(409)
	testutil.Call(t, b.h.WorkflowRunnerRetire, b.request("POST", "retire", map[string]string{"generation": b.cred.Generation})).Want(200)
	var recovered wr.Credentials
	testutil.Call(t, b.h.WorkflowRunnerRotate, b.request("POST", "rotate", nil)).Want(200).JSON(&recovered)
	current.Generation = recovered.Generation
	testutil.Call(t, b.h.WorkflowRunnerAcknowledge, b.request("POST", "ack", current)).Want(200)
	if !b.h.WorkflowRunnerEligible(context.Background(), parseUUID(testWorkspaceID), b.revisionID) {
		t.Fatal("pair retirement did not permit recovery")
	}
}

func TestWorkflowAuthenticatedControlRenewalPreservesActiveGeneration(t *testing.T) {
	b := newWorkflowBoundary(t)
	testutil.Call(t, b.h.WorkflowRunnerAcknowledge, b.request("POST", "ack", b.ready)).Want(200)
	var daemonID string
	dbfx.QueryRow(t, "SELECT daemon_id FROM workflow_daemon WHERE control_hash=$1", auth.HashToken(b.control)).Scan(&daemonID)
	request := map[string]any{"workspace_id": testWorkspaceID, "daemon_id": daemonID, "workflow_capable": true, "workflow_code_risk_accepted": true, "runner_version": "0.1.0", "node_version": "22.23.3", "workflow_control_token": b.control}
	var response struct {
		Token string `json:"workflow_control_token"`
	}
	testutil.Call(t, b.h.DaemonRegister, newRequest("POST", "/api/daemon/register", request)).Want(200).JSON(&response)
	if response.Token != b.control {
		t.Fatal("routine authenticated renewal replaced live control authority")
	}
	if !b.h.WorkflowRunnerEligible(context.Background(), parseUUID(testWorkspaceID), b.revisionID) {
		t.Fatal("routine renewal invalidated active generation")
	}
}

func TestWorkflowFreshControlFencesOldPairAndPermitsSubsequentRotation(t *testing.T) {
	for _, overlap := range []bool{false, true} {
		t.Run(fmt.Sprintf("overlap=%t", overlap), func(t *testing.T) {
			b := newWorkflowBoundary(t)
			testutil.Call(t, b.h.WorkflowRunnerAcknowledge, b.request("POST", "ack", b.ready)).Want(200)
			if overlap {
				var next wr.Credentials
				testutil.Call(t, b.h.WorkflowRunnerRotate, b.request("POST", "rotate", nil)).Want(200).JSON(&next)
				current := b.ready
				current.Generation = next.Generation
				testutil.Call(t, b.h.WorkflowRunnerAcknowledge, b.request("POST", "ack", current)).Want(200)
			}
			var daemonID string
			dbfx.QueryRow(t, "SELECT daemon_id FROM workflow_daemon WHERE control_hash=$1", auth.HashToken(b.control)).Scan(&daemonID)
			var response struct {
				Token string `json:"workflow_control_token"`
			}
			req := map[string]any{"workspace_id": testWorkspaceID, "daemon_id": daemonID, "workflow_capable": true, "workflow_code_risk_accepted": true, "runner_version": "0.1.0", "node_version": "22.23.3"}
			testutil.Call(t, b.h.DaemonRegister, newRequest("POST", "/api/daemon/register", req)).Want(200).JSON(&response)
			testutil.Call(t, b.h.WorkflowRunnerAcknowledge, b.request("POST", "ack", b.ready)).Want(401)
			b.control = response.Token
			var fresh wr.Credentials
			testutil.Call(t, b.h.WorkflowRunnerRotate, b.request("POST", "rotate", nil)).Want(200).JSON(&fresh)
			current := b.ready
			current.Generation = fresh.Generation
			testutil.Call(t, b.h.WorkflowRunnerAcknowledge, b.request("POST", "ack", current)).Want(200)
			testutil.Call(t, b.h.WorkflowRunnerRotate, b.request("POST", "rotate", nil)).Want(200)
		})
	}
}

func (p workflowBoundaryTenant) Ensure(context.Context, pgtype.UUID) (hatchet.TenantCredentials, error) {
	return hatchet.TenantCredentials{TenantID: p.tenant, Token: "tenant-server-only"}, nil
}
func (p workflowBoundaryTenant) MappedCredentials(ctx context.Context, ws pgtype.UUID) (hatchet.TenantCredentials, error) {
	return p.Ensure(ctx, ws)
}
func (p workflowBoundaryTenant) WorkerCredentials(context.Context, pgtype.UUID, string, string) (hatchet.TenantCredentials, error) {
	return hatchet.TenantCredentials{TenantID: p.tenant, Token: "tenant-worker-only", TokenID: uuid.NewString(), ExpiresAt: time.Now().Add(time.Hour)}, nil
}

type workflowBoundaryVerifier struct{ before func() }

func (v workflowBoundaryVerifier) VerifyRegistration(_ context.Context, _ hatchet.TenantCredentials, e wr.Registration) error {
	if v.before != nil {
		v.before()
	}
	if e.EngineWorkflowID != "20000000-0000-0000-0000-000000000002" || e.EngineVersionID != "30000000-0000-0000-0000-000000000003" {
		return errors.New("registration mismatch")
	}
	return nil
}

func TestWorkflowRevocationDuringEngineCheckCannotActivatePendingRevision(t *testing.T) {
	for _, rotate := range []bool{false, true} {
		t.Run(fmt.Sprintf("rotate=%v", rotate), func(t *testing.T) {
			b := newWorkflowBoundary(t)
			entered, release := make(chan struct{}), make(chan struct{})
			b.h.WorkflowControl.Verifier = workflowBoundaryVerifier{before: func() { close(entered); <-release }}
			response := httptest.NewRecorder()
			done := make(chan struct{})
			go func() { defer close(done); b.h.WorkflowRunnerAcknowledge(response, b.request("POST", "ack", b.ready)) }()
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("verifier did not start")
			}
			if rotate {
				testutil.Call(t, b.h.WorkflowRunnerRotate, b.request("POST", "rotate", nil)).Want(200)
			} else {
				testutil.Call(t, b.h.WorkflowRunnerRevoke, b.request("POST", "revoke", nil)).Want(200)
			}
			close(release)
			select {
			case <-done:
			case <-time.After(3 * time.Second):
				t.Fatal("acknowledgement deadlocked")
			}
			if response.Code != 401 {
				t.Fatalf("old generation response %d: %s", response.Code, response.Body)
			}
			workflow, err := b.h.WorkflowSvc.Load(context.Background(), parseUUID(testWorkspaceID), b.workflowID)
			if err != nil || workflow.DefinitionVersion != 0 {
				t.Fatalf("revoked generation activated revision: version=%d err=%v", workflow.DefinitionVersion, err)
			}
		})
	}
}

func TestWorkflowLeaseFailureRollsBackRevisionActivation(t *testing.T) {
	b := newWorkflowBoundary(t)
	dbfx.Exec(t, fmt.Sprintf(`CREATE FUNCTION test_workflow_lease_failure() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.id='%s'::uuid THEN RAISE EXCEPTION 'injected lease failure'; END IF; RETURN NEW; END $$`, b.cred.Generation))
	t.Cleanup(func() { dbfx.Exec(t, "DROP FUNCTION IF EXISTS test_workflow_lease_failure() CASCADE") })
	dbfx.Exec(t, "CREATE TRIGGER test_workflow_lease_failure BEFORE UPDATE OF lease_until ON workflow_runner_token FOR EACH ROW EXECUTE FUNCTION test_workflow_lease_failure()")
	response := testutil.Call(t, b.h.WorkflowRunnerAcknowledge, b.request("POST", "ack", b.ready))
	if response.Code < 400 {
		t.Fatal("injected lease failure accepted")
	}
	w, err := b.h.WorkflowSvc.Load(context.Background(), parseUUID(testWorkspaceID), b.workflowID)
	if err != nil || w.DefinitionVersion != 0 {
		t.Fatalf("failed lease committed activation: version=%d err=%v", w.DefinitionVersion, err)
	}
}

func TestWorkflowConflictingAcknowledgementRemainsVisibleWithoutChangingCurrent(t *testing.T) {
	b := newWorkflowBoundary(t)
	actor := service.WorkflowActor{HumanID: parseUUID(testUserID), AuditID: parseUUID(testUserID), AuditType: "member"}
	pub, err := b.h.WorkflowSvc.PublishRevision(context.Background(), parseUUID(testWorkspaceID), b.workflowID, actor, 0, "competing-revision", json.RawMessage(`{"steps":[{"id":"a","parents":[]}]}`))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = b.h.WorkflowSvc.AcknowledgeRevision(context.Background(), parseUUID(testWorkspaceID), pub.Revision.ID, pub.Revision.DefinitionHash, true); err != nil {
		t.Fatal(err)
	}
	testutil.Call(t, b.h.WorkflowRunnerAcknowledge, b.request("POST", "ack", b.ready)).Want(409)
	var status, reason string
	dbfx.QueryRow(t, "SELECT validation_status,COALESCE(validation_error,'') FROM workflow_revision WHERE id=$1", b.revisionID).Scan(&status, &reason)
	if status != "invalid" || reason != "workflow_version_conflict" {
		t.Fatalf("conflict not visible: %s %s", status, reason)
	}
	w, err := b.h.WorkflowSvc.Load(context.Background(), parseUUID(testWorkspaceID), b.workflowID)
	if err != nil || w.CurrentRevisionID != pub.Revision.ID {
		t.Fatal("stale revision changed current", err)
	}
}

type workflowBoundary struct {
	workspaceID string
	h           Handler
	control     string
	ready       wr.Message
	cred        wr.Credentials
	workflowID  pgtype.UUID
	revisionID  pgtype.UUID
}

func newWorkflowBoundary(t *testing.T) *workflowBoundary {
	return newWorkflowBoundaryInWorkspace(t, testWorkspaceID)
}
func newWorkflowBoundaryInWorkspace(t *testing.T, workspaceID string) *workflowBoundary {
	t.Helper()
	b := &workflowBoundary{h: *testHandler, workspaceID: workspaceID}
	b.h.WorkflowSvc = service.NewWorkflowService(b.h.Queries, testPool)
	b.h.WorkflowSvc.Ready = func() bool { return true }
	b.h.WorkflowSvc.Eligible = b.h.WorkflowRunnerEligible
	tenant := uuid.NewString()
	dbfx.Exec(t, "INSERT INTO workflow_hatchet_tenant(workspace_id,tenant_id,token_encrypted) VALUES($1,$2,'test-only') ON CONFLICT(workspace_id) DO NOTHING", workspaceID, tenant)
	dbfx.QueryRow(t, "SELECT tenant_id FROM workflow_hatchet_tenant WHERE workspace_id=$1", workspaceID).Scan(&tenant)
	b.h.WorkflowControl = &WorkflowControl{Ready: func() bool { return true }, Tenants: workflowBoundaryTenant{tenant}, Verifier: workflowBoundaryVerifier{}}
	dbfx.Cleanup(t, "DELETE FROM workflow_hatchet_tenant WHERE workspace_id=$1", workspaceID)
	daemonID := "workflow-boundary-" + uuid.NewString()
	req := map[string]any{"workspace_id": workspaceID, "daemon_id": daemonID, "workflow_capable": true, "workflow_code_risk_accepted": true, "runner_version": "0.1.0", "node_version": "22.23.3"}
	var enrollment struct {
		Token string `json:"workflow_control_token"`
	}
	testutil.Call(t, b.h.DaemonRegister, newRequest("POST", "/api/daemon/register", req)).Want(200).JSON(&enrollment)
	b.control = enrollment.Token
	dbfx.Cleanup(t, "DELETE FROM workflow_daemon WHERE workspace_id=$1 AND daemon_id=$2", workspaceID, daemonID)
	dbfx.Cleanup(t, "DELETE FROM workflow_runner_token WHERE workspace_id=$1 AND daemon_id=$2", workspaceID, daemonID)
	actor := service.WorkflowActor{HumanID: parseUUID(testUserID), AuditID: parseUUID(testUserID), AuditType: "member"}
	w, err := b.h.WorkflowSvc.Create(context.Background(), parseUUID(workspaceID), actor, "boundary", "boundary-"+uuid.NewString(), "")
	if err != nil {
		t.Fatal(err)
	}
	b.workflowID = w.ID
	dbfx.Cleanup(t, "DELETE FROM workflow WHERE id=$1", w.ID)
	dbfx.Cleanup(t, "DELETE FROM workflow_revision WHERE workflow_id=$1", w.ID)
	dbfx.Cleanup(t, "DELETE FROM workflow_run WHERE workflow_id=$1", w.ID)
	pub, err := b.h.WorkflowSvc.PublishRevision(context.Background(), parseUUID(workspaceID), w.ID, actor, 0, "test-only-source", json.RawMessage(`{"steps":[{"id":"a","parents":[]}]}`))
	if err != nil {
		t.Fatal(err)
	}
	b.revisionID = pub.Revision.ID
	testutil.Call(t, b.h.WorkflowRunnerRotate, b.request("POST", "rotate", nil)).Want(200).JSON(&b.cred)
	action := service.WorkflowActionName(pub.Revision, "a")
	b.ready = wr.Message{Type: "ready", Protocol: 1, WorkspaceID: workspaceID, TenantID: tenant, Generation: b.cred.Generation, RunnerVersion: "0.1.0", NodeVersion: "22.23.3", Registrations: []wr.Registration{{Revision: wireRevision(pub.Revision), EngineWorkflowID: "20000000-0000-0000-0000-000000000002", EngineVersionID: "30000000-0000-0000-0000-000000000003", Actions: []wr.Action{{StepID: "a", TaskName: action, ActionID: pub.Revision.HatchetName + ":" + action}}}}}
	return b
}
func (b *workflowBoundary) request(method, path string, body any) *http.Request {
	r := newRequest(method, "/api/daemon/workflows/"+path, body)
	r.Header.Set("Authorization", "Bearer "+b.control)
	r.Header.Set("X-Workspace-ID", b.workspaceID)
	return r
}

func TestWorkflowAcknowledgementRejectsStaleForgedAndCrossTenantEvidence(t *testing.T) {
	b := newWorkflowBoundary(t)
	for _, tc := range []struct {
		name   string
		status int
		mutate func(*wr.Message)
	}{
		{"workspace", 400, func(m *wr.Message) { m.WorkspaceID = uuid.NewString() }},
		{"tenant", 401, func(m *wr.Message) { m.TenantID = uuid.NewString() }},
		{"generation", 401, func(m *wr.Message) { m.Generation = uuid.NewString() }},
		{"source-hash", 409, func(m *wr.Message) { m.Registrations[0].Hash = strings.Repeat("0", 64) }},
		{"graph", 409, func(m *wr.Message) { m.Registrations[0].Graph.Steps[0].ID = "forged" }},
		{"version", 409, func(m *wr.Message) { m.Registrations[0].Version++ }},
		{"action", 409, func(m *wr.Message) { m.Registrations[0].Actions[0].ActionID = "forged" }},
		{"engine-version", 409, func(m *wr.Message) { m.Registrations[0].EngineVersionID = uuid.NewString() }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw, _ := json.Marshal(b.ready)
			var m wr.Message
			_ = json.Unmarshal(raw, &m)
			tc.mutate(&m)
			testutil.Call(t, b.h.WorkflowRunnerAcknowledge, b.request("POST", "ack", m)).Want(tc.status)
			if b.h.WorkflowRunnerEligible(context.Background(), parseUUID(testWorkspaceID), b.revisionID) {
				t.Fatal("unverified evidence enabled revision")
			}
		})
	}
	testutil.Call(t, b.h.WorkflowRunnerAcknowledge, b.request("POST", "ack", b.ready)).Want(200)
	if !b.h.WorkflowRunnerEligible(context.Background(), parseUUID(testWorkspaceID), b.revisionID) {
		t.Fatal("checked evidence did not enable revision")
	}
	dbfx.Exec(t, "UPDATE workflow_runner_token SET lease_until=now()-interval '1 second' WHERE id=$1", b.cred.Generation)
	if b.h.WorkflowRunnerEligible(context.Background(), parseUUID(testWorkspaceID), b.revisionID) {
		t.Fatal("expired registration lease eligible")
	}
	var next wr.Credentials
	testutil.Call(t, b.h.WorkflowRunnerRotate, b.request("POST", "rotate", nil)).Want(200).JSON(&next)
	m := b.ready
	m.Generation = next.Generation
	testutil.Call(t, b.h.WorkflowRunnerAcknowledge, b.request("POST", "ack", m)).Want(200)
	testutil.Call(t, b.h.WorkflowRunnerRetire, b.request("POST", "retire", map[string]string{"generation": b.cred.Generation})).Want(200)
	testutil.Call(t, b.h.WorkflowRunnerAcknowledge, b.request("POST", "ack", b.ready)).Want(401)
}

func TestWorkflowActionsBindFrozenPrincipalRevisionAndLiveAuthority(t *testing.T) {
	b := newWorkflowBoundary(t)
	testutil.Call(t, b.h.WorkflowRunnerAcknowledge, b.request("POST", "ack", b.ready)).Want(200)
	w, err := b.h.WorkflowSvc.Load(context.Background(), parseUUID(testWorkspaceID), b.workflowID)
	if err != nil {
		t.Fatal(err)
	}
	principal := plainMember(t, "workflow-action-principal")
	dbfx.Exec(t, "INSERT INTO workflow_collaborator(workspace_id,workflow_id,user_id,role,created_by_id) VALUES($1,$2,$3,'editor',$4)", testWorkspaceID, b.workflowID, principal, testUserID)
	dbfx.Cleanup(t, "DELETE FROM workflow_collaborator WHERE workflow_id=$1", b.workflowID)
	actor := service.WorkflowActor{HumanID: parseUUID(principal), AuditID: parseUUID(principal), AuditType: "member"}
	run, err := b.h.WorkflowSvc.AdmitRun(context.Background(), w, actor, "manual", pgtype.UUID{}, json.RawMessage(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	dbfx.Exec(t, "UPDATE workflow_run SET status='running',submission_started_at=now(),submission_deadline=now()+interval '55 minutes' WHERE id=$1", run.ID)
	b.h.WorkflowControl.Action = func(_ context.Context, a WorkflowActionAuthority, _ WorkflowActionRequest) (json.RawMessage, error) {
		if a.Principal != parseUUID(principal) || a.Run.ID != run.ID || a.Revision.ID != b.revisionID {
			t.Error("caller selected action authority")
		}
		return json.RawMessage(`{"ok":true}`), nil
	}
	request := func(step string) *http.Request {
		id := uuidToString(run.ID)
		path := "/api/workflow-runs/" + id + "/actions"
		body, _ := json.Marshal(WorkflowActionRequest{Protocol: 1, RunID: id, StepID: step, Operation: "issues.get", Payload: json.RawMessage(`{}`)})
		r := withURLParam(newRequest("POST", path, nil), "id", id)
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
	testutil.Call(t, b.h.WorkflowRunnerAction, request("a")).Want(200)
	testutil.Call(t, b.h.WorkflowRunnerAction, request("forged")).Want(403)
	bad := request("a")
	bad.Header.Set("X-Multica-Workflow-Signature", strings.Repeat("0", 64))
	testutil.Call(t, b.h.WorkflowRunnerAction, bad).Want(401)
	bad = request("a")
	bad.Header.Set("X-Workspace-ID", uuid.NewString())
	testutil.Call(t, b.h.WorkflowRunnerAction, bad).Want(410)
	dbfx.Exec(t, "DELETE FROM member WHERE workspace_id=$1 AND user_id=$2", testWorkspaceID, principal)
	testutil.Call(t, b.h.WorkflowRunnerAction, request("a")).Want(403)
	dbfx.Exec(t, "UPDATE workflow_run SET cancel_requested_at=now(),cancel_requested_by_id=$2 WHERE id=$1", run.ID, testUserID)
	testutil.Call(t, b.h.WorkflowRunnerAction, request("a")).Want(410)
}

func TestWorkflowEnrollmentRejectsMachineIdentityAndPreservesAgentRegistration(t *testing.T) {
	h := *testHandler
	h.WorkflowControl = &WorkflowControl{Ready: func() bool { return true }}
	base := map[string]any{"workspace_id": testWorkspaceID, "daemon_id": "workflow-machine", "workflow_capable": true, "workflow_code_risk_accepted": true, "runner_version": "0.1.0", "node_version": "22.23.3"}
	dbfx.Cleanup(t, "DELETE FROM workflow_daemon WHERE workspace_id=$1 AND daemon_id='workflow-machine'", testWorkspaceID)
	r := newRequest("POST", "/api/daemon/register", base)
	r.Header.Set("X-Actor-Source", "task_token")
	testutil.Call(t, h.DaemonRegister, r).Want(400)
	base["runtimes"] = []map[string]any{{"name": "workflow-agent-health", "type": "claude", "status": "online"}}
	r = newDaemonTokenRequest("POST", "/api/daemon/register", base, testWorkspaceID, "workflow-machine")
	response := testutil.Call(t, h.DaemonRegister, r).Want(200).Map()
	if response["workflow_control_token"] != "" {
		t.Fatal("ordinary daemon token enrolled workflow authority")
	}
	dbfx.Cleanup(t, "DELETE FROM agent_runtime WHERE workspace_id=$1 AND daemon_id='workflow-machine'", testWorkspaceID)
	dbfx.Cleanup(t, "DELETE FROM workflow_daemon WHERE workspace_id=$1 AND daemon_id='workflow-machine'", testWorkspaceID)
}

func TestWorkflowControlRejectsUnenrolledAndCrossWorkspace(t *testing.T) {
	h := *testHandler
	h.WorkflowControl = &WorkflowControl{Ready: func() bool { return true }}
	request := func(token, ws string) *http.Request {
		r := newRequest("GET", "/api/daemon/workflows/revisions", nil)
		r.Header.Set("Authorization", "Bearer "+token)
		r.Header.Set("X-Workspace-ID", ws)
		return r
	}
	testutil.Call(t, h.WorkflowRunnerRevisions, request("wdc_unknown", testWorkspaceID)).Want(401)
	token := "wdc_" + uuid.NewString()
	dbfx.Exec(t, "INSERT INTO workflow_daemon(workspace_id,daemon_id,owner_id,control_hash,control_expires_at,capable,runner_version,node_version) VALUES($1,'workflow-boundary',$2,$3,now()+interval '1 hour',true,'0.1.0','22.23.3')", testWorkspaceID, testUserID, auth.HashToken(token))
	dbfx.Cleanup(t, "DELETE FROM workflow_daemon WHERE workspace_id=$1 AND daemon_id='workflow-boundary'", testWorkspaceID)
	testutil.Call(t, h.WorkflowRunnerRevisions, request(token, uuid.NewString())).Want(401)
	testutil.Call(t, h.WorkflowRunnerRevisions, request(token, testWorkspaceID)).Want(200)
	dbfx.Exec(t, "UPDATE workflow_daemon SET capable=false WHERE workspace_id=$1 AND daemon_id='workflow-boundary'", testWorkspaceID)
	testutil.Call(t, h.WorkflowRunnerRevisions, request(token, testWorkspaceID)).Want(401)
	if h.WorkflowRunnerEligible(context.Background(), parseUUID(testWorkspaceID), parseUUID(uuid.NewString())) {
		t.Fatal("unregistered daemon eligible")
	}
}

func TestWorkflowControlRenewalExpiryAndOtherHumanCannotTakeDaemonIdentity(t *testing.T) {
	b := newWorkflowBoundary(t)
	var daemonID string
	dbfx.QueryRow(t, "SELECT daemon_id FROM workflow_daemon WHERE control_hash=$1", auth.HashToken(b.control)).Scan(&daemonID)
	registration := map[string]any{"workspace_id": testWorkspaceID, "daemon_id": daemonID, "workflow_capable": true, "workflow_code_risk_accepted": true, "runner_version": "0.1.0", "node_version": "22.23.3"}
	other := plainMember(t, "workflow-other-owner")
	r := newRequest("POST", "/api/daemon/register", registration)
	r.Header.Set("X-User-ID", other)
	testutil.Call(t, b.h.DaemonRegister, r).Want(400)
	testutil.Call(t, b.h.WorkflowRunnerRevisions, b.request("GET", "revisions", nil)).Want(200)
	var enrollment struct {
		Token string `json:"workflow_control_token"`
	}
	response := testutil.Call(t, b.h.DaemonRegister, newRequest("POST", "/api/daemon/register", registration)).Want(200)
	response.JSON(&enrollment)
	if response.Header().Get("Cache-Control") != "no-store" {
		t.Fatal("credential enrollment may be cached")
	}
	testutil.Call(t, b.h.WorkflowRunnerRevisions, b.request("GET", "revisions", nil)).Want(401)
	b.control = enrollment.Token
	testutil.Call(t, b.h.WorkflowRunnerRevisions, b.request("GET", "revisions", nil)).Want(200)
	dbfx.Exec(t, "UPDATE workflow_daemon SET control_expires_at=now()-interval '1 second' WHERE control_hash=$1", auth.HashToken(b.control))
	testutil.Call(t, b.h.WorkflowRunnerRevisions, b.request("GET", "revisions", nil)).Want(401)
}

func TestWorkflowOnlyRegistrationNeedsCurrentHumanAndExplicitRiskConsent(t *testing.T) {
	h := *testHandler
	h.WorkflowControl = &WorkflowControl{Ready: func() bool { return true }}
	req := map[string]any{"workspace_id": testWorkspaceID, "daemon_id": "workflow-only", "workflow_capable": true, "runner_version": "0.1.0", "node_version": "22.23.3"}
	testutil.Call(t, h.DaemonRegister, newRequest("POST", "/api/daemon/register", req)).Want(400)
	req["workflow_code_risk_accepted"] = true
	var response struct {
		Token string `json:"workflow_control_token"`
	}
	testutil.Call(t, h.DaemonRegister, newRequest("POST", "/api/daemon/register", req)).Want(200).JSON(&response)
	dbfx.Cleanup(t, "DELETE FROM workflow_daemon WHERE workspace_id=$1 AND daemon_id='workflow-only'", testWorkspaceID)
	if response.Token == "" {
		t.Fatal("workflow-only host not enrolled")
	}
}
