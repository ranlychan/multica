package daemon

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/multica-ai/multica/server/internal/events"
	"github.com/multica-ai/multica/server/internal/handler"
	"github.com/multica-ai/multica/server/internal/hatchet"
	"github.com/multica-ai/multica/server/internal/service"
	"github.com/multica-ai/multica/server/internal/testutil"
	"github.com/multica-ai/multica/server/internal/util"
	"github.com/multica-ai/multica/server/internal/util/secretbox"
	wr "github.com/multica-ai/multica/server/internal/workflowrunner"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

type workflowLiveEngine struct {
	client *hatchet.Client
	cred   hatchet.TenantCredentials
}

type workflowLiveTenants struct {
	handler.WorkflowTenantProvider
	t *testing.T
}

func uuidToString(id pgtype.UUID) string { return util.UUIDToString(id) }
func parseUUID(id string) pgtype.UUID {
	value, err := util.ParseUUID(id)
	if err != nil {
		panic(err)
	}
	return value
}

func (p workflowLiveTenants) WorkerCredentials(ctx context.Context, ws pgtype.UUID, daemonID, expires string) (hatchet.TenantCredentials, error) {
	cred, err := p.WorkflowTenantProvider.WorkerCredentials(ctx, ws, daemonID, expires)
	if err != nil {
		p.t.Logf("worker credential provisioning: %v", err)
	}
	return cred, err
}

type workflowLiveTransport struct{ t *testing.T }

func (p workflowLiveTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	response, err := http.DefaultTransport.RoundTrip(r)
	if response != nil && response.StatusCode >= 300 {
		p.t.Logf("engine %s %s status=%d cookie=%t location=%s", r.Method, r.URL.Path, response.StatusCode, r.Header.Get("Cookie") != "", response.Header.Get("Location"))
	}
	return response, err
}

func (e workflowLiveEngine) Submit(ctx context.Context, r db.WorkflowRun, v db.WorkflowRevision) (string, error) {
	raw, err := e.client.Submit(ctx, e.cred, hatchet.SubmitRequest{RunID: uuidToString(r.ID), WorkflowName: v.HatchetName, Input: r.Input})
	if err != nil {
		return "", err
	}
	var result struct {
		Run struct {
			Metadata struct {
				ID string `json:"id"`
			} `json:"metadata"`
		} `json:"run"`
	}
	err = json.Unmarshal(raw, &result)
	return result.Run.Metadata.ID, err
}
func (e workflowLiveEngine) Discover(context.Context, db.WorkflowRun) (string, error) { return "", nil }
func (e workflowLiveEngine) Inspect(context.Context, db.WorkflowRun) (service.WorkflowEngineState, error) {
	return service.WorkflowEngineState{}, fmt.Errorf("unused")
}
func (e workflowLiveEngine) Cancel(ctx context.Context, r db.WorkflowRun) error {
	return e.client.Cancel(ctx, e.cred, uuidToString(r.HatchetRunID))
}

func TestWorkflowGoPackagedControlLive(t *testing.T) {
	if os.Getenv("MULTICA_WORKFLOW_CONTROL_LIVE") != "1" {
		t.Skip("explicit disposable engine/Node proof")
	}
	node := os.Getenv("MULTICA_WORKFLOW_TEST_NODE")
	if node == "" {
		t.Fatal("explicit Node22 required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	testPool, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(testPool.Close)
	dbfx := testutil.New(testPool, "", "")
	testUserID := dbfx.User(t, "5b lifecycle proof", fmt.Sprintf("workflow-5b-%d@test.invalid", time.Now().UnixNano()))
	testWorkspaceID := dbfx.Insert(t, "workspace", testutil.Cols{"name": "5b lifecycle", "slug": fmt.Sprintf("workflow-5b-%d", time.Now().UnixNano())})
	dbfx.Member(t, testWorkspaceID, testUserID, "owner")
	queries := db.New(testPool)
	root := t.TempDir()
	port := func() string {
		ln, e := net.Listen("tcp", "127.0.0.1:0")
		if e != nil {
			t.Fatal(e)
		}
		defer ln.Close()
		return ln.Addr().String()
	}
	name := fmt.Sprintf("multica-5b-proof-%d", time.Now().UnixNano())
	command := func(args ...string) string {
		out, e := exec.CommandContext(ctx, "docker", args...).CombinedOutput()
		if e != nil {
			t.Fatalf("disposable docker command: %v %s", e, out)
		}
		return strings.TrimSpace(string(out))
	}
	command("run", "--detach", "--rm", "--name", name, "-p", "127.0.0.1::5432", "-e", "POSTGRES_PASSWORD=workflow-proof", "-e", "POSTGRES_DB=hatchet", "postgres:15.6", "-c", "max_connections=500")
	defer exec.Command("docker", "rm", "-f", name).Run()
	pgport := command("port", name, "5432/tcp")
	for i := 0; i < 60; i++ {
		if exec.Command("docker", "exec", name, "pg_isready", "-U", "postgres").Run() == nil {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	config := hatchet.Config{Enabled: true, DatabaseURL: "postgres://postgres:workflow-proof@" + pgport + "/hatchet?sslmode=disable", APIURL: "http://" + port(), GRPCAddress: port(), TLSStrategy: "mtls", StateDir: filepath.Join(root, "state"), BinaryDir: os.Getenv("HATCHET_BIN_DIR"), ProvisioningEmail: "proof@example.com", ProvisioningPassword: "DisposablePassword123!", StartupTimeout: 90 * time.Second, ShutdownTimeout: 15 * time.Second}
	supervisor := hatchet.NewSupervisor(config, nil)
	defer func() {
		stop, c := context.WithTimeout(context.Background(), 20*time.Second)
		defer c()
		_ = supervisor.Shutdown(stop)
	}()
	if e := supervisor.Start(ctx); e != nil {
		t.Fatal(e)
	}
	engine, _ := hatchet.NewClient(config.APIURL, &http.Client{Transport: workflowLiveTransport{t}, Timeout: 30 * time.Second})
	box, _ := secretbox.New(make([]byte, secretbox.KeySize))
	tenants, e := hatchet.NewSQLTenantService(engine, queries, testPool, box, config.ProvisioningEmail, config.ProvisioningPassword)
	if e != nil {
		t.Fatal(e)
	}
	dbfx.Cleanup(t, "DELETE FROM workflow_hatchet_tenant WHERE workspace_id=$1", testWorkspaceID)
	h := handler.Handler{Queries: queries, TxStarter: testPool, Bus: events.New()}
	h.WorkflowSvc = service.NewWorkflowService(h.Queries, testPool)
	h.WorkflowControl = &handler.WorkflowControl{Ready: supervisor.Ready, Tenants: workflowLiveTenants{tenants, t}, Verifier: engine}
	h.WorkflowSvc.Ready = supervisor.Ready
	h.WorkflowSvc.Eligible = h.WorkflowRunnerEligible
	router := chi.NewRouter()
	router.Post("/api/daemon/register", func(w http.ResponseWriter, r *http.Request) {
		r.Header.Set("X-User-ID", testUserID)
		h.DaemonRegister(w, r)
	})
	router.Get("/api/daemon/workflows/revisions", h.WorkflowRunnerRevisions)
	var issued sync.Map
	router.Post("/api/daemon/workflows/rotate", func(w http.ResponseWriter, r *http.Request) {
		rec := httptest.NewRecorder()
		h.WorkflowRunnerRotate(rec, r)
		if rec.Code == 200 {
			var c wr.Credentials
			_ = json.Unmarshal(rec.Body.Bytes(), &c)
			issued.Store(c.Generation, c)
		}
		for k, values := range rec.Header() {
			w.Header()[k] = values
		}
		w.WriteHeader(rec.Code)
		_, _ = w.Write(rec.Body.Bytes())
	})
	router.Post("/api/daemon/workflows/ack", h.WorkflowRunnerAcknowledge)
	router.Post("/api/daemon/workflows/prepare", func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		r.Body = io.NopCloser(bytes.NewReader(raw))
		var evidence wr.Message
		_ = json.Unmarshal(raw, &evidence)
		cred, err := tenants.Ensure(r.Context(), parseUUID(testWorkspaceID))
		if err != nil {
			t.Error(err)
			w.WriteHeader(500)
			return
		}
		req, _ := http.NewRequestWithContext(r.Context(), "GET", config.APIURL+"/api/v1/tenants/"+cred.TenantID+"/worker", nil)
		req.Header.Set("Authorization", "Bearer "+cred.Token)
		response, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Error(err)
			w.WriteHeader(500)
			return
		}
		defer response.Body.Close()
		var workers struct {
			Rows []struct {
				Name string `json:"name"`
			} `json:"rows"`
		}
		if response.StatusCode != 200 || json.NewDecoder(response.Body).Decode(&workers) != nil {
			t.Error("candidate worker gate inspection failed")
			w.WriteHeader(500)
			return
		}
		for _, worker := range workers.Rows {
			if strings.Contains(worker.Name, evidence.Generation) {
				t.Error("candidate assignment stream started before checked product action lease")
				w.WriteHeader(500)
				return
			}
		}
		h.WorkflowRunnerPrepare(w, r)
	})
	router.Post("/api/daemon/workflows/retire", h.WorkflowRunnerRetire)
	router.Post("/api/daemon/workflows/validation-failed", h.WorkflowRunnerValidationFailed)
	router.Post("/api/daemon/workflows/revoke", h.WorkflowRunnerRevoke)
	router.Post("/api/workflow-runs/{id}/actions", h.WorkflowRunnerAction)
	product := httptest.NewServer(router)
	defer product.Close()
	call := func(method, path, token, workspace string, body, out any) int {
		data, _ := json.Marshal(body)
		req, _ := http.NewRequestWithContext(ctx, method, product.URL+path, bytes.NewReader(data))
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("X-Workspace-ID", workspace)
		resp, e := product.Client().Do(req)
		if e != nil {
			t.Fatal(e)
		}
		defer resp.Body.Close()
		raw, _ := io.ReadAll(resp.Body)
		if resp.StatusCode == 200 && out != nil {
			if e = json.Unmarshal(raw, out); e != nil {
				t.Fatal(e)
			}
		}
		if resp.StatusCode != 200 {
			t.Logf("control %s status=%d response=%s", path, resp.StatusCode, raw)
		}
		return resp.StatusCode
	}
	entry, _ := filepath.Abs("../../../packages/workflow-runner/dist/cli.js")
	runner := &WorkflowRunner{Config: WorkflowRunnerConfig{Enabled: true, AcceptedCodeRisk: true, Workspaces: []string{testWorkspaceID}, Node: node, Entry: entry, Cache: filepath.Join(root, "cache"), EngineAddress: config.GRPCAddress, TLS: wr.TLS{Strategy: "mtls", ServerName: "cluster", CAFile: filepath.Join(config.StateDir, "certs/ca.cert"), CertFile: filepath.Join(config.StateDir, "certs/client-worker.pem"), KeyFile: filepath.Join(config.StateDir, "certs/client-worker.key")}}}
	version, e := runner.Probe(ctx)
	if e != nil {
		t.Fatal(e)
	}
	t.Logf("actual Go packaged IPC runtime Node%s", version)
	var enrollment struct {
		Token string `json:"workflow_control_token"`
	}
	if call("POST", "/api/daemon/register", "", testWorkspaceID, map[string]any{"workspace_id": testWorkspaceID, "daemon_id": "5b-live", "workflow_capable": true, "workflow_code_risk_accepted": true, "runner_version": "0.1.0", "node_version": version}, &enrollment) != 200 {
		t.Fatal("enrollment")
	}
	dbfx.Cleanup(t, "DELETE FROM workflow_daemon WHERE workspace_id=$1 AND daemon_id='5b-live'", testWorkspaceID)
	dbfx.Cleanup(t, "DELETE FROM workflow_runner_token WHERE workspace_id=$1 AND daemon_id='5b-live'", testWorkspaceID)
	ws := parseUUID(testWorkspaceID)
	actor := service.WorkflowActor{HumanID: parseUUID(testUserID), AuditID: parseUUID(testUserID), AuditType: "member"}
	workflow, e := h.WorkflowSvc.Create(ctx, ws, actor, "Go IPC proof", "go-ipc-proof", "")
	if e != nil {
		t.Fatal(e)
	}
	dbfx.Cleanup(t, "DELETE FROM workflow WHERE id=$1", workflow.ID)
	dbfx.Cleanup(t, "DELETE FROM workflow_revision WHERE workflow_id=$1", workflow.ID)
	dbfx.Cleanup(t, "DELETE FROM workflow_run WHERE workflow_id=$1", workflow.ID)
	source := func(revision int) string {
		return fmt.Sprintf(`import {defineWorkflow} from '@multica/workflow-sdk'; export default defineWorkflow({steps:[{id:'work',retries:1,timeoutMs:180000,run:async ctx=>{if(ctx.input.retry && ctx.attempt===0) throw Error('retry');if(ctx.input.hold) await ctx.issues.get('hold');if(ctx.input.sleep) await ctx.sleep(45000);await ctx.issues.get('after');return {revision:%d,attempt:ctx.attempt};}}]});`, revision)
	}
	graph := json.RawMessage(`{"steps":[{"id":"work","parents":[]}]}`)
	publish, e := h.WorkflowSvc.PublishRevision(ctx, ws, workflow.ID, actor, 0, source(1), graph)
	if e != nil {
		t.Fatal(e)
	}
	manager := newWorkflowManager(runner.Config, NewClient(product.URL))
	manager.enroll(testWorkspaceID, enrollment.Token)
	defer manager.close()
	watched := map[string]bool{testWorkspaceID: true}
	manager.reconcile(ctx, watched)
	generation := manager.generations[testWorkspaceID]
	child, oldGeneration := generation.process, generation.ready.Generation
	if child == nil {
		t.Fatal("initial actual manager generation failed")
	}
	if !h.WorkflowRunnerEligible(ctx, ws, publish.Revision.ID) {
		t.Fatal("ack did not grant revision eligibility")
	}
	serverCred, e := tenants.Ensure(ctx, ws)
	if e != nil {
		t.Fatal(e)
	}
	h.WorkflowSvc.Engine = workflowLiveEngine{engine, serverCred}
	entered, release := make(chan string, 1), make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	type actionObservation struct{ generation, run string }
	after := make(chan actionObservation, 10)
	h.WorkflowControl.Action = func(actionCtx context.Context, a handler.WorkflowActionAuthority, request handler.WorkflowActionRequest) (json.RawMessage, error) {
		if a.Principal != actor.HumanID {
			return nil, fmt.Errorf("wrong frozen principal")
		}
		var payload struct {
			ID string `json:"id"`
		}
		_ = json.Unmarshal(request.Payload, &payload)
		if payload.ID == "hold" {
			entered <- uuidToString(a.Runner.ID)
			select {
			case <-release:
			case <-actionCtx.Done():
				return nil, actionCtx.Err()
			}
		}
		if payload.ID == "after" {
			t.Logf("SDK after action run=%s generation=%s", uuidToString(a.Run.ID), uuidToString(a.Runner.ID))
			after <- actionObservation{uuidToString(a.Runner.ID), uuidToString(a.Run.ID)}
		}
		return json.RawMessage(`{"ok":true}`), nil
	}
	workflow, _ = h.WorkflowSvc.Load(ctx, ws, workflow.ID)
	sleepRun, e := h.WorkflowSvc.AdmitRun(ctx, workflow, actor, "manual", pgtype.UUID{}, json.RawMessage(`{"sleep":true}`))
	if e != nil {
		t.Fatal(e)
	}
	sleepRun, e = h.WorkflowSvc.SubmitRun(ctx, service.RunIntent{WorkspaceID: ws, RunID: sleepRun.ID})
	if e != nil {
		t.Fatal(e)
	}
	evicted := false
	var evictionLast json.RawMessage
	for deadline := time.Now().Add(15 * time.Second); time.Now().Before(deadline); {
		raw, err := engine.Detail(ctx, serverCred, uuidToString(sleepRun.HatchetRunID))
		evictionLast = raw
		if err != nil && err.Error() != "hatchet: HTTP 404" {
			t.Fatal(err)
		}
		var detail struct {
			Tasks []struct {
				IsEvicted bool `json:"isEvicted"`
			} `json:"tasks"`
		}
		_ = json.Unmarshal(raw, &detail)
		if len(detail.Tasks) > 0 && detail.Tasks[0].IsEvicted {
			evicted = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !evicted {
		t.Fatalf("R1 durable sleep was not acknowledged evicted before transition: %s", evictionLast)
	}
	t.Log("R1 durable sleep acknowledged evicted before manager switch")
	oldRun, e := h.WorkflowSvc.AdmitRun(ctx, workflow, actor, "manual", pgtype.UUID{}, json.RawMessage(`{"hold":true}`))
	if e != nil {
		t.Fatal(e)
	}
	oldRun, e = h.WorkflowSvc.SubmitRun(ctx, service.RunIntent{WorkspaceID: ws, RunID: oldRun.ID})
	if e != nil {
		t.Fatal(e)
	}
	select {
	case actual := <-entered:
		if actual != oldGeneration {
			t.Fatal("R1 assigned unexpected generation")
		}
	case <-time.After(15 * time.Second):
		t.Fatal("R1 did not enter SDK action")
	}
	bad, e := h.WorkflowSvc.PublishRevision(ctx, ws, workflow.ID, actor, 1, "export default {steps:[]}", graph)
	if e != nil {
		t.Fatal(e)
	}
	manager.reconcile(ctx, watched)
	if generation.process != child {
		t.Fatal("invalid candidate replaced active R1")
	}
	select {
	case <-child.done:
		t.Fatal("invalid candidate stopped R1")
	default:
	}
	var invalid string
	dbfx.QueryRow(t, "SELECT validation_status FROM workflow_revision WHERE id=$1", bad.Revision.ID).Scan(&invalid)
	if invalid != "invalid" {
		t.Fatal("invalid candidate not visible")
	}
	publish2, e := h.WorkflowSvc.PublishRevision(ctx, ws, workflow.ID, actor, 1, source(2), graph)
	if e != nil {
		t.Fatal(e)
	}
	manager.reconcile(ctx, watched)
	if generation.process == child || generation.retiring == nil || generation.retiring.process != child {
		t.Fatal("manager did not stage retiring/current pair")
	}
	select {
	case <-child.done:
		t.Fatal("routine update killed active R1")
	default:
	}
	ready := generation.ready
	t.Logf("lifecycle old=%s current=%s hotRun=%s sleepingRun=%s", oldGeneration, ready.Generation, uuidToString(oldRun.ID), uuidToString(sleepRun.ID))
	value, _ := issued.Load(ready.Generation)
	cred := value.(wr.Credentials)
	// A routine refresh is deferred while valid old work owns the retiring slot.
	started := generation.started
	generation.started = time.Now().Add(-46 * time.Minute)
	manager.reconcile(ctx, watched)
	generation.started = started
	if generation.ready.Generation != ready.Generation || generation.retiring == nil {
		t.Fatal("routine refresh bypassed retiring pair backpressure")
	}
	close(release)
	select {
	case actual := <-after:
		if actual.generation != oldGeneration || actual.run != uuidToString(oldRun.ID) {
			t.Fatal("R1 after-switch SDK action used different generation")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("retiring R1 SDK action was denied")
	}
	if len(ready.Registrations) != 2 || !h.WorkflowRunnerEligible(ctx, ws, publish2.Revision.ID) {
		t.Fatal("queued immutable old revision lost")
	}
	workflow, _ = h.WorkflowSvc.Load(ctx, ws, workflow.ID)
	newRun, e := h.WorkflowSvc.AdmitRun(ctx, workflow, actor, "manual", pgtype.UUID{}, json.RawMessage(`{"retry":true}`))
	if e != nil {
		t.Fatal(e)
	}
	for _, run := range []db.WorkflowRun{oldRun, sleepRun, newRun} {
		submitted := run
		if !run.HatchetRunID.Valid {
			submitted, e = h.WorkflowSvc.SubmitRun(ctx, service.RunIntent{WorkspaceID: ws, RunID: run.ID})
		}
		if e != nil || !submitted.HatchetRunID.Valid {
			t.Fatal("submit", e)
		}
		deadline := time.Now().Add(90 * time.Second)
		var last json.RawMessage
		complete := false
		for time.Now().Before(deadline) {
			last, e = engine.Detail(ctx, serverCred, uuidToString(submitted.HatchetRunID))
			// The pin may acknowledge Submit before its read projection is visible.
			// Poll only that exact 404 within the same fixed completion deadline.
			if e != nil && e.Error() == "hatchet: HTTP 404" {
				time.Sleep(100 * time.Millisecond)
				continue
			}
			if e != nil {
				t.Fatal(e)
			}
			var detail struct {
				Run struct {
					Status string          `json:"status"`
					Output json.RawMessage `json:"output"`
				} `json:"run"`
			}
			_ = json.Unmarshal(last, &detail)
			if detail.Run.Status == "COMPLETED" {
				var output struct {
					Value struct {
						Revision int `json:"revision"`
						Attempt  int `json:"attempt"`
					} `json:"value"`
				}
				wantRevision, wantAttempt := 2, 1
				if run.ID != newRun.ID {
					wantRevision, wantAttempt = 1, 0
				}
				if json.Unmarshal(detail.Run.Output, &output) != nil || output.Value.Revision != wantRevision || output.Value.Attempt != wantAttempt {
					t.Fatalf("immutable revision/retry output mismatch: %s", detail.Run.Output)
				}
				t.Logf("completed revision=%d output=%s", wantRevision, detail.Run.Output)
				complete = true
				break
			}
			manager.reconcile(ctx, watched)
			time.Sleep(time.Second)
		}
		if !complete {
			diagnostic, _ := os.CreateTemp("", "multica-5b-retry-failure-*.json")
			if diagnostic != nil {
				_, _ = diagnostic.Write(last)
				_ = diagnostic.Close()
				t.Log("retained retry evidence:", diagnostic.Name())
			}
			t.Fatalf("actual retry/durable run timeout: %s", last)
		}
	}
	observedSleep := false
	var remaining []actionObservation
	for len(after) > 0 {
		actual := <-after
		remaining = append(remaining, actual)
		if actual.run == uuidToString(sleepRun.ID) {
			if actual.generation != ready.Generation {
				t.Fatalf("sleeping R1 used generation=%s expected=%s", actual.generation, ready.Generation)
			}
			observedSleep = true
		}
	}
	if !observedSleep {
		t.Fatalf("sleeping R1 SDK action not observed: remaining=%+v expected run=%s generation=%s newRun=%s", remaining, uuidToString(sleepRun.ID), ready.Generation, uuidToString(newRun.ID))
	}
	if call("GET", "/api/daemon/workflows/revisions", enrollment.Token, "10000000-0000-0000-0000-000000000001", nil, nil) != 401 {
		t.Fatal("cross workspace accepted")
	}
	if call("POST", "/api/daemon/workflows/revoke", enrollment.Token, testWorkspaceID, nil, nil) != 200 {
		t.Fatal("revoke")
	}
	if h.WorkflowRunnerEligible(ctx, ws, publish2.Revision.ID) {
		t.Fatal("revoked generation still eligible")
	}
	if call("POST", "/api/daemon/workflows/ack", enrollment.Token, testWorkspaceID, ready, nil) != 401 {
		t.Fatal("revoked ack")
	}
	req, _ := http.NewRequestWithContext(ctx, "POST", product.URL+"/api/workflow-runs/"+uuidToString(newRun.ID)+"/actions", strings.NewReader(`{}`))
	req.Header.Set("Authorization", "Bearer "+cred.RunnerToken)
	req.Header.Set("X-Workspace-ID", testWorkspaceID)
	req.Header.Set("X-Multica-Workflow-Generation", cred.Generation)
	resp, e := product.Client().Do(req)
	if e != nil {
		t.Fatal(e)
	}
	resp.Body.Close()
	if resp.StatusCode != 410 {
		t.Fatal("revoked product action", resp.StatusCode)
	}
	manager.reconcile(ctx, watched)
	if generation.process != nil {
		select {
		case <-generation.process.done:
		default:
			t.Fatal("revoked current worker survived manager reconcile")
		}
	}
	t.Log("actual manager invalid draft preserved R1; staged prepare/activation; retiring R1 SDK action authorized; immutable R1/R2 retry; revoke/action denial PASS")
}
