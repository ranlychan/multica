package daemon

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
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
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
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

// Keep the real engine response private until the worker's first action finishes.
type workflowLivePreACKEngine struct {
	workflowLiveEngine
	action  <-chan struct{}
	lost    bool
	submits atomic.Int32
}

func (e *workflowLivePreACKEngine) Submit(ctx context.Context, r db.WorkflowRun, v db.WorkflowRevision) (string, error) {
	e.submits.Add(1)
	id, err := e.workflowLiveEngine.Submit(ctx, r, v)
	if err != nil {
		return "", err
	}
	select {
	case <-e.action:
	case <-ctx.Done():
		return "", ctx.Err()
	}
	if e.lost {
		return "", errors.New("test accepted response lost")
	}
	return id, nil
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
	workflowGoPackagedControlLive(t, false)
}

func TestWorkflowGoPackagedLongHotAssignmentLive(t *testing.T) {
	if os.Getenv("MULTICA_WORKFLOW_LONG_HOT_LIVE") != "1" {
		t.Skip("explicit long-hot assignment boundary proof")
	}
	workflowGoPackagedControlLive(t, true)
}

func workflowGoPackagedControlLive(t *testing.T, longHot bool) {
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
	assignments, err := hatchet.NewAssignmentVerifier(ctx, config.DatabaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer assignments.Close()
	if assignments.Check(ctx) == nil {
		t.Fatal("assignment ready before engine migrations")
	}
	supervisor := hatchet.NewSupervisor(config, nil)
	defer func() {
		stop, c := context.WithTimeout(context.Background(), 20*time.Second)
		defer c()
		_ = supervisor.Shutdown(stop)
	}()
	if e := supervisor.Start(ctx); e != nil {
		t.Fatal(e)
	}
	if err := assignments.Check(ctx); err != nil {
		t.Fatal("assignment schema unavailable after migrations", err)
	}
	engine, _ := hatchet.NewClient(config.APIURL, &http.Client{Transport: workflowLiveTransport{t}, Timeout: 30 * time.Second})
	box, _ := secretbox.New(make([]byte, secretbox.KeySize))
	tenants, e := hatchet.NewSQLTenantService(engine, queries, testPool, box, config.ProvisioningEmail, config.ProvisioningPassword)
	if e != nil {
		t.Fatal(e)
	}
	dbfx.Cleanup(t, "DELETE FROM workflow_hatchet_tenant WHERE workspace_id=$1", testWorkspaceID)
	h := handler.Handler{Queries: queries, TxStarter: testPool, Bus: events.New()}
	h.TaskService = service.NewTaskService(queries, testPool, nil, h.Bus)
	h.IssueService = service.NewIssueService(queries, testPool, h.Bus, nil, h.TaskService)
	h.AutopilotService = service.NewAutopilotService(queries, testPool, h.Bus, h.TaskService)
	h.WorkflowSvc = service.NewWorkflowService(h.Queries, testPool)
	h.WorkflowControl = &handler.WorkflowControl{Ready: supervisor.Ready, Tenants: workflowLiveTenants{tenants, t}, Verifier: engine, Assignments: assignments}
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
	type actionResponse struct {
		request    handler.WorkflowActionRequest
		generation string
		status     int
	}
	responses := make(chan actionResponse, 16)
	router.Post("/api/workflow-runs/{id}/actions", func(w http.ResponseWriter, r *http.Request) {
		if !longHot {
			h.WorkflowRunnerAction(w, r)
			return
		}
		raw, _ := io.ReadAll(r.Body)
		r.Body = io.NopCloser(bytes.NewReader(raw))
		var req handler.WorkflowActionRequest
		_ = json.Unmarshal(raw, &req)
		rec := httptest.NewRecorder()
		h.WorkflowRunnerAction(rec, r)
		responses <- actionResponse{req, r.Header.Get("X-Multica-Workflow-Generation"), rec.Code}
		for k, values := range rec.Header() {
			w.Header()[k] = values
		}
		w.WriteHeader(rec.Code)
		_, _ = w.Write(rec.Body.Bytes())
	})
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
	dbfx.Cleanup(t, "DELETE FROM workflow_operation WHERE run_id IN (SELECT id FROM workflow_run WHERE workflow_id=$1)", workflow.ID)
	dbfx.Cleanup(t, "DELETE FROM issue WHERE origin_type='workflow' AND origin_id IN (SELECT id FROM workflow_run WHERE workflow_id=$1)", workflow.ID)
	source := func(revision int) string {
		hotCode := ""
		if longHot {
			hotCode = `if(ctx.input.longhot){await ctx.issues.get('started');await new Promise(r=>setTimeout(r,40000));}`
		}
		return fmt.Sprintf(`import {defineWorkflow} from '@multica/workflow-sdk'; export default defineWorkflow({steps:[{id:'work',retries:1,timeoutMs:180000,run:async ctx=>{const p={title:'Live workflow business '+ctx.runId};const first=await ctx.issues.create('live-business',p);const replay=await ctx.issues.create('live-business',p);if(first.id!==replay.id)throw Error('business replay');%s if(ctx.input.retry && ctx.attempt===0) throw Error('retry');if(ctx.input.hold) await ctx.issues.get('hold');if(ctx.input.sleep) await ctx.sleep(45000);await ctx.issues.get('after');return {revision:%d,attempt:ctx.attempt};}}]});`, hotCode, revision)
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
	// Delivered-but-unprepared candidates count toward the engine credential pair.
	// Observe the exact pin's authoritative token row without exporting this DB
	// URL or server authority to a worker (test-only read, never production writes).
	enginePool, err := pgxpool.New(ctx, config.DatabaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer enginePool.Close()
	if longHot {
		serverCred, err := tenants.MappedCredentials(ctx, ws)
		if err != nil {
			t.Fatal(err)
		}
		h.WorkflowSvc.Engine = workflowLiveEngine{engine, serverCred}
		h.WorkflowControl.Action = func(ctx context.Context, a handler.WorkflowActionAuthority, req handler.WorkflowActionRequest) (json.RawMessage, error) {
			if req.Operation == "issues.create" {
				return h.WorkflowBusinessAction(ctx, a, req)
			}
			return json.RawMessage(`{"ok":true}`), nil
		}
		workflow, _ = h.WorkflowSvc.Load(ctx, ws, workflow.ID)
		run, err := h.WorkflowSvc.AdmitRun(ctx, workflow, actor, "manual", pgtype.UUID{}, json.RawMessage(`{"longhot":true}`))
		if err != nil {
			t.Fatal(err)
		}
		var original actionResponse
		select {
		case original = <-responses:
		case <-time.After(15 * time.Second):
			t.Fatal("long-hot run did not enter SDK")
		}
		if original.generation != oldGeneration || original.status != 200 {
			t.Fatalf("long-hot initial action failed: %+v", original)
		}
		publish2, err := h.WorkflowSvc.PublishRevision(ctx, ws, workflow.ID, actor, 1, source(2), graph)
		if err != nil {
			t.Fatal(err)
		}
		manager.reconcile(ctx, watched)
		if generation.retiring == nil || generation.retiring.process != child || !h.WorkflowRunnerEligible(ctx, ws, publish2.Revision.ID) {
			t.Fatal("long-hot staged replacement failed")
		}
		t.Logf("long-hot old=%s current=%s local=%s engine=%s task=%s", oldGeneration, generation.ready.Generation, uuidToString(run.ID), original.request.EngineRunID, original.request.EngineTaskID)
		var oldWorkerID string
		if err := enginePool.QueryRow(ctx, `SELECT id::text FROM "Worker" WHERE "tenantId"=$1 AND name=$2`, serverCred.TenantID, "multica-"+oldGeneration).Scan(&oldWorkerID); err != nil {
			t.Fatal(err)
		}
		// JS-active work is neither a durable sleep nor an HTTP request held past
		// the transport's10second deadline. Inspect only normal engine-owned rows.
		deadline := time.NewTimer(50 * time.Second)
		defer deadline.Stop()
		for {
			select {
			case response := <-responses:
				var payload struct {
					ID string `json:"id"`
				}
				_ = json.Unmarshal(response.request.Payload, &payload)
				t.Logf("long-hot observed SDK generation=%s invocation=%d payloadID=%s status=%d", response.generation, response.request.InvocationCount, payload.ID, response.status)
				var heartbeatAge float64
				var active, deleted bool
				var assigned *string
				var invocation int
				if err := enginePool.QueryRow(ctx, `SELECT extract(epoch FROM now()-w."lastHeartbeatAt"),w."isActive",w."deletedAt" IS NOT NULL,rt.worker_id::text,lf.latest_invocation_count FROM "Worker" w JOIN v1_lookup_table k ON k.tenant_id=w."tenantId" AND k.external_id=$2 JOIN v1_task t ON (t.id,t.inserted_at)=(k.task_id,k.inserted_at) LEFT JOIN v1_task_runtime rt ON (rt.task_id,rt.task_inserted_at,rt.retry_count)=(t.id,t.inserted_at,t.retry_count) JOIN v1_durable_event_log_file lf ON (lf.durable_task_id,lf.durable_task_inserted_at)=(t.id,t.inserted_at) WHERE w."tenantId"=$1 AND w.name=$3`, serverCred.TenantID, original.request.EngineTaskID, "multica-"+oldGeneration).Scan(&heartbeatAge, &active, &deleted, &assigned, &invocation); err != nil {
					t.Fatal(err)
				}
				assignedID := "<none>"
				if assigned != nil {
					assignedID = *assigned
				}
				t.Logf("long-hot engine state oldHeartbeatAge=%.3fs oldActive=%t oldDeleted=%t currentWorker=%s invocation=%d", heartbeatAge, active, deleted, assignedID, invocation)
				if response.generation != oldGeneration {
					t.Error("long-hot work unexpectedly reassigned during retirement")
				}
				if response.generation == oldGeneration && payload.ID == "after" {
					if response.status != 200 {
						t.Fatalf("long-hot retiring SDK lost current assignment before original credential/step deadline: status=%d engine=%s task=%s", response.status, original.request.EngineRunID, original.request.EngineTaskID)
					}
					if invocation != original.request.InvocationCount || response.request.InvocationCount != original.request.InvocationCount || assignedID != oldWorkerID || active || deleted || heartbeatAge > 10 {
						t.Fatalf("long-hot continuation changed assignment or stopped heartbeat: worker=%s expected=%s invocation=%d expected=%d active=%t deleted=%t heartbeatAge=%.3f", assignedID, oldWorkerID, invocation, original.request.InvocationCount, active, deleted, heartbeatAge)
					}
					return
				}
			case <-deadline.C:
				t.Fatalf("long-hot retiring SDK never returned engine=%s task=%s", original.request.EngineRunID, original.request.EngineTaskID)
			}
		}
	}
	engineRevoked := func(generation string) bool {
		t.Helper()
		var tokenID string
		if err := testPool.QueryRow(ctx, "SELECT engine_token_id FROM workflow_runner_token WHERE id=$1", generation).Scan(&tokenID); err != nil {
			t.Fatal(err)
		}
		var revoked bool
		if err := enginePool.QueryRow(ctx, `SELECT revoked FROM "APIToken" WHERE id=$1`, tokenID).Scan(&revoked); err != nil {
			t.Fatal(err)
		}
		return revoked
	}
	var abandoned, replacement wr.Credentials
	if call("POST", "/api/daemon/workflows/rotate", enrollment.Token, testWorkspaceID, nil, &abandoned) != 200 || call("POST", "/api/daemon/workflows/rotate", enrollment.Token, testWorkspaceID, nil, &replacement) != 200 {
		t.Fatal("candidate cleanup rotation")
	}
	if !engineRevoked(abandoned.Generation) || engineRevoked(oldGeneration) {
		t.Fatal("delivered cleanup did not revoke exact candidate and preserve current")
	}
	probeRevision := generation.ready.Registrations[0].Revision
	probeRevision.SourcePath, e = runner.CacheSource(source(1), probeRevision.Hash)
	if e != nil {
		t.Fatal(e)
	}
	probe := func(c wr.Credentials) error {
		probeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		p, _, err := runner.startWorker(ctx, probeCtx, wr.Descriptor{Protocol: 1, WorkspaceID: testWorkspaceID, TenantID: c.TenantID, Generation: c.Generation, Engine: wr.Engine{Token: c.EngineToken, HostPort: runner.Config.EngineAddress, TLS: runner.Config.TLS}, Product: wr.Product{Origin: product.URL, RunnerToken: c.RunnerToken}, Revisions: []wr.Revision{probeRevision}, ActivationGate: true, DrainTimeoutMS: 1000})
		if p != nil {
			p.Stop(time.Second)
		}
		return err
	}
	if probe(replacement) != nil {
		t.Fatal("healthy replacement credential cannot register through packaged gRPC")
	}
	if probe(abandoned) == nil {
		t.Fatal("superseded never-used worker JWT still accepted new packaged registration")
	}
	if call("POST", "/api/daemon/workflows/retire", enrollment.Token, testWorkspaceID, map[string]string{"generation": replacement.Generation}, nil) != 200 || !engineRevoked(replacement.Generation) || engineRevoked(oldGeneration) {
		t.Fatal("exact candidate retirement cleanup")
	}
	if call("POST", "/api/daemon/workflows/ack", enrollment.Token, testWorkspaceID, generation.ready, nil) != 200 {
		t.Fatal("candidate cleanup disrupted healthy current")
	}
	t.Log("delivered candidate JWT target revoked; new packaged gRPC registration rejected; healthy current preserved")
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
	type actionObservation struct {
		generation, run string
		request         handler.WorkflowActionRequest
	}
	after := make(chan actionObservation, 10)
	var preACKActions sync.Map
	var observedAssignments sync.Map
	h.WorkflowControl.Action = func(actionCtx context.Context, a handler.WorkflowActionAuthority, request handler.WorkflowActionRequest) (json.RawMessage, error) {
		if a.Principal != actor.HumanID {
			return nil, fmt.Errorf("wrong frozen principal")
		}
		if request.Operation == "issues.create" {
			return h.WorkflowBusinessAction(actionCtx, a, request)
		}
		var payload struct {
			ID string `json:"id"`
		}
		_ = json.Unmarshal(request.Payload, &payload)
		var registrations map[string]wr.Registration
		_ = json.Unmarshal(a.Runner.Registrations, &registrations)
		registration := registrations[uuidToString(a.Run.RevisionID)]
		binding := hatchet.Assignment{TenantID: a.Runner.TenantID, EngineRunID: request.EngineRunID, EngineTaskID: request.EngineTaskID, WorkflowID: registration.EngineWorkflowID, VersionID: registration.EngineVersionID, Retry: request.Attempt, Invocation: request.InvocationCount, WorkerName: "multica-" + uuidToString(a.Runner.ID)}
		for _, step := range registration.Actions {
			if step.StepID == request.StepID {
				binding.TaskName, binding.ActionID = step.TaskName, step.ActionID
			}
		}
		observedAssignments.Store(uuidToString(a.Run.ID), binding)
		// These queries inspect actual assigned rows, never fixtures inserted in the engine DB.
		for name, mutate := range map[string]func(*hatchet.Assignment){
			"tenant": func(b *hatchet.Assignment) { b.TenantID = uuid.NewString() }, "run": func(b *hatchet.Assignment) { b.EngineRunID = uuid.NewString() }, "task": func(b *hatchet.Assignment) { b.EngineTaskID = uuid.NewString() }, "workflow": func(b *hatchet.Assignment) { b.WorkflowID = uuid.NewString() }, "version": func(b *hatchet.Assignment) { b.VersionID = uuid.NewString() }, "step": func(b *hatchet.Assignment) { b.TaskName += "-wrong" }, "action": func(b *hatchet.Assignment) { b.ActionID += "-wrong" }, "worker": func(b *hatchet.Assignment) { b.WorkerName = "multica-" + uuid.NewString() }, "retry": func(b *hatchet.Assignment) { b.Retry++ }, "invocation": func(b *hatchet.Assignment) { b.Invocation++ },
		} {
			wrong := binding
			mutate(&wrong)
			if err := assignments.VerifyAssignment(actionCtx, wrong); !errors.Is(err, hatchet.ErrAssignmentDenied) {
				t.Errorf("%s selector accepted/unavailable: %v run=%s task=%s", name, err, request.EngineRunID, request.EngineTaskID)
			}
		}
		if binding.Retry > 0 {
			old := binding
			old.Retry--
			if err := assignments.VerifyAssignment(actionCtx, old); !errors.Is(err, hatchet.ErrAssignmentDenied) {
				t.Errorf("old retry accepted: %v", err)
			}
		}
		if binding.Invocation > 1 {
			old := binding
			old.Invocation--
			if err := assignments.VerifyAssignment(actionCtx, old); !errors.Is(err, hatchet.ErrAssignmentDenied) {
				t.Errorf("old invocation accepted: %v", err)
			}
		}
		if signal, ok := preACKActions.Load(uuidToString(a.Run.ID)); ok {
			var location string
			var size int
			var insertedAt time.Time
			if err := enginePool.QueryRow(actionCtx, `SELECT p.location::text,octet_length(p.inline_content::text) FROM v1_lookup_table k JOIN v1_payload p ON (p.id,p.inserted_at)=(k.task_id,k.inserted_at) AND p.tenant_id=k.tenant_id WHERE k.tenant_id=$1 AND k.external_id=$2 AND p.type='TASK_INPUT'`, a.Runner.TenantID, request.EngineTaskID).Scan(&location, &size); err != nil || location != "INLINE" {
				t.Errorf("pre-ACK actual input unavailable: %s %v", location, err)
			}
			if err := enginePool.QueryRow(actionCtx, `SELECT t.inserted_at FROM v1_lookup_table k JOIN v1_task t ON (t.id,t.inserted_at)=(k.task_id,k.inserted_at) AND t.tenant_id=k.tenant_id WHERE k.tenant_id=$1 AND k.external_id=$2`, a.Runner.TenantID, request.EngineTaskID).Scan(&insertedAt); err != nil {
				t.Error(err)
			}
			preACK := binding
			preACK.LocalRunID = uuidToString(a.Run.ID)
			preACK.SubmissionStartedAt = a.Run.SubmissionStartedAt.Time
			preACK.SubmissionDeadline = a.Run.SubmissionDeadline.Time
			if err := assignments.VerifyAssignment(actionCtx, preACK); err != nil {
				t.Errorf("actual inline input binding failed: %v", err)
			}
			wrongInput := preACK
			wrongInput.LocalRunID = uuid.NewString()
			if err := assignments.VerifyAssignment(actionCtx, wrongInput); !errors.Is(err, hatchet.ErrAssignmentInputPending) {
				t.Errorf("unmatched reserved input accepted: %v", err)
			}
			lateStart := preACK
			lateStart.SubmissionStartedAt = insertedAt.Add(time.Microsecond)
			if err := assignments.VerifyAssignment(actionCtx, lateStart); !errors.Is(err, hatchet.ErrAssignmentInputPending) {
				t.Errorf("task timestamp before first submission accepted: %v inserted=%s", err, insertedAt.Format(time.RFC3339Nano))
			}
			t.Logf("pre-ACK action bound engine=%s task=%s input=%s bytes=%d", request.EngineRunID, request.EngineTaskID, location, size)
			t.Logf("first-bind timestamp started=%s taskInserted=%s deadline=%s", preACK.SubmissionStartedAt.Format(time.RFC3339Nano), insertedAt.Format(time.RFC3339Nano), preACK.SubmissionDeadline.Format(time.RFC3339Nano))
			close(signal.(chan struct{}))
			preACKActions.Delete(uuidToString(a.Run.ID))
		}
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
			after <- actionObservation{uuidToString(a.Runner.ID), uuidToString(a.Run.ID), request}
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
	registration := generation.ready.Registrations[0]
	evictedBinding := hatchet.Assignment{TenantID: serverCred.TenantID, EngineRunID: uuidToString(sleepRun.HatchetRunID), WorkflowID: registration.EngineWorkflowID, VersionID: registration.EngineVersionID, TaskName: registration.Actions[0].TaskName, ActionID: registration.Actions[0].ActionID, WorkerName: "multica-" + oldGeneration}
	if err := enginePool.QueryRow(ctx, `SELECT t.external_id::text,t.retry_count,lf.latest_invocation_count FROM v1_task t JOIN v1_durable_event_log_file lf ON (lf.durable_task_id,lf.durable_task_inserted_at)=(t.id,t.inserted_at) AND lf.tenant_id=t.tenant_id WHERE t.tenant_id=$1 AND t.workflow_run_id=$2 AND t.step_readable_id=$3`, serverCred.TenantID, evictedBinding.EngineRunID, evictedBinding.TaskName).Scan(&evictedBinding.EngineTaskID, &evictedBinding.Retry, &evictedBinding.Invocation); err != nil {
		t.Fatal(err)
	}
	if err := assignments.VerifyAssignment(ctx, evictedBinding); !errors.Is(err, hatchet.ErrAssignmentDenied) {
		t.Fatalf("evicted runtime accepted: %v %+v", err, evictedBinding)
	}
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
	// The pinned stop unregisters before awaiting futures. Wait for that actual
	// scheduling state so this proves the retiring hot call after unregister.
	unregistered := false
	for deadline := time.Now().Add(3 * time.Second); time.Now().Before(deadline); {
		var paused, inactive, deleted bool
		if err := enginePool.QueryRow(ctx, `SELECT "isPaused",NOT "isActive","deletedAt" IS NOT NULL FROM "Worker" WHERE "tenantId"=$1 AND name=$2`, serverCred.TenantID, "multica-"+oldGeneration).Scan(&paused, &inactive, &deleted); err != nil {
			t.Fatal(err)
		}
		if inactive {
			t.Logf("retiring worker paused=%t inactive=%t deleted=%t before hot action", paused, inactive, deleted)
			unregistered = true
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if !unregistered {
		t.Fatal("retiring hot call fixture did not observe unregister")
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
	resumedValue, ok := observedAssignments.Load(uuidToString(sleepRun.ID))
	if !ok {
		t.Fatal("missing resumed assignment")
	}
	resumed := resumedValue.(hatchet.Assignment)
	if resumed.EngineTaskID != evictedBinding.EngineTaskID || resumed.Invocation <= evictedBinding.Invocation || resumed.WorkerName == evictedBinding.WorkerName {
		t.Fatalf("no actual durable reassignment: old=%+v resumed=%+v", evictedBinding, resumed)
	}
	if err := assignments.VerifyAssignment(ctx, evictedBinding); !errors.Is(err, hatchet.ErrAssignmentDenied) {
		t.Fatalf("stale reassigned tuple accepted: %v", err)
	}
	t.Logf("evicted/reassigned tuple denied task=%s oldInvocation=%d resumedInvocation=%d", resumed.EngineTaskID, evictedBinding.Invocation, resumed.Invocation)
	for _, r := range []db.WorkflowRun{sleepRun, newRun} {
		var issues, receipts int
		if err := testPool.QueryRow(ctx, "SELECT count(*) FROM issue WHERE origin_type='workflow' AND origin_id=$1", r.ID).Scan(&issues); err != nil {
			t.Fatal(err)
		}
		if err := testPool.QueryRow(ctx, "SELECT count(*) FROM workflow_operation WHERE run_id=$1 AND status='completed'", r.ID).Scan(&receipts); err != nil {
			t.Fatal(err)
		}
		if issues != 1 || receipts != 1 {
			t.Fatalf("real assignment/business replay duplicated: run=%s issues=%d receipts=%d", uuidToString(r.ID), issues, receipts)
		}
	}
	t.Log("real native assignment + actual atomic issue producer: one issue/receipt across native retry and durable invocation replay")
	manager.reconcile(ctx, watched)
	if generation.retiring != nil || !engineRevoked(oldGeneration) || engineRevoked(ready.Generation) {
		t.Fatal("completed retiring engine credential not targeted independently of healthy current")
	}
	// Explicitly force the first SDK call to win the local ACK race, once with
	// successful response and once with accepted-response loss at maximum input.
	for _, lost := range []bool{false, true} {
		h.WorkflowSvc.Engine = nil
		input := json.RawMessage(`{}`)
		if lost {
			input = json.RawMessage(`{"pad":"` + strings.Repeat("x", (64<<10)-10) + `"}`)
			if len(input) != 64<<10 {
				t.Fatal("maximum payload fixture size", len(input))
			}
		}
		pending, err := h.WorkflowSvc.AdmitRun(ctx, workflow, actor, "manual", pgtype.UUID{}, input)
		if err != nil {
			t.Fatal(err)
		}
		signal := make(chan struct{})
		preACKActions.Store(uuidToString(pending.ID), signal)
		ambiguous := &workflowLivePreACKEngine{workflowLiveEngine: workflowLiveEngine{engine, serverCred}, action: signal, lost: lost}
		h.WorkflowSvc.Engine = ambiguous
		if _, err = h.WorkflowSvc.SubmitRun(ctx, service.RunIntent{WorkspaceID: ws, RunID: pending.ID}); err != nil {
			t.Fatal(err)
		}
		bound, err := queries.GetWorkflowRunInWorkspace(ctx, db.GetWorkflowRunInWorkspaceParams{ID: pending.ID, WorkspaceID: ws})
		if err != nil || !bound.HatchetRunID.Valid || bound.Status != "running" {
			t.Fatalf("first SDK action failed to bind before ACK lost=%t row=%+v err=%v", lost, bound, err)
		}
		if _, err = h.WorkflowSvc.SubmitRun(ctx, service.RunIntent{WorkspaceID: ws, RunID: pending.ID}); err != nil || ambiguous.submits.Load() != 1 {
			t.Fatalf("accepted first action caused second Submit lost=%t submits=%d err=%v", lost, ambiguous.submits.Load(), err)
		}
		for _, id := range []pgtype.UUID{bound.HatchetRunID, parseUUID(uuid.NewString())} {
			_, _ = queries.AcknowledgeWorkflowRun(ctx, db.AcknowledgeWorkflowRunParams{ID: pending.ID, WorkspaceID: ws, HatchetRunID: id})
			_, _ = queries.AcknowledgeWorkflowSubmissionAttempt(ctx, db.AcknowledgeWorkflowSubmissionAttemptParams{ID: pending.ID, WorkspaceID: ws, HatchetRunID: id, SubmissionAttempts: bound.SubmissionAttempts})
		}
		current, err := queries.GetWorkflowRunInWorkspace(ctx, db.GetWorkflowRunInWorkspaceParams{ID: pending.ID, WorkspaceID: ws})
		if err != nil || current.HatchetRunID != bound.HatchetRunID {
			t.Fatal("late trusted ACK replaced first engine ID", err)
		}
		t.Logf("first action before ACK lost=%t run=%s engine=%s submits=%d", lost, uuidToString(pending.ID), uuidToString(bound.HatchetRunID), ambiguous.submits.Load())
	}
	h.WorkflowSvc.Engine = workflowLiveEngine{engine, serverCred}
	if call("GET", "/api/daemon/workflows/revisions", enrollment.Token, "10000000-0000-0000-0000-000000000001", nil, nil) != 401 {
		t.Fatal("cross workspace accepted")
	}
	if call("POST", "/api/daemon/workflows/revoke", enrollment.Token, testWorkspaceID, nil, nil) != 200 {
		t.Fatal("revoke")
	}
	if h.WorkflowRunnerEligible(ctx, ws, publish2.Revision.ID) {
		t.Fatal("revoked generation still eligible")
	}
	if !engineRevoked(ready.Generation) {
		t.Fatal("global revoke left current engine credential valid")
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
