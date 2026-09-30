package hatchet

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// This opt-in test creates and removes only its own container and child processes.
// It requires the four pinned binaries and the SDK probe built from the same pin.
func TestPinnedLiveContract(t *testing.T) {
	if os.Getenv("HATCHET_LIVE") != "1" {
		t.Skip("set HATCHET_LIVE=1 for disposable Postgres/engine probes")
	}
	bin, workerBin := os.Getenv("HATCHET_BIN_DIR"), os.Getenv("HATCHET_PROBE_WORKER")
	for _, path := range []string{filepath.Join(bin, "hatchet-admin"), filepath.Join(bin, "hatchet-migrate"), filepath.Join(bin, "hatchet-api"), filepath.Join(bin, "hatchet-engine"), workerBin} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("required pinned binary %q: %v", path, err)
		}
	}
	root := t.TempDir()
	if capture := os.Getenv("HATCHET_CAPTURE_DIR"); capture != "" {
		root = capture
		if err := os.MkdirAll(root, 0700); err != nil {
			t.Fatal(err)
		}
	}
	name := "multica-hatchet-probe-" + strconv.FormatInt(time.Now().UnixNano(), 36)
	l := &liveProbe{t: t, root: root, client: &http.Client{Timeout: 10 * time.Second}, responses: map[string]json.RawMessage{}}
	defer func() {
		data, _ := json.MarshalIndent(l.responses, "", "  ")
		_ = os.WriteFile(filepath.Join(root, "responses.json"), data, 0600)
	}()
	l.command(nil, "docker", "run", "--detach", "--rm", "--name", name, "-p", "127.0.0.1::5432", "-e", "POSTGRES_PASSWORD=probe-local-only", "-e", "POSTGRES_DB=hatchet", "postgres:15.6", "-c", "max_connections=500")
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		if out, err := exec.CommandContext(ctx, "docker", "rm", "-f", name).CombinedOutput(); err != nil {
			t.Errorf("remove own container: %v %s", err, out)
		}
	})
	port := strings.TrimSpace(l.command(nil, "docker", "port", name, "5432/tcp"))
	apiPort, grpcPort, healthPort := freePort(t), freePort(t), freePort(t)
	l.origin = "http://127.0.0.1:" + apiPort
	env := append(os.Environ(), "DATABASE_URL=postgres://postgres:probe-local-only@"+port+"/hatchet?sslmode=disable",
		"DATABASE_MAX_CONNS=15", "DATABASE_MIN_CONNS=1", "SERVER_MSGQUEUE_KIND=postgres", "SERVER_MSGQUEUE_PUBSUB_KIND=postgres",
		"SERVER_PORT="+apiPort, "SERVER_URL="+l.origin, "SERVER_GRPC_PORT="+grpcPort, "SERVER_GRPC_BIND_ADDRESS=127.0.0.1",
		"SERVER_GRPC_BROADCAST_ADDRESS=127.0.0.1:"+grpcPort, "SERVER_INTERNAL_CLIENT_INTERNAL_GRPC_BROADCAST_ADDRESS=127.0.0.1:"+grpcPort,
		"SERVER_HEALTHCHECK_PORT="+healthPort, "SERVER_TLS_STRATEGY=tls", "SERVER_GRPC_INSECURE=false",
		"SERVER_SECURITY_CHECK_ENABLED=false", "SERVER_AUTH_COOKIE_INSECURE=true", "SERVER_AUTH_COOKIE_DOMAIN=127.0.0.1",
		"SERVER_ALLOW_CREATE_TENANT=true", "SERVER_AUTH_BASIC_AUTH_ENABLED=true", "SERVER_AUTH_SET_EMAIL_VERIFIED=true",
		"SERVER_ANALYTICS_POSTHOG_ENABLED=false", "SERVER_ANALYTICS_AGGREGATE_ENABLED=false", "SERVER_PROMETHEUS_ENABLED=false",
		"ADMIN_EMAIL=probe@example.com", "ADMIN_PASSWORD=ProbePassword123!", "SERVER_LOGGER_LEVEL=warn")
	l.wait("postgres", 30*time.Second, func() bool { return exec.Command("docker", "exec", name, "pg_isready", "-U", "postgres").Run() == nil })
	l.command(env, filepath.Join(bin, "hatchet-migrate"))
	config := filepath.Join(root, "config")
	l.command(env, filepath.Join(bin, "hatchet-admin"), "quickstart", "--generated-config-dir", config, "--cert-dir", filepath.Join(root, "certs"), "--skip", "seed", "--overwrite=false")
	l.command(env, filepath.Join(bin, "hatchet-admin"), "seed", "--config", config)
	engine := l.start(env, "engine", filepath.Join(bin, "hatchet-engine"), "--config", config)
	l.start(env, "api", filepath.Join(bin, "hatchet-api"), "--config", config)
	l.wait("API", 45*time.Second, func() bool { status, _ := l.request("GET", "/api/v1/meta", nil, "", nil); return status > 0 })
	jar, _ := cookiejar.New(nil)
	user := &http.Client{Jar: jar, Timeout: 10 * time.Second}
	status, body := l.request("POST", "/api/v1/users/login", map[string]any{"email": "probe@example.com", "password": "ProbePassword123!"}, "", user)
	l.requireStatus("login", status, 200, body)
	l.capture("login", status, map[string]any{"authenticated": true})
	newTenant := func(slug string) string {
		status, body := l.request("POST", "/api/v1/tenants", map[string]any{"name": slug, "slug": slug, "engineVersion": "V1"}, "", user)
		l.requireStatus("create tenant", status, 200, body)
		l.capture("tenant-"+slug, status, json.RawMessage(body))
		return jsonString(t, body, "metadata", "id")
	}
	tenantA, tenantB := newTenant("probe-a"), newTenant("probe-b")
	mint := func(id string) string {
		return strings.TrimSpace(l.command(env, filepath.Join(bin, "hatchet-admin"), "token", "create", "--config", config, "--tenant-id", id, "--name", "multica-contract", "--expiresIn", "1h"))
	}
	tokenA, tokenB := mint(tenantA), mint(tenantB)
	listPath := "/api/v1/stable/tenants/" + tenantA + "/workflow-runs?only_tasks=false&since=" + url.QueryEscape(time.Now().Add(-time.Minute).UTC().Format(time.RFC3339))
	status, body = l.request("GET", listPath, nil, tokenB, nil)
	if status != 403 {
		t.Fatalf("cross-tenant access: got %d, want 403: %s", status, body)
	}
	l.capture("cross-tenant", status, json.RawMessage(body))
	status, body = l.request("POST", "/api/v1/tenants", map[string]any{"name": "forbidden", "slug": "forbidden"}, tokenA, nil)
	l.requireStatus("tenant token cannot provision", status, 403, body)
	l.capture("tenant-token-provisioning", status, json.RawMessage(body))
	effects := filepath.Join(root, "effects.txt")
	workerEnv := append(os.Environ(), "HATCHET_CLIENT_TOKEN="+tokenA, "HATCHET_CLIENT_HOST_PORT=127.0.0.1:"+grpcPort, "HATCHET_CLIENT_SERVER_URL="+l.origin, "HATCHET_CLIENT_TLS_STRATEGY=tls", "HATCHET_CLIENT_TLS_ROOT_CA_FILE="+filepath.Join(root, "certs", "ca.cert"), "PROBE_EFFECTS="+effects)
	worker1 := l.start(append(workerEnv, "PROBE_REVISION=r1"), "worker-r1", workerBin)
	registered := func(rev string) {
		l.wait("register "+rev, 45*time.Second, func() bool {
			status, body := l.request("GET", "/api/v1/tenants/"+tenantA+"/workflows", nil, tokenA, nil)
			return status == 200 && bytes.Contains(body, []byte("mc-contract-"+rev))
		})
	}
	registered("r1")
	runID := "ba5be5a3-4b5f-4cd1-9240-6f63730ab001"
	input := map[string]any{"multica_run_id": runID, "delay": 12, "payload": map[string]any{"message": "probe"}}
	trigger := func(rev string, input map[string]any) (int, []byte) {
		return l.request("POST", "/api/v1/stable/tenants/"+tenantA+"/workflow-runs/trigger", map[string]any{"workflowName": "mc-contract-" + rev, "input": input, "return_only_id": true}, tokenA, nil)
	}
	// A real forwarding proxy lets the engine accept the request, then closes the
	// connection before the caller receives a response. Recovery cannot rely on it.
	type acceptedResponse struct {
		status int
		body   []byte
	}
	accepted := make(chan acceptedResponse, 1)
	faultProxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload any
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			http.Error(w, "bad request", 400)
			return
		}
		code, response := l.request("POST", "/api/v1/stable/tenants/"+tenantA+"/workflow-runs/trigger", payload, tokenA, nil)
		accepted <- acceptedResponse{code, response}
		conn, _, err := w.(http.Hijacker).Hijack()
		if err == nil {
			_ = conn.Close()
		}
	}))
	defer faultProxy.Close()
	payload, _ := json.Marshal(map[string]any{"workflowName": "mc-contract-r1", "input": input, "return_only_id": true})
	resp, submitErr := l.client.Post(faultProxy.URL, "application/json", bytes.NewReader(payload))
	if resp != nil {
		_ = resp.Body.Close()
	}
	if submitErr == nil {
		t.Fatal("fault proxy unexpectedly delivered a response")
	}
	observed := <-accepted
	status, body = observed.status, observed.body
	l.requireStatus("submit", status, 200, body)
	l.capture("ambiguous-response", 0, map[string]any{"connection_closed_after_accept": true})
	l.capture("submit", status, json.RawMessage(body))
	engineID := jsonString(t, body, "run", "metadata", "id")
	status, body = trigger("r1", input)
	if status < 400 {
		t.Fatalf("duplicate did not reject: %d %s", status, body)
	}
	l.capture("duplicate", status, json.RawMessage(body))
	reconcilePath := listPath + "&idempotency_keys=" + runID + "&limit=100"
	l.wait("idempotency reconciliation", 45*time.Second, func() bool {
		status, body := l.request("GET", reconcilePath, nil, tokenA, nil)
		if status != 200 {
			return false
		}
		var result struct {
			Rows []struct {
				Metadata struct {
					ID string `json:"id"`
				} `json:"metadata"`
			} `json:"rows"`
		}
		if json.Unmarshal(body, &result) != nil || len(result.Rows) == 0 {
			return false
		}
		if len(result.Rows) != 1 || result.Rows[0].Metadata.ID != engineID {
			t.Fatalf("idempotency did not converge: %s", body)
		}
		l.capture("reconcile", status, json.RawMessage(body))
		return true
	})
	getRun := func(id string) (int, []byte) {
		return l.request("GET", "/api/v1/stable/workflow-runs/"+id, nil, tokenA, nil)
	}
	var taskID string
	l.wait("memo and durable wait", 45*time.Second, func() bool {
		status, body := getRun(engineID)
		if status != 200 {
			return false
		}
		var result struct {
			Tasks []struct {
				Metadata struct {
					ID string `json:"id"`
				} `json:"metadata"`
			} `json:"tasks"`
		}
		_ = json.Unmarshal(body, &result)
		if len(result.Tasks) == 0 {
			return false
		}
		taskID = result.Tasks[0].Metadata.ID
		status, body = l.request("GET", "/api/v1/stable/tenants/"+tenantA+"/durable-tasks/"+taskID, nil, tokenA, nil)
		if status != 200 || !bytes.Contains(body, []byte("SLEEP")) {
			return false
		}
		l.capture("durable-wait", status, json.RawMessage(body))
		return true
	})
	// Register the next revision while r1 is sleeping, then crash both r1's worker
	// and the engine. Recovery must use r1 and must not repeat its durable memo.
	l.start(append(workerEnv, "PROBE_REVISION=r2"), "worker-r2", workerBin)
	registered("r2")
	worker1.kill()
	engine.kill()
	engine = l.start(env, "engine-restarted", filepath.Join(bin, "hatchet-engine"), "--config", config)
	_ = engine
	l.start(append(workerEnv, "PROBE_REVISION=r1"), "worker-r1-restarted", workerBin)
	l.wait("durable recovery", 150*time.Second, func() bool {
		status, body := getRun(engineID)
		if status != 200 {
			return false
		}
		state := jsonString(t, body, "run", "status")
		if state == "FAILED" || state == "CANCELLED" {
			t.Fatalf("recovery failed: %s", body)
		}
		if state != "COMPLETED" {
			return false
		}
		if !bytes.Contains(body, []byte(`"revision":"r1"`)) {
			t.Fatalf("revision drift: %s", body)
		}
		l.capture("recovered-run", status, json.RawMessage(body))
		return true
	})
	effectData, err := os.ReadFile(effects)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(string(effectData), runID+" r1\n") != 1 {
		t.Fatalf("memo effect repeated or missing: %q", effectData)
	}
	status, body = l.request("GET", "/api/v1/stable/tasks/"+taskID+"/logs?limit=10", nil, tokenA, nil)
	l.requireStatus("logs", status, 200, body)
	if !bytes.Contains(body, []byte("probe r1")) {
		t.Fatalf("expected task log missing: %s", body)
	}
	l.capture("logs", status, json.RawMessage(body))
	status, body = l.request("GET", "/api/v1/stable/workflow-runs/"+engineID+"/status", nil, tokenA, nil)
	l.requireStatus("status", status, 200, body)
	if string(body) != "\"COMPLETED\"\n" && string(body) != "\"COMPLETED\"" {
		t.Fatalf("unexpected status wire shape: %s", body)
	}
	l.capture("status", status, json.RawMessage(body))
	// Product replay is a fresh UUID plus active revision, never tasks/replay.
	input["multica_run_id"] = "ba5be5a3-4b5f-4cd1-9240-6f63730ab002"
	input["delay"] = 0
	status, body = trigger("r2", input)
	l.requireStatus("fresh replay submit", status, 200, body)
	replayID := jsonString(t, body, "run", "metadata", "id")
	if replayID == engineID {
		t.Fatal("replay reused original engine ID")
	}
	l.wait("replay completion", 45*time.Second, func() bool {
		status, body := getRun(replayID)
		if status != 200 || jsonString(t, body, "run", "status") != "COMPLETED" {
			return false
		}
		if !bytes.Contains(body, []byte(`"revision":"r2"`)) {
			t.Fatalf("replay revision mismatch: %s", body)
		}
		l.capture("product-replay", status, json.RawMessage(body))
		return true
	})
	input["multica_run_id"] = "ba5be5a3-4b5f-4cd1-9240-6f63730ab003"
	input["delay"] = 90
	status, body = trigger("r2", input)
	l.requireStatus("cancel submit", status, 200, body)
	cancelID := jsonString(t, body, "run", "metadata", "id")
	for i := 0; i < 2; i++ {
		status, body = l.request("POST", "/api/v1/stable/tenants/"+tenantA+"/tasks/cancel", map[string]any{"externalIds": []string{cancelID}}, tokenA, nil)
		l.requireStatus("repeated cancel", status, 200, body)
		l.capture(fmt.Sprintf("cancel-%d", i), status, json.RawMessage(body))
	}
	l.wait("cancelled", 45*time.Second, func() bool {
		status, body := getRun(cancelID)
		if status == 200 && jsonString(t, body, "run", "status") == "CANCELLED" {
			l.capture("cancelled-run", status, json.RawMessage(body))
			return true
		}
		return false
	})
	t.Log("live contract passed: tenant isolation, input-CEL idempotency, revision names, memo/sleep crash recovery, logs/state, fresh replay, repeated cancellation")
}

type liveProbe struct {
	t            *testing.T
	root, origin string
	client       *http.Client
	responses    map[string]json.RawMessage
}

func TestCapturedLiveContract(t *testing.T) {
	data, err := os.ReadFile("testdata/live-responses.json")
	if err != nil {
		t.Fatal(err)
	}
	var captured map[string]struct {
		Status int             `json:"status"`
		Body   json.RawMessage `json:"body"`
	}
	if err := json.Unmarshal(data, &captured); err != nil {
		t.Fatal(err)
	}
	for name, status := range map[string]int{"tenant-probe-a": 200, "tenant-probe-b": 200, "cross-tenant": 403, "tenant-token-provisioning": 403, "submit": 200, "duplicate": 500, "reconcile": 200, "recovered-run": 200, "status": 200, "logs": 200, "product-replay": 200, "cancel-0": 200, "cancel-1": 200, "cancelled-run": 200, "durable-wait": 200} {
		if captured[name].Status != status {
			t.Errorf("%s: captured status %d, want %d", name, captured[name].Status, status)
		}
	}
	if got := jsonString(t, captured["recovered-run"].Body, "run", "status"); got != "COMPLETED" {
		t.Fatalf("recovery status %s", got)
	}
	if got := jsonString(t, captured["cancelled-run"].Body, "run", "status"); got != "CANCELLED" {
		t.Fatalf("cancel status %s", got)
	}
	var state string
	if err := json.Unmarshal(captured["status"].Body, &state); err != nil || state != "COMPLETED" {
		t.Fatalf("status API is not a completed JSON string: %s", captured["status"].Body)
	}
	original := jsonString(t, captured["submit"].Body, "run", "metadata", "id")
	recovered := jsonString(t, captured["recovered-run"].Body, "run", "metadata", "id")
	replayed := jsonString(t, captured["product-replay"].Body, "run", "metadata", "id")
	if original != recovered || original == replayed {
		t.Fatal("recovery/replay run identity contract changed")
	}
}

func (l *liveProbe) command(env []string, name string, args ...string) string {
	l.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Env = env
	out, err := cmd.CombinedOutput()
	if err != nil {
		l.t.Fatalf("%s %v: %v\n%s", name, args, err, out)
	}
	return string(out)
}

type liveProcess struct {
	cmd  *exec.Cmd
	done chan error
}

func (p *liveProcess) kill() {
	select {
	case <-p.done:
		return
	default:
	}
	_ = p.cmd.Process.Kill()
	<-p.done
}
func (l *liveProbe) start(env []string, label, name string, args ...string) *liveProcess {
	l.t.Helper()
	f, err := os.Create(filepath.Join(l.root, label+".log"))
	if err != nil {
		l.t.Fatal(err)
	}
	cmd := exec.Command(name, args...)
	cmd.Env = env
	cmd.Stdout = f
	cmd.Stderr = f
	if err := cmd.Start(); err != nil {
		l.t.Fatal(err)
	}
	p := &liveProcess{cmd: cmd, done: make(chan error, 1)}
	go func() { p.done <- cmd.Wait(); close(p.done); _ = f.Close() }()
	l.t.Cleanup(p.kill)
	return p
}
func (l *liveProbe) wait(label string, timeout time.Duration, fn func() bool) {
	l.t.Helper()
	until := time.Now().Add(timeout)
	for time.Now().Before(until) {
		if fn() {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	l.t.Fatalf("timed out waiting for %s; logs: %s", label, l.root)
}
func (l *liveProbe) request(method, path string, body any, token string, client *http.Client) (int, []byte) {
	data, _ := json.Marshal(body)
	req, err := http.NewRequest(method, l.origin+path, bytes.NewReader(data))
	if err != nil {
		l.t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if client == nil {
		client = l.client
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, []byte(err.Error())
	}
	defer resp.Body.Close()
	out, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	return resp.StatusCode, out
}
func (l *liveProbe) requireStatus(label string, got, want int, body []byte) {
	l.t.Helper()
	if got != want {
		l.t.Fatalf("%s: HTTP %d, want %d: %s (logs %s)", label, got, want, body, l.root)
	}
}
func (l *liveProbe) capture(label string, status int, body any) {
	data, err := json.Marshal(map[string]any{"status": status, "body": body})
	if err != nil {
		l.t.Fatal(err)
	}
	l.responses[label] = data
}
func freePort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return strconv.Itoa(l.Addr().(*net.TCPAddr).Port)
}
func jsonString(t *testing.T, data []byte, path ...string) string {
	t.Helper()
	var v any
	if err := json.Unmarshal(data, &v); err != nil {
		t.Fatal(err)
	}
	for _, key := range path {
		m, ok := v.(map[string]any)
		if !ok {
			t.Fatalf("missing %v in %s", path, data)
		}
		v = m[key]
	}
	s, ok := v.(string)
	if !ok {
		t.Fatalf("missing string %v in %s", path, data)
	}
	return s
}
