package hatchet

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"net/http"
	"net/http/cookiejar"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestHatchetSupervisorPinnedLifecycle(t *testing.T) {
	if os.Getenv("HATCHET_SUPERVISOR_LIVE") != "1" {
		t.Skip("opt-in disposable pinned supervisor smoke")
	}
	root := t.TempDir()
	bin := os.Getenv("HATCHET_BIN_DIR")
	name := "multica-hatchet-supervisor-" + strconv.FormatInt(time.Now().UnixNano(), 36)
	l := &liveProbe{t: t, root: root, client: &http.Client{Timeout: 5 * time.Second}, responses: map[string]json.RawMessage{}}
	l.command(nil, "docker", "run", "--detach", "--rm", "--name", name, "-p", "127.0.0.1::5432", "-e", "POSTGRES_PASSWORD=disposable", "-e", "POSTGRES_DB=hatchet", "postgres:15.6", "-c", "max_connections=500")
	t.Cleanup(func() { _ = exec.Command("docker", "rm", "-f", name).Run() })
	dbport := strings.TrimSpace(l.command(nil, "docker", "port", name, "5432/tcp"))
	l.wait("postgres", 30*time.Second, func() bool { return exec.Command("docker", "exec", name, "pg_isready", "-U", "postgres").Run() == nil })
	apiPort, grpcPort := freePort(t), freePort(t)
	c := Config{Enabled: true, DatabaseURL: "postgres://postgres:disposable@" + dbport + "/hatchet?sslmode=disable", APIURL: "http://127.0.0.1:" + apiPort, GRPCAddress: "0.0.0.0:" + grpcPort, TLSStrategy: "mtls", StateDir: filepath.Join(root, "state"), BinaryDir: bin, ProvisioningEmail: "supervisor@example.com", ProvisioningPassword: "DisposablePassword123!", StartupTimeout: 90 * time.Second, ShutdownTimeout: 15 * time.Second}
	s := NewSupervisor(c, nil)
	ctx := context.Background()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		_ = s.Shutdown(ctx)
	})
	if err := s.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if !s.Ready() {
		t.Fatal("not ready")
	}
	hashes, err := stateHashes(c)
	if err != nil {
		t.Fatal(err)
	}
	// Inspect the actual private REST and health listeners, rather than trusting URL configuration.
	for _, p := range s.children {
		out, err := exec.Command("lsof", "-nP", "-a", "-p", strconv.Itoa(p.cmd.Process.Pid), "-iTCP", "-sTCP:LISTEN").CombinedOutput()
		if err != nil {
			t.Fatalf("listener inspection: %v", err)
		}
		t.Logf("%s listeners:\n%s", filepath.Base(p.cmd.Path), out)
		for _, line := range strings.Split(string(out), "\n") {
			if strings.Contains(line, "(LISTEN)") && !strings.Contains(line, ":"+grpcPort+" ") && !strings.Contains(line, "127.0.0.1:") {
				t.Fatalf("public REST/health listener: %s", line)
			}
		}
	}
	ca, _ := os.ReadFile(filepath.Join(c.StateDir, "certs/ca.cert"))
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(ca)
	noCert, err := tls.Dial("tcp", "127.0.0.1:"+grpcPort, &tls.Config{RootCAs: pool, ServerName: "cluster", MinVersion: tls.VersionTLS12})
	if err == nil {
		noCert.SetReadDeadline(time.Now().Add(time.Second))
		_, err = noCert.Read(make([]byte, 1))
		noCert.Close()
	}
	if err == nil {
		t.Fatal("public gRPC accepted no mTLS certificate")
	}
	t.Log("public gRPC rejected certificate-less connection")
	l.origin = c.APIURL
	jar, _ := cookiejar.New(nil)
	user := &http.Client{Jar: jar, Timeout: 5 * time.Second}
	status, body := l.request("POST", "/api/v1/users/login", map[string]string{"email": c.ProvisioningEmail, "password": c.ProvisioningPassword}, "", user)
	l.requireStatus("login", status, 200, body)
	status, body = l.request("POST", "/api/v1/tenants", map[string]string{"name": "supervisor", "slug": "supervisor", "engineVersion": "V1"}, "", user)
	l.requireStatus("tenant", status, 200, body)
	var tenant tenantWire
	if json.Unmarshal(body, &tenant) != nil || !validID(tenant.Metadata.ID) {
		t.Fatal("tenant response")
	}
	status, body = l.request("POST", "/api/v1/tenants/"+tenant.Metadata.ID+"/api-tokens", map[string]string{"name": "multica-worker-smoke", "expiresIn": "1h"}, "", user)
	l.requireStatus("mint", status, 200, body)
	var token struct {
		Token string `json:"token"`
	}
	json.Unmarshal(body, &token)
	id, err := workerTokenID(token.Token)
	if err != nil {
		t.Fatal(err)
	}
	status, body = l.request("POST", "/api/v1/tenants/"+tenant.Metadata.ID+"/api-tokens", map[string]string{"name": "multica-server", "expiresIn": "1h"}, "", user)
	l.requireStatus("server mint", status, 200, body)
	var serverToken struct {
		Token string `json:"token"`
	}
	_ = json.Unmarshal(body, &serverToken)
	workerBin := os.Getenv("HATCHET_PROBE_WORKER")
	if workerBin == "" {
		t.Fatal("probe worker required")
	}
	workerEnv := append(os.Environ(), "HATCHET_CLIENT_TOKEN="+token.Token, "HATCHET_CLIENT_HOST_PORT=127.0.0.1:"+grpcPort, "HATCHET_CLIENT_SERVER_URL="+c.APIURL, "HATCHET_CLIENT_TLS_STRATEGY=mtls", "HATCHET_CLIENT_TLS_SERVER_NAME=cluster", "HATCHET_CLIENT_TLS_ROOT_CA_FILE="+filepath.Join(c.StateDir, "certs/ca.cert"), "HATCHET_CLIENT_TLS_CERT_FILE="+filepath.Join(c.StateDir, "certs/client-worker.pem"), "HATCHET_CLIENT_TLS_KEY_FILE="+filepath.Join(c.StateDir, "certs/client-worker.key"), "PROBE_REVISION=r1", "PROBE_EFFECTS="+filepath.Join(root, "effects"), "PROBE_COMPLETIONS="+filepath.Join(root, "completed"))
	worker := l.start(workerEnv, "authenticated-worker", workerBin)
	l.wait("registered worker", 20*time.Second, func() bool {
		status, body := l.request("GET", "/api/v1/tenants/"+tenant.Metadata.ID+"/workflows", nil, token.Token, nil)
		return status == 200 && strings.Contains(string(body), "mc-contract-r1")
	})
	status, body = l.request("POST", "/api/v1/stable/tenants/"+tenant.Metadata.ID+"/workflow-runs/trigger", map[string]any{"workflowName": "mc-contract-r1", "input": map[string]any{"multica_run_id": "before-revoke-stream", "delay": 0}}, serverToken.Token, nil)
	l.requireStatus("pre-revoke trigger", status, 200, body)
	l.wait("authenticated stream performed pre-revoke task", 20*time.Second, func() bool {
		raw, _ := os.ReadFile(filepath.Join(root, "completed"))
		return strings.Contains(string(raw), "before-revoke-stream")
	})
	preLog, _ := os.ReadFile(filepath.Join(root, "authenticated-worker.log"))
	preActions := strings.Count(string(preLog), "received action")
	status, body = l.request("POST", "/api/v1/api-tokens/"+id, nil, "", user)
	l.requireStatus("revoke", status, 204, body)
	// Pinned token repository caches for5s. Allow TTL+margin before asserting
	// fresh worker authentication; revocation is not an immediate authorization gate.
	time.Sleep(7 * time.Second)
	authCtx, authCancel := context.WithTimeout(ctx, 3*time.Second)
	defer authCancel()
	cmd := exec.CommandContext(authCtx, workerBin)
	cmd.Env = workerEnv
	cmd.Dir = root
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatal("revoked worker authenticated")
	}
	if !strings.Contains(strings.ToLower(string(out)), "unauthenticated") || strings.Contains(string(out), "Registered worker with id") {
		t.Fatalf("new worker failed without revocation evidence: %s", strings.ReplaceAll(string(out), token.Token, "[redacted]"))
	}
	t.Log("revoked credential rejected new worker authentication")
	status, body = l.request("POST", "/api/v1/stable/tenants/"+tenant.Metadata.ID+"/workflow-runs/trigger", map[string]any{"workflowName": "mc-contract-r1", "input": map[string]any{"multica_run_id": "post-revoke-stream", "delay": 0}}, serverToken.Token, nil)
	l.requireStatus("post-revoke trigger", status, 200, body)
	l.wait("existing stream received post-revoke assignment", 20*time.Second, func() bool {
		raw, _ := os.ReadFile(filepath.Join(root, "authenticated-worker.log"))
		return strings.Count(string(raw), "received action") > preActions
	})
	t.Log("existing authenticated action stream received assignment after revoke/cache expiry; revocation does not force-disconnect that stream")
	time.Sleep(500 * time.Millisecond)
	if err := worker.cmd.Process.Signal(syscall.Signal(0)); err != nil {
		t.Log("existing authenticated worker terminated after revoke")
	} else {
		t.Log("existing authenticated worker process survived revoke; process survival alone does not prove established stream authorization")
	}
	worker.cmd.Process.Kill()
	shutdownCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	if err := s.Shutdown(shutdownCtx); err != nil {
		t.Fatal(err)
	}
	if s.Ready() {
		t.Fatal("ready after shutdown")
	}
	if err := s.Start(ctx); err != nil {
		t.Fatal(err)
	}
	after, err := stateHashes(c)
	if err != nil {
		t.Fatal(err)
	}
	for file, sum := range hashes.Files {
		if after.Files[file] != sum {
			t.Fatalf("restart changed %s", file)
		}
	}
	t.Log("actual graceful shutdown/restart preserved all config/TLS/keyset hashes")
}
