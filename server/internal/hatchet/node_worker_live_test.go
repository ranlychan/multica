package hatchet

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/cookiejar"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// Test-only bridge: the product server never executes workspace-authored code.
func TestPackagedNodeWorkerLive(t *testing.T) {
	if os.Getenv("MULTICA_NODE_WORKER_LIVE") != "1" {
		t.Skip("opt-in disposable Node worker proof")
	}
	root := t.TempDir()
	l := &liveProbe{t: t, root: root, client: &http.Client{Timeout: 5 * time.Second}, responses: map[string]json.RawMessage{}}
	name := "multica-node-proof-" + strconv.FormatInt(time.Now().UnixNano(), 36)
	l.command(nil, "docker", "run", "--detach", "--rm", "--name", name, "-p", "127.0.0.1::5432", "-e", "POSTGRES_PASSWORD=node-proof", "-e", "POSTGRES_DB=hatchet", "postgres:15.6", "-c", "max_connections=500")
	t.Cleanup(func() { _ = exec.Command("docker", "rm", "-f", name).Run() })
	port := strings.TrimSpace(l.command(nil, "docker", "port", name, "5432/tcp"))
	l.wait("postgres", 30*time.Second, func() bool { return exec.Command("docker", "exec", name, "pg_isready", "-U", "postgres").Run() == nil })
	c := Config{Enabled: true, DatabaseURL: "postgres://postgres:node-proof@" + port + "/hatchet?sslmode=disable", APIURL: "http://127.0.0.1:" + freePort(t), GRPCAddress: "127.0.0.1:" + freePort(t), TLSStrategy: "mtls", StateDir: filepath.Join(root, "state"), BinaryDir: os.Getenv("HATCHET_BIN_DIR"), ProvisioningEmail: "node@example.com", ProvisioningPassword: "DisposablePassword123!", StartupTimeout: 90 * time.Second, ShutdownTimeout: 15 * time.Second}
	s := NewSupervisor(c, nil)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		_ = s.Shutdown(ctx)
	})
	if err := s.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	l.origin = c.APIURL
	jar, _ := cookiejar.New(nil)
	user := &http.Client{Jar: jar, Timeout: 5 * time.Second}
	status, body := l.request("POST", "/api/v1/users/login", map[string]string{"email": c.ProvisioningEmail, "password": c.ProvisioningPassword}, "", user)
	l.requireStatus("login", status, 200, body)
	status, body = l.request("POST", "/api/v1/tenants", map[string]string{"name": "node-proof", "slug": "node-proof", "engineVersion": "V1"}, "", user)
	l.requireStatus("tenant", status, 200, body)
	tenant := jsonString(t, body, "metadata", "id")
	mint := func(label string) string {
		status, body := l.request("POST", "/api/v1/tenants/"+tenant+"/api-tokens", map[string]string{"name": label, "expiresIn": "1h"}, "", user)
		l.requireStatus("mint", status, 200, body)
		return jsonString(t, body, "token")
	}
	workerToken, serverToken := mint("node-worker"), mint("test-only-server")
	data, _ := json.Marshal(map[string]any{"root": root, "tenant": tenant, "workerToken": workerToken, "serverToken": serverToken, "api": c.APIURL, "grpc": c.GRPCAddress, "certs": filepath.Join(c.StateDir, "certs"), "authoredBoundaryOnly": os.Getenv("MULTICA_NODE_BOUNDARY_ONLY") == "1"})
	script, err := filepath.Abs("../../../packages/workflow-runner/test/live-proof.mjs")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, "node", script)
	cmd.Stdin = strings.NewReader(string(data))
	cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "LANG=C"}
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err = cmd.Run()
	if err != nil {
		t.Fatalf("Node proof: %v", err)
	}
}
