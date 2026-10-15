package daemon

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	wr "github.com/multica-ai/multica/server/internal/workflowrunner"
)

func TestWorkflowProbeFailsClosedWithoutOptInRuntimeTLSOrReachability(t *testing.T) {
	s := httptest.NewUnstartedServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	s.EnableHTTP2 = true
	s.StartTLS()
	defer s.Close()
	root := t.TempDir()
	ca := filepath.Join(root, "ca.pem")
	if err := os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: s.Certificate().Raw}), 0600); err != nil {
		t.Fatal(err)
	}
	entry, _ := filepath.Abs("../../../packages/workflow-runner/dist/cli.js")
	base := WorkflowRunnerConfig{Enabled: true, AcceptedCodeRisk: true, Workspaces: []string{"a"}, Node: workflowTestNode(t), Entry: entry, EngineAddress: strings.TrimPrefix(s.URL, "https://"), TLS: wr.TLS{Strategy: "tls", ServerName: "example.com", CAFile: ca}}
	for _, tc := range []struct {
		name, code string
		change     func(*WorkflowRunnerConfig)
	}{
		{"disabled", "workflow_disabled", func(c *WorkflowRunnerConfig) { c.Enabled = false }},
		{"risk", "workflow_disabled", func(c *WorkflowRunnerConfig) { c.AcceptedCodeRisk = false }},
		{"workspace", "workflow_disabled", func(c *WorkflowRunnerConfig) { c.Workspaces = nil }},
		{"node", "workflow_node_unavailable", func(c *WorkflowRunnerConfig) { c.Node = filepath.Join(root, "missing") }},
		{"entry", "workflow_runner_unavailable", func(c *WorkflowRunnerConfig) { c.Entry = filepath.Join(root, "missing") }},
		{"ca", "workflow_tls_unavailable", func(c *WorkflowRunnerConfig) { c.TLS.CAFile = filepath.Join(root, "missing") }},
		{"client-key", "workflow_tls_unavailable", func(c *WorkflowRunnerConfig) { c.TLS.Strategy = "mtls" }},
		{"identity", "workflow_engine_unavailable", func(c *WorkflowRunnerConfig) { c.TLS.ServerName = "wrong.example" }},
		{"wildcard", "workflow_engine_unavailable", func(c *WorkflowRunnerConfig) { c.EngineAddress = "0.0.0.0:1234" }},
		{"unreachable", "workflow_engine_unavailable", func(c *WorkflowRunnerConfig) { c.EngineAddress = "127.0.0.1:1" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := base
			tc.change(&c)
			if _, err := (&WorkflowRunner{Config: c}).Probe(context.Background()); err == nil || err.Error() != tc.code {
				t.Fatalf("got %v want %s", err, tc.code)
			}
		})
	}
	if _, err := (&WorkflowRunner{Config: base}).Probe(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func workflowOwnedTestProcess(t *testing.T, stuck bool) *WorkflowProcess {
	t.Helper()
	code := `process.on('SIGTERM',()=>{}); process.on('message',m=>{if(m.mode==='drain')process.exit(0)});setInterval(()=>{},1000);`
	if stuck {
		code = `process.on('SIGTERM',()=>{});process.on('message',()=>{});setInterval(()=>{},1000);`
	}
	return workflowOwnedCommand(t, code)
}
func workflowOwnedCommand(t *testing.T, code string) *WorkflowProcess {
	t.Helper()
	entry := filepath.Join(t.TempDir(), "worker.cjs")
	if err := os.WriteFile(entry, []byte(code+`process.send({type:'started'});`), 0600); err != nil {
		t.Fatal(err)
	}
	p, err := (&WorkflowRunner{Config: WorkflowRunnerConfig{Node: workflowTestNode(t), Entry: entry}}).start(context.Background(), map[string]string{"mode": "worker"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { p.Stop(time.Millisecond) })
	select {
	case m := <-p.messages:
		if m.Type != "started" {
			t.Fatal("test worker did not start")
		}
	case <-time.After(time.Second):
		t.Fatal("test worker did not become observable")
	}
	return p
}

func TestWorkflowManagerStopsWorkerErrorBeforeRenewingEligibility(t *testing.T) {
	p := workflowOwnedCommand(t, `process.once('message',()=>process.send({type:'error',code:'workflow_worker_failed'}));process.on('message',m=>{if(m.mode==='drain')process.exit(0)});setInterval(()=>{},1000);`)
	// Observe the real IPC scanner's error before giving the generation back.
	select {
	case m := <-p.messages:
		p.messages <- m
	case <-time.After(time.Second):
		t.Fatal("worker did not report failure")
	}
	acks := 0
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/ack") {
			acks++
		}
		w.Write([]byte(`{"revisions":[]}`))
	}))
	defer s.Close()
	m := newWorkflowManager(WorkflowRunnerConfig{Enabled: true, AcceptedCodeRisk: true, Workspaces: []string{"ws"}}, NewClient(s.URL))
	m.generations["ws"] = &workflowGeneration{control: "wdc_test", process: p, started: time.Now(), signature: wr.Hash("[]")}
	m.reconcile(context.Background(), map[string]bool{"ws": true})
	if acks != 0 {
		t.Fatal("failed worker renewed registration lease")
	}
	select {
	case <-p.done:
	case <-time.After(time.Second):
		t.Fatal("failed worker still owned")
	}
	if !m.generations["ws"].next.After(time.Now()) {
		t.Fatal("failed worker not backed off")
	}
}

func TestWorkflowInvalidDraftPreservesHealthyOldGeneration(t *testing.T) {
	source := `import {defineWorkflow} from '@multica/workflow-sdk';export default defineWorkflow({steps:[{id:'a',run:async()=>null}]});`
	graph := wr.Graph{Steps: []wr.Step{{ID: "a", Parents: []string{}}}}
	old := wr.Revision{RevisionID: "old", Source: source, Hash: wr.Hash(source), Graph: graph, GraphDigest: wr.GraphDigest(graph)}
	bad := wr.Revision{RevisionID: "bad", Source: "export default {steps:[]}", Hash: wr.Hash("export default {steps:[]}"), Graph: graph, GraphDigest: wr.GraphDigest(graph)}
	rotations := 0
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/revisions"):
			json.NewEncoder(w).Encode(map[string]any{"revisions": []wr.Revision{old, bad}})
		case strings.HasSuffix(r.URL.Path, "/rotate"):
			rotations++
			w.WriteHeader(503)
		default:
			w.Write([]byte(`{}`))
		}
	}))
	defer s.Close()
	entry, _ := filepath.Abs("../../../packages/workflow-runner/dist/cli.js")
	m := newWorkflowManager(WorkflowRunnerConfig{Enabled: true, AcceptedCodeRisk: true, Workspaces: []string{"ws"}, Node: workflowTestNode(t), Entry: entry, Cache: filepath.Join(t.TempDir(), "cache")}, NewClient(s.URL))
	p := workflowOwnedTestProcess(t, false)
	raw, _ := json.Marshal([]wr.Revision{old})
	m.generations["ws"] = &workflowGeneration{control: "wdc_test", process: p, started: time.Now(), signature: wr.Hash(string(raw)), ready: wr.Message{Type: "ready"}}
	m.reconcile(context.Background(), map[string]bool{"ws": true})
	select {
	case <-p.done:
		t.Fatal("invalid draft stopped healthy immutable old generation")
	default:
	}
	if rotations != 0 {
		t.Fatal("invalid draft rotated old product authority")
	}
}

func TestWorkflowRetirementUsesCredentialDeadlineWithoutStoppingCurrent(t *testing.T) {
	current, old := workflowOwnedTestProcess(t, false), workflowOwnedTestProcess(t, true)
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte(`{"revisions":[]}`)) }))
	defer s.Close()
	m := newWorkflowManager(WorkflowRunnerConfig{Enabled: true, AcceptedCodeRisk: true, Workspaces: []string{"ws"}}, NewClient(s.URL))
	g := &workflowGeneration{control: "wdc_test", process: current, started: time.Now(), signature: wr.Hash("[]"), retiring: &workflowGeneration{process: old, expires: time.Now().Add(-time.Second), ready: wr.Message{Generation: "old"}}}
	m.generations["ws"] = g
	m.reconcile(context.Background(), map[string]bool{"ws": true})
	select {
	case <-old.done:
	default:
		t.Fatal("expired retiring credential left child active")
	}
	select {
	case <-current.done:
		t.Fatal("retirement stopped current generation")
	default:
	}
	if g.retiring != nil {
		t.Fatal("retired pair did not release update backpressure")
	}
}

func TestWorkflowCandidateCacheFailurePreservesHealthyCurrentAndLease(t *testing.T) {
	source := `import {defineWorkflow} from '@multica/workflow-sdk';export default defineWorkflow({steps:[{id:'a',run:async()=>null}]});`
	graph := wr.Graph{Steps: []wr.Step{{ID: "a", Parents: []string{}}}}
	revision := wr.Revision{RevisionID: "candidate", Source: source, Hash: wr.Hash(source), Graph: graph, GraphDigest: wr.GraphDigest(graph)}
	acks, rotations := 0, 0
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/revisions"):
			json.NewEncoder(w).Encode(map[string]any{"revisions": []wr.Revision{revision}})
		case strings.HasSuffix(r.URL.Path, "/rotate"):
			rotations++
			w.WriteHeader(503)
		case strings.HasSuffix(r.URL.Path, "/ack"):
			var message wr.Message
			if err := json.NewDecoder(r.Body).Decode(&message); err != nil || message.Generation != "healthy-current" {
				t.Error("candidate cache failure acknowledged something other than existing current")
			}
			acks++
			w.Write([]byte(`{}`))
		default:
			t.Errorf("unexpected candidate request: %s", r.URL.Path)
			w.WriteHeader(500)
		}
	}))
	defer s.Close()
	// A regular file deterministically prevents cache directory creation even as root.
	cache := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(cache, []byte("occupied"), 0600); err != nil {
		t.Fatal(err)
	}
	entry, _ := filepath.Abs("../../../packages/workflow-runner/dist/cli.js")
	m := newWorkflowManager(WorkflowRunnerConfig{Enabled: true, AcceptedCodeRisk: true, Workspaces: []string{"ws"}, Node: workflowTestNode(t), Entry: entry, Cache: cache}, NewClient(s.URL))
	p := workflowOwnedTestProcess(t, false)
	g := &workflowGeneration{control: "wdc_test", process: p, started: time.Now(), signature: wr.Hash("[]"), ready: wr.Message{Type: "ready", Generation: "healthy-current"}}
	m.generations["ws"] = g
	m.reconcile(context.Background(), map[string]bool{"ws": true})
	select {
	case <-p.done:
		t.Fatal("candidate cache failure stopped healthy current")
	default:
	}
	if rotations != 0 || acks != 1 || !g.next.After(time.Now()) {
		t.Fatalf("candidate failure rotated=%d old lease renewals=%d backed off=%t", rotations, acks, g.next.After(time.Now()))
	}
}

func TestWorkflowCurrentCrashDoesNotForceStopValidRetiringWork(t *testing.T) {
	current, old := workflowOwnedTestProcess(t, false), workflowOwnedTestProcess(t, true)
	current.Stop(time.Second)
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte(`{"revisions":[]}`)) }))
	defer s.Close()
	m := newWorkflowManager(WorkflowRunnerConfig{Enabled: true, AcceptedCodeRisk: true, Workspaces: []string{"ws"}}, NewClient(s.URL))
	g := &workflowGeneration{control: "wdc_test", process: current, started: time.Now(), signature: wr.Hash("[]"), ready: wr.Message{Generation: "failed-current"}, retiring: &workflowGeneration{process: old, expires: time.Now().Add(time.Minute), ready: wr.Message{Generation: "old"}}}
	m.generations["ws"] = g
	m.reconcile(context.Background(), map[string]bool{"ws": true})
	select {
	case <-old.done:
		t.Fatal("current crash force-stopped valid retiring work")
	default:
	}
	if g.process != nil || g.retiring == nil || !g.next.After(time.Now()) {
		t.Fatal("failed current did not back off independently")
	}
}

func TestWorkflowManagerStopsRemovedDisabledAndRevokedOwnedChildren(t *testing.T) {
	for _, reason := range []string{"removed", "disabled", "revoked", "close"} {
		t.Run(reason, func(t *testing.T) {
			p := workflowOwnedTestProcess(t, false)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(401) }))
			defer server.Close()
			m := newWorkflowManager(WorkflowRunnerConfig{Enabled: true, AcceptedCodeRisk: true, Workspaces: []string{"ws"}}, NewClient(server.URL))
			m.generations["ws"] = &workflowGeneration{control: "wdc_test", process: p, started: time.Now()}
			watched := map[string]bool{"ws": true}
			switch reason {
			case "removed":
				delete(watched, "ws")
			case "disabled":
				m.runner.Config.Enabled = false
			case "close":
				m.close()
			}
			m.reconcile(context.Background(), watched)
			select {
			case <-p.done:
			case <-time.After(time.Second):
				t.Fatal("owned worker survived capability removal")
			}
			if reason == "revoked" {
				g := m.generations["ws"]
				if g.process != nil || !g.next.After(time.Now()) {
					t.Fatal("revoked child not stopped/backed off")
				}
			}
		})
	}
}

func TestWorkflowForcedDrainIsBoundedAndCrashBackoffDoesNotSpin(t *testing.T) {
	p := workflowOwnedTestProcess(t, true)
	started := time.Now()
	p.Stop(30 * time.Millisecond)
	if time.Since(started) > 3*time.Second {
		t.Fatal("unbounded forced drain")
	}
	select {
	case <-p.done:
	default:
		t.Fatal("drain returned before process exit")
	}
	requests := 0
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { requests++; w.WriteHeader(401) }))
	defer s.Close()
	m := newWorkflowManager(WorkflowRunnerConfig{Enabled: true, AcceptedCodeRisk: true, Workspaces: []string{"ws"}}, NewClient(s.URL))
	m.generations["ws"] = &workflowGeneration{control: "wdc_test"}
	m.reconcile(context.Background(), map[string]bool{"ws": true})
	for i := 0; i < 10; i++ {
		m.reconcile(context.Background(), map[string]bool{"ws": true})
	}
	if requests != 1 {
		t.Fatal("failed generation spun instead of backing off", requests)
	}
	if workflowBackoff(1) != 2*time.Second || workflowBackoff(100) != 64*time.Second {
		t.Fatal("unbounded crash backoff")
	}
}
