package daemon

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
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

func TestWorkflowFinalAckFailureKeepsCurrentProcessAndRetiresCandidate(t *testing.T) {
	node := workflowTestNode(t)
	source := `export default {steps:[{id:'a',run:async()=>null}]};`
	graph := wr.Graph{Steps: []wr.Step{{ID: "a", Parents: []string{}}}}
	revision := wr.Revision{RevisionID: "proposal", Source: source, Hash: wr.Hash(source), Graph: graph, GraphDigest: wr.GraphDigest(graph)}
	validation, err := json.Marshal(wr.Validation{Hash: revision.Hash, Graph: graph, GraphDigest: revision.GraphDigest})
	if err != nil {
		t.Fatal(err)
	}
	// Only the native registration transport is replaced. Real owned child IPC,
	// activation, stop, HTTP ACK failure and manager process ownership run here.
	entry := filepath.Join(t.TempDir(), "candidate.cjs")
	code := fmt.Sprintf(`let d; process.on('message',m=>{
if(m.mode==='validate')process.send({type:'validated',result:%s});
if(m.mode==='worker'){d=m.descriptor;process.send({type:'ready',protocol:1,workspace_id:d.workspace_id,tenant_id:d.tenant_id,generation:d.generation});}
if(m.mode==='activate')process.send({type:'active',generation:d.generation});
if(m.mode==='drain')process.exit(0);
});`, validation)
	if err = os.WriteFile(entry, []byte(code), 0600); err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	candidateAcks, currentAcks := 0, 0
	retired := []string{}
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		switch {
		case strings.HasSuffix(r.URL.Path, "/revisions"):
			json.NewEncoder(w).Encode(map[string]any{"revisions": []wr.Revision{revision}})
		case strings.HasSuffix(r.URL.Path, "/rotate"):
			json.NewEncoder(w).Encode(wr.Credentials{WorkspaceID: "ws", TenantID: "tenant", Generation: "candidate", ExpiresAt: time.Now().Add(time.Hour)})
		case strings.HasSuffix(r.URL.Path, "/ack"):
			var message wr.Message
			if err := json.NewDecoder(r.Body).Decode(&message); err != nil {
				t.Error(err)
			}
			if message.Generation == "candidate" {
				candidateAcks++
				w.WriteHeader(http.StatusConflict)
			} else if message.Generation == "healthy" {
				currentAcks++
			} else {
				t.Errorf("unexpected generation ACK %q", message.Generation)
			}
		case strings.HasSuffix(r.URL.Path, "/retire"):
			var message struct {
				Generation string `json:"generation"`
			}
			if err := json.NewDecoder(r.Body).Decode(&message); err != nil {
				t.Error(err)
			}
			retired = append(retired, message.Generation)
		}
	}))
	defer s.Close()
	m := newWorkflowManager(WorkflowRunnerConfig{Enabled: true, AcceptedCodeRisk: true, Workspaces: []string{"ws"}, Node: node, Entry: entry, Cache: filepath.Join(t.TempDir(), "cache")}, NewClient(s.URL))
	current := workflowOwnedTestProcess(t, false)
	g := &workflowGeneration{control: "wdc_test", process: current, started: time.Now(), ready: wr.Message{Generation: "healthy"}, expires: time.Now().Add(time.Hour)}
	m.generations["ws"] = g
	m.reconcile(context.Background(), map[string]bool{"ws": true})
	if g.process != current || g.ready.Generation != "healthy" || g.retiring != nil {
		t.Fatal("failed final ACK changed manager current/retiring process ownership")
	}
	select {
	case <-current.done:
		t.Fatal("failed final ACK stopped healthy current")
	default:
	}
	mu.Lock()
	defer mu.Unlock()
	if candidateAcks != 1 || currentAcks < 2 || len(retired) != 1 || retired[0] != "candidate" || !g.next.After(time.Now()) {
		t.Fatalf("candidate failure cleanup/renewal: candidate ACKs=%d current ACKs=%d retired=%v", candidateAcks, currentAcks, retired)
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
	if rotations != 0 || acks != 2 || !g.next.After(time.Now()) {
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

func TestWorkflowCandidateBackoffNeverSkipsCurrentOrRetiringLease(t *testing.T) {
	acks := 0
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/ack") {
			acks++
		}
		w.Write([]byte(`{"revisions":[]}`))
	}))
	defer s.Close()
	m := newWorkflowManager(WorkflowRunnerConfig{Enabled: true, AcceptedCodeRisk: true, Workspaces: []string{"ws"}}, NewClient(s.URL))
	m.generations["ws"] = &workflowGeneration{control: "wdc_test", process: workflowOwnedTestProcess(t, false), next: time.Now().Add(64 * time.Second), retiring: &workflowGeneration{process: workflowOwnedTestProcess(t, false), expires: time.Now().Add(time.Minute)}}
	for i := 0; i < 3; i++ {
		m.reconcile(context.Background(), map[string]bool{"ws": true})
	}
	if acks != 6 {
		t.Fatalf("candidate backoff skipped lease heartbeats: got%d want6", acks)
	}
}

func TestWorkflowSlowWorkspaceDoesNotStarveOtherLease(t *testing.T) {
	blocked := make(chan struct{})
	var mu sync.Mutex
	acks := map[string]int{}
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws := r.Header.Get("X-Workspace-ID")
		if strings.HasSuffix(r.URL.Path, "/revisions") {
			<-blocked
		}
		if strings.HasSuffix(r.URL.Path, "/ack") {
			mu.Lock()
			acks[ws]++
			mu.Unlock()
		}
		w.Write([]byte(`{"revisions":[]}`))
	}))
	defer s.Close()
	m := newWorkflowManager(WorkflowRunnerConfig{Enabled: true, AcceptedCodeRisk: true, Workspaces: []string{"a", "b"}}, NewClient(s.URL))
	for _, ws := range []string{"a", "b"} {
		m.generations[ws] = &workflowGeneration{control: "wdc_test", process: workflowOwnedTestProcess(t, false)}
	}
	done := make(chan struct{})
	go func() { defer close(done); m.reconcile(context.Background(), map[string]bool{"a": true, "b": true}) }()
	time.Sleep(200 * time.Millisecond)
	mu.Lock()
	both := acks["a"] > 0 && acks["b"] > 0
	mu.Unlock()
	close(blocked)
	<-done
	if !both {
		t.Fatal("one slow workspace starved independent current leases")
	}
}

func TestWorkflowCandidateBackoffMaintainsLeasesAcrossFullLeaseInterval(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	last := map[string]time.Time{}
	acks := 0
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/ack") {
			var message wr.Message
			_ = json.NewDecoder(r.Body).Decode(&message)
			mu.Lock()
			last[message.Generation] = time.Now()
			acks++
			mu.Unlock()
		}
		w.Write([]byte(`{"revisions":[]}`))
	}))
	defer s.Close()
	m := newWorkflowManager(WorkflowRunnerConfig{Enabled: true, AcceptedCodeRisk: true, Workspaces: []string{"ws"}}, NewClient(s.URL))
	current, old := workflowOwnedTestProcess(t, false), workflowOwnedTestProcess(t, false)
	m.generations["ws"] = &workflowGeneration{control: "wdc_test", process: current, ready: wr.Message{Generation: "current"}, next: time.Now().Add(64 * time.Second), retiring: &workflowGeneration{process: old, ready: wr.Message{Generation: "retiring"}, expires: time.Now().Add(time.Minute)}}
	started := time.Now()
	for i := 0; i < 6; i++ {
		if i > 0 {
			time.Sleep(10 * time.Second)
		}
		m.reconcile(context.Background(), map[string]bool{"ws": true})
		mu.Lock()
		healthy := time.Since(last["current"]) < 45*time.Second && time.Since(last["retiring"]) < 45*time.Second
		mu.Unlock()
		if !healthy {
			t.Fatal("backoff expired an otherwise healthy action lease")
		}
	}
	if time.Since(started) < 45*time.Second || acks != 12 {
		t.Fatal("full action lease interval not covered")
	}
}

func TestWorkflowTwoSlowCandidatesRenewIndependentlyAndRemovalFencesOwner(t *testing.T) {
	source := `import {defineWorkflow} from '@multica/workflow-sdk';export default defineWorkflow({steps:[{id:'a',run:async()=>null}]});`
	graph := wr.Graph{Steps: []wr.Step{{ID: "a", Parents: []string{}}}}
	revision := wr.Revision{RevisionID: "candidate", Source: source, Hash: wr.Hash(source), Graph: graph, GraphDigest: wr.GraphDigest(graph)}
	entered := make(chan string, 2)
	release := make(chan struct{})
	var releaseOnce sync.Once
	var mu sync.Mutex
	acks := map[string]int{}
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws := r.Header.Get("X-Workspace-ID")
		switch {
		case strings.HasSuffix(r.URL.Path, "/revisions"):
			json.NewEncoder(w).Encode(map[string]any{"revisions": []wr.Revision{revision}})
		case strings.HasSuffix(r.URL.Path, "/ack"):
			mu.Lock()
			acks[ws]++
			mu.Unlock()
			w.Write([]byte(`{}`))
		case strings.HasSuffix(r.URL.Path, "/rotate"):
			entered <- ws
			select {
			case <-release:
			case <-r.Context().Done():
			}
			w.WriteHeader(503)
		default:
			w.Write([]byte(`{}`))
		}
	}))
	defer func() { releaseOnce.Do(func() { close(release) }); s.Close() }()
	entry, _ := filepath.Abs("../../../packages/workflow-runner/dist/cli.js")
	m := newWorkflowManager(WorkflowRunnerConfig{Enabled: true, AcceptedCodeRisk: true, Workspaces: []string{"a", "b"}, Node: workflowTestNode(t), Entry: entry, Cache: filepath.Join(t.TempDir(), "cache")}, NewClient(s.URL))
	for _, ws := range []string{"a", "b"} {
		m.generations[ws] = &workflowGeneration{control: "wdc_old_" + ws, process: workflowOwnedTestProcess(t, false), started: time.Now()}
	}
	done := make(chan struct{})
	go func() { defer close(done); m.reconcile(context.Background(), map[string]bool{"a": true, "b": true}) }()
	for i := 0; i < 2; i++ {
		select {
		case <-entered:
		case <-time.After(3 * time.Second):
			t.Fatal("candidate preparation serialized across workspaces")
		}
	}
	time.Sleep(11 * time.Second)
	mu.Lock()
	both := acks["a"] >= 2 && acks["b"] >= 2
	mu.Unlock()
	if !both {
		t.Fatal("slow candidates starved checked generation heartbeats")
	}
	removed := make(chan struct{})
	go func() { m.remove("a"); m.enroll("a", "wdc_fresh"); close(removed) }()
	select {
	case <-removed:
	case <-time.After(4 * time.Second):
		t.Fatal("removal could not cancel stale preparation")
	}
	if m.generations["a"].control != "wdc_fresh" {
		t.Fatal("stale owner replaced fresh enrollment")
	}
	// Release the remaining workspace without requiring a preparation timeout.
	releaseOnce.Do(func() { close(release) })
	<-done
	if m.generations["a"].control != "wdc_fresh" {
		t.Fatal("stale candidate completion overwrote fresh owner")
	}
}

func TestWorkflowRunLoopTicksRenewFastPairWhileBodyAndControlRenewalBlock(t *testing.T) {
	// Production uses10s ticks and45s leases; scale only scheduler time100x.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	release := make(chan struct{})
	renewEntered := make(chan struct{}, 1)
	var mu sync.Mutex
	last := map[string]time.Time{}
	counts := map[string]int{}
	gap := time.Duration(0)
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Workspace-ID") == "slow" {
			select {
			case <-r.Context().Done():
			case <-release:
			}
			w.WriteHeader(503)
			return
		}
		if strings.HasSuffix(r.URL.Path, "/ack") {
			var message wr.Message
			_ = json.NewDecoder(r.Body).Decode(&message)
			mu.Lock()
			now := time.Now()
			if !last[message.Generation].IsZero() && now.Sub(last[message.Generation]) > gap {
				gap = now.Sub(last[message.Generation])
			}
			last[message.Generation] = now
			counts[message.Generation]++
			mu.Unlock()
		}
		w.Write([]byte(`{"revisions":[]}`))
	}))
	defer func() { close(release); s.Close() }()
	m := newWorkflowManager(WorkflowRunnerConfig{Enabled: true, AcceptedCodeRisk: true, Workspaces: []string{"fast", "slow"}}, NewClient(s.URL))
	m.generations["fast"] = &workflowGeneration{control: "wdc_fast", enrolled: time.Now(), process: workflowOwnedTestProcess(t, false), ready: wr.Message{Generation: "current"}, next: time.Now().Add(64 * time.Second), retiring: &workflowGeneration{process: workflowOwnedTestProcess(t, false), ready: wr.Message{Generation: "retiring"}, expires: time.Now().Add(time.Minute)}}
	m.generations["slow"] = &workflowGeneration{control: "wdc_slow", enrolled: time.Now().Add(-13 * time.Hour)}
	done := make(chan struct{})
	go func() {
		defer close(done)
		m.run(ctx, 100*time.Millisecond, func() map[string]bool { return map[string]bool{"fast": true, "slow": true} }, func(renewCtx context.Context, ws string) {
			renewEntered <- struct{}{}
			<-renewCtx.Done()
			m.enrollForRegistration(renewCtx, ws, "wdc_slow", "wdc_stale")
		})
	}()
	select {
	case <-renewEntered:
	case <-time.After(time.Second):
		cancel()
		<-done
		t.Fatal("blocked body prevented independent control renewal scheduling")
	}
	time.Sleep(650 * time.Millisecond)
	mu.Lock()
	healthy := counts["current"] >= 5 && counts["retiring"] >= 5 && gap < 450*time.Millisecond
	mu.Unlock()
	m.remove("slow")
	m.enroll("slow", "wdc_fresh")
	m.mu.Lock()
	fresh := m.generations["slow"].control
	m.mu.Unlock()
	cancel()
	<-done
	if !healthy {
		t.Fatal("actual scheduler loop starved fast pair behind blocked workspace/renewal")
	}
	if fresh != "wdc_fresh" {
		t.Fatal("cancelled control renewal committed over fresh owner")
	}
}

func TestWorkflowCandidateTimeoutNeverConsumesCurrentLeaseBudget(t *testing.T) {
	for _, slow := range []string{"drafts", "rotate"} {
		t.Run(slow, func(t *testing.T) {
			t.Parallel()
			source := `import {defineWorkflow} from '@multica/workflow-sdk';export default defineWorkflow({steps:[{id:'a',run:async()=>null}]});`
			graph := wr.Graph{Steps: []wr.Step{{ID: "a", Parents: []string{}}}}
			revisions := []wr.Revision{{RevisionID: "candidate", Source: source, Hash: wr.Hash(source), Graph: graph, GraphDigest: wr.GraphDigest(graph)}}
			if slow == "drafts" {
				revisions = nil
				for i := 0; i < 7; i++ {
					revisions = append(revisions, wr.Revision{RevisionID: "invalid", Source: "while(true){}", Hash: wr.Hash("while(true){}"), Graph: graph, GraphDigest: wr.GraphDigest(graph)})
				}
			}
			var mu sync.Mutex
			acks := 0
			release := make(chan struct{})
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case strings.HasSuffix(r.URL.Path, "/revisions"):
					json.NewEncoder(w).Encode(map[string]any{"revisions": revisions})
				case strings.HasSuffix(r.URL.Path, "/rotate"):
					select {
					case <-r.Context().Done():
					case <-release:
					}
				case strings.HasSuffix(r.URL.Path, "/ack"):
					mu.Lock()
					acks++
					mu.Unlock()
					w.Write([]byte(`{}`))
				default:
					w.Write([]byte(`{}`))
				}
			}))
			defer func() { close(release); s.Close() }()
			entry, _ := filepath.Abs("../../../packages/workflow-runner/dist/cli.js")
			m := newWorkflowManager(WorkflowRunnerConfig{Enabled: true, AcceptedCodeRisk: true, Workspaces: []string{"ws"}, Node: workflowTestNode(t), Entry: entry, Cache: filepath.Join(t.TempDir(), "cache")}, NewClient(s.URL))
			p := workflowOwnedTestProcess(t, false)
			m.generations["ws"] = &workflowGeneration{control: "wdc_test", process: p, signature: wr.Hash("[]"), started: time.Now()}
			m.reconcile(context.Background(), map[string]bool{"ws": true})
			select {
			case <-p.done:
				t.Fatal("candidate timeout stopped healthy current")
			default:
			}
			mu.Lock()
			defer mu.Unlock()
			if acks < 2 {
				t.Fatal("candidate timeout exhausted independent current lease renewal")
			}
		})
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
