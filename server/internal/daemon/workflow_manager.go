package daemon

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	wr "github.com/multica-ai/multica/server/internal/workflowrunner"
)

func workflowConfigFromEnv(root string) WorkflowRunnerConfig {
	return WorkflowRunnerConfig{Enabled: os.Getenv("MULTICA_WORKFLOW_ENABLED") == "true", AcceptedCodeRisk: os.Getenv("MULTICA_WORKFLOW_ACCEPT_CODE_RISK") == "true", Workspaces: strings.FieldsFunc(os.Getenv("MULTICA_WORKFLOW_WORKSPACES"), func(r rune) bool { return r == ',' }), Node: os.Getenv("MULTICA_WORKFLOW_NODE"), Entry: os.Getenv("MULTICA_WORKFLOW_RUNNER"), Cache: filepath.Join(root, ".workflow-cache"), EngineAddress: os.Getenv("MULTICA_WORKFLOW_ENGINE_ADDRESS"), TLS: wr.TLS{Strategy: os.Getenv("MULTICA_WORKFLOW_TLS_STRATEGY"), ServerName: os.Getenv("MULTICA_WORKFLOW_TLS_SERVER_NAME"), CAFile: os.Getenv("MULTICA_WORKFLOW_TLS_CA"), CertFile: os.Getenv("MULTICA_WORKFLOW_TLS_CERT"), KeyFile: os.Getenv("MULTICA_WORKFLOW_TLS_KEY")}}
}

type workflowGeneration struct {
	owner         *workflowOwner
	retiring      *workflowGeneration
	expires       time.Time
	control       string
	enrolled      time.Time
	process       *WorkflowProcess
	ready         wr.Message
	signature     string
	started, next time.Time
	failures      int
}

// The map lock protects owner identity; each owner serializes only its own
// workspace. Cancellation fences stale preparation on removal/re-enrollment.
type workflowOwner struct {
	mu       sync.Mutex
	ctx      context.Context
	cancel   context.CancelFunc
	renewing bool // protected by the manager map lock, independent of preparation
}

func stopWorkflowGeneration(g *workflowGeneration, timeout time.Duration) {
	if g.process != nil {
		g.process.Stop(timeout)
		g.process = nil
	}
	if g.retiring != nil && g.retiring.process != nil {
		g.retiring.process.Stop(timeout)
		g.retiring = nil
	}
}
func stopWorkflowOwner(g *workflowGeneration, timeout time.Duration) {
	if g.owner != nil {
		g.owner.cancel()
		g.owner.mu.Lock()
		defer g.owner.mu.Unlock()
	}
	stopWorkflowGeneration(g, timeout)
}

type workflowManager struct {
	mu          sync.Mutex
	jobs        sync.WaitGroup
	closed      bool
	runner      *WorkflowRunner
	client      *Client
	generations map[string]*workflowGeneration
}

func newWorkflowManager(config WorkflowRunnerConfig, client *Client) *workflowManager {
	if !config.Enabled {
		return nil
	}
	return &workflowManager{runner: &WorkflowRunner{Config: config}, client: client, generations: map[string]*workflowGeneration{}}
}
func (m *workflowManager) enrolled(ws string) bool {
	return m.runner.Config.Enabled && m.runner.Config.AcceptedCodeRisk && slices.Contains(m.runner.Config.Workspaces, ws)
}
func (m *workflowManager) enroll(ws, token string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return
	}
	m.enrollLocked(ws, token)
}
func (m *workflowManager) enrollForRegistration(ctx context.Context, ws, expected, token string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed || ctx.Err() != nil {
		return
	}
	old := m.generations[ws]
	if (old == nil && expected != "") || (old != nil && old.control != expected) {
		return
	}
	m.enrollLocked(ws, token)
}
func (m *workflowManager) enrollLocked(ws, token string) {
	old := m.generations[ws]
	if old != nil && old.control == token {
		old.enrolled = time.Now()
		return
	}
	if old != nil {
		stopWorkflowOwner(old, 2*time.Second)
	}
	if token == "" {
		delete(m.generations, ws)
		return
	}
	m.generations[ws] = &workflowGeneration{control: token, enrolled: time.Now()}
}
func (m *workflowManager) remove(ws string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if g := m.generations[ws]; g != nil {
		stopWorkflowOwner(g, 2*time.Second)
		delete(m.generations, ws)
	}
}
func (m *workflowManager) close() {
	m.mu.Lock()
	m.closed = true
	for _, g := range m.generations {
		stopWorkflowOwner(g, 3*time.Second)
	}
	clear(m.generations)
	m.mu.Unlock()
	m.jobs.Wait()
}
func (d *Daemon) workflowRegistration(ctx context.Context, ws string) map[string]any {
	if d.workflow == nil {
		return nil
	}
	m := d.workflow
	result := map[string]any{"workflow_capable": false, "runner_version": "0.1.0", "workflow_code_risk_accepted": m.runner.Config.AcceptedCodeRisk}
	m.mu.Lock()
	if existing := m.generations[ws]; existing != nil {
		result["workflow_control_token"] = existing.control
	}
	m.mu.Unlock()
	if !m.enrolled(ws) {
		return result
	}
	d.logger.Warn(WorkflowCodeWarning)
	version, err := m.runner.Probe(ctx)
	if err != nil {
		d.logger.Warn("workflow runtime unavailable", "reason", err.Error())
		return result
	}
	result["workflow_capable"] = true
	result["node_version"] = version
	return result
}

// Dedicated control transport never changes the client's ordinary daemon credential.
func (c *Client) workflowRequest(ctx context.Context, ws, token, method, suffix string, body, out any) error {
	data, err := json.Marshal(body)
	if err != nil {
		return errors.New("workflow_control_failed")
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+"/api/daemon/workflows/"+suffix, bytes.NewReader(data))
	if err != nil {
		return errors.New("workflow_control_failed")
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("X-Workspace-ID", ws)
	req.Header.Set("Content-Type", "application/json")
	client := *c.client
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	response, err := client.Do(req)
	if err != nil {
		return errors.New("workflow_control_failed")
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		return &requestError{StatusCode: response.StatusCode, Body: "workflow_control_failed"}
	}
	if out != nil {
		return json.NewDecoder(io.LimitReader(response.Body, 70<<20)).Decode(out)
	}
	return nil
}
func workflowBackoff(failures int) time.Duration {
	if failures > 6 {
		failures = 6
	}
	if failures < 1 {
		failures = 1
	}
	return time.Second * time.Duration(1<<failures)
}
func (m *workflowManager) reconcile(ctx context.Context, watched map[string]bool) {
	m.scheduleReconcile(ctx, watched)()
}
func (m *workflowManager) scheduleReconcile(ctx context.Context, watched map[string]bool) func() {
	m.mu.Lock()
	var work sync.WaitGroup
	if m.closed {
		m.mu.Unlock()
		return func() {}
	}
	for ws, g := range m.generations {
		if !watched[ws] || !m.enrolled(ws) {
			stopWorkflowOwner(g, 2*time.Second)
			delete(m.generations, ws)
			continue
		}
		if g.owner == nil {
			ownerCtx, cancel := context.WithCancel(ctx)
			g.owner = &workflowOwner{ctx: ownerCtx, cancel: cancel}
		}
		if !g.owner.mu.TryLock() {
			continue
		}
		work.Add(1)
		m.jobs.Add(1)
		go func(ws string, g *workflowGeneration) {
			defer work.Done()
			defer m.jobs.Done()
			defer g.owner.mu.Unlock()
			if g.owner.ctx.Err() == nil {
				m.reconcileWorkspace(g.owner.ctx, ws, g)
			}
		}(ws, g)
	}
	m.mu.Unlock()
	return work.Wait
}
func (m *workflowManager) renewWorkflowLease(ctx context.Context, ws, control string, ready wr.Message) error {
	leaseCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return m.client.workflowRequest(leaseCtx, ws, control, "POST", "ack", ready, nil)
}
func (m *workflowManager) reconcileWorkspace(ctx context.Context, ws string, g *workflowGeneration) {
	stop := func() {
		if g.process != nil {
			g.process.Stop(2 * time.Second)
			g.process = nil
		}
		if g.retiring != nil && g.retiring.process != nil {
			g.retiring.process.Stop(2 * time.Second)
			g.retiring = nil
		}
	}
	// Inspect current process before renewing it. A failed current never
	// invalidates independently authorized retiring work.
	if g.process != nil {
		failed := false
		select {
		case <-g.process.done:
			failed = true
		default:
		}
		select {
		case message, open := <-g.process.messages:
			failed = failed || !open || message.Type == "error" || message.Type == "drained"
		default:
		}
		if failed {
			g.process.Stop(time.Second)
			g.process = nil
			cleanupCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			_ = m.client.workflowRequest(cleanupCtx, ws, g.control, "POST", "retire", map[string]string{"generation": g.ready.Generation}, nil)
			cancel()
			if g.retiring != nil {
				_ = m.renewWorkflowLease(ctx, ws, g.control, g.retiring.ready)
			}
			g.failures++
			g.next = time.Now().Add(workflowBackoff(g.failures))
			return
		}
	}
	var response struct {
		Revisions []wr.Revision `json:"revisions"`
	}
	var err error
	if err == nil && g.retiring != nil {
		old := g.retiring
		done := false
		select {
		case <-old.process.done:
			done = true
		default:
		}
		if !done && !old.expires.IsZero() && time.Now().After(old.expires) {
			old.process.Stop(time.Millisecond)
			done = true
		}
		if done {
			cleanupCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			err = m.client.workflowRequest(cleanupCtx, ws, g.control, "POST", "retire", map[string]string{"generation": old.ready.Generation}, nil)
			cancel()
			if err == nil {
				g.retiring = nil
			}
		} else {
			err = m.renewWorkflowLease(ctx, ws, g.control, old.ready)
		}
	}
	if err == nil && g.process != nil {
		err = m.renewWorkflowLease(ctx, ws, g.control, g.ready)
	}
	if err != nil {
		stop()
		g.failures++
		g.next = time.Now().Add(workflowBackoff(g.failures))
		return
	}
	if time.Now().Before(g.next) {
		return
	}
	callCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	// Snapshot checked generations for independent renewal while preparation
	// consumes its own budget. Never share that budget with their heartbeats.
	messages := []wr.Message{}
	if g.process != nil {
		messages = append(messages, g.ready)
	}
	if g.retiring != nil {
		messages = append(messages, g.retiring.ready)
	}
	heartCtx, stopHeart := context.WithCancel(ctx)
	heartDone := make(chan error, 1)
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-heartCtx.Done():
				heartDone <- nil
				return
			case <-ticker.C:
				for _, message := range messages {
					if e := m.renewWorkflowLease(heartCtx, ws, g.control, message); e != nil {
						if heartCtx.Err() != nil {
							heartDone <- nil
						} else {
							heartDone <- e
							cancel()
						}
						return
					}
				}
			}
		}
	}()
	defer func() {
		stopHeart()
		if e := <-heartDone; e != nil {
			stop()
			g.failures++
			g.next = time.Now().Add(workflowBackoff(g.failures))
		}
	}()
	err = m.client.workflowRequest(callCtx, ws, g.control, "GET", "revisions", nil, &response)
	if err != nil && callCtx.Err() != nil && ctx.Err() == nil {
		g.failures++
		g.next = time.Now().Add(workflowBackoff(g.failures))
		return
	}
	if err == nil && len(response.Revisions) > 256 {
		err = errors.New("workflow_revision_limit")
	}
	raw, _ := json.Marshal(response.Revisions)
	signature := wr.Hash(string(raw))
	if err == nil && g.process != nil {
		failed := false
		select {
		case <-g.process.done:
			failed = true
		default:
		}
		select {
		case message, open := <-g.process.messages:
			failed = failed || !open || message.Type == "error" || message.Type == "drained"
		default:
		}
		if failed {
			g.process.Stop(time.Second)
			g.process = nil
			_ = m.client.workflowRequest(callCtx, ws, g.control, "POST", "retire", map[string]string{"generation": g.ready.Generation}, nil)
			g.failures++
			g.next = time.Now().Add(workflowBackoff(g.failures))
			cancel()
			return
		}
	}
	if err == nil && g.retiring == nil && (g.process == nil || signature != g.signature || time.Since(g.started) > 45*time.Minute) && len(response.Revisions) > 0 {
		revisions := make([]wr.Revision, 0, len(response.Revisions))
		accepted := make([]wr.Revision, 0, len(response.Revisions))
		var preparationErr error
		for _, rev := range response.Revisions {
			if callCtx.Err() != nil {
				preparationErr = callCtx.Err()
				break
			}
			validation, e := m.runner.Validate(callCtx, rev.Source)
			if e != nil || validation.Hash != rev.Hash || validation.GraphDigest != rev.GraphDigest || wr.GraphDigest(rev.Graph) != rev.GraphDigest {
				_ = m.client.workflowRequest(callCtx, ws, g.control, "POST", "validation-failed", map[string]string{"revision_id": rev.RevisionID, "hash": rev.Hash}, nil)
				continue
			}
			accepted = append(accepted, rev)
			rev.SourcePath, e = m.runner.CacheSource(rev.Source, rev.Hash)
			if e != nil {
				preparationErr = e
				break
			}
			rev.Source = ""
			revisions = append(revisions, rev)
		}
		if callCtx.Err() != nil {
			preparationErr = callCtx.Err()
		}
		acceptedRaw, _ := json.Marshal(accepted)
		signature = wr.Hash(string(acceptedRaw))
		if preparationErr != nil {
			// A failed candidate cache is not loss of current authority. Keep the
			// existing worker and renew its checked lease while preparation backs off.
			g.failures++
			g.next = time.Now().Add(workflowBackoff(g.failures))
		}
		if preparationErr == nil && len(revisions) > 0 && (g.process == nil || signature != g.signature || time.Since(g.started) > 45*time.Minute) {
			var credentials wr.Credentials
			candidateErr := m.client.workflowRequest(callCtx, ws, g.control, "POST", "rotate", nil, &credentials)
			if candidateErr == nil {
				descriptor := wr.Descriptor{Protocol: 1, WorkspaceID: ws, TenantID: credentials.TenantID, Generation: credentials.Generation, Engine: wr.Engine{Token: credentials.EngineToken, HostPort: m.runner.Config.EngineAddress, TLS: m.runner.Config.TLS}, Product: wr.Product{Origin: m.client.baseURL, RunnerToken: credentials.RunnerToken}, Revisions: revisions, DrainTimeoutMS: 30000, ActivationGate: true}
				var candidate *WorkflowProcess
				var ready wr.Message
				if credentials.WorkspaceID != ws {
					candidateErr = errors.New("workflow_validation_mismatch")
				} else {
					candidate, ready, candidateErr = m.runner.startWorker(ctx, callCtx, descriptor)
				}
				if candidateErr == nil {
					candidateErr = m.client.workflowRequest(callCtx, ws, g.control, "POST", "prepare", ready, nil)
				}
				if candidateErr == nil {
					candidateErr = candidate.Activate(callCtx, credentials.Generation)
				}
				if candidateErr == nil {
					candidateErr = m.client.workflowRequest(callCtx, ws, g.control, "POST", "ack", ready, nil)
				}
				if candidateErr == nil {
					if g.process != nil {
						old := &workflowGeneration{process: g.process, ready: g.ready, expires: g.expires, control: g.control}
						g.retiring = old
						_ = old.process.Retire()
					}
					g.process, g.ready = candidate, ready
					g.signature = signature
					g.started = time.Now()
					g.expires = credentials.ExpiresAt
				} else {
					if candidate != nil {
						candidate.Stop(time.Second)
					}
					cleanupCtx, cleanupCancel := context.WithTimeout(ctx, 5*time.Second)
					_ = m.client.workflowRequest(cleanupCtx, ws, g.control, "POST", "retire", map[string]string{"generation": credentials.Generation}, nil)
					cleanupCancel()
				}
			}
			if candidateErr != nil {
				g.failures++
				g.next = time.Now().Add(workflowBackoff(g.failures))
				if g.process == nil {
					err = candidateErr
				}
			}
		}
	}
	if err == nil && g.process != nil {
		err = m.renewWorkflowLease(ctx, ws, g.control, g.ready)
	}
	if err != nil {
		stop()
		g.failures++
		g.next = time.Now().Add(workflowBackoff(g.failures))
	} else if g.process != nil && time.Since(g.started) > time.Minute {
		g.failures = 0
	}
}

// The scheduler never waits for a different workspace's body or human control
// renewal. Each owner retains its cancellation boundary until all jobs finish.
func (m *workflowManager) run(ctx context.Context, interval time.Duration, watched func() map[string]bool, renew func(context.Context, string)) {
	defer m.close()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		m.scheduleReconcile(ctx, watched())
		// Control identity is renewed before its 24h expiry through authenticated enrollment.
		m.mu.Lock()
		for ws, g := range m.generations {
			if time.Since(g.enrolled) > 12*time.Hour && g.owner != nil && !g.owner.renewing {
				owner := g.owner
				owner.renewing = true
				m.jobs.Add(1)
				go func(ws string) {
					defer m.jobs.Done()
					renew(owner.ctx, ws)
					m.mu.Lock()
					owner.renewing = false
					m.mu.Unlock()
				}(ws)
			}
		}
		m.mu.Unlock()
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
func (d *Daemon) workflowLoop(ctx context.Context) {
	d.workflow.run(ctx, 10*time.Second, func() map[string]bool {
		d.mu.Lock()
		defer d.mu.Unlock()
		watched := map[string]bool{}
		for ws := range d.workspaces {
			watched[ws] = true
		}
		return watched
	}, func(ctx context.Context, ws string) { _ = d.reregisterWorkspaceAfterRuntimeGone(ctx, ws) })
}
