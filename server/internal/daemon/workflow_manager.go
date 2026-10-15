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
type workflowManager struct {
	mu          sync.Mutex
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
	old := m.generations[ws]
	if old != nil && old.control == token {
		old.enrolled = time.Now()
		return
	}
	if old != nil && old.process != nil {
		old.process.Stop(2 * time.Second)
	}
	if old != nil && old.retiring != nil && old.retiring.process != nil {
		old.retiring.process.Stop(2 * time.Second)
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
		if g.process != nil {
			g.process.Stop(2 * time.Second)
		}
		if g.retiring != nil && g.retiring.process != nil {
			g.retiring.process.Stop(2 * time.Second)
		}
		delete(m.generations, ws)
	}
}
func (m *workflowManager) close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, g := range m.generations {
		if g.process != nil {
			g.process.Stop(3 * time.Second)
		}
		if g.retiring != nil && g.retiring.process != nil {
			g.retiring.process.Stop(3 * time.Second)
		}
	}
	clear(m.generations)
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
	m.mu.Lock()
	defer m.mu.Unlock()
	for ws, g := range m.generations {
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
		if !watched[ws] || !m.enrolled(ws) {
			stop()
			delete(m.generations, ws)
			continue
		}
		if time.Now().Before(g.next) {
			continue
		}
		callCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		var response struct {
			Revisions []wr.Revision `json:"revisions"`
		}
		err := m.client.workflowRequest(callCtx, ws, g.control, "GET", "revisions", nil, &response)
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
				err = m.client.workflowRequest(callCtx, ws, g.control, "POST", "retire", map[string]string{"generation": old.ready.Generation}, nil)
				if err == nil {
					g.retiring = nil
				}
			} else {
				err = m.client.workflowRequest(callCtx, ws, g.control, "POST", "ack", old.ready, nil)
			}
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
				continue
			}
		}
		if err == nil && g.retiring == nil && (g.process == nil || signature != g.signature || time.Since(g.started) > 45*time.Minute) && len(response.Revisions) > 0 {
			revisions := make([]wr.Revision, 0, len(response.Revisions))
			accepted := make([]wr.Revision, 0, len(response.Revisions))
			var preparationErr error
			for _, rev := range response.Revisions {
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
						candidate, ready, candidateErr = m.runner.Start(ctx, descriptor)
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
							old := *g
							old.retiring = nil
							g.retiring = &old
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
						_ = m.client.workflowRequest(callCtx, ws, g.control, "POST", "retire", map[string]string{"generation": credentials.Generation}, nil)
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
			err = m.client.workflowRequest(callCtx, ws, g.control, "POST", "ack", g.ready, nil)
		}
		if err != nil {
			stop()
			g.failures++
			g.next = time.Now().Add(workflowBackoff(g.failures))
		} else if g.process != nil && time.Since(g.started) > time.Minute {
			g.failures = 0
		}
		cancel()
	}
}
func (d *Daemon) workflowLoop(ctx context.Context) {
	defer d.workflow.close()
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		d.mu.Lock()
		watched := map[string]bool{}
		for ws := range d.workspaces {
			watched[ws] = true
		}
		d.mu.Unlock()
		d.workflow.reconcile(ctx, watched)
		// Control identity is renewed before its 24h expiry through authenticated enrollment.
		d.workflow.mu.Lock()
		var renew []string
		for ws, g := range d.workflow.generations {
			if time.Since(g.enrolled) > 12*time.Hour {
				renew = append(renew, ws)
			}
		}
		d.workflow.mu.Unlock()
		for _, ws := range renew {
			_ = d.reregisterWorkspaceAfterRuntimeGone(ctx, ws)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
