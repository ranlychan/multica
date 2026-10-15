// Package workflowrunner defines the private packaged Node IPC/control contract.
package workflowrunner

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
	"time"
	"unicode/utf16"
)

type Step struct {
	ID      string   `json:"id"`
	Parents []string `json:"parents"`
}
type Graph struct {
	Steps []Step `json:"steps"`
}
type Validation struct {
	Hash        string `json:"hash"`
	Graph       Graph  `json:"graph"`
	GraphDigest string `json:"graph_digest"`
}
type Revision struct {
	WorkflowID  string `json:"workflow_id"`
	RevisionID  string `json:"revision_id"`
	Version     int32  `json:"version"`
	Hash        string `json:"hash"`
	EngineName  string `json:"engine_workflow_name"`
	SourcePath  string `json:"source_path,omitempty"`
	Source      string `json:"source,omitempty"`
	Graph       Graph  `json:"graph"`
	GraphDigest string `json:"graph_digest"`
}
type Action struct {
	StepID   string `json:"step_id"`
	TaskName string `json:"task_name"`
	ActionID string `json:"action_id"`
}
type Registration struct {
	Revision
	EngineWorkflowID string   `json:"engine_workflow_id"`
	EngineVersionID  string   `json:"engine_version_id"`
	Actions          []Action `json:"actions"`
}
type TLS struct {
	Strategy   string `json:"strategy"`
	ServerName string `json:"server_name"`
	CAFile     string `json:"ca_file"`
	CertFile   string `json:"cert_file,omitempty"`
	KeyFile    string `json:"key_file,omitempty"`
}
type Engine struct {
	Token    string `json:"token"`
	HostPort string `json:"host_port"`
	TLS      TLS    `json:"tls"`
}
type Product struct {
	Origin      string `json:"origin"`
	RunnerToken string `json:"runner_token"`
}
type Descriptor struct {
	Protocol       int        `json:"protocol"`
	WorkspaceID    string     `json:"workspace_id"`
	TenantID       string     `json:"tenant_id"`
	Generation     string     `json:"generation"`
	Engine         Engine     `json:"engine"`
	Product        Product    `json:"product"`
	Revisions      []Revision `json:"revisions"`
	DrainTimeoutMS int        `json:"drain_timeout_ms"`
	ActivationGate bool       `json:"activation_gate,omitempty"`
}
type Message struct {
	Type          string         `json:"type"`
	Protocol      int            `json:"protocol"`
	RunnerVersion string         `json:"runner_version"`
	NodeVersion   string         `json:"node_version"`
	WorkspaceID   string         `json:"workspace_id"`
	TenantID      string         `json:"tenant_id"`
	Generation    string         `json:"generation"`
	Registrations []Registration `json:"registrations"`
	Result        Validation     `json:"result"`
	Code          string         `json:"code"`
}
type Credentials struct {
	WorkspaceID string    `json:"workspace_id"`
	TenantID    string    `json:"tenant_id"`
	Generation  string    `json:"generation"`
	EngineToken string    `json:"engine_token"`
	RunnerToken string    `json:"runner_token"`
	ExpiresAt   time.Time `json:"expires_at"`
}

func Hash(source string) string { s := sha256.Sum256([]byte(source)); return hex.EncodeToString(s[:]) }
func jsLess(a, b string) bool {
	x, y := utf16.Encode([]rune(a)), utf16.Encode([]rune(b))
	for i := 0; i < len(x) && i < len(y); i++ {
		if x[i] != y[i] {
			return x[i] < y[i]
		}
	}
	return len(x) < len(y)
}

// GraphDigest matches JavaScript JSON.stringify, including UTF16 sort ordering.
func GraphDigest(g Graph) string {
	copy := Graph{Steps: make([]Step, len(g.Steps))}
	for i, s := range g.Steps {
		copy.Steps[i] = Step{s.ID, append([]string{}, s.Parents...)}
		sort.Slice(copy.Steps[i].Parents, func(a, b int) bool { return jsLess(copy.Steps[i].Parents[a], copy.Steps[i].Parents[b]) })
	}
	sort.Slice(copy.Steps, func(i, j int) bool { return jsLess(copy.Steps[i].ID, copy.Steps[j].ID) })
	var b bytes.Buffer
	e := json.NewEncoder(&b)
	e.SetEscapeHTML(false)
	_ = e.Encode(copy)
	raw := bytes.TrimSuffix(b.Bytes(), []byte("\n"))
	raw = bytes.ReplaceAll(raw, []byte(`\u2028`), []byte("\u2028"))
	raw = bytes.ReplaceAll(raw, []byte(`\u2029`), []byte("\u2029"))
	return Hash(string(raw))
}
