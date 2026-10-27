package daemon

import (
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	wr "github.com/multica-ai/multica/server/internal/workflowrunner"
)

func TestWorkflowAuthoringEnvironment(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("credential-free fake Node fixture requires /bin/sh")
	}
	dir := t.TempDir()
	node := filepath.Join(dir, "node")
	entry := filepath.Join(dir, "cli.js")
	if err := os.WriteFile(node, []byte("#!/bin/sh\n[ \"$1\" = --version ] || exit 9\n[ -z \"$MULTICA_TOKEN$MULTICA_WORKFLOW_TLS_KEY\" ] || exit 8\nprintf 'v22.9.0\\n'\n"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(entry, []byte("// packaged runner fixture"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("MULTICA_TOKEN", "daemon-secret")
	t.Setenv("MULTICA_WORKFLOW_TLS_KEY", "private-key")
	// Authoring works with local tools even when engine execution is disabled.
	d := New(Config{Workflow: WorkflowRunnerConfig{Node: node, Entry: entry, EngineAddress: "private-engine", TLS: wr.TLS{KeyFile: "private-key"}}}, slog.Default())
	if got := d.workflowAuthoringEnv; len(got) != 2 || got["MULTICA_WORKFLOW_NODE"] != node || got["MULTICA_WORKFLOW_RUNNER"] != entry {
		t.Fatalf("authoring environment = %v; want only configured Node and runner paths", got)
	}
	agentEnv := d.taskMulticaEnvironment(Task{}, "", "task-token", "", "", "", 0, 0, "")
	if agentEnv["MULTICA_WORKFLOW_NODE"] != node || agentEnv["MULTICA_WORKFLOW_RUNNER"] != entry || agentEnv["MULTICA_TOKEN"] != "task-token" {
		t.Fatalf("task environment did not preserve local authoring tools and task identity: %v", agentEnv)
	}
	home := t.TempDir()
	inherited := []string{"MULTICA_WORKFLOW_ENABLED=true", "MULTICA_WORKFLOW_ACCEPT_CODE_RISK=true", "MULTICA_WORKFLOW_WORKSPACES=private-workspace", "MULTICA_WORKFLOW_ENGINE_ADDRESS=private-engine", "MULTICA_WORKFLOW_TLS_KEY=private-key", "MULTICA_TOKEN=daemon-secret"}
	if err := configureCodexTaskShellEnvironment("codex", home, inherited, agentEnv, nil, slog.Default()); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(home, "config.toml"))
	if err != nil {
		t.Fatal(err)
	}
	policy := string(raw)
	for _, name := range []string{"MULTICA_WORKFLOW_NODE", "MULTICA_WORKFLOW_RUNNER"} {
		if !strings.Contains(policy, name) {
			t.Errorf("Codex shell policy drops %s", name)
		}
	}
	for _, denied := range []string{"MULTICA_WORKFLOW_ENABLED", "MULTICA_WORKFLOW_ACCEPT_CODE_RISK", "MULTICA_WORKFLOW_WORKSPACES", "MULTICA_WORKFLOW_ENGINE_ADDRESS", "MULTICA_WORKFLOW_TLS_KEY", "daemon-secret", "private-key"} {
		if strings.Contains(policy, denied) {
			t.Errorf("Codex shell policy leaks %s", denied)
		}
	}
}

func TestWorkflowAuthoringEnvironmentRejectsUnavailableTools(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("fake Node version fixtures require /bin/sh")
	}
	for _, tc := range []struct {
		name, version               string
		missingRunner, relativeNode bool
	}{
		{name: "old node", version: "v20.0.0"},
		{name: "malformed version", version: "not-node"},
		{name: "missing runner", version: "v22.0.0", missingRunner: true},
		{name: "relative node", version: "v22.0.0", relativeNode: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			node := filepath.Join(dir, "node")
			entry := filepath.Join(dir, "cli.js")
			if err := os.WriteFile(node, []byte("#!/bin/sh\nprintf '"+tc.version+"\\n'\n"), 0700); err != nil {
				t.Fatal(err)
			}
			if !tc.missingRunner {
				if err := os.WriteFile(entry, nil, 0600); err != nil {
					t.Fatal(err)
				}
			}
			if tc.relativeNode {
				node = "node"
			}
			d := New(Config{Workflow: WorkflowRunnerConfig{Node: node, Entry: entry}}, slog.Default())
			if len(d.workflowAuthoringEnv) != 0 {
				t.Fatalf("invalid configuration propagated: %v", d.workflowAuthoringEnv)
			}
		})
	}
}
