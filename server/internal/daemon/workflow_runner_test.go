package daemon

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func workflowTestNode(t *testing.T) string {
	t.Helper()
	p := os.Getenv("MULTICA_WORKFLOW_TEST_NODE")
	if p == "" {
		t.Skip("explicit Node22 executable required")
	}
	return p
}

func TestWorkflowPackagedValidationAndPrivateCache(t *testing.T) {
	node := workflowTestNode(t)
	entry, _ := filepath.Abs("../../../packages/workflow-runner/dist/cli.js")
	runner := &WorkflowRunner{Config: WorkflowRunnerConfig{Node: node, Entry: entry, Cache: filepath.Join(t.TempDir(), "cache")}}
	source := `import { defineWorkflow } from '@multica/workflow-sdk'; export default defineWorkflow({steps:[{id:'hello',run:async()=>42}]});`
	result, err := runner.Validate(context.Background(), source)
	if err != nil {
		t.Fatal(err)
	}
	if result.Hash != "a64aaf6d0232df6bd4b49e9d915a6ce1fadfbb675ef01ed2821d24ffab57607e" {
		t.Fatalf("unexpected source hash: %s", result.Hash)
	}
	path, err := runner.CacheSource(source, result.Hash)
	if err != nil {
		t.Fatal(err)
	}
	stat, _ := os.Stat(path)
	if stat.Mode().Perm() != 0600 {
		t.Fatal("source cache is not private")
	}
	if _, err = runner.CacheSource(source+"bad", result.Hash); err == nil {
		t.Fatal("accepted hash mismatch")
	}
	after, _ := os.ReadFile(path)
	if string(after) != source {
		t.Fatal("failed replacement damaged old immutable revision")
	}
}

func TestWorkflowValidationTimeoutAndMissingRuntime(t *testing.T) {
	node := workflowTestNode(t)
	entry, _ := filepath.Abs("../../../packages/workflow-runner/dist/cli.js")
	runner := &WorkflowRunner{Config: WorkflowRunnerConfig{Node: node, Entry: entry, ValidationTimeout: 300 * time.Millisecond}}
	ctx := context.Background()
	if _, err := runner.Validate(ctx, `while(true){}; export default {};`); err == nil {
		t.Fatal("unbounded authored validation")
	}
	runner.Config.Node = filepath.Join(t.TempDir(), "missing")
	if _, err := runner.Validate(ctx, "export default {}"); err == nil {
		t.Fatal("missing runtime accepted")
	}
}
