package daemon

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/multica-ai/multica/server/internal/daemon/processtree"
)

// workflowAuthoringEnvironment checks local authoring tools once at startup.
// Local source validation needs neither engine credentials nor execution
// enrollment. Forward only these paths; the task retains its own API identity.
func workflowAuthoringEnvironment(config WorkflowRunnerConfig) map[string]string {
	if !filepath.IsAbs(config.Node) || !filepath.IsAbs(config.Entry) {
		return nil
	}
	for _, path := range []string{config.Node, config.Entry} {
		if info, err := os.Stat(path); err != nil || !info.Mode().IsRegular() {
			return nil
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	cmd := exec.Command(config.Node, "--version")
	cmd.Env = []string{"LANG=C"}
	raw, err := processtree.Output(ctx, cmd, time.Second)
	version := strings.TrimPrefix(strings.TrimSpace(string(raw)), "v")
	major, _ := strconv.Atoi(strings.Split(version, ".")[0])
	if err != nil || major < 22 {
		return nil
	}
	return map[string]string{
		"MULTICA_WORKFLOW_NODE":   config.Node,
		"MULTICA_WORKFLOW_RUNNER": config.Entry,
	}
}
