//go:build !windows

package daemon

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// A CLI-only cancellation must also stop the separate authored-code validator.
// Detached validation previously survived its Go owner as a CPU-running orphan.
func TestWorkflowValidationDescendantExitsOnTimeoutAndShutdown(t *testing.T) {
	for _, shutdown := range []bool{false, true} {
		t.Run(fmt.Sprintf("shutdown=%v", shutdown), func(t *testing.T) {
			entry, _ := filepath.Abs("../../../packages/workflow-runner/dist/cli.js")
			runner := &WorkflowRunner{Config: WorkflowRunnerConfig{Node: workflowTestNode(t), Entry: entry, ValidationTimeout: 1500 * time.Millisecond}}
			pidfile := filepath.Join(t.TempDir(), "validator.pid")
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			result := make(chan error, 1)
			go func() {
				_, err := runner.Validate(ctx, fmt.Sprintf(`process.getBuiltinModule('node:fs').writeFileSync(%q,String(process.pid));while(true){};export default {};`, pidfile))
				result <- err
			}()
			deadline := time.Now().Add(time.Second)
			pid := 0
			for time.Now().Before(deadline) {
				raw, _ := os.ReadFile(pidfile)
				pid, _ = strconv.Atoi(strings.TrimSpace(string(raw)))
				if pid > 0 {
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
			if pid <= 0 {
				t.Fatal("packaged validator did not reach authored source")
			}
			t.Cleanup(func() { _ = syscall.Kill(pid, syscall.SIGKILL) })
			if shutdown {
				cancel()
			}
			select {
			case err := <-result:
				if err == nil {
					t.Fatal("infinite validator accepted")
				}
			case <-time.After(5 * time.Second):
				t.Fatal("validation owner did not stop")
			}
			deadline = time.Now().Add(2 * time.Second)
			for time.Now().Before(deadline) {
				if syscall.Kill(pid, 0) == syscall.ESRCH {
					return
				}
				time.Sleep(10 * time.Millisecond)
			}
			t.Fatalf("authored-code validator pid %d survived Go cancellation", pid)
		})
	}
}
