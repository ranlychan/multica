//go:build !windows

package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"
)

func TestHatchetTerminationAfterEngineStartup(t *testing.T) {
	root := t.TempDir()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	child := exec.Command(exe, "-test.run=^TestHatchetSignalHelper$")
	child.Env = append(os.Environ(), "MULTICA_SIGNAL_HELPER="+root)
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	defer child.Process.Kill()
	deadline := time.Now().Add(5 * time.Second)
	var pid int
	for time.Now().Before(deadline) {
		raw, err := os.ReadFile(filepath.Join(root, "started"))
		if err == nil {
			pid, _ = strconv.Atoi(string(raw))
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if pid == 0 {
		t.Fatal("engine helper did not start")
	}
	defer syscall.Kill(-pid, syscall.SIGKILL)
	if err := child.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if err := child.Wait(); err != nil {
		t.Fatalf("post-start signal bypassed graceful cleanup: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "drained")); err != nil {
		t.Fatal("engine drain did not execute")
	}
	if err := syscall.Kill(-pid, 0); err != syscall.ESRCH {
		t.Fatalf("owned engine group survives termination: %v", err)
	}
}

func TestHatchetSignalHelper(t *testing.T) {
	root := os.Getenv("MULTICA_SIGNAL_HELPER")
	if root == "" {
		return
	}
	_, stopStartup, quit, stopSignals := workflowSignals(context.Background())
	defer stopSignals()
	engine := exec.Command("/bin/sleep", "60")
	engine.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := engine.Start(); err != nil {
		t.Fatal(err)
	}
	defer syscall.Kill(-engine.Process.Pid, syscall.SIGKILL)
	// This is the transition after Start succeeds and before main finishes
	// setting up handlers/listeners. Signal registration must span that interval.
	stopStartup()
	if err := os.WriteFile(filepath.Join(root, "started"), []byte(fmt.Sprint(engine.Process.Pid)), 0600); err != nil {
		t.Fatal(err)
	}
	select {
	case <-quit:
	case <-time.After(5 * time.Second):
		t.Fatal("termination signal lost after engine startup")
	}
	_ = syscall.Kill(-engine.Process.Pid, syscall.SIGTERM)
	_ = engine.Wait()
	if err := os.WriteFile(filepath.Join(root, "drained"), []byte("done"), 0600); err != nil {
		t.Fatal(err)
	}
}
