//go:build windows

package hatchet

import "os/exec"

// Bundled engine packaging currently supports Unix hosts. Windows builds keep
// disabled deployment behavior; enabled startup is rejected before execution.
func ownProcess(cmd *exec.Cmd)             {}
func terminateProcess(cmd *exec.Cmd) error { return cmd.Process.Kill() }
func killProcess(cmd *exec.Cmd) error      { return cmd.Process.Kill() }
