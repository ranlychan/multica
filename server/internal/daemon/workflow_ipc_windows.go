//go:build windows

package daemon

import (
	"errors"
	"net"
	"os"
)

// ExtraFiles is unavailable on Windows; fail closed until a named-pipe owner exists.
func workflowSocket() (net.Conn, *os.File, error) {
	return nil, nil, errors.New("workflow_ipc_unavailable")
}
