//go:build !windows

package daemon

import (
	"golang.org/x/sys/unix"
	"net"
	"os"
)

func workflowSocket() (net.Conn, *os.File, error) {
	pair, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM, 0)
	if err != nil {
		return nil, nil, err
	}
	parent := os.NewFile(uintptr(pair[0]), "workflow-parent")
	child := os.NewFile(uintptr(pair[1]), "workflow-child")
	conn, err := net.FileConn(parent)
	_ = parent.Close()
	if err != nil {
		_ = child.Close()
	}
	return conn, child, err
}
