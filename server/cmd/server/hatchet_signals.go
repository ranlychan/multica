package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
)

// workflowSignals keeps termination queued while the startup context is
// released and HTTP initialization finishes, and throughout engine drain.
func workflowSignals(parent context.Context) (context.Context, context.CancelFunc, <-chan os.Signal, func()) {
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
	startup, stopStartup := signal.NotifyContext(parent, os.Interrupt, syscall.SIGTERM)
	stopSignals := func() { stopStartup(); signal.Stop(quit) }
	return startup, stopStartup, quit, stopSignals
}
