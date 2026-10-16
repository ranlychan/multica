package hatchet

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func TestWorkflowAssignmentLazyReadOnlyPoolAndInvalidSelectors(t *testing.T) {
	ctx := context.Background()
	v, err := NewAssignmentVerifier(ctx, "postgres://ignored:secret@127.0.0.1:1/unmigrated?sslmode=disable")
	if err != nil {
		t.Fatal("construction connected before migrations", err)
	}
	defer v.Close()
	if err = v.Check(ctx); !errors.Is(err, ErrAssignmentUnavailable) {
		t.Fatal("unavailable engine accepted", err)
	}
	a := Assignment{TenantID: "00000000-0000-0000-0000-000000000001", EngineRunID: "00000000-0000-0000-0000-000000000002", EngineTaskID: "00000000-0000-0000-0000-000000000003", WorkflowID: "00000000-0000-0000-0000-000000000004", VersionID: "00000000-0000-0000-0000-000000000005", WorkerName: "multica-00000000-0000-0000-0000-000000000006", TaskName: "work", ActionID: "workflow:work", Invocation: 1}
	for _, mutate := range []func(*Assignment){func(a *Assignment) { a.EngineTaskID = "bad" }, func(a *Assignment) { a.Retry = -1 }, func(a *Assignment) { a.Invocation = 0 }, func(a *Assignment) { a.WorkerName = "any" }, func(a *Assignment) {
		a.LocalRunID = a.EngineRunID
		a.SubmissionStartedAt = time.Now().Add(-time.Hour)
		a.SubmissionDeadline = time.Now().Add(-5 * time.Minute)
	}} {
		wrong := a
		mutate(&wrong)
		if err = v.VerifyAssignment(ctx, wrong); !errors.Is(err, ErrAssignmentDenied) {
			t.Fatal("invalid selectors reached unavailable DB", err)
		}
	}
}

func TestWorkflowAssignmentConnectionEnforcesReadOnlySessionAndTransaction(t *testing.T) {
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		t.Skip("explicit disposable app DB required")
	}
	v, err := NewAssignmentVerifier(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	ctx := context.Background()
	tx, err := v.pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly, IsoLevel: pgx.RepeatableRead})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	var readonly, primary bool
	var searchPath, timeout, zone string
	if err = tx.QueryRow(ctx, `SELECT current_setting('transaction_read_only')='on' AND current_setting('default_transaction_read_only')='on',NOT pg_is_in_recovery(),current_setting('search_path'),current_setting('statement_timeout'),current_setting('TimeZone')`).Scan(&readonly, &primary, &searchPath, &timeout, &zone); err != nil {
		t.Fatal(err)
	}
	if !readonly || !primary || searchPath != "pg_catalog,public" || timeout != "2s" || zone != "UTC" {
		t.Fatalf("unsafe connection: %t %t %q %q %q", readonly, primary, searchPath, timeout, zone)
	}
	if err = v.Check(ctx); !errors.Is(err, ErrAssignmentUnavailable) {
		t.Fatal("app schema accepted as pinned engine", err)
	}
}
