package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

// Each test uses a private schema; DATABASE_URL must point at a disposable test database.
func TestWorkflowDurableContracts(t *testing.T) {
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		t.Skip("requires DATABASE_URL")
	}
	ctx := context.Background()
	c, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(ctx)
	schema := "workflow_contract_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	exec := func(sql string) {
		t.Helper()
		if _, err := c.Exec(ctx, sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	exec("CREATE SCHEMA " + schema)
	defer c.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE")
	exec("SET search_path TO " + schema)
	exec("CREATE TABLE issue(id UUID PRIMARY KEY, origin_type TEXT); CREATE TABLE agent_task_queue(id UUID PRIMARY KEY)")
	files, err := filepath.Glob("../../migrations/*.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(files)
	var applied []string
	for _, file := range files {
		if filepath.Base(file) < "564_" || filepath.Base(file) >= "591_" {
			continue
		}
		b, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		exec(string(b))
		applied = append(applied, file)
	}
	reject := func(sql string) {
		t.Helper()
		if _, err := c.Exec(ctx, sql); err == nil {
			t.Fatalf("expected rejection: %s", sql)
		}
	}
	ws, wf, rev, principal, trig, run := uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString()
	u := func(s string) pgtype.UUID { return pgtype.UUID{Bytes: uuid.MustParse(s), Valid: true} }
	q := db.New(c)
	exec(fmt.Sprintf("INSERT INTO workflow(id,workspace_id,name,slug,created_by_type,created_by_id) VALUES ('%s','%s','test','test','member','%s')", wf, ws, principal))
	t.Run("immutable revisions and increasing version", func(t *testing.T) {
		exec(fmt.Sprintf("INSERT INTO workflow_revision(id,workspace_id,workflow_id,version,definition,definition_hash,definition_graph,hatchet_name,created_by_id) VALUES ('%s','%s','%s',1,'source','hash','{}','engine-v1','%s')", rev, ws, wf, principal))
		reject(fmt.Sprintf("UPDATE workflow_revision SET definition='changed' WHERE id='%s'", rev))
		reject(fmt.Sprintf("UPDATE workflow_revision SET hatchet_name='changed' WHERE id='%s'", rev))
		reject(fmt.Sprintf("INSERT INTO workflow_revision(workspace_id,workflow_id,version,definition,definition_hash,definition_graph,hatchet_name,created_by_id) VALUES ('%s','%s',1,'other','other','{}','other','%s')", ws, wf, principal))
		reject(fmt.Sprintf("INSERT INTO workflow_revision(workspace_id,workflow_id,version,definition,definition_hash,definition_graph,hatchet_name,created_by_id) VALUES ('%s','%s',0,'other','other','{}','other','%s')", ws, wf, principal))
		exec(fmt.Sprintf("INSERT INTO workflow_revision(workspace_id,workflow_id,version,definition,definition_hash,definition_graph,hatchet_name,created_by_id) VALUES ('%s','%s',3,'new','new','{}','engine-v3','%s')", ws, wf, principal))
		reject(fmt.Sprintf("INSERT INTO workflow_revision(workspace_id,workflow_id,version,definition,definition_hash,definition_graph,hatchet_name,created_by_id) VALUES ('%s','%s',2,'old','old','{}','engine-v2','%s')", ws, wf, principal))
	})
	t.Run("immutable trigger principal separate from audit actor", func(t *testing.T) {
		exec(fmt.Sprintf("INSERT INTO workflow_trigger(id,workspace_id,workflow_id,kind,principal_user_id,created_by_type,created_by_id,token_hash) VALUES ('%s','%s','%s','api','%s','agent','%s','api-hash')", trig, ws, wf, principal, uuid.NewString()))
		reject(fmt.Sprintf("UPDATE workflow_trigger SET principal_user_id='%s' WHERE id='%s'", uuid.NewString(), trig))
		exec(fmt.Sprintf("UPDATE workflow_trigger SET label='edited' WHERE id='%s'", trig))
	})
	t.Run("schedule deduplicated while different slots allowed", func(t *testing.T) {
		insert := fmt.Sprintf("INSERT INTO workflow_run(id,workspace_id,workflow_id,revision_id,definition_hash,principal_user_id,trigger_id,source,planned_at,idempotency_key) VALUES ('%s','%s','%s','%s','hash','%s','%s','schedule','2026-09-30','%s')", run, ws, wf, rev, principal, trig, run)
		exec(insert)
		reject(strings.Replace(insert, run, uuid.NewString(), -1))
		exec(strings.Replace(strings.Replace(insert, run, uuid.NewString(), -1), "2026-09-30", "2026-10-01", 1))
		reject(fmt.Sprintf("UPDATE workflow_run SET revision_id='%s' WHERE id='%s'", uuid.NewString(), run))
	})
	t.Run("runner uniqueness revocation and expiry", func(t *testing.T) {
		insert := fmt.Sprintf("INSERT INTO workflow_runner_token(workspace_id,daemon_id,token_hash,expires_at) VALUES ('%s','daemon','runner-hash',now()+interval '1 day')", ws)
		exec(insert)
		reject(strings.Replace(insert, "runner-hash", "runner-hash2", 1))
		exec("UPDATE workflow_runner_token SET revoked_at=now()")
		exec(strings.Replace(insert, "runner-hash", "runner-hash2", 1))
		reject(fmt.Sprintf("INSERT INTO workflow_runner_token(workspace_id,daemon_id,token_hash,expires_at) VALUES ('%s','other','invalid',now()-interval '1 day')", ws))
		exec(fmt.Sprintf("INSERT INTO workflow_runner_token(workspace_id,daemon_id,token_hash,created_at,expires_at) VALUES ('%s','expired','expired-hash',now()-interval '2 days',now()-interval '1 day')", ws))
		if _, err := q.GetActiveWorkflowRunnerToken(ctx, db.GetActiveWorkflowRunnerTokenParams{TokenHash: "expired-hash", WorkspaceID: u(ws), DaemonID: "expired"}); err != pgx.ErrNoRows {
			t.Fatalf("expired token admitted: %v", err)
		}
	})
	t.Run("receipt survives attempts and rejects duplicate side effect", func(t *testing.T) {
		insert := fmt.Sprintf("INSERT INTO workflow_operation(workspace_id,run_id,step_id,operation_key,payload_hash,attempt,status,result) VALUES ('%s','%s','step','key','payload',1,'completed','{\"id\":42}')", ws, run)
		exec(insert)
		reject(strings.Replace(insert, "1,'completed'", "2,'completed'", 1))
		var result string
		err := c.QueryRow(ctx, "SELECT result->>'id' FROM workflow_operation WHERE operation_key='key'").Scan(&result)
		if err != nil || result != "42" {
			t.Fatalf("durable result=%q err=%v", result, err)
		}
		reject("UPDATE workflow_operation SET payload_hash='changed' WHERE operation_key='key'")
		reject("UPDATE workflow_operation SET result='{}' WHERE operation_key='key'")
	})
	t.Run("workspace lookups fail closed and submission window cannot reset", func(t *testing.T) {
		if _, err := q.GetWorkflowInWorkspace(ctx, db.GetWorkflowInWorkspaceParams{ID: u(wf), WorkspaceID: u(uuid.NewString())}); err != pgx.ErrNoRows {
			t.Fatalf("cross workspace lookup: %v", err)
		}
		now := time.Now().UTC()
		params := db.BeginWorkflowRunSubmissionParams{ID: u(run), WorkspaceID: u(ws), SubmissionStartedAt: pgtype.Timestamptz{Time: now, Valid: true}, SubmissionDeadline: pgtype.Timestamptz{Time: now.Add(time.Minute), Valid: true}}
		if _, err := q.BeginWorkflowRunSubmission(ctx, params); err != nil {
			t.Fatal(err)
		}
		if _, err := q.BeginWorkflowRunSubmission(ctx, params); err != pgx.ErrNoRows {
			t.Fatalf("repeated submission admission: %v", err)
		}
		reject(fmt.Sprintf("UPDATE workflow_run SET submission_deadline=now()+interval '1 day' WHERE id='%s'", run))
	})
	t.Run("publish validates revision and compares version", func(t *testing.T) {
		p := db.PublishWorkflowRevisionCASParams{ID: u(wf), WorkspaceID: u(ws), ID_2: u(rev)}
		if _, err := q.PublishWorkflowRevisionCAS(ctx, p); err != pgx.ErrNoRows {
			t.Fatalf("unvalidated publication: %v", err)
		}
		exec(fmt.Sprintf("UPDATE workflow_revision SET validation_status='valid' WHERE id='%s'", rev))
		if _, err := q.PublishWorkflowRevisionCAS(ctx, p); err != nil {
			t.Fatal(err)
		}
		if _, err := q.PublishWorkflowRevisionCAS(ctx, p); err != pgx.ErrNoRows {
			t.Fatalf("stale publish CAS: %v", err)
		}
	})
	t.Run("tenant deletion state survives missing workspace", func(t *testing.T) {
		exec(fmt.Sprintf("INSERT INTO workflow_hatchet_tenant(workspace_id,tenant_id,token_encrypted,deletion_state) VALUES ('%s','tenant','ciphertext','pending')", ws))
		task := uuid.NewString()
		exec(fmt.Sprintf("INSERT INTO agent_task_queue(id,workflow_run_id) VALUES ('%s','%s')", task, run))
		tx, err := c.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		cleanup := q.WithTx(tx)
		for _, fn := range []func(context.Context, pgtype.UUID) error{cleanup.DeleteWorkflowOperationsByWorkspace, cleanup.DeleteWorkflowRunsByWorkspace, cleanup.DeleteWorkflowTriggersByWorkspace, cleanup.DeleteWorkflowCollaboratorsByWorkspace, cleanup.DeleteWorkflowRevisionsByWorkspace, cleanup.DeleteWorkflowRunnerTokensByWorkspace, cleanup.DeleteWorkflowsByWorkspace} {
			if err := fn(ctx, u(ws)); err != nil {
				tx.Rollback(ctx)
				t.Fatal(err)
			}
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		var retained int
		if err := c.QueryRow(ctx, "SELECT count(*) FROM agent_task_queue WHERE id=$1", task).Scan(&retained); err != nil || retained != 1 {
			t.Fatalf("in flight task retained=%d err=%v", retained, err)
		}
		var n int
		err = c.QueryRow(ctx, "SELECT count(*) FROM workflow_hatchet_tenant WHERE deletion_state='pending'").Scan(&n)
		if err != nil || n != 1 {
			t.Fatalf("retained tenants=%d err=%v", n, err)
		}
	})
	t.Run("no foreign keys", func(t *testing.T) {
		var n int
		err := c.QueryRow(ctx, "SELECT count(*) FROM pg_constraint WHERE contype='f' AND connamespace=$1::regnamespace", schema).Scan(&n)
		if err != nil || n != 0 {
			t.Fatalf("foreign keys=%d err=%v", n, err)
		}
	})
	for i := len(applied) - 1; i >= 0; i-- {
		b, err := os.ReadFile(strings.Replace(applied[i], ".up.sql", ".down.sql", 1))
		if err != nil {
			t.Fatal(err)
		}
		exec(string(b))
	}
	var exists bool
	if err := c.QueryRow(ctx, "SELECT to_regclass('workflow') IS NOT NULL").Scan(&exists); err != nil || exists {
		t.Fatalf("rollback workflow exists=%v err=%v", exists, err)
	}
}
