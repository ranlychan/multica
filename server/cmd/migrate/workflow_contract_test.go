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
	exec("CREATE TABLE issue(id UUID PRIMARY KEY, origin_type TEXT); CREATE TABLE agent_task_queue(id UUID PRIMARY KEY); CREATE TABLE member(workspace_id UUID NOT NULL,user_id UUID NOT NULL)")
	files, err := filepath.Glob("../../migrations/*.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(files)
	var applied []string
	for _, file := range files {
		if filepath.Base(file) < "564_" || filepath.Base(file) >= "602_" {
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
	exec(fmt.Sprintf("INSERT INTO member(workspace_id,user_id) VALUES ('%s','%s')", ws, principal))
	t.Run("immutable revisions and increasing version", func(t *testing.T) {
		exec(fmt.Sprintf("INSERT INTO workflow_revision(id,workspace_id,workflow_id,version,definition,definition_hash,definition_graph,hatchet_name,created_by_id) VALUES ('%s','%s','%s',1,'source','hash','{}','engine-v1','%s')", rev, ws, wf, principal))
		reject(fmt.Sprintf("UPDATE workflow_revision SET definition='changed' WHERE id='%s'", rev))
		reject(fmt.Sprintf("UPDATE workflow_revision SET hatchet_name='changed' WHERE id='%s'", rev))
		reject(fmt.Sprintf("UPDATE workflow_revision SET expected_current_version=0 WHERE id='%s'", rev))
		reject(fmt.Sprintf("UPDATE workflow_revision SET audit_actor_id=gen_random_uuid() WHERE id='%s'", rev))
		reject(fmt.Sprintf("UPDATE workflow SET created_by_id=gen_random_uuid() WHERE id='%s'", wf))
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
	newRun := func() string {
		id := uuid.NewString()
		exec(fmt.Sprintf("INSERT INTO workflow_run(id,workspace_id,workflow_id,revision_id,definition_hash,source,idempotency_key) VALUES ('%s','%s','%s','%s','hash','manual','%s')", id, ws, wf, rev, id))
		return id
	}
	t.Run("submission rejects incomplete unordered and null windows", func(t *testing.T) {
		for _, pair := range []string{"now(),NULL", "NULL,now()", "now(),now()", "now(),now()-interval '1 day'"} {
			id := newRun()
			values := strings.Split(pair, ",")
			if _, err := c.Exec(ctx, fmt.Sprintf("UPDATE workflow_run SET submission_started_at=%s,submission_deadline=%s WHERE id='%s'", values[0], values[1], id)); err == nil {
				t.Errorf("accepted invalid window %s", pair)
			}
		}
		now := pgtype.Timestamptz{Time: time.Now().UTC(), Valid: true}
		for _, pair := range [][2]pgtype.Timestamptz{{{}, {}}, {now, {}}, {{}, now}, {now, now}} {
			id := newRun()
			if _, err := q.BeginWorkflowRunSubmission(ctx, db.BeginWorkflowRunSubmissionParams{ID: u(id), WorkspaceID: u(ws), SubmissionStartedAt: pair[0], SubmissionDeadline: pair[1]}); err != pgx.ErrNoRows {
				t.Errorf("invalid begin window accepted or not CAS rejection: %v", err)
			}
		}
		for _, status := range []string{"submitting", "running"} {
			id := newRun()
			if _, err := c.Exec(ctx, fmt.Sprintf("UPDATE workflow_run SET status='%s' WHERE id='%s'", status, id)); err == nil {
				t.Errorf("schema permits %s without submission window", status)
			}
		}
	})
	t.Run("terminal runs cannot reopen or overwrite durable outcomes", func(t *testing.T) {
		id := newRun()
		params := db.FinishWorkflowRunCASParams{ID: u(id), WorkspaceID: u(ws), Status: "completed", Status_2: "queued", Output: []byte(`{"result":42}`)}
		finished, err := q.FinishWorkflowRunCAS(ctx, params)
		if err != nil {
			t.Fatal(err)
		}
		for _, status := range []string{"queued", "failed", "completed"} {
			params.Status = status
			params.Status_2 = "completed"
			params.Output = []byte(`{}`)
			if _, err := q.FinishWorkflowRunCAS(ctx, params); err != pgx.ErrNoRows {
				t.Errorf("terminal rewrite to %s accepted: %v", status, err)
			}
		}
		actual, err := q.GetWorkflowRunInWorkspace(ctx, db.GetWorkflowRunInWorkspaceParams{ID: u(id), WorkspaceID: u(ws)})
		if err != nil {
			t.Fatal(err)
		}
		if string(actual.Output) != string(finished.Output) || actual.FinishedAt != finished.FinishedAt || actual.Status != "completed" {
			t.Errorf("terminal outcome overwritten: %#v", actual)
		}
		for _, update := range []string{"status='queued'", "status='failed'", "output='{}'", "finished_at=now()+interval '1 day'"} {
			if _, err := c.Exec(ctx, fmt.Sprintf("UPDATE workflow_run SET %s WHERE id='%s'", update, id)); err == nil {
				t.Errorf("schema permits terminal mutation %s", update)
			}
		}
		id = newRun()
		params.ID = u(id)
		params.Status = "running"
		params.Status_2 = "queued"
		if _, err := q.FinishWorkflowRunCAS(ctx, params); err != pgx.ErrNoRows {
			t.Errorf("finish accepted nonterminal target: %v", err)
		}
		for _, status := range []string{"failed", "cancelled", "skipped"} {
			params.ID = u(newRun())
			params.Status = status
			params.Status_2 = "queued"
			params.ReasonCode = pgtype.Text{String: "test", Valid: true}
			if _, err := q.FinishWorkflowRunCAS(ctx, params); err != nil {
				t.Fatal(err)
			}
			params.Status_2 = status
			params.Status = "queued"
			if _, err := q.FinishWorkflowRunCAS(ctx, params); err != pgx.ErrNoRows {
				t.Errorf("reopened %s: %v", status, err)
			}
			if _, err := c.Exec(ctx, "UPDATE workflow_run SET output='{}',finished_at=now()+interval '1 day' WHERE id=$1", params.ID); err == nil {
				t.Errorf("schema overwrote %s outcome", status)
			}
		}
	})
	t.Run("tenant deletion completion requires current claim", func(t *testing.T) {
		// Claim eligibility uses PostgreSQL now(), so fixture leases must use the
		// same clock rather than assuming the host and database clocks agree.
		var expiredLease, futureLease pgtype.Timestamptz
		if err := c.QueryRow(ctx, "SELECT now()-interval '1 minute', now()+interval '1 hour'").Scan(&expiredLease, &futureLease); err != nil {
			t.Fatal(err)
		}
		for _, outcome := range []string{"failed", "deleted"} {
			tenantWS := uuid.NewString()
			exec(fmt.Sprintf("INSERT INTO workflow_hatchet_tenant(workspace_id,tenant_id,token_encrypted,deletion_state,next_deletion_attempt_at) VALUES ('%s','tenant','ciphertext','pending',now()-interval '1 minute')", tenantWS))
			claimParams := db.ClaimWorkflowHatchetTenantDeletionParams{WorkspaceID: u(tenantWS), NextDeletionAttemptAt: expiredLease}
			a, err := q.ClaimWorkflowHatchetTenantDeletion(ctx, claimParams)
			if err != nil {
				t.Fatal(err)
			}
			claimParams.NextDeletionAttemptAt = futureLease
			b, err := q.ClaimWorkflowHatchetTenantDeletion(ctx, claimParams)
			if err != nil {
				t.Fatal(err)
			}
			if b.DeletionAttempts != a.DeletionAttempts+1 {
				t.Fatal("reclaim did not advance generation")
			}
			finishParams := db.FinishWorkflowHatchetTenantDeletionParams{WorkspaceID: u(tenantWS), DeletionState: outcome, ExpectedAttempt: a.DeletionAttempts, NextDeletionAttemptAt: futureLease}
			_, err = q.FinishWorkflowHatchetTenantDeletion(ctx, finishParams)
			if err != pgx.ErrNoRows {
				t.Errorf("stale %s completion accepted: %v", outcome, err)
			}
			current, err := q.GetWorkflowHatchetTenant(ctx, u(tenantWS))
			if err != nil {
				t.Fatal(err)
			}
			if current.DeletionState != "deleting" || current.DeletionAttempts != b.DeletionAttempts {
				t.Errorf("stale claimant changed active claim: %#v", current)
			}
			finishParams.ExpectedAttempt = b.DeletionAttempts
			finished, err := q.FinishWorkflowHatchetTenantDeletion(ctx, finishParams)
			if err != nil {
				t.Fatal(err)
			}
			if finished.DeletionState != outcome || finished.DeletionAttempts != b.DeletionAttempts {
				t.Fatalf("current claim result: %#v", finished)
			}
		}
	})
	t.Run("storage cleanup retains tenant tombstone and task rows", func(t *testing.T) {
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
	var tables, functions, taskColumn int
	if err := c.QueryRow(ctx, "SELECT count(*) FROM pg_tables WHERE schemaname=$1 AND tablename LIKE 'workflow%'", schema).Scan(&tables); err != nil || tables != 0 {
		t.Fatalf("rollback workflow tables=%d err=%v", tables, err)
	}
	if err := c.QueryRow(ctx, "SELECT count(*) FROM pg_proc WHERE pronamespace=$1::regnamespace", schema).Scan(&functions); err != nil || functions != 0 {
		t.Fatalf("rollback functions=%d err=%v", functions, err)
	}
	if err := c.QueryRow(ctx, "SELECT count(*) FROM information_schema.columns WHERE table_schema=$1 AND table_name='agent_task_queue' AND column_name='workflow_run_id'", schema).Scan(&taskColumn); err != nil || taskColumn != 0 {
		t.Fatalf("rollback task column=%d err=%v", taskColumn, err)
	}
	for _, origin := range []string{"autopilot", "quick_create", "lark_chat", "slack_chat", "agent_create", "dingtalk_chat", "wecom_chat", "telegram_chat"} {
		exec(fmt.Sprintf("INSERT INTO issue(id,origin_type) VALUES ('%s','%s')", uuid.NewString(), origin))
	}
	reject(fmt.Sprintf("INSERT INTO issue(id,origin_type) VALUES ('%s','workflow')", uuid.NewString()))
}
