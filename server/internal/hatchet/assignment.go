package hatchet

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrAssignmentDenied = errors.New("workflow_assignment_denied")
var ErrAssignmentUnavailable = errors.New("workflow_assignment_unavailable")

type AssignmentVerifier struct{ pool *pgxpool.Pool }

func NewAssignmentVerifier(ctx context.Context, databaseURL string) (*AssignmentVerifier, error) {
	c, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, ErrAssignmentUnavailable
	}
	c.MaxConns, c.MinConns = 4, 0
	c.ConnConfig.RuntimeParams["default_transaction_read_only"] = "on"
	c.ConnConfig.RuntimeParams["search_path"] = "pg_catalog,public"
	c.ConnConfig.RuntimeParams["statement_timeout"] = "2000"
	c.ConnConfig.RuntimeParams["timezone"] = "UTC"
	c.ConnConfig.ConnectTimeout = 2 * time.Second
	p, err := pgxpool.NewWithConfig(ctx, c)
	if err != nil {
		return nil, ErrAssignmentUnavailable
	}
	return &AssignmentVerifier{pool: p}, nil
}
func (v *AssignmentVerifier) Close() { v.pool.Close() }
func (v *AssignmentVerifier) Check(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	tx, err := v.pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return ErrAssignmentUnavailable
	}
	defer tx.Rollback(ctx)
	return assignmentSchema(ctx, tx)
}

// VerifyAssignment reads one primary database snapshot. It does not lock the
// engine or grant a lease extending to the subsequent application transaction.
func (v *AssignmentVerifier) VerifyAssignment(ctx context.Context, a Assignment) error {
	for _, id := range []string{a.TenantID, a.EngineRunID, a.EngineTaskID, a.WorkflowID, a.VersionID} {
		if !validID(id) {
			return ErrAssignmentDenied
		}
	}
	if a.Retry < 0 || a.Retry > math.MaxInt32 || a.Invocation < 1 || a.Invocation > math.MaxInt32 || len(a.TaskName) == 0 || len(a.TaskName) > 512 || len(a.ActionID) == 0 || len(a.ActionID) > 1024 || !strings.HasPrefix(a.WorkerName, "multica-") || !validID(strings.TrimPrefix(a.WorkerName, "multica-")) {
		return ErrAssignmentDenied
	}
	preACK := a.LocalRunID != ""
	if preACK && (!validID(a.LocalRunID) || a.SubmissionStartedAt.IsZero() || !a.SubmissionDeadline.After(a.SubmissionStartedAt) || a.SubmissionDeadline.Sub(a.SubmissionStartedAt) > 55*time.Minute || !time.Now().Before(a.SubmissionDeadline)) {
		return ErrAssignmentDenied
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	tx, err := v.pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly, IsoLevel: pgx.RepeatableRead})
	if err != nil {
		return ErrAssignmentUnavailable
	}
	defer tx.Rollback(ctx)
	if err = assignmentSchema(ctx, tx); err != nil {
		return err
	}
	var inputMatches bool
	err = tx.QueryRow(ctx, assignmentSQL, a.TenantID, a.EngineTaskID, a.EngineRunID, a.WorkflowID, a.VersionID, a.TaskName, a.ActionID, a.Retry, a.Invocation, a.WorkerName, a.LocalRunID, a.SubmissionStartedAt, a.SubmissionDeadline).Scan(&inputMatches)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrAssignmentDenied
	}
	if err != nil {
		return ErrAssignmentUnavailable
	}
	if preACK && !inputMatches {
		return ErrAssignmentInputPending
	}
	return nil
}

// Missing/offloaded input can only wait for the normal trusted Submit/Discover
// ACK; it must never be replaced by a caller-echoed local UUID.
var ErrAssignmentInputPending = errors.New("workflow_assignment_input_pending")

const assignmentSQL = `
SELECT CASE WHEN $11::text='' THEN true ELSE EXISTS (
 SELECT 1 FROM public.v1_payload p WHERE p.tenant_id=t.tenant_id AND p.id=t.id
 AND p.inserted_at=t.inserted_at AND p.type='TASK_INPUT' AND p.location='INLINE'
 AND p.inline_content #>> '{input,multica_run_id}'=$11::text
 AND t.inserted_at >= $12::timestamptz AND t.inserted_at <= $13::timestamptz
) END
FROM public.v1_task t
JOIN public.v1_task_runtime rt ON (rt.task_id,rt.task_inserted_at,rt.retry_count)=(t.id,t.inserted_at,t.retry_count) AND rt.tenant_id=t.tenant_id
JOIN public.v1_durable_event_log_file lf ON (lf.durable_task_id,lf.durable_task_inserted_at)=(t.id,t.inserted_at) AND lf.tenant_id=t.tenant_id
JOIN public."Worker" w ON w.id=rt.worker_id AND w."tenantId"=t.tenant_id
WHERE (t.id,t.inserted_at)=(SELECT task_id,inserted_at FROM public.v1_lookup_table WHERE tenant_id=$1::uuid AND external_id=$2::uuid)
AND t.tenant_id=$1::uuid AND t.external_id=$2::uuid AND t.workflow_run_id=$3::uuid
AND t.workflow_id=$4::uuid AND t.workflow_version_id=$5::uuid
AND t.step_readable_id=$6::text AND t.action_id=$7::text
AND t.retry_count=$8::integer AND lf.latest_invocation_count=$9::integer
AND t.is_durable IS TRUE AND rt.evicted_at IS NULL AND rt.timeout_at>CURRENT_TIMESTAMP
AND w.name=$10::text AND w."deletedAt" IS NULL`

// Required shape at VERSION's exact pin. This is not proof of a binary commit;
// the supervisor's build receipts enforce that separate deployment boundary.
var assignmentColumns = map[string]string{
	"v1_lookup_table.tenant_id": "uuid!", "v1_lookup_table.external_id": "uuid!", "v1_lookup_table.task_id": "int8", "v1_lookup_table.inserted_at": "timestamptz!",
	"v1_task.id": "int8!", "v1_task.inserted_at": "timestamptz!", "v1_task.tenant_id": "uuid!", "v1_task.external_id": "uuid!", "v1_task.workflow_id": "uuid!", "v1_task.workflow_run_id": "uuid!", "v1_task.workflow_version_id": "uuid!", "v1_task.step_id": "uuid!", "v1_task.step_readable_id": "text!", "v1_task.action_id": "text!", "v1_task.retry_count": "int4!", "v1_task.is_durable": "bool",
	"v1_task_runtime.task_id": "int8!", "v1_task_runtime.task_inserted_at": "timestamptz!", "v1_task_runtime.retry_count": "int4!", "v1_task_runtime.tenant_id": "uuid!", "v1_task_runtime.worker_id": "uuid", "v1_task_runtime.evicted_at": "timestamptz", "v1_task_runtime.timeout_at": "timestamp!",
	"v1_durable_event_log_file.durable_task_id": "int8!", "v1_durable_event_log_file.durable_task_inserted_at": "timestamptz!", "v1_durable_event_log_file.tenant_id": "uuid!", "v1_durable_event_log_file.latest_invocation_count": "int4!",
	"Worker.id": "uuid!", "Worker.tenantId": "uuid!", "Worker.name": "text!", "Worker.deletedAt": "timestamp",
	"v1_payload.tenant_id": "uuid!", "v1_payload.id": "int8!", "v1_payload.inserted_at": "timestamptz!", "v1_payload.type": "v1_payload_type!", "v1_payload.location": "v1_payload_location!", "v1_payload.inline_content": "jsonb",
}
var assignmentKeys = map[string]string{
	"v1_lookup_table": "external_id", "v1_task": "id,inserted_at", "v1_task_runtime": "task_id,task_inserted_at,retry_count", "v1_durable_event_log_file": "durable_task_id,durable_task_inserted_at", "Worker": "id", "v1_payload": "tenant_id,inserted_at,id,type",
}

func assignmentSchema(ctx context.Context, tx pgx.Tx) error {
	var safe bool
	if err := tx.QueryRow(ctx, `SELECT current_setting('transaction_read_only')='on' AND current_setting('default_transaction_read_only')='on' AND current_setting('search_path')='pg_catalog,public' AND current_setting('statement_timeout')='2s' AND current_setting('TimeZone')='UTC' AND NOT pg_is_in_recovery()`).Scan(&safe); err != nil || !safe {
		return ErrAssignmentUnavailable
	}
	tables := []string{"v1_lookup_table", "v1_task", "v1_task_runtime", "v1_durable_event_log_file", "Worker", "v1_payload"}
	rows, err := tx.Query(ctx, `SELECT c.relname||'.'||a.attname,t.typname,a.attnotnull FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace JOIN pg_catalog.pg_attribute a ON a.attrelid=c.oid JOIN pg_catalog.pg_type t ON t.oid=a.atttypid WHERE n.nspname='public' AND c.relkind IN ('r','p') AND c.relname=ANY($1::text[]) AND a.attnum>0 AND NOT a.attisdropped`, tables)
	if err != nil {
		return ErrAssignmentUnavailable
	}
	found := map[string]string{}
	for rows.Next() {
		var name, typ string
		var required bool
		if rows.Scan(&name, &typ, &required) != nil {
			rows.Close()
			return ErrAssignmentUnavailable
		}
		if required {
			typ += "!"
		}
		found[name] = typ
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return ErrAssignmentUnavailable
	}
	for name, typ := range assignmentColumns {
		if found[name] != typ {
			return fmt.Errorf("%w: schema column %s", ErrAssignmentUnavailable, name)
		}
	}
	rows, err = tx.Query(ctx, `SELECT c.relname,string_agg(a.attname,',' ORDER BY k.ord) FROM pg_catalog.pg_constraint p JOIN pg_catalog.pg_class c ON c.oid=p.conrelid JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace CROSS JOIN LATERAL unnest(p.conkey) WITH ORDINALITY k(num,ord) JOIN pg_catalog.pg_attribute a ON a.attrelid=c.oid AND a.attnum=k.num WHERE p.contype='p' AND n.nspname='public' AND c.relname=ANY($1::text[]) GROUP BY c.relname`, tables)
	if err != nil {
		return ErrAssignmentUnavailable
	}
	found = map[string]string{}
	for rows.Next() {
		var name, key string
		if rows.Scan(&name, &key) != nil {
			rows.Close()
			return ErrAssignmentUnavailable
		}
		found[name] = key
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return ErrAssignmentUnavailable
	}
	for name, key := range assignmentKeys {
		if found[name] != key {
			return fmt.Errorf("%w: schema key %s", ErrAssignmentUnavailable, name)
		}
	}
	var enums bool
	err = tx.QueryRow(ctx, `SELECT (SELECT array_agg(e.enumlabel::text ORDER BY e.enumsortorder) FROM pg_catalog.pg_enum e JOIN pg_catalog.pg_type t ON t.oid=e.enumtypid JOIN pg_catalog.pg_namespace n ON n.oid=t.typnamespace WHERE n.nspname='public' AND t.typname='v1_payload_type')=ARRAY['TASK_INPUT','DAG_INPUT','TASK_OUTPUT','TASK_EVENT_DATA','USER_EVENT_INPUT','DURABLE_EVENT_LOG_ENTRY_DATA','DURABLE_EVENT_LOG_ENTRY_RESULT_DATA'] AND (SELECT array_agg(e.enumlabel::text ORDER BY e.enumsortorder) FROM pg_catalog.pg_enum e JOIN pg_catalog.pg_type t ON t.oid=e.enumtypid JOIN pg_catalog.pg_namespace n ON n.oid=t.typnamespace WHERE n.nspname='public' AND t.typname='v1_payload_location')=ARRAY['INLINE','EXTERNAL']`).Scan(&enums)
	if err != nil || !enums {
		return fmt.Errorf("%w: schema enums", ErrAssignmentUnavailable)
	}
	return nil
}

// Assignment combines untrusted per-call selectors with server-checked registration
// and local submission identity. It is never sent to the daemon or browser.
type Assignment struct {
	TenantID, EngineRunID, EngineTaskID                   string
	WorkflowID, VersionID, TaskName, ActionID, WorkerName string
	Retry, Invocation                                     int
	// Pre-ACK only. Empty LocalRunID means the persisted engine ID was checked.
	LocalRunID                              string
	SubmissionStartedAt, SubmissionDeadline time.Time
}
