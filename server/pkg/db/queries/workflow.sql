-- Every resource lookup and mutation carries workspace scope. Global scans are scheduler-only.
-- name: ListWorkflows :many
SELECT * FROM workflow WHERE workspace_id=$1 ORDER BY created_at DESC;
-- name: ListWorkflowsPage :many
SELECT * FROM workflow WHERE workspace_id=$1 ORDER BY created_at DESC,id DESC LIMIT $2 OFFSET $3;
-- name: GetWorkflowInWorkspace :one
SELECT * FROM workflow WHERE id=$1 AND workspace_id=$2;
-- name: LockWorkflowActionMembership :one
SELECT * FROM member WHERE workspace_id=$1 AND user_id=$2 FOR SHARE;
-- name: LockWorkflowActionRun :one
SELECT * FROM workflow_run WHERE id=$1 AND workspace_id=$2 FOR UPDATE;
-- name: LockWorkflowActionRuntimeConsent :one
-- After workspace/local authority, but before any agent/squad/Autopilot lock:
-- runtime teardown owns runtime then agent then Autopilot. FOR SHARE also
-- conflicts with the independent owner/visibility UPDATE until business commit.
SELECT * FROM agent_runtime WHERE id=$1 AND workspace_id=$2 FOR SHARE;
-- name: LockWorkflowActionCollaborators :many
SELECT * FROM workflow_collaborator WHERE workflow_id=$1 AND workspace_id=$2 ORDER BY user_id FOR SHARE;
-- name: LockWorkflowAgentInvocationTargets :many
SELECT * FROM agent_invocation_target WHERE agent_id=$1 ORDER BY id FOR SHARE;
-- name: LockWorkflowAutopilotCollaborators :many
SELECT * FROM autopilot_collaborator WHERE autopilot_id=$1 ORDER BY user_id FOR SHARE;
-- name: LockWorkflowForUpdate :one
SELECT * FROM workflow WHERE id=$1 AND workspace_id=$2 FOR UPDATE;
-- name: CreateWorkflow :one
INSERT INTO workflow(workspace_id,project_id,name,slug,description,hatchet_name,default_agent_id,created_by_type,created_by_id,audit_actor_type,audit_actor_id)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) RETURNING *;
-- name: UpdateWorkflowCAS :one
UPDATE workflow SET name=$3,slug=$4,description=$5,project_id=$6,default_agent_id=$7,status=$8,updated_at=now()
WHERE id=$1 AND workspace_id=$2 AND updated_at=$9 RETURNING *;
-- name: DeleteWorkflow :exec
DELETE FROM workflow WHERE id=$1 AND workspace_id=$2;
-- name: CreateWorkflowRevision :one
INSERT INTO workflow_revision(workspace_id,workflow_id,version,definition,definition_hash,definition_graph,hatchet_name,created_by_id,audit_actor_type,audit_actor_id,expected_current_version)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) RETURNING *;
-- name: GetWorkflowRevisionInWorkspace :one
SELECT * FROM workflow_revision WHERE id=$1 AND workspace_id=$2;
-- name: ListWorkflowRevisions :many
SELECT * FROM workflow_revision WHERE workflow_id=$1 AND workspace_id=$2 ORDER BY version DESC;
-- name: LatestWorkflowRevisionVersion :one
SELECT COALESCE(MAX(version),0)::integer FROM workflow_revision WHERE workflow_id=$1 AND workspace_id=$2;
-- name: ListWorkflowRevisionsPage :many
SELECT * FROM workflow_revision WHERE workflow_id=$1 AND workspace_id=$2 ORDER BY version DESC LIMIT $3 OFFSET $4;
-- name: SetWorkflowRevisionValidation :one
UPDATE workflow_revision SET validation_status=$3,validation_error=$4 WHERE id=$1 AND workspace_id=$2 RETURNING *;
-- name: PublishWorkflowRevisionCAS :one
UPDATE workflow w SET current_revision_id=r.id,definition_version=r.version,updated_at=now()
FROM workflow_revision r
WHERE w.id=$1 AND w.workspace_id=$2 AND w.definition_version=$3
AND r.id=$4 AND r.workspace_id=w.workspace_id AND r.workflow_id=w.id AND r.validation_status='valid'
AND r.version>w.definition_version RETURNING w.*;
-- name: MarkWorkflowRevisionPublished :exec
UPDATE workflow_revision SET published_at=now() WHERE id=$1 AND workspace_id=$2 AND validation_status='valid';
-- name: ListWorkflowTriggers :many
SELECT * FROM workflow_trigger WHERE workflow_id=$1 AND workspace_id=$2 ORDER BY created_at;
-- name: GetWorkflowTriggerInWorkspace :one
SELECT * FROM workflow_trigger WHERE id=$1 AND workspace_id=$2;
-- name: GetWorkflowTriggerByTokenHash :one
SELECT * FROM workflow_trigger WHERE token_hash=$1 AND workspace_id=$2 AND kind='api' AND enabled;
-- name: ResolveWorkflowIngressToken :one
SELECT * FROM workflow_trigger WHERE token_hash=$1 AND kind='api' AND enabled;
-- name: CreateWorkflowTrigger :one
INSERT INTO workflow_trigger(workspace_id,workflow_id,kind,principal_user_id,enabled,cron_expression,timezone,next_run_at,input_template,label,token_hash,created_by_type,created_by_id)
VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13) RETURNING *;
-- name: UpdateWorkflowTrigger :one
UPDATE workflow_trigger SET enabled=$3,cron_expression=$4,timezone=$5,next_run_at=$6,input_template=$7,label=$8,updated_at=now()
WHERE id=$1 AND workspace_id=$2 RETURNING *;
-- name: RotateWorkflowTriggerToken :one
UPDATE workflow_trigger SET token_hash=$3,updated_at=now() WHERE id=$1 AND workspace_id=$2 AND kind='api' RETURNING *;
-- name: DeleteWorkflowTrigger :exec
DELETE FROM workflow_trigger WHERE id=$1 AND workspace_id=$2;
-- name: LockDueWorkflowTriggers :many
SELECT t.* FROM workflow_trigger t JOIN workflow w ON w.id=t.workflow_id AND w.workspace_id=t.workspace_id
JOIN workflow_revision r ON r.id=w.current_revision_id AND r.workspace_id=w.workspace_id AND r.workflow_id=w.id
AND r.validation_status='valid' AND r.published_at IS NOT NULL
WHERE t.workspace_id=$1 AND t.enabled AND t.kind='schedule' AND t.next_run_at <= $2 AND w.status='active'
ORDER BY t.next_run_at LIMIT $3 FOR UPDATE OF t SKIP LOCKED;
-- name: AdvanceWorkflowTriggerCAS :one
UPDATE workflow_trigger SET next_run_at=$3,last_fired_at=$4,updated_at=now()
WHERE id=$1 AND workspace_id=$2 AND next_run_at=$5 RETURNING *;
-- name: CreateWorkflowRun :one
INSERT INTO workflow_run(id,workspace_id,workflow_id,revision_id,definition_hash,principal_user_id,trigger_id,source,planned_at,idempotency_key,input,status,reason_code,originator_user_id,accountable_user_id,originator_source)
VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$1,$10,$11,$12,$6,$6,$13)
RETURNING *;
-- name: GetWorkflowRunInWorkspace :one
SELECT * FROM workflow_run WHERE id=$1 AND workspace_id=$2;
-- name: GetWorkflowRunByHatchetID :one
SELECT * FROM workflow_run WHERE hatchet_run_id=$1 AND workspace_id=$2;
-- name: GetScheduledWorkflowRun :one
SELECT * FROM workflow_run WHERE trigger_id=$1 AND workspace_id=$2 AND planned_at=$3 AND source='schedule';
-- name: ListWorkflowRuns :many
SELECT * FROM workflow_run WHERE workflow_id=$1 AND workspace_id=$2 ORDER BY triggered_at DESC LIMIT $3 OFFSET $4;
-- name: ListWorkflowRunsToReconcile :many
SELECT * FROM workflow_run WHERE workspace_id=$1 AND status IN ('queued','submitting','running') ORDER BY triggered_at LIMIT $2;
-- name: ClaimWorkflowRunsToReconcile :many
WITH candidates AS (
 SELECT wr.id FROM workflow_run wr WHERE wr.workspace_id=$1 AND wr.status IN ('queued','submitting','running')
 AND wr.last_reconciled_at < $3
 ORDER BY wr.last_reconciled_at,wr.id LIMIT $2 FOR UPDATE OF wr SKIP LOCKED
)
UPDATE workflow_run r SET last_reconciled_at=GREATEST(clock_timestamp(),r.last_reconciled_at+interval '1 microsecond')
FROM candidates c WHERE r.id=c.id AND r.workspace_id=$1 RETURNING r.*;
-- name: SkipDepartedUnsubmittedWorkflowRun :one
UPDATE workflow_run r SET status='skipped',reason_code='workflow_principal_departed',finished_at=now()
WHERE r.id=$1 AND r.workspace_id=$2 AND
 ((r.status='queued' AND r.submission_started_at IS NULL) OR
  (r.status='submitting' AND r.submission_attempts=0 AND r.hatchet_run_id IS NULL))
AND NOT EXISTS (SELECT 1 FROM member m WHERE m.workspace_id=r.workspace_id AND m.user_id=r.principal_user_id)
RETURNING r.*;
-- name: BeginWorkflowRunSubmission :one
UPDATE workflow_run r SET status='submitting',submission_started_at=$3,submission_deadline=$4
WHERE r.id=$1 AND r.workspace_id=$2 AND r.status='queued' AND r.submission_started_at IS NULL
AND r.cancel_requested_at IS NULL
AND EXISTS (SELECT 1 FROM member m WHERE m.workspace_id=r.workspace_id AND m.user_id=r.principal_user_id)
AND $3::timestamptz IS NOT NULL AND $4::timestamptz IS NOT NULL AND $4::timestamptz>$3::timestamptz RETURNING *;
-- name: ClaimWorkflowRunSubmissionAttempt :one
UPDATE workflow_run r SET submission_attempts=submission_attempts+1,submission_lease_until=now()+interval '45 seconds'
WHERE r.id=$1 AND r.workspace_id=$2 AND r.status='submitting' AND r.hatchet_run_id IS NULL AND r.cancel_requested_at IS NULL
AND r.submission_deadline>now()+interval '30 seconds' AND r.submission_attempts<5
AND (r.submission_lease_until IS NULL OR r.submission_lease_until<=now())
AND EXISTS (SELECT 1 FROM member m WHERE m.workspace_id=r.workspace_id AND m.user_id=r.principal_user_id) RETURNING *;
-- name: CancelUnsubmittedWorkflowRun :one
UPDATE workflow_run SET status='cancelled',finished_at=now()
WHERE id=$1 AND workspace_id=$2 AND status='queued' AND submission_started_at IS NULL AND cancel_requested_at IS NOT NULL RETURNING *;
-- name: ProjectWorkflowRun :one
UPDATE workflow_run SET log_projection=$3,step_projection=$4
WHERE id=$1 AND workspace_id=$2 AND status='running' AND pending_terminal_status IS NULL
AND last_reconciled_at=sqlc.arg('last_reconciled_at')::timestamptz RETURNING *;
-- name: ProjectWorkflowSteps :one
UPDATE workflow_run SET step_projection=$3
WHERE id=$1 AND workspace_id=$2 AND status='running'
AND last_reconciled_at=sqlc.arg('last_reconciled_at')::timestamptz
AND (pending_terminal_status IS NULL OR pending_terminal_status=sqlc.arg('observed_terminal_status')::text) RETURNING *;
-- name: StageWorkflowTerminalProjection :one
UPDATE workflow_run SET pending_terminal_status=COALESCE(pending_terminal_status,sqlc.arg('terminal_status')::text),
 pending_terminal_output=COALESCE(pending_terminal_output,sqlc.arg('terminal_output')::jsonb),
 final_projection_attempts=final_projection_attempts+1
WHERE id=$1 AND workspace_id=$2 AND status='running'
AND last_reconciled_at=sqlc.arg('last_reconciled_at')::timestamptz
AND sqlc.arg('terminal_status')::text IN ('completed','failed','cancelled')
AND (pending_terminal_status IS NULL OR pending_terminal_status=sqlc.arg('terminal_status')::text) RETURNING *;
-- name: AdvanceWorkflowTerminalProjectionRetry :one
UPDATE workflow_run SET final_projection_attempts=final_projection_attempts+1
WHERE id=$1 AND workspace_id=$2 AND status='running' AND pending_terminal_status IS NOT NULL
AND last_reconciled_at=sqlc.arg('last_reconciled_at')::timestamptz RETURNING *;
-- name: FinishWorkflowRunWithCompleteProjection :one
UPDATE workflow_run SET status=sqlc.arg('terminal_status')::text,
 output=COALESCE(pending_terminal_output,sqlc.arg('terminal_output')::jsonb),
 log_projection=sqlc.arg('log_projection')::jsonb,step_projection=sqlc.arg('step_projection')::jsonb,
 reason_code=NULL,finished_at=now()
WHERE id=$1 AND workspace_id=$2 AND status='running'
AND last_reconciled_at=sqlc.arg('last_reconciled_at')::timestamptz
AND sqlc.arg('terminal_status')::text IN ('completed','failed','cancelled')
AND (pending_terminal_status IS NULL OR pending_terminal_status=sqlc.arg('terminal_status')::text) RETURNING *;
-- name: FinishWorkflowRunWithIncompleteProjection :one
UPDATE workflow_run SET status=pending_terminal_status,output=pending_terminal_output,
 reason_code='workflow_final_logs_incomplete',finished_at=now()
WHERE id=$1 AND workspace_id=$2 AND status='running' AND pending_terminal_status IS NOT NULL
AND final_projection_attempts>=sqlc.arg('min_attempts')::int
AND last_reconciled_at=sqlc.arg('last_reconciled_at')::timestamptz RETURNING *;
-- name: ListWorkflowRuntimeWorkspaces :many
SELECT DISTINCT workspace_id FROM workflow_run WHERE status IN ('queued','submitting','running')
UNION SELECT DISTINCT workspace_id FROM workflow_trigger WHERE enabled AND kind='schedule';
-- name: WorkflowDatabaseTime :one
SELECT now()::timestamptz;
-- name: AcknowledgeWorkflowRun :one
UPDATE workflow_run SET status='running',hatchet_run_id=$3,started_at=now()
WHERE id=$1 AND workspace_id=$2 AND status='submitting' RETURNING *;
-- name: BindWorkflowActionEngineRun :one
UPDATE workflow_run SET status='running',hatchet_run_id=$3,started_at=now()
WHERE id=$1 AND workspace_id=$2 AND status='submitting' AND hatchet_run_id IS NULL
AND cancel_requested_at IS NULL AND submission_attempts>0
AND submission_started_at IS NOT NULL AND submission_deadline>clock_timestamp()
AND submission_deadline <= submission_started_at + interval '55 minutes'
RETURNING *;
-- name: AcknowledgeWorkflowSubmissionAttempt :one
UPDATE workflow_run SET status='running',hatchet_run_id=$3,started_at=now()
WHERE id=$1 AND workspace_id=$2 AND status='submitting' AND submission_attempts=$4 AND submission_lease_until>now() RETURNING *;
-- name: FinishWorkflowRunCAS :one
UPDATE workflow_run SET status=$3,output=$4,reason_code=$5,failure_reason=$6,finished_at=now()
WHERE id=$1 AND workspace_id=$2 AND status=$7 AND status IN ('queued','submitting','running')
AND $3::text IN ('completed','failed','cancelled','skipped') RETURNING *;
-- name: RequestWorkflowRunCancellation :one
UPDATE workflow_run SET cancel_requested_at=now(),cancel_requested_by_id=$3
WHERE id=$1 AND workspace_id=$2 AND status IN ('queued','submitting','running') AND cancel_requested_at IS NULL RETURNING *;
-- name: CreateWorkflowRunnerToken :one
INSERT INTO workflow_runner_token(workspace_id,daemon_id,token_hash,label,created_by_id,expires_at)
VALUES($1,$2,$3,$4,$5,$6) RETURNING *;
-- name: GetActiveWorkflowRunnerToken :one
SELECT * FROM workflow_runner_token WHERE token_hash=$1 AND workspace_id=$2 AND daemon_id=$3 AND revoked_at IS NULL AND expires_at>now();
-- name: RevokeWorkflowRunnerTokens :exec
UPDATE workflow_runner_token SET revoked_at=now() WHERE workspace_id=$1 AND daemon_id=$2 AND revoked_at IS NULL;
-- name: TouchWorkflowRunnerToken :exec
UPDATE workflow_runner_token SET last_used_at=now() WHERE id=$1 AND workspace_id=$2 AND revoked_at IS NULL AND expires_at>now();
-- name: ListWorkflowCollaborators :many
SELECT * FROM workflow_collaborator WHERE workflow_id=$1 AND workspace_id=$2;
-- name: AddWorkflowCollaborator :one
INSERT INTO workflow_collaborator(workspace_id,workflow_id,user_id,role,created_by_id) VALUES($1,$2,$3,$4,$5)
ON CONFLICT(workspace_id,workflow_id,user_id) DO UPDATE SET role=EXCLUDED.role RETURNING *;
-- name: DeleteWorkflowCollaborator :exec
DELETE FROM workflow_collaborator WHERE workflow_id=$1 AND workspace_id=$2 AND user_id=$3;
-- name: CreateWorkflowHatchetTenant :one
INSERT INTO workflow_hatchet_tenant(workspace_id,tenant_id,token_encrypted) VALUES($1,$2,$3)
ON CONFLICT(workspace_id) DO NOTHING RETURNING *;
-- name: GetWorkflowHatchetTenant :one
SELECT * FROM workflow_hatchet_tenant WHERE workspace_id=$1;
-- name: QueueWorkflowHatchetTenantDeletion :exec
UPDATE workflow_hatchet_tenant SET deletion_state='pending',deletion_requested_at=now(),next_deletion_attempt_at=now(),updated_at=now()
WHERE workspace_id=$1 AND deletion_state='active';
-- name: ClaimWorkflowHatchetTenantDeletion :one
UPDATE workflow_hatchet_tenant SET deletion_state='deleting',deletion_attempts=deletion_attempts+1,next_deletion_attempt_at=$2,updated_at=now()
WHERE workspace_id=$1 AND deletion_state IN ('pending','failed','deleting') AND next_deletion_attempt_at<=now() RETURNING *;
-- name: ListWorkflowHatchetTenantDeletions :many
SELECT * FROM workflow_hatchet_tenant WHERE deletion_state IN ('pending','failed','deleting') AND next_deletion_attempt_at<=now() ORDER BY next_deletion_attempt_at LIMIT $1;
-- name: FinishWorkflowHatchetTenantDeletion :one
UPDATE workflow_hatchet_tenant SET deletion_state=$2,deletion_error=$3,next_deletion_attempt_at=$4,
deleted_at=CASE WHEN $2::text='deleted' THEN now() ELSE NULL END,updated_at=now()
WHERE workspace_id=$1 AND deletion_state='deleting' AND deletion_attempts=sqlc.arg('expected_attempt')
AND next_deletion_attempt_at>now() AND $2::text IN ('failed','deleted')
AND ($2::text='deleted' OR $4::timestamptz IS NOT NULL) RETURNING *;
-- name: CreateWorkflowOperation :one
INSERT INTO workflow_operation(workspace_id,run_id,step_id,operation_key,operation_name,payload_hash,attempt)
VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT(workspace_id,run_id,step_id,operation_key) DO NOTHING RETURNING *;
-- name: LockWorkflowOperation :one
SELECT * FROM workflow_operation WHERE workspace_id=$1 AND run_id=$2 AND step_id=$3 AND operation_key=$4 FOR UPDATE;
-- name: GetWorkflowOperation :one
SELECT * FROM workflow_operation WHERE workspace_id=$1 AND run_id=$2 AND step_id=$3 AND operation_key=$4;
-- name: CompleteWorkflowOperation :one
UPDATE workflow_operation SET status='completed',result=$5,completed_at=now()
WHERE workspace_id=$1 AND run_id=$2 AND step_id=$3 AND operation_key=$4 AND payload_hash=$6 AND status='pending' RETURNING *;
-- name: FailWorkflowOperation :one
UPDATE workflow_operation SET status='failed',error_code=$5,completed_at=now()
WHERE workspace_id=$1 AND run_id=$2 AND step_id=$3 AND operation_key=$4 AND payload_hash=$6 AND status='pending' RETURNING *;
