-- Every resource lookup and mutation carries workspace scope. Global scans are scheduler-only.
-- name: ListWorkflows :many
SELECT * FROM workflow WHERE workspace_id=$1 ORDER BY created_at DESC;
-- name: GetWorkflowInWorkspace :one
SELECT * FROM workflow WHERE id=$1 AND workspace_id=$2;
-- name: LockWorkflowForUpdate :one
SELECT * FROM workflow WHERE id=$1 AND workspace_id=$2 FOR UPDATE;
-- name: CreateWorkflow :one
INSERT INTO workflow(workspace_id,project_id,name,slug,description,hatchet_name,default_agent_id,created_by_type,created_by_id)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) RETURNING *;
-- name: UpdateWorkflowCAS :one
UPDATE workflow SET name=$3,slug=$4,description=$5,project_id=$6,default_agent_id=$7,status=$8,updated_at=now()
WHERE id=$1 AND workspace_id=$2 AND updated_at=$9 RETURNING *;
-- name: DeleteWorkflow :exec
DELETE FROM workflow WHERE id=$1 AND workspace_id=$2;
-- name: CreateWorkflowRevision :one
INSERT INTO workflow_revision(workspace_id,workflow_id,version,definition,definition_hash,definition_graph,hatchet_name,created_by_id)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8) RETURNING *;
-- name: GetWorkflowRevisionInWorkspace :one
SELECT * FROM workflow_revision WHERE id=$1 AND workspace_id=$2;
-- name: ListWorkflowRevisions :many
SELECT * FROM workflow_revision WHERE workflow_id=$1 AND workspace_id=$2 ORDER BY version DESC;
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
-- name: BeginWorkflowRunSubmission :one
UPDATE workflow_run SET status='submitting',submission_started_at=$3,submission_deadline=$4
WHERE id=$1 AND workspace_id=$2 AND status='queued' AND submission_started_at IS NULL RETURNING *;
-- name: AcknowledgeWorkflowRun :one
UPDATE workflow_run SET status='running',hatchet_run_id=$3,started_at=now()
WHERE id=$1 AND workspace_id=$2 AND status='submitting' RETURNING *;
-- name: FinishWorkflowRunCAS :one
UPDATE workflow_run SET status=$3,output=$4,reason_code=$5,failure_reason=$6,finished_at=now()
WHERE id=$1 AND workspace_id=$2 AND status=$7 RETURNING *;
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
-- name: FinishWorkflowHatchetTenantDeletion :exec
UPDATE workflow_hatchet_tenant SET deletion_state=$2,deletion_error=$3,next_deletion_attempt_at=$4,
deleted_at=CASE WHEN $2::text='deleted' THEN now() ELSE NULL END,updated_at=now()
WHERE workspace_id=$1 AND deletion_state='deleting';
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
-- Cleanup runs in the caller's workspace deletion transaction. Task rows intentionally survive.
-- name: DeleteWorkflowOperationsByWorkspace :exec
DELETE FROM workflow_operation WHERE workspace_id=$1;
-- name: DeleteWorkflowRunsByWorkspace :exec
DELETE FROM workflow_run WHERE workspace_id=$1;
-- name: DeleteWorkflowTriggersByWorkspace :exec
DELETE FROM workflow_trigger WHERE workspace_id=$1;
-- name: DeleteWorkflowCollaboratorsByWorkspace :exec
DELETE FROM workflow_collaborator WHERE workspace_id=$1;
-- name: DeleteWorkflowRevisionsByWorkspace :exec
DELETE FROM workflow_revision WHERE workspace_id=$1;
-- name: DeleteWorkflowRunnerTokensByWorkspace :exec
DELETE FROM workflow_runner_token WHERE workspace_id=$1;
-- name: DeleteWorkflowsByWorkspace :exec
DELETE FROM workflow WHERE workspace_id=$1;
