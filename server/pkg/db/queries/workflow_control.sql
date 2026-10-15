-- name: EnrollWorkflowDaemon :one
INSERT INTO workflow_daemon(workspace_id,daemon_id,owner_id,control_hash,control_expires_at,capable,runner_version,node_version)
VALUES($1,$2,$3,$4,$5,$6,$7,$8)
ON CONFLICT(workspace_id,daemon_id) DO UPDATE SET control_hash=EXCLUDED.control_hash,control_expires_at=EXCLUDED.control_expires_at,
capable=EXCLUDED.capable,runner_version=EXCLUDED.runner_version,node_version=EXCLUDED.node_version
WHERE workflow_daemon.owner_id=EXCLUDED.owner_id
RETURNING *;
-- name: AuthorizeWorkflowControl :one
SELECT d.* FROM workflow_daemon d
JOIN workspace w ON w.id=d.workspace_id
JOIN member m ON m.workspace_id=d.workspace_id AND m.user_id=d.owner_id
WHERE d.control_hash=$1 AND d.workspace_id=$2 AND d.control_expires_at>now() AND d.capable;
-- name: LockWorkflowControl :one
SELECT d.* FROM workflow_daemon d
JOIN workspace w ON w.id=d.workspace_id
JOIN member m ON m.workspace_id=d.workspace_id AND m.user_id=d.owner_id
WHERE d.control_hash=$1 AND d.workspace_id=$2 AND d.control_expires_at>now() AND d.capable
FOR UPDATE OF d;
-- name: ListWorkflowRunnerRevisions :many
SELECT r.* FROM workflow_revision r JOIN workflow w ON w.id=r.workflow_id AND w.workspace_id=r.workspace_id
WHERE r.workspace_id=$1 AND (
 (w.status='active' AND (r.validation_status='pending' OR r.id=w.current_revision_id))
 OR EXISTS(SELECT 1 FROM workflow_run run WHERE run.workspace_id=r.workspace_id AND run.revision_id=r.id AND run.status IN ('queued','submitting','running'))
) ORDER BY r.workflow_id,r.version LIMIT 257;
-- name: CreateWorkflowRunnerGeneration :one
INSERT INTO workflow_runner_token(workspace_id,daemon_id,token_hash,created_by_id,expires_at,control_hash,tenant_id,phase)
VALUES($1,$2,$3,$4,$5,$6,$7,'candidate') RETURNING *;
-- name: AuthorizeWorkflowRunner :one
SELECT t.* FROM workflow_runner_token t
JOIN workflow_daemon d ON d.workspace_id=t.workspace_id AND d.daemon_id=t.daemon_id AND d.control_hash=t.control_hash
JOIN workspace w ON w.id=d.workspace_id
JOIN member m ON m.workspace_id=d.workspace_id AND m.user_id=d.owner_id
JOIN workflow_hatchet_tenant ht ON ht.workspace_id=t.workspace_id AND ht.tenant_id=t.tenant_id AND ht.deletion_state='active'
WHERE t.token_hash=$1 AND t.workspace_id=$2 AND t.id=$3 AND t.revoked_at IS NULL AND t.expires_at>now()
AND d.capable AND d.control_expires_at>now();
-- name: GetWorkflowRunnerGeneration :one
SELECT * FROM workflow_runner_token WHERE workspace_id=$1 AND daemon_id=$2 AND id=$3 AND control_hash=$4 AND revoked_at IS NULL AND expires_at>now();
-- name: LeaseWorkflowRunnerGeneration :execrows
UPDATE workflow_runner_token SET registrations=$5,lease_until=now()+interval '45 seconds',last_used_at=now()
WHERE workspace_id=$1 AND daemon_id=$2 AND id=$3 AND control_hash=$4 AND revoked_at IS NULL AND expires_at>now();
-- name: WorkflowRunnerEligible :one
SELECT EXISTS(SELECT 1 FROM workflow_runner_token t
JOIN workflow_daemon d ON d.workspace_id=t.workspace_id AND d.daemon_id=t.daemon_id AND d.control_hash=t.control_hash
JOIN member m ON m.workspace_id=d.workspace_id AND m.user_id=d.owner_id
JOIN workflow_hatchet_tenant ht ON ht.workspace_id=t.workspace_id AND ht.tenant_id=t.tenant_id AND ht.deletion_state='active'
WHERE t.workspace_id=$1 AND t.phase='current' AND t.registrations ? sqlc.arg(revision_id)::text AND t.lease_until>now() AND t.revoked_at IS NULL AND t.expires_at>now() AND d.capable AND d.control_expires_at>now());
-- name: RevokeWorkflowCandidateAndExpiredGenerations :exec
UPDATE workflow_runner_token SET revoked_at=now() WHERE workspace_id=$1 AND daemon_id=$2 AND revoked_at IS NULL AND (phase='candidate' OR expires_at<=now());
-- name: WorkflowRunnerRetirementPending :one
SELECT EXISTS(SELECT 1 FROM workflow_runner_token WHERE workspace_id=$1 AND daemon_id=$2 AND phase='retiring' AND revoked_at IS NULL AND expires_at>now());
-- name: DemoteWorkflowRunnerCurrent :exec
UPDATE workflow_runner_token SET phase='retiring' WHERE workspace_id=$1 AND daemon_id=$2 AND phase='current' AND revoked_at IS NULL;
-- name: PromoteWorkflowRunnerCandidate :execrows
UPDATE workflow_runner_token SET phase='current' WHERE workspace_id=$1 AND daemon_id=$2 AND id=$3 AND phase='candidate' AND revoked_at IS NULL AND expires_at>now();
-- name: RetireWorkflowRunnerGeneration :execrows
UPDATE workflow_runner_token SET revoked_at=now() WHERE workspace_id=$1 AND daemon_id=$2 AND id=$3 AND control_hash=$4 AND revoked_at IS NULL;
-- name: DeleteWorkflowDaemonsByWorkspace :exec
DELETE FROM workflow_daemon WHERE workspace_id=$1;
