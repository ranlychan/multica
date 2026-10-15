CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS idx_workflow_runner_generation_phase ON workflow_runner_token (workspace_id, daemon_id, phase) WHERE revoked_at IS NULL;
