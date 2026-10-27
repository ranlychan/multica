CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS idx_workflow_runner_token_workspace_daemon ON workflow_runner_token (workspace_id, daemon_id) WHERE revoked_at IS NULL;
