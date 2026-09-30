CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_workflow_run_active ON workflow_run (workspace_id, triggered_at) WHERE status IN ('queued','submitting','running');
