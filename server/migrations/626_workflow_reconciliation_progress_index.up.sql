CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_workflow_run_reconciliation_progress ON workflow_run(workspace_id,last_reconciled_at,id) WHERE status IN ('queued','submitting','running');
