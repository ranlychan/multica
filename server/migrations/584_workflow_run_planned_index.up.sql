CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS idx_workflow_run_planned ON workflow_run(workspace_id,trigger_id,planned_at) WHERE source='schedule';
