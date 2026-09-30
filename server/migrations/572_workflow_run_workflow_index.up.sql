-- Serves the detail view's run list: every run of one workflow, newest first.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_workflow_run_workflow
    ON workflow_run (workflow_id, triggered_at DESC);
