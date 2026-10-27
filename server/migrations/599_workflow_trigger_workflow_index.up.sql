-- Serves the detail view's trigger list and the delete-workflow cleanup, both
-- of which read every trigger of one workflow.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_workflow_trigger_workflow
    ON workflow_trigger (workflow_id);
