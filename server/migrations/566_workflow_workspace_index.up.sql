-- Serves the list view: one workspace, newest first. The list is the tab's
-- landing surface and is read on every visit, so it should not sort the
-- workspace's whole workflow set per request.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_workflow_workspace_created
    ON workflow (workspace_id, created_at DESC);
