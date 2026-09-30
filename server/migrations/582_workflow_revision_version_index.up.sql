CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS idx_workflow_revision_version ON workflow_revision(workspace_id,workflow_id,version);
