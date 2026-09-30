-- slug is the identifier a person or an agent types (`multica workflow get
-- nightly-triage`), so it must resolve to exactly one workflow within a
-- workspace. Unique per workspace, not globally: two workspaces may each have
-- a "nightly-triage".
CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS idx_workflow_workspace_slug
    ON workflow (workspace_id, slug);
