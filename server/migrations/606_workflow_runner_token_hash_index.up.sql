-- Lookup on every authenticated workflow-runtime request, so it is the hot
-- path. Unique because a hash collision would mean two machines presenting the
-- same credential, which must never authenticate ambiguously.
CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS idx_workflow_runner_token_hash
    ON workflow_runner_token (token_hash);
