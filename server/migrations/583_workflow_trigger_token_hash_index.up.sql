CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS idx_workflow_trigger_token_hash ON workflow_trigger(token_hash) WHERE token_hash IS NOT NULL;
