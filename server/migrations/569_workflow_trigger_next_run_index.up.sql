-- Serves the scheduler's due-trigger scan, which is the only reader of
-- next_run_at and runs on every tick. Partial on the same predicate the query
-- filters by, so disabled and non-schedule triggers never enter the index —
-- the same shape as idx_autopilot_trigger_next_run.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_workflow_trigger_next_run
    ON workflow_trigger (next_run_at)
    WHERE enabled = true AND kind = 'schedule';
