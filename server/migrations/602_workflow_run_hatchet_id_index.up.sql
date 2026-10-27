-- The reconciler's upsert key: it reads the engine's run state by hatchet_run_id
-- and merges it into the local row. Unique, so a retry or a duplicated
-- reconciliation cannot fork one engine run into two local rows. Partial,
-- because a run that never reached the engine legitimately has no engine id and
-- many rows may be NULL.
CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS idx_workflow_run_hatchet_id
    ON workflow_run (hatchet_run_id)
    WHERE hatchet_run_id IS NOT NULL;
