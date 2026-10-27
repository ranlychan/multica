-- Serves "which tasks did this run dispatch" from the run detail view, and the
-- run's status sync when a task finishes. Partial, because the overwhelming
-- majority of tasks are not workflow-dispatched and should not be indexed.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_agent_task_queue_workflow_run
    ON agent_task_queue (workflow_run_id)
    WHERE workflow_run_id IS NOT NULL;
