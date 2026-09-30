-- Link agent tasks to the workflow run that dispatched them, the same way
-- migration 042 linked them to autopilot_run_id. A workflow step that calls an
-- agent is dispatched through the ordinary task queue, so this column is what
-- lets the run detail view show which agent run a step produced, and lets the
-- run's terminal status be derived from the task's.
--
-- No foreign key: workflow_run and agent_task_queue deletion are
-- application-owned, and the task outlives the workflow if the workflow is
-- deleted while a run is in flight.
ALTER TABLE agent_task_queue ADD COLUMN IF NOT EXISTS workflow_run_id UUID;
