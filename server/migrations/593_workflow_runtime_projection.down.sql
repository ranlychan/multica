ALTER TABLE workflow_run DROP COLUMN IF EXISTS submission_attempts, DROP COLUMN IF EXISTS submission_lease_until,
 DROP COLUMN IF EXISTS log_projection, DROP COLUMN IF EXISTS step_projection;
