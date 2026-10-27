ALTER TABLE workflow_run DROP COLUMN IF EXISTS pending_terminal_status,
 DROP COLUMN IF EXISTS pending_terminal_output,
 DROP COLUMN IF EXISTS final_projection_attempts;
