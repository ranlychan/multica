DROP TRIGGER IF EXISTS workflow_cancel_request_immutable ON workflow_run;
DROP FUNCTION IF EXISTS workflow_cancel_request_guard();
ALTER TABLE workflow_run DROP CONSTRAINT IF EXISTS workflow_cancel_request_pair,
 DROP COLUMN IF EXISTS cancel_requested_at, DROP COLUMN IF EXISTS cancel_requested_by_id;
