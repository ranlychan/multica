DROP TRIGGER IF EXISTS autopilot_workflow_immutable ON autopilot_run;
DROP FUNCTION IF EXISTS autopilot_run_workflow_guard();
ALTER TABLE autopilot_run DROP CONSTRAINT IF EXISTS autopilot_workflow_principal_pair;
