ALTER TABLE autopilot_run ADD COLUMN IF NOT EXISTS workflow_run_id UUID;
ALTER TABLE autopilot_run ADD COLUMN IF NOT EXISTS principal_user_id UUID;
