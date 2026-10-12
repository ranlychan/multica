ALTER TABLE workflow_run ADD COLUMN IF NOT EXISTS submission_attempts INTEGER NOT NULL DEFAULT 0 CHECK (submission_attempts >= 0),
 ADD COLUMN IF NOT EXISTS submission_lease_until TIMESTAMPTZ,
 ADD COLUMN IF NOT EXISTS log_projection JSONB NOT NULL DEFAULT '[]',
 ADD COLUMN IF NOT EXISTS step_projection JSONB NOT NULL DEFAULT '[]';
