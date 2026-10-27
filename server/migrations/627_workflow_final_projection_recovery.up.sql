ALTER TABLE workflow_run ADD COLUMN IF NOT EXISTS pending_terminal_status TEXT CHECK (pending_terminal_status IS NULL OR pending_terminal_status IN ('completed','failed','cancelled')),
 ADD COLUMN IF NOT EXISTS pending_terminal_output JSONB,
 ADD COLUMN IF NOT EXISTS final_projection_attempts INTEGER NOT NULL DEFAULT 0 CHECK (final_projection_attempts >= 0);
