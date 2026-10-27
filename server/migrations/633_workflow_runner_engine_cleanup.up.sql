ALTER TABLE workflow_runner_token ADD COLUMN engine_token_id TEXT NOT NULL DEFAULT '', ADD COLUMN engine_revoked_at TIMESTAMPTZ;
