ALTER TABLE workflow_runner_token ADD COLUMN IF NOT EXISTS engine_issuance_unknown BOOLEAN NOT NULL DEFAULT TRUE;
