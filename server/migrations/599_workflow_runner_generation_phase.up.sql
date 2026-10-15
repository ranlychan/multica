ALTER TABLE workflow_runner_token ADD COLUMN IF NOT EXISTS phase text NOT NULL DEFAULT 'current' CHECK (phase IN ('candidate','current','retiring'));
