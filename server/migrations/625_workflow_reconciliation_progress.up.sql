ALTER TABLE workflow_run ADD COLUMN IF NOT EXISTS last_reconciled_at TIMESTAMPTZ NOT NULL DEFAULT now();
COMMENT ON COLUMN workflow_run.last_reconciled_at IS 'Durable fair scan marker advanced atomically before engine requests; oldest attempted active rows receive the next bounded turn.';
