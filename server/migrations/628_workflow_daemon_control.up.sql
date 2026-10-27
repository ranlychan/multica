CREATE TABLE IF NOT EXISTS workflow_daemon (
 workspace_id uuid NOT NULL,
 daemon_id text NOT NULL,
 owner_id uuid NOT NULL,
 control_hash text NOT NULL,
 control_expires_at timestamptz NOT NULL,
 capable boolean NOT NULL DEFAULT false,
 runner_version text NOT NULL,
 node_version text NOT NULL,
 PRIMARY KEY(workspace_id,daemon_id)
);
ALTER TABLE workflow_runner_token ADD COLUMN IF NOT EXISTS control_hash text NOT NULL DEFAULT '';
ALTER TABLE workflow_runner_token ADD COLUMN IF NOT EXISTS tenant_id text NOT NULL DEFAULT '';
ALTER TABLE workflow_runner_token ADD COLUMN IF NOT EXISTS registrations jsonb NOT NULL DEFAULT '{}';
ALTER TABLE workflow_runner_token ADD COLUMN IF NOT EXISTS lease_until timestamptz;
