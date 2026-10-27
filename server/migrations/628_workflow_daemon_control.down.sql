ALTER TABLE workflow_runner_token DROP COLUMN IF EXISTS lease_until;
ALTER TABLE workflow_runner_token DROP COLUMN IF EXISTS registrations;
ALTER TABLE workflow_runner_token DROP COLUMN IF EXISTS tenant_id;
ALTER TABLE workflow_runner_token DROP COLUMN IF EXISTS control_hash;
DROP TABLE IF EXISTS workflow_daemon;
