-- Retained after workspace deletion until asynchronous engine cleanup succeeds.
-- token_encrypted is base64 secretbox ciphertext from the existing application helper.
CREATE TABLE IF NOT EXISTS workflow_hatchet_tenant (
 workspace_id UUID PRIMARY KEY, tenant_id TEXT NOT NULL, token_encrypted TEXT NOT NULL,
 deletion_state TEXT NOT NULL DEFAULT 'active' CHECK (deletion_state IN ('active','pending','deleting','deleted','failed')),
 deletion_requested_at TIMESTAMPTZ, deletion_attempts INTEGER NOT NULL DEFAULT 0,
 next_deletion_attempt_at TIMESTAMPTZ, deletion_error TEXT, deleted_at TIMESTAMPTZ,
 created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
