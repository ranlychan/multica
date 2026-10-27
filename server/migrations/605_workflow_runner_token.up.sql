CREATE TABLE IF NOT EXISTS workflow_runner_token (
 id UUID PRIMARY KEY DEFAULT gen_random_uuid(), workspace_id UUID NOT NULL, daemon_id TEXT NOT NULL,
 token_hash TEXT NOT NULL, label TEXT, created_by_id UUID, last_used_at TIMESTAMPTZ,
 revoked_at TIMESTAMPTZ, expires_at TIMESTAMPTZ NOT NULL,
 created_at TIMESTAMPTZ NOT NULL DEFAULT now(), CHECK (expires_at > created_at)
);
