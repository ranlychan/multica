CREATE TABLE IF NOT EXISTS workflow_collaborator (
 id UUID PRIMARY KEY DEFAULT gen_random_uuid(), workspace_id UUID NOT NULL, workflow_id UUID NOT NULL,
 user_id UUID NOT NULL, role TEXT NOT NULL CHECK (role IN ('viewer','editor')),
 created_by_id UUID NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
