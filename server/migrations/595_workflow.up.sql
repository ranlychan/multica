CREATE TABLE IF NOT EXISTS workflow (
 id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
 workspace_id UUID NOT NULL, project_id UUID,
 name TEXT NOT NULL, slug TEXT NOT NULL, description TEXT,
 hatchet_name TEXT NOT NULL DEFAULT '',
 current_revision_id UUID, definition_version INTEGER NOT NULL DEFAULT 0 CHECK (definition_version >= 0),
 status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active','paused','archived')),
 default_agent_id UUID,
 created_by_type TEXT NOT NULL CHECK (created_by_type IN ('member','agent')),
 created_by_id UUID NOT NULL, last_run_at TIMESTAMPTZ,
 created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
