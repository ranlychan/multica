CREATE TABLE IF NOT EXISTS workflow_trigger (
 id UUID PRIMARY KEY DEFAULT gen_random_uuid(), workspace_id UUID NOT NULL, workflow_id UUID NOT NULL,
 kind TEXT NOT NULL CHECK (kind IN ('schedule','api')),
 principal_user_id UUID NOT NULL,
 enabled BOOLEAN NOT NULL DEFAULT true, cron_expression TEXT, timezone TEXT NOT NULL DEFAULT 'UTC',
 next_run_at TIMESTAMPTZ, input_template JSONB, label TEXT, last_fired_at TIMESTAMPTZ,
 token_hash TEXT,
 created_by_type TEXT NOT NULL CHECK (created_by_type IN ('member','agent')), created_by_id UUID NOT NULL,
 created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 CHECK ((kind='api' AND token_hash IS NOT NULL) OR (kind='schedule' AND token_hash IS NULL))
);
CREATE FUNCTION workflow_trigger_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NEW.principal_user_id IS DISTINCT FROM OLD.principal_user_id OR
    NEW.created_by_type IS DISTINCT FROM OLD.created_by_type OR NEW.created_by_id IS DISTINCT FROM OLD.created_by_id THEN
  RAISE EXCEPTION 'workflow trigger principal and creator are immutable';
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER workflow_trigger_immutable BEFORE UPDATE ON workflow_trigger FOR EACH ROW EXECUTE FUNCTION workflow_trigger_guard();
COMMENT ON COLUMN workflow_trigger.principal_user_id IS 'Immutable human authority, separate from the member or agent audit actor; membership is revalidated at dispatch.';
