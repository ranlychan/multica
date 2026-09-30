CREATE TABLE IF NOT EXISTS workflow_revision (
 id UUID PRIMARY KEY DEFAULT gen_random_uuid(), workspace_id UUID NOT NULL, workflow_id UUID NOT NULL,
 version INTEGER NOT NULL CHECK (version > 0), definition TEXT NOT NULL,
 definition_language TEXT NOT NULL DEFAULT 'typescript' CHECK (definition_language='typescript'),
 definition_hash TEXT NOT NULL, definition_graph JSONB NOT NULL, hatchet_name TEXT NOT NULL,
 validation_status TEXT NOT NULL DEFAULT 'pending' CHECK (validation_status IN ('pending','valid','invalid')),
 validation_error TEXT, published_at TIMESTAMPTZ, created_by_id UUID NOT NULL,
 created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE FUNCTION workflow_revision_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP='INSERT' THEN
  -- Serializes revision allocation for one workflow, including competing publishers.
  PERFORM id FROM workflow WHERE id=NEW.workflow_id AND workspace_id=NEW.workspace_id FOR UPDATE;
  IF EXISTS (SELECT 1 FROM workflow_revision WHERE workflow_id=NEW.workflow_id AND workspace_id=NEW.workspace_id AND version>=NEW.version) THEN
   RAISE EXCEPTION 'workflow revision version must increase';
  END IF;
 ELSE
  IF ROW(NEW.id,NEW.workspace_id,NEW.workflow_id,NEW.version,NEW.definition,NEW.definition_language,NEW.definition_hash,NEW.definition_graph,NEW.hatchet_name,NEW.created_by_id,NEW.created_at)
  IS DISTINCT FROM ROW(OLD.id,OLD.workspace_id,OLD.workflow_id,OLD.version,OLD.definition,OLD.definition_language,OLD.definition_hash,OLD.definition_graph,OLD.hatchet_name,OLD.created_by_id,OLD.created_at) THEN
   RAISE EXCEPTION 'workflow revision source and identity are immutable';
  END IF;
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER workflow_revision_immutable BEFORE INSERT OR UPDATE ON workflow_revision FOR EACH ROW EXECUTE FUNCTION workflow_revision_guard();
