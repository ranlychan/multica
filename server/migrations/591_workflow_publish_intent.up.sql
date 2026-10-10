ALTER TABLE workflow ADD COLUMN IF NOT EXISTS audit_actor_type TEXT, ADD COLUMN IF NOT EXISTS audit_actor_id UUID;
ALTER TABLE workflow_revision ADD COLUMN IF NOT EXISTS audit_actor_type TEXT, ADD COLUMN IF NOT EXISTS audit_actor_id UUID,
 ADD COLUMN IF NOT EXISTS expected_current_version INTEGER CHECK (expected_current_version >= 0);
COMMENT ON COLUMN workflow_revision.expected_current_version IS 'Original publish CAS, retained for deferred trusted validation across restarts.';
CREATE FUNCTION workflow_authority_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF ROW(NEW.created_by_type,NEW.created_by_id,NEW.audit_actor_type,NEW.audit_actor_id)
 IS DISTINCT FROM ROW(OLD.created_by_type,OLD.created_by_id,OLD.audit_actor_type,OLD.audit_actor_id) THEN
  RAISE EXCEPTION 'workflow creator and audit identity are immutable';
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER workflow_authority_immutable BEFORE UPDATE ON workflow FOR EACH ROW EXECUTE FUNCTION workflow_authority_guard();
CREATE FUNCTION workflow_publish_intent_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF ROW(NEW.audit_actor_type,NEW.audit_actor_id,NEW.expected_current_version)
 IS DISTINCT FROM ROW(OLD.audit_actor_type,OLD.audit_actor_id,OLD.expected_current_version) THEN
  RAISE EXCEPTION 'workflow revision audit and original publish CAS are immutable';
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER workflow_publish_intent_immutable BEFORE UPDATE ON workflow_revision FOR EACH ROW EXECUTE FUNCTION workflow_publish_intent_guard();
