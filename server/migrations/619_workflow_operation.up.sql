CREATE TABLE IF NOT EXISTS workflow_operation (
 id UUID PRIMARY KEY DEFAULT gen_random_uuid(), workspace_id UUID NOT NULL, run_id UUID NOT NULL,
 step_id TEXT NOT NULL, operation_key TEXT NOT NULL, operation_name TEXT NOT NULL DEFAULT '',
 payload_hash TEXT NOT NULL, attempt INTEGER NOT NULL CHECK (attempt > 0),
 status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending','completed','failed')),
 result JSONB, error_code TEXT, created_at TIMESTAMPTZ NOT NULL DEFAULT now(), completed_at TIMESTAMPTZ,
 CHECK (status <> 'completed' OR result IS NOT NULL)
);
CREATE FUNCTION workflow_operation_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF ROW(NEW.workspace_id,NEW.run_id,NEW.step_id,NEW.operation_key,NEW.operation_name,NEW.payload_hash)
 IS DISTINCT FROM ROW(OLD.workspace_id,OLD.run_id,OLD.step_id,OLD.operation_key,OLD.operation_name,OLD.payload_hash) THEN
  RAISE EXCEPTION 'workflow operation identity and payload are immutable';
 END IF;
 IF OLD.status='completed' AND NEW IS DISTINCT FROM OLD THEN
  RAISE EXCEPTION 'completed workflow operation receipt is immutable';
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER workflow_operation_immutable BEFORE UPDATE ON workflow_operation FOR EACH ROW EXECUTE FUNCTION workflow_operation_guard();
