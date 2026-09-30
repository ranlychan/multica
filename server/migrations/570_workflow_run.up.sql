CREATE TABLE IF NOT EXISTS workflow_run (
 id UUID PRIMARY KEY DEFAULT gen_random_uuid(), workspace_id UUID NOT NULL, workflow_id UUID NOT NULL,
 revision_id UUID NOT NULL, definition_hash TEXT NOT NULL, principal_user_id UUID,
 trigger_id UUID, source TEXT NOT NULL CHECK (source IN ('schedule','manual','api')),
 status TEXT NOT NULL DEFAULT 'queued' CHECK (status IN ('queued','submitting','running','completed','failed','cancelled','skipped')),
 planned_at TIMESTAMPTZ, idempotency_key UUID NOT NULL,
 submission_started_at TIMESTAMPTZ, submission_deadline TIMESTAMPTZ,
 hatchet_run_id UUID, input JSONB, output JSONB, issue_id UUID, task_id UUID,
 originator_user_id UUID, accountable_user_id UUID, originator_source TEXT,
 reason_code TEXT, failure_reason TEXT,
 triggered_at TIMESTAMPTZ NOT NULL DEFAULT now(), started_at TIMESTAMPTZ, finished_at TIMESTAMPTZ,
 duration_ms BIGINT, created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 CHECK (idempotency_key=id),
 CHECK (source <> 'schedule' OR (trigger_id IS NOT NULL AND planned_at IS NOT NULL)),
 CHECK ((submission_started_at IS NULL AND submission_deadline IS NULL) OR
        (submission_started_at IS NOT NULL AND submission_deadline IS NOT NULL AND submission_deadline > submission_started_at)),
 CHECK (status NOT IN ('submitting','running') OR (submission_started_at IS NOT NULL AND submission_deadline IS NOT NULL)),
 CHECK (status <> 'skipped' OR reason_code IS NOT NULL)
);
CREATE FUNCTION workflow_run_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF OLD.status IN ('completed','failed','cancelled','skipped') AND NEW IS DISTINCT FROM OLD THEN
  RAISE EXCEPTION 'terminal workflow run is immutable';
 END IF;
 IF ROW(NEW.workspace_id,NEW.workflow_id,NEW.revision_id,NEW.definition_hash,NEW.principal_user_id,NEW.trigger_id,NEW.source,NEW.planned_at,NEW.idempotency_key,NEW.input)
 IS DISTINCT FROM ROW(OLD.workspace_id,OLD.workflow_id,OLD.revision_id,OLD.definition_hash,OLD.principal_user_id,OLD.trigger_id,OLD.source,OLD.planned_at,OLD.idempotency_key,OLD.input) THEN
 RAISE EXCEPTION 'workflow run admission snapshot is immutable';
 END IF;
 IF OLD.submission_started_at IS NOT NULL AND
 ROW(NEW.submission_started_at,NEW.submission_deadline) IS DISTINCT FROM ROW(OLD.submission_started_at,OLD.submission_deadline) THEN
 RAISE EXCEPTION 'submission retry window is immutable';
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER workflow_run_immutable BEFORE UPDATE ON workflow_run FOR EACH ROW EXECUTE FUNCTION workflow_run_guard();
