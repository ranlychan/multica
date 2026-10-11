ALTER TABLE workflow_run ADD COLUMN IF NOT EXISTS cancel_requested_at TIMESTAMPTZ,
 ADD COLUMN IF NOT EXISTS cancel_requested_by_id UUID;
ALTER TABLE workflow_run ADD CONSTRAINT workflow_cancel_request_pair CHECK
 ((cancel_requested_at IS NULL) = (cancel_requested_by_id IS NULL));
COMMENT ON COLUMN workflow_run.cancel_requested_at IS 'Durable cancellation outbox; retain active state until engine cancellation or terminal outcome is confirmed.';
CREATE FUNCTION workflow_cancel_request_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF OLD.cancel_requested_at IS NOT NULL AND
 ROW(NEW.cancel_requested_at,NEW.cancel_requested_by_id) IS DISTINCT FROM ROW(OLD.cancel_requested_at,OLD.cancel_requested_by_id) THEN
  RAISE EXCEPTION 'workflow cancellation request is immutable';
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER workflow_cancel_request_immutable BEFORE UPDATE ON workflow_run FOR EACH ROW EXECUTE FUNCTION workflow_cancel_request_guard();
