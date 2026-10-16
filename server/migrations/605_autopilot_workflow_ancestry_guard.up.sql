ALTER TABLE autopilot_run ADD CONSTRAINT autopilot_workflow_principal_pair
CHECK ((workflow_run_id IS NULL) = (principal_user_id IS NULL));
CREATE FUNCTION autopilot_run_workflow_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF ROW(NEW.workflow_run_id,NEW.principal_user_id) IS DISTINCT FROM
    ROW(OLD.workflow_run_id,OLD.principal_user_id) THEN
  RAISE EXCEPTION 'autopilot workflow ancestry is immutable';
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER autopilot_workflow_immutable BEFORE UPDATE ON autopilot_run
FOR EACH ROW EXECUTE FUNCTION autopilot_run_workflow_guard();
