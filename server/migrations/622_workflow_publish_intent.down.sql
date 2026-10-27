DROP TRIGGER IF EXISTS workflow_publish_intent_immutable ON workflow_revision;
DROP FUNCTION IF EXISTS workflow_publish_intent_guard();
DROP TRIGGER IF EXISTS workflow_authority_immutable ON workflow;
DROP FUNCTION IF EXISTS workflow_authority_guard();
ALTER TABLE workflow_revision DROP COLUMN IF EXISTS expected_current_version, DROP COLUMN IF EXISTS audit_actor_type, DROP COLUMN IF EXISTS audit_actor_id;
ALTER TABLE workflow DROP COLUMN IF EXISTS audit_actor_type, DROP COLUMN IF EXISTS audit_actor_id;
