-- Restore the pre-workflow allowed set. NOT VALID again, for the same lock
-- reason as the up migration: narrowing a constraint still has to be verified
-- against existing rows, and an issue already stamped 'workflow' would fail the
-- new check. Leaving the constraint NOT VALID keeps those rows readable and
-- makes the failure mode a rejected write rather than a broken table.
ALTER TABLE issue DROP CONSTRAINT IF EXISTS issue_origin_type_check;
ALTER TABLE issue ADD CONSTRAINT issue_origin_type_check
    CHECK (origin_type IN ('autopilot', 'quick_create', 'lark_chat', 'slack_chat', 'agent_create', 'dingtalk_chat', 'wecom_chat', 'telegram_chat'))
    NOT VALID;
