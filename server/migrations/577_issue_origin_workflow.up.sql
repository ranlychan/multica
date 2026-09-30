-- Extend issue.origin_type for issues created by a workflow step. origin_id
-- stores the workflow_run id, so the issue links back to the run that made it
-- and the run can show what it produced.
--
-- This only widens the allowed set. Recreate the CHECK as NOT VALID so the
-- ACCESS EXCLUSIVE lock is held briefly; migration 578 performs the table scan
-- under SHARE UPDATE EXCLUSIVE without blocking normal reads and writes — the
-- same split migrations 366/367 use.
ALTER TABLE issue DROP CONSTRAINT IF EXISTS issue_origin_type_check;
ALTER TABLE issue ADD CONSTRAINT issue_origin_type_check
    CHECK (origin_type IN ('autopilot', 'quick_create', 'lark_chat', 'slack_chat', 'agent_create', 'dingtalk_chat', 'wecom_chat', 'telegram_chat', 'workflow'))
    NOT VALID;
