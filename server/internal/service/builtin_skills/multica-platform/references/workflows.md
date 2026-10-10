# Workflows

Workflows are immutable TypeScript revisions with a static DAG and workspace-scoped triggers and run history. The application server stores source; only an eligible trusted daemon may validate, register, and execute it. Authored code runs with the daemon OS user's full permissions.

Use authenticated `/api/workflows` endpoints with `X-Workspace-ID`. List/detail/history are readable by workspace members even with the engine disabled. Creator, workspace owner/admin, and editor collaborators can author, delete and run. Viewer collaborators cannot write. Only creator/owner/admin can manage collaborators. Agent task requests spend the current human originator's grants and retain the agent audit identity separately; terminal or unattributed tasks cannot author.

Routes:

| Action | Endpoint |
| --- | --- |
| List/create | `GET/POST /api/workflows` |
| Detail/update/delete | `GET/PATCH/DELETE /api/workflows/{id}` |
| Revision list/publish | `GET/POST /api/workflows/{id}/revisions` |
| Revision detail/validation status | `GET /api/workflows/{id}/revisions/{revisionId}`, `POST .../{revisionId}/validate` |
| Collaborators | `GET/POST .../{id}/collaborators`, `DELETE .../{id}/collaborators/{userId}` |
| Triggers | `GET/POST .../{id}/triggers`, `PATCH/DELETE .../{id}/triggers/{triggerId}` |
| Rotate API bearer | `POST .../{id}/triggers/{triggerId}/rotate-token` |
| Manual run/history | `POST .../{id}/run`, `GET .../{id}/runs` |
| Run detail/logs/replay/cancel | `GET .../{id}/runs/{runId}`, `GET .../{runId}/logs`, `POST .../{runId}/replay`, `POST .../{runId}/cancel` |

Create accepts `name`, lowercase `slug`, optional `description`. PATCH accepts those fields, `status` (`active/paused/archived`), optional workspace-bound `project_id` and `default_agent_id` (empty string clears), and mandatory `expected_updated_at` from the last detail response. Publish accepts mandatory `expected_version`, `definition` and `definition_graph` shaped as `{"steps":[{"id":"first"},{"id":"next","parents":["first"]}]}`. Graph nodes must be unique with existing parents and no cycles. A graph alone never proves that source agrees with it. Author-provided validation flags are rejected.

Publish returns HTTP202 with the persisted `revision` and `registration_state=pending_validation`. Current revision stays unchanged until trusted daemon acknowledgement. The original expected-current-version is durable across restart; stale validation yields `version_conflict` and leaves that failed revision visible. Revision engine names are distinct and immutable. Validation status endpoint never validates code on the server. Trusted daemon transport is added separately.

Collaborators accept `user_id` and `role=viewer|editor` for an existing workspace member. Triggers accept `kind=api|schedule`, optional `enabled`, `input_template`, `label`; schedules require five-field `cron_expression` and IANA `timezone` (default UTC). Trigger principal is the creating human permanently; edits cannot replace it. Triggers always use the current revision and cannot select a version.

API trigger creation/rotation returns its bearer token once. Send JSON to unauthenticated `POST /api/workflow-triggers/{token}`. Workspace and human are derived from the stored hash and trigger row; request identity headers do not grant authority. Never paste these credentials into logs, comments or docs. Rotation invalidates the old token.

Manual run accepts optional `input` JSON. Replay creates a fresh local UUID using current revision and the requesting human, reusing prior input; it is not an engine replay. Local admission precedes engine contact. Disabled/unready runtime produces a persisted `skipped` run with `workflow_engine_unavailable`. An unpublished/paused workflow reports `workflow_unrunnable`. Cancellation is idempotent for terminal runs; local cancellation intent is persisted, and engine cancellation/reconciliation is wired by the scheduler task. Product run responses project state without raw engine detail, input/output or credentials.

Limits exported by `service/workflow.go`: source256KiB, graph128KiB, input64KiB,256 steps, depth32; pages default50/max100 with offset<=100000. Logs are projected typed entries, capped at200 entries/64KiB and redacted for recognizable bearer/password/token secrets. Before log transport is connected, log API returns `available=false`, empty entries and `workflow_log_transport_pending`. Arbitrary authored log prose should never contain secrets; redaction cannot recognize every possible secret.

Stable errors include `workflow_not_found`, `workflow_forbidden`, `workflow_no_originator`, `workflow_version_conflict`, `workflow_invalid_graph`, `workflow_invalid_source`, `workflow_invalid_payload`, `workflow_invalid_pagination`, and `workflow_conflict`. Do not report a202 response as completed execution; inspect run state.
