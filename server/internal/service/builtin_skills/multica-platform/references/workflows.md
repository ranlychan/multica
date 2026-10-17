# Workflows

Workflows are immutable TypeScript revisions with a static DAG, workspace-managed
schedule/API triggers, and durable run history. An [Autopilot](autopilots.md)
dispatches an issue or direct agent task through `create_issue` or `run_only`;
a Workflow coordinates multiple authored steps, durable sleeps, and product SDK
actions. Authored code runs with the executing OS user's full permissions.

## Read and author through the CLI

Start with `multica workflow list --output json`, then
`multica workflow get <workflow-id> --output json`. IDs are full UUIDs from reads,
not display names or slugs. All commands accept `--output json` (the default);
JSON goes to stdout, warnings to stderr. Revision `definition` source is redacted
from CLI responses. Never merge stderr into parsed output.
The shared `--server-url`, `--workspace-id`, profile and task-token rules apply.

Source is accepted only through `--file`: a regular UTF-8 TypeScript file, read
once and bounded to 256 KiB. No inline source flag or stdin source is supported.
Local validation needs Node.js 22+ and the installed packaged workflow runner:
set `MULTICA_WORKFLOW_NODE` to the Node executable (otherwise `node` is found on
PATH), and `MULTICA_WORKFLOW_RUNNER` to the packaged runner entrypoint supplied
by your installation. Validation uses the existing credential-free local runner;
it imports trusted source with local OS-user privileges. It needs no engine
credentials, runtime enrollment, or workspace API access. Import restrictions,
resource bounds, and a subprocess are not a security sandbox.

```bash
multica workflow validate --file ./workflow-source --output json
multica workflow create --name 'Review build' --slug review-build --file ./workflow-source --output json
multica workflow get <workflow-id> --output json
multica workflow get <workflow-id> --revision <revision-id> --output json
multica workflow update <workflow-id> --file ./workflow-source --expected-version <observed-definition-version> --output json
multica workflow update <workflow-id> --name 'Review release' --expected-updated-at '<observed-updated-at>' --output json
multica workflow archive <workflow-id> --expected-updated-at '<observed-updated-at>' --output json
```

`validate` returns `hash`, extracted `graph`, and `graph_digest`; it does not
publish or execute steps. `create` validates locally before any write, creates
the identity, then submits revision 1 with expected version 0. Its JSON contains
`workflow`, `revision`, and `registration_state`. If identity creation succeeds
but publication fails, the error names that persisted identity: inspect it and
use `update`, rather than repeating `create` and creating duplicates.

`get` exposes the workflow's `definition_version` and `updated_at`. Source updates
require the explicit observed `--expected-version`; metadata updates and archive
require the exact `--expected-updated-at` timestamp, preserving fractional
seconds. Source and metadata updates are separate commands. Metadata also accepts
`--slug`, `--description`, `--status active|paused|archived`, `--project-id`, and
`--default-agent-id` (empty string clears the latter two). No command silently
fetches and substitutes a fresh expected value or retries a conflict.

Publication returns HTTP 202 and `registration_state=pending_validation`. It is
not proof of successful validation or execution: only an eligible trusted daemon
can validate/register and advance the current revision. Inspect
`get --revision <revision-id>` for `validation_status` and `validation_error`.
The original expected version is retained while validation is deferred. A stale
expected version returns public code `workflow_version_conflict`; a revision
whose deferred acknowledgement loses that race records validation error
`workflow_version_conflict` and registration state `version_conflict`, remains
visible, and does not advance the current revision.
Inspect and reconcile a colleague's work before deliberately submitting again.
Historical and in-flight runs always retain their immutable revision/hash.

## TypeScript authoring contract

The source file exports `defineWorkflow` from `@multica/workflow-sdk`:

```ts
import { defineWorkflow } from '@multica/workflow-sdk';

export default defineWorkflow({ steps: [
  { id: 'create', run: async ctx => {
    return ctx.issues.create('create-ticket', { title: 'Review build' });
  } },
  { id: 'follow-up', parents: ['create'], run: async ctx => {
    await ctx.log('info', 'Created ticket', ctx.parents.create);
    await ctx.sleep(1000);
    return { ready: true };
  } },
] });
```

Step IDs are unique; parents must exist and the DAG must be acyclic. Inputs and
step results are JSON (64 KiB/depth 32), graph128 KiB/256 steps; retries0–10 and
timeouts/sleeps1ms–24h. Context includes `input`, `parents` keyed by authored step
ID, `runId`, `stepId`, and zero-based engine `attempt`.

Product operations are `issues.create(key, payload)`, `issues.get(id)`,
`issues.waitForStatus(id, status, { timeoutMs? })`,
`agents.dispatch(key, { agent_id, issue_id })`, `agents.waitForRun(id, options?)`,
`autopilots.run(key, { autopilot_id, payload? })`, and
`autopilots.waitForRun(id, options?)`. Agent dispatch is issue-backed: standalone
prompt/chat dispatch is unsupported (`workflow_invalid_payload`). Issue status
uses the workspace's exact status key. Mutations require a nonempty stable author
key, reused across retries; never include engine attempt/invocation in that key.
The server independently scopes each effect to verified run/step/key; reusing it
with another operation/payload conflicts. Responses contain product IDs/status
and omit raw task results, runtime configuration, paths, and credentials.

Definitions cannot declare cron, event, or webhook triggers. A source-level
`cron`, event/webhook declaration, arbitrary SDK import, or other unsupported
definition property fails validation. The Multica trigger resource is the only
ingress, so human authority and the local run ledger are established before any
engine submission. A static graph submitted by an author is never sufficient
proof that source agrees with it; the trusted daemon extracts and verifies it.

## Workspace triggers and runtime eligibility

```bash
multica workflow trigger list <workflow-id> --output json
multica workflow trigger add <workflow-id> --kind schedule --cron '0 9 * * 1-5' --timezone Asia/Shanghai --output json
multica workflow trigger add <workflow-id> --kind api --output json
multica workflow trigger remove <workflow-id> <trigger-id> --output json
multica workflow trigger rotate-token <workflow-id> <trigger-id> --output json
```

Schedule cron has five fields; timezone is an IANA name, default UTC. `add` also
accepts `--label`, `--enabled=false`, and `--input-file ./input.json`. API triggers
do not accept cron/timezone flags. Triggers always select the current revision,
cannot pin a version, and permanently retain the human creator as principal;
membership is rechecked on every fire. Missing/departed principals yield visible
`skipped` runs. Authoring, archive, and trigger management use creator,
workspace owner/admin, or editor-collaborator authority. Viewer collaborators
cannot write; only creator/owner/admin manages collaborators. Agent tasks use
the current human originator's grants and keep agent audit identity separately;
terminal/unattributed tasks cannot author. Never replace a rejected task token
with a member/profile credential.

API creation/rotation returns a bearer token once. CLI output redacts it by
default. To deliberately capture it privately, append `--show-secrets --output
json` to `trigger add --kind api` or `trigger rotate-token`. The one-time JSON
includes `token` and full `invocation_url`. Do not run those commands in shared
transcripts, CI logs, comments, docs, or PRs. If discarded, rotate again; reads
cannot recover it. Rotation immediately invalidates the old token.
Invoke the returned URL with JSON via unauthenticated
`POST /api/workflow-triggers/{token}`. The stored token hash determines workspace
and human identity; supplied identity headers grant no authority. Hatchet REST
is private and is never a public trigger surface.

An eligible daemon requires explicit workflow enablement, acceptance of arbitrary
code risk, workspace enrollment, Node22+, the packaged runner, valid engine TLS
material/reachability, and a current acknowledged registration for the revision.
Runtime enablement uses `MULTICA_WORKFLOW_ENABLED=true`,
`MULTICA_WORKFLOW_ACCEPT_CODE_RISK=true`, `MULTICA_WORKFLOW_WORKSPACES`,
`MULTICA_WORKFLOW_NODE`, `MULTICA_WORKFLOW_RUNNER`,
`MULTICA_WORKFLOW_ENGINE_ADDRESS`, and `MULTICA_WORKFLOW_TLS_*` settings supplied
by the deployment. Inspect `multica runtime list --output json` and coordinate
trusted daemon configuration with its operator. CLI commands do not select a
runtime or provide Hatchet credentials. Normal agent execution remains available
when the daemon cannot advertise workflow capability.

The application server never executes authored code. Publication remains pending
without eligible validation; there is no server-side validation fallback.
`MULTICA_HATCHET_ENABLED=false` is the safe deployment default: CRUD/history stay
readable but new runs are unavailable. Manual, replay, and API admission without
a ready engine/eligible revision runtime return HTTP503
`workflow_engine_unavailable` and persist a `skipped` ledger row. Unpublished or
paused workflows report `workflow_unrunnable`. Scheduled admission without an
eligible runtime also records a skipped occurrence; the next occurrence advances.
Unpublished schedules defer without fake revision snapshots; after publication
the stored overdue planned occurrence fires once, then advances past current
time. Internal delivery is at least once, but `(trigger_id, planned_at)` is unique
in the local run ledger. A local run UUID is the engine submission idempotency key.

## Run, inspect, cancel, and replay

```bash
multica workflow run <workflow-id> --input-file ./input.json --output json
multica workflow runs <workflow-id> --limit 50 --offset 0 --output json
multica workflow get <workflow-id> --run <run-id> --output json
multica workflow logs <workflow-id> <run-id> --limit 50 --offset 0 --output json
multica workflow cancel <workflow-id> <run-id> --output json
multica workflow replay <workflow-id> <run-id> --output json
```

Manual `--input-file` is optional (default `{}`), bounded JSON64 KiB/depth32.
Manual and replay runs act as the requesting human. Replay creates a fresh local
UUID, uses the current revision and earlier input, and can incur real work/budget;
it is not an engine replay pinned to the old revision. HTTP202 acknowledges local
admission, not completed execution. Read persisted run/step state to track it.

`list`, `runs`, and `logs` return one page: `--limit`1–100 (default50), `--offset`
0–100000. The matching HTTP query parameters are `limit` and `offset`. Advance
offset by the number of returned rows; an empty/short page ends that current
view. There is no automatic tail/follow or hidden full-history pull. Logs are a
persisted, typed, redacted projection capped at200 entries/64 KiB, not the engine's
complete history; pages continue only within that projection. `available=true`
means the projection is readable, not complete. `available=false` with
`workflow_log_transport_pending` means no transport is configured, not that no
logs were emitted. Known projections remain readable during engine outages.

`logs` also reads run detail to expose its `reason_code`.
`workflow_final_logs_incomplete` means final log recovery exhausted three
persisted attempts: known logs and the observed engine outcome are preserved;
terminal rows remain immutable. CLI warns on stderr and retains that reason in
JSON. Never interpret an empty page, a terminal status, or `available=true` as
proof of complete log history. No pagination cursor can recover omitted entries.

Cancellation is an idempotent durable request, exposed as `cancel_requested_at`.
Queued cancellation atomically prevents submission. Ambiguous submissions can
still attach a late engine ID; cancellation is delivered and finalization waits
for confirmed cancellation or a terminal engine outcome. Terminal runs remain
unchanged. A cancel response does not prove the authored process has stopped.

Run list/mutation responses expose local state without raw engine output or
credentials. Run detail includes bounded local input with credential fields and
recognized bearer strings recursively redacted; oversized/deep historical input
becomes `{"_redacted":true}`. CLI applies the same credential projection and log
bounds. Never put credentials in input prose or authored logs: redaction cannot
recognize every arbitrary secret.

## HTTP contract for integrations

Use authenticated `/api/workflows` with `X-Workspace-ID`:

| Action | Endpoint/body |
| --- | --- |
| List/create | `GET/POST /api/workflows`; create: `name`, lowercase `slug`, optional `description` |
| Detail/update/delete | `GET/PATCH/DELETE /api/workflows/{id}`; PATCH: metadata plus mandatory `expected_updated_at` |
| Revision list/publish | `GET/POST .../{id}/revisions`; publish: `expected_version`, exact source string `definition`, extracted `definition_graph` |
| Revision status | `GET .../{id}/revisions/{revisionId}`, `POST .../{revisionId}/validate` (status only, never server execution) |
| Collaborators | `GET/POST .../{id}/collaborators` (`user_id`, `role=viewer|editor`), `DELETE .../{id}/collaborators/{userId}` |
| Triggers | `GET/POST .../{id}/triggers`, `PATCH/DELETE .../{id}/triggers/{triggerId}`; `kind=api|schedule`, `enabled`, `input_template`, `label`, schedule `cron_expression`/`timezone` |
| Rotate API bearer | `POST .../{id}/triggers/{triggerId}/rotate-token` |
| Manual/history | `POST .../{id}/run` with optional `input`, `GET .../{id}/runs` |
| Run detail/logs/replay/cancel | `GET .../{id}/runs/{runId}`, `GET .../{runId}/logs`, `POST .../{runId}/replay`, `POST .../{runId}/cancel` |

Example extracted graph: `{"steps":[{"id":"first","parents":[]}]}`.
Author-supplied validation flags are rejected. Stable errors include
`workflow_not_found`, `workflow_forbidden`, `workflow_no_originator`,
`workflow_version_conflict`, `workflow_invalid_graph`, `workflow_invalid_source`,
`workflow_invalid_payload`, `workflow_invalid_pagination`, `workflow_conflict`,
`workflow_engine_unavailable`, and `workflow_unrunnable`. Shared CLI HTTP error
classification/exit codes remain intact. Investigate persisted state before
retrying mutations that may already have succeeded.
