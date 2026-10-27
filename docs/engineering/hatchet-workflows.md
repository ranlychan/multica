# Hatchet Workflows Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> `superpowers:subagent-driven-development` (recommended) or
> `superpowers:executing-plans` to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add workspace-scoped, durable TypeScript workflows that run on opted-in
Multica runtime machines, can call Multica through a constrained SDK, and remain
observable and controllable from the CLI and the shared web/Desktop UI.

**Architecture:** Multica owns definitions, immutable revisions, triggers,
authorization, and a workspace-scoped run ledger. A bundled Hatchet deployment
owns orchestration inside one Hatchet tenant per Multica workspace. Daemons run a
packaged Node worker only for workspaces and runtimes that explicitly enable the
capability; the application server never executes workspace-authored code.

**Tech stack:** Go 1.26, PostgreSQL/sqlc, Hatchet `v0.109.14` pinned by tag and
commit, Node.js 22+, TypeScript, pnpm, TanStack Query, React, Vitest, and Playwright.

**Spec:** The Product contract and Architecture decisions in this document.

## Global constraints

- Workflow definitions are arbitrary code. Creating, editing, archiving, or
  managing triggers follows the existing Autopilot writer model (creator,
  workspace owner/admin, or explicit collaborator). A task-authenticated agent
  may perform those actions only when its task has a current human originator who
  passes the same check; the server stores that human as the frozen principal.
- The server host never runs workspace-authored workflow code. A deployment with
  no eligible daemon reports the workflow as unrunnable; it does not fall back to
  executing the worker beside `multica-server`.
- Each Multica workspace maps to a distinct Hatchet tenant and receives only
  tenant-scoped worker credentials. Neither a daemon nor a browser receives the
  Hatchet admin token or a token for another workspace.
- Definitions cannot declare cron, event, or webhook triggers. Multica is the
  only ingress and always creates the local run row before calling Hatchet.
- Every run references an immutable definition revision and records its hash.
  Editing a workflow creates a new revision; it never changes what an in-flight
  or historical run means.
- Manual and replay runs act as the member making that request. Schedule and API
  runs act as the trigger's immutable human creator, whose membership is checked
  again on every fire. A missing or departed principal produces a visible
  `skipped` run with a stable reason code.
- Schedule delivery is at-least-once internally and exactly-once in the local run
  ledger for `(trigger_id, planned_at)`. The local run UUID is the idempotency key
  used for Hatchet submission and reconciliation.
- External API triggers enter through a rotatable Multica bearer URL. Hatchet's
  REST API is private and is never the public workflow trigger surface.
- Follow repository database rules: no foreign keys or cascades; cleanup is
  application-owned; every non-primary index is created in its own
  `CREATE [UNIQUE] INDEX CONCURRENTLY` migration.
- TypeScript workers require Node.js 22 or newer. Daemons advertise workflow
  capability only after checking Node, the packaged runner, TLS material, and
  engine reachability. Normal agent execution remains available when that check
  fails.
- `MULTICA_HATCHET_ENABLED=false` is the safe default. Disabled deployments keep
  CRUD and history readable but reject new runs with a stable unavailable error.
- Definition input and SDK responses are JSON values. Secrets, filesystem paths,
  and Hatchet credentials are never returned by workflow read APIs or logs.

## Review focus

- Duplicate scheduler delivery or an ambiguous Hatchet timeout must converge on
  one local run and at most one engine run; Task 4 owns the regression tests.
- A trigger creator removed between scheduling and dispatch must yield `skipped`
  without contacting Hatchet; Task 4 owns the authorization matrix.
- Editing a definition while an older revision is queued, sleeping, or running
  must not switch that run to new code; Tasks 1 and 5 own the revision tests.
- A daemon credential for workspace A must not register, start, read, or mutate a
  run in workspace B; Tasks 2 and 5 own the tenant and runner-token tests.
- A daemon or engine disappearing during durable sleep must resume the run after
  recovery without repeating a completed side effect; Task 8 owns the end-to-end
  durability scenario.

---

## Product contract

### Workflow versus Autopilot

An **Autopilot** dispatches one agent run per trigger and then ends. If the work
has to wait, the agent persists a wakeup on the issue and finishes; satisfying
that wakeup starts a new run. Continuity comes from the issue rather than from a
blocked agent process.

A **Workflow** holds its own durable execution. Its definition describes a DAG;
Hatchet retries failed steps and persists durable sleeps. A workflow can wait for
days and continue after the engine or worker restarts. No agent is involved unless
a step explicitly dispatches one.

Use Workflows for ordered pipelines, recurring reports, migrations, and work with
meaningful “then” relationships. Use Autopilot when the primary operation is
“look at this and decide.” Neither replaces the other.

### Product surface

工作流 is a tab inside 自动化 beside Autopilot and issue wakeups. It lists every
workflow in the current workspace with its name, description, active revision,
trigger kinds, status, last run, next run, and runtime availability. Selecting a
workflow opens its detail page: read-only source for the selected revision, its
static DAG, triggers, and paginated run history.

A run detail shows the revision and input used for that execution, live DAG state,
step attempts, timestamps, and bounded logs. **Replay** starts a new run with the
same input and current active revision, acting as the member who requested the
replay. **Cancel** idempotently requests cancellation of an in-flight run. Neither
operation mutates the original run.

### Creation and editing

The primary authoring flow is CLI-first. An agent writes a TypeScript definition,
runs `multica workflow validate`, creates or updates the workflow, then adds a
schedule or API trigger. The same CLI can inspect a definition, list and inspect
runs, read bounded logs, replay, and cancel.

Validation performs schema and static-contract checks without workflow
credentials. It rejects top-level trigger declarations, unknown SDK imports,
duplicate step IDs, cycles, invalid retry/timeout settings, and a graph that
exceeds the documented limits. It does not promise that arbitrary user code is
safe or that external services are reachable.

An existing Autopilot can be invoked from a workflow step through the SDK. There
is no separate “wrap” storage model in v1: the CLI may generate a one-step
definition that calls the chosen Autopilot, but the saved artifact is an ordinary
workflow revision.

### Authority and trust

The authoritative principal is always a human user ID resolved by the server.
When a task-authenticated agent creates a workflow or trigger, the audit actor is
the agent, while the frozen run principal is the task's human originator. A task
without a resolvable human cannot create or re-arm persistent execution.

At fire time, the server rechecks that the frozen principal is still a workspace
member and still has every grant needed by the requested SDK action. The runner
credential identifies a daemon; it never supplies or overrides the human
principal. SDK endpoints bind `(workspace_id, daemon_id, workflow_run_id)` before
authorizing an action.

A definition executes as the daemon's OS user. It can access anything that user
can access, independent of Multica's API authorization. Enabling workflows on a
runtime therefore means trusting approved workflow authors as much as code run by
an agent on that runtime. The UI and CLI show this warning before the first
workflow is enabled for a runtime.

## Architecture decisions

### Engine lifecycle and isolation

`hatchet-admin`, `hatchet-migrate`, `hatchet-api`, and `hatchet-engine` are built
from the tag and commit in `server/internal/hatchet/VERSION`. The supervisor runs
migrations before API/engine startup, waits for health, drains on shutdown, and
never regenerates an existing keyset automatically.

Hatchet uses a dedicated database URL (`MULTICA_HATCHET_DATABASE_URL`). It may
point at the same PostgreSQL cluster as Multica but not at the Multica application
database. Bootstrap fails with operator guidance when the database or required
privileges are missing; the application does not assume it may create databases.

One Hatchet tenant is provisioned per Multica workspace. Multica stores the
tenant identifier and encrypted server-side token; daemon workers receive only a
least-privileged, tenant-scoped worker credential through authenticated daemon
APIs. Tenant
provisioning is idempotent and deleting a Multica workspace revokes tokens and
schedules tenant cleanup without making workspace deletion depend on Hatchet
availability.

Only the gRPC worker endpoint may be reachable from runtime machines. It requires
TLS; public exposure additionally requires mTLS. The REST/API and admin endpoints
remain loopback/private and are reachable only by `multica-server`.

### Definitions and revisions

`workflow` stores identity, workspace scope, mutable metadata, status, current
revision, and immutable `hatchet_name`. `workflow_revision` stores source,
language, content hash, static graph, author/audit actor, and monotonically
increasing version. Publishing uses a transaction and compare-and-swap on the
current version so concurrent edits cannot silently overwrite one another.

`workflow_collaborator` follows the existing Autopilot access model.
`workflow_hatchet_tenant` stores the workspace-to-tenant mapping and an encrypted
server-side token. `workflow_operation` records the deterministic keys and results
of SDK mutations. None of these tables uses a foreign key; application services
validate and clean up every relationship.

The daemon fetches revisions for workspaces it watches, writes them to a private
cache with restrictive permissions, validates the hash, and atomically replaces
the worker generation. New runs target the new registration only after the daemon
reports the expected `(workflow_id, revision, hash)`. Existing runs remain bound
to their original Hatchet workflow version.

No application-server parser executes definition code. The packaged runner loads
the module in a credential-free validation child process, produces the static
graph, then starts the credentialed Hatchet worker only after validation succeeds.
A failed revision remains visible with its validation error and is not made
current.

### Triggers, idempotency, and runs

`workflow_trigger` supports `schedule` and `api` kinds. Schedule rows own cron,
timezone, and `next_run_at`. API rows own a hashed, rotatable bearer token. Both
store a frozen `principal_user_id` plus separate audit actor fields. Editing a
trigger never rewrites its principal; explicit re-arming creates a new trigger.

The scheduler claims due rows using the repository's lease pattern. In one
transaction it creates `workflow_run(planned_at, revision_id, input, principal)`
and advances `next_run_at`. A unique partial index on `(trigger_id, planned_at)`
turns retries or lease theft into a lookup of the same row. Submission to Hatchet
happens after commit with the run UUID as idempotency key. `queued` rows with no
engine ID are retried by the reconciler, so a crash in the commit/submission gap
does not lose work.

Manual and replay endpoints follow the same local-first submission path. An API
trigger resolves its workspace and trigger from the bearer-token hash, never from
request headers. Payload size and JSON-depth limits are enforced before the run
row is created.

The local run ledger is the product source of truth for workspace scope,
principal, revision, input, status, and links to issues/tasks. Hatchet remains the
source of truth for step attempts and live execution state. The reconciler maps
engine states to Multica's `queued | running | completed | failed | cancelled |
skipped` lifecycle and stores bounded summaries; it never invents completion when
the engine is unreachable.

### Worker SDK

`@multica/workflow-sdk` exposes a deliberately small surface:

- `issues.create`, `issues.get`, and `issues.waitForStatus`;
- `agents.dispatch` and `agents.waitForRun`;
- `autopilots.run` and `autopilots.waitForRun`;
- `sleep`, structured logging, and access to validated JSON input.

Waiting uses Hatchet durable sleep plus polling with exponential backoff capped at
30 seconds. SDK mutations accept a deterministic operation key derived from
`(workflow_run_id, step_id, attempt, operation_name)` so a resumed step cannot
repeat a completed Multica side effect. Agent dispatch continues to insert through
the existing task service, preserving admission, readiness, quota, attribution,
and agent visibility rules.

The runner token is per workspace and daemon, hashed at rest, time-bounded, and
rotated through the daemon control channel. Every SDK request carries run and step
identity; the server binds them to that workspace and verifies the run/step against
Hatchet state before executing a mutation. The token authenticates the trusted
machine, not the definition or the human principal. Revocation stops new SDK
calls; it does not rewrite historical runs.

## Non-goals for v1

- Workflow-authored cron, events, webhooks, or direct Hatchet API access.
- Server-side execution fallback, untrusted sandboxing, or container isolation.
- A browser code editor, visual workflow builder, arbitrary npm dependency
  installation, or secrets management specific to workflows.
- Mobile authoring or run-control UI. Mobile may render linked issues/tasks but
  is not part of the initial workflow surface.
- Event-driven workflow triggers, cross-workspace workflows, or migration of
  existing Autopilots into a new storage type.
- Dynamic code changes inside an in-flight run. A run always uses one revision.

## File map

| Area | Responsibility |
| --- | --- |
| `server/internal/hatchet/` | Version pin, compatibility probe, admin/API client, tenant provisioning, TLS, bootstrap, and process supervision |
| `server/pkg/db/queries/workflow.sql` | Workflow, revision, trigger, run, runner-token, and idempotency queries |
| `server/internal/service/workflow.go` | Authorization, publishing, local-first submission, SDK actions, replay, cancel, and cleanup |
| `server/internal/handler/workflow.go` | Human/task-authenticated CRUD and run APIs |
| `server/internal/handler/workflow_runner.go` | Daemon-token and run-bound SDK APIs |
| `server/cmd/server/workflow_schedule_job.go` | Schedule leasing, submission retry, and reconciliation loops |
| `server/internal/daemon/workflow_runner.go` | Capability probe, revision cache, worker generations, token rotation, and shutdown |
| `packages/workflow-sdk/` | Public TypeScript authoring API and JSON contracts |
| `packages/workflow-runner/` | Hatchet worker, validation child process, graph extraction, and server SDK transport |
| `server/cmd/multica/cmd_workflow.go` | CLI authoring, triggers, runs, logs, replay, and cancel |
| `packages/core/workflows/` | Response schemas, Query options, and mutations |
| `packages/views/workflows/` | Shared web/Desktop list, detail, DAG, triggers, history, and run detail |
| `apps/web/.../workflows/`, `apps/desktop/...` | Platform route wiring only |

## Implementation tasks

### Task 0: Prove the pinned Hatchet contract

**Files:**

- Modify: `server/internal/hatchet/VERSION`
- Modify: `server/internal/hatchet/README.md`
- Create: `server/internal/hatchet/compatibility_test.go`
- Create: `server/internal/hatchet/testdata/`

**Interfaces:**

- Produces a checked compatibility manifest covering binary names/flags, tenant
  creation and token minting, idempotent run submission, workflow version
  registration, durable sleep, cancellation, replay, logs, and run-state APIs.
- Later tasks must call only endpoints and flags pinned by this test; no task may
  import Hatchet's internal Go REST client.

- [ ] Build the four binaries from the pinned tag/commit and capture `--help` plus
  API fixtures for the required operations.
- [ ] Add a test that fails when the tag does not resolve to the pinned commit or
  when a required endpoint/field/flag changes.
- [ ] Run `go test ./internal/hatchet -run TestPinnedCompatibility -count=1 -v`
  from `server/`; expect PASS.
- [ ] Record upstream upgrade and rollback limits in `README.md`, including that
  database migrations are forward operations and keysets must not be regenerated.
- [ ] Commit as `test(hatchet): pin engine compatibility contract`.

### Task 1: Finalize schema, revisions, and cleanup

**Files:**

- Modify: `server/migrations/595_workflow.up.sql` through
  `server/migrations/611_agent_task_queue_workflow_run_index.up.sql` and matching
  down migrations while they remain unmerged
- Create: `server/migrations/612_workflow_revision.up.sql`
- Create: `server/migrations/612_workflow_revision.down.sql`
- Create: `server/migrations/613_workflow_revision_version_index.up.sql`
- Create: `server/migrations/613_workflow_revision_version_index.down.sql`
- Create: `server/migrations/614_workflow_trigger_token_hash_index.up.sql`
- Create: `server/migrations/614_workflow_trigger_token_hash_index.down.sql`
- Create: `server/migrations/615_workflow_run_planned_index.up.sql`
- Create: `server/migrations/615_workflow_run_planned_index.down.sql`
- Create: `server/migrations/616_workflow_collaborator.up.sql`
- Create: `server/migrations/616_workflow_collaborator.down.sql`
- Create: `server/migrations/617_workflow_collaborator_index.up.sql`
- Create: `server/migrations/617_workflow_collaborator_index.down.sql`
- Create: `server/migrations/618_workflow_hatchet_tenant.up.sql`
- Create: `server/migrations/618_workflow_hatchet_tenant.down.sql`
- Create: `server/migrations/619_workflow_operation.up.sql`
- Create: `server/migrations/619_workflow_operation.down.sql`
- Create: `server/migrations/620_workflow_operation_key_index.up.sql`
- Create: `server/migrations/620_workflow_operation_key_index.down.sql`
- Create: `server/pkg/db/queries/workflow.sql`
- Modify: `server/pkg/db/queries/workspace_delete.sql`
- Regenerate: `server/pkg/db/generated/`
- Test: `server/cmd/migrate/workflow_contract_test.go`

**Interfaces:**

- Produces immutable `WorkflowRevision`, trigger `PrincipalUserID`, and run
  `(RevisionID, PlannedAt, IdempotencyKey)` fields used by every later task.
- Produces sqlc queries whose tenant-scoped lookups require both resource ID and
  workspace ID when they influence authorization.

- [ ] Write migration tests for fresh up/down, concurrent indexes, schedule
  deduplication, immutable trigger principal, revision monotonicity, and workspace
  deletion with historical runs.
- [ ] Run `go test ./cmd/migrate -run Workflow -count=1`; expect the new tests to
  fail before the schema/query changes.
- [ ] Revise the draft schema so source/graph live on immutable revisions, API
  tokens are hashed, active runner tokens are unique per workspace/daemon, runs
  snapshot revision/hash/principal/reason code, tenant tokens use the existing
  secretbox-at-rest pattern, collaborators are explicit, and SDK operation
  results are durable.
- [ ] Add workspace deletion queries in dependency order, preserving in-flight
  task rows according to the existing no-FK policy.
- [ ] Run `make sqlc`, then rerun the focused migration tests; expect PASS.
- [ ] Run `git diff --check`; expect no whitespace errors.
- [ ] Commit as `feat(workflows): add durable workflow schema`.

### Task 2: Bootstrap and supervise isolated Hatchet tenants

**Files:**

- Create: `server/internal/hatchet/config.go`
- Create: `server/internal/hatchet/client.go`
- Create: `server/internal/hatchet/bootstrap.go`
- Create: `server/internal/hatchet/supervisor.go`
- Create: `server/internal/hatchet/tenant.go`
- Create matching `*_test.go` files in `server/internal/hatchet/`
- Modify: `server/cmd/server/main.go`
- Modify: `server/cmd/server/shutdown.go`
- Modify: `Makefile`
- Modify: `Dockerfile`
- Modify: `.github/workflows/ci.yml`

**Interfaces:**

- Produces `hatchet.Client` operations for tenant lifecycle, workflow submission,
  run inspection, logs, cancellation, and replay.
- Produces `Supervisor.Start(ctx)`, `Supervisor.Ready()`, and
  `Supervisor.Shutdown(ctx)`; Task 4 consumes only those public interfaces.

- [ ] Write failing tests for disabled mode, missing database, first bootstrap,
  idempotent restart, partial-process startup cleanup, health timeout, graceful
  shutdown, workspace tenant isolation, and keyset preservation.
- [ ] Implement strict environment parsing and redacted diagnostics for
  `MULTICA_HATCHET_*` configuration.
- [ ] Implement migrate-before-start supervision and tenant provisioning using
  the pinned compatibility fixtures from Task 0.
- [ ] Add opt-in build/package targets without making a normal `make build`
  perform a network clone.
- [ ] Run `go test ./internal/hatchet ./cmd/server -run 'Hatchet|Shutdown' -count=1`;
  expect PASS.
- [ ] Commit as `feat(hatchet): supervise bundled workflow engine`.

### Task 3: Add workflow domain APIs and immutable publishing

**Files:**

- Create: `server/internal/service/workflow.go`
- Create: `server/internal/service/workflow_test.go`
- Create: `server/internal/handler/workflow.go`
- Create: `server/internal/handler/workflow_test.go`
- Modify: `server/internal/handler/handler.go`
- Modify: `server/cmd/server/router.go`
- Modify: `server/internal/service/builtin_skills/multica-platform/references/autopilots.md`
- Create: `server/internal/service/builtin_skills/multica-platform/references/workflows.md`

**Interfaces:**

- Produces workspace-scoped CRUD, revision validation/publish, collaborator,
  trigger, run list/detail, bounded log, manual run, replay, and cancel endpoints.
- `PublishRevision` requires `expected_version` and returns the persisted revision
  plus registration state; it never makes an unvalidated revision current.

- [ ] Write handler/service tests for creator/admin/collaborator access, agent-task
  human-originator resolution, cross-workspace IDs, concurrent publish conflict,
  invalid graphs, payload limits, pagination, redaction, replay authority, and
  idempotent cancel.
- [ ] Implement resource loaders that bind workflow, trigger, revision, and run IDs
  to the requested workspace before any principal is read.
- [ ] Implement CRUD and publish with stable machine-readable error codes.
- [ ] Add authenticated routes under `/api/workflows` and keep runner routes out of
  the human middleware group.
- [ ] Add the unauthenticated bearer ingress at
  `/api/workflow-triggers/{token}`; resolve workspace and principal exclusively
  from the stored token hash and trigger row.
- [ ] Run `go test ./internal/service ./internal/handler -run Workflow -count=1`;
  expect PASS.
- [ ] Commit as `feat(workflows): add authoring and run APIs`.

### Task 4: Implement local-first dispatch, scheduling, and reconciliation

**Files:**

- Extend: `server/internal/service/workflow.go`
- Create: `server/cmd/server/workflow_schedule_job.go`
- Create: `server/cmd/server/workflow_schedule_job_test.go`
- Create: `server/cmd/server/workflow_reconciler.go`
- Create: `server/cmd/server/workflow_reconciler_test.go`
- Modify: `server/cmd/server/main.go`
- Modify: `server/cmd/server/listeners.go`

**Interfaces:**

- Produces `SubmitRun(ctx, RunIntent) (WorkflowRun, error)` using the local run UUID
  as Hatchet idempotency key.
- Produces scheduler and reconciler jobs that are safe under multiple server
  replicas and stolen leases.

- [ ] Write failing tests for the Review focus duplicate-delivery and departed-
  principal cases, plus commit/submission crashes, ambiguous timeout, engine
  outage, disabled workflow, paused trigger, archived revision, cancellation
  races, replay, and terminal-state monotonicity.
- [ ] Implement the transactional schedule claim and local run insert/advance.
- [ ] Implement post-commit Hatchet submission and retry of `queued` rows without
  an engine ID.
- [ ] Implement state reconciliation and bounded log/step projection without
  treating engine unavailability as run failure.
- [ ] Run `go test ./cmd/server -run 'WorkflowSchedule|WorkflowReconcile' -count=1`;
  expect PASS.
- [ ] Commit as `feat(workflows): dispatch and reconcile durable runs`.

### Task 5: Package the SDK and daemon worker

**Files:**

- Create: `packages/workflow-sdk/package.json`
- Create: `packages/workflow-sdk/src/index.ts`
- Create: `packages/workflow-sdk/src/contracts.ts`
- Create tests beside SDK sources
- Create: `packages/workflow-runner/package.json`
- Create: `packages/workflow-runner/src/validate.ts`
- Create: `packages/workflow-runner/src/worker.ts`
- Create: `packages/workflow-runner/src/transport.ts`
- Create tests beside runner sources
- Create: `server/internal/handler/workflow_runner.go`
- Create: `server/internal/handler/workflow_runner_test.go`
- Create: `server/internal/daemon/workflow_runner.go`
- Create: `server/internal/daemon/workflow_runner_test.go`
- Modify: `server/internal/daemon/daemon.go`
- Modify: `server/internal/daemon/client.go`
- Modify: `pnpm-workspace.yaml`

**Interfaces:**

- Produces the documented `@multica/workflow-sdk` API and a bundled runner entry
  point with `validate`, `graph`, and `worker` modes.
- Produces daemon registration fields `(workflow_capable, runner_version,
  node_version)` and revision acknowledgements `(workflow_id, version, hash)`.

- [ ] Write failing SDK tests for JSON validation, deterministic operation keys,
  polling backoff, response redaction, and error mapping.
- [ ] Write failing daemon/handler tests for the Review focus revision and tenant
  boundaries, token rotation/revocation, atomic cache replacement, stale hashes,
  validation timeout, missing Node, worker crash backoff, and graceful drain.
- [ ] Implement the small SDK surface and run-bound request signing.
- [ ] Implement credential-free validation/graph extraction and credentialed worker
  startup as separate child processes.
- [ ] Implement daemon capability reporting and one worker generation per watched
  workspace/tenant; do not reduce normal agent runtime health when workflows are
  unavailable.
- [ ] Run `pnpm --filter @multica/workflow-sdk --filter @multica/workflow-runner test`;
  expect PASS.
- [ ] Run `go test ./internal/daemon ./internal/handler -run Workflow -count=1`
  from `server/`; expect PASS.
- [ ] Commit as `feat(workflows): run tenant-isolated daemon workers`.

### Task 6: Add agent dispatch and Autopilot SDK actions

**Files:**

- Extend: `server/internal/service/workflow.go`
- Extend: `server/internal/handler/workflow_runner.go`
- Modify: `server/internal/service/task.go`
- Modify: `server/internal/service/autopilot.go`
- Modify: `server/pkg/db/queries/autopilot.sql`
- Test: `server/internal/service/workflow_action_test.go`
- Test: `server/internal/handler/workflow_runner_action_test.go`

**Interfaces:**

- Produces idempotent runner actions for issue create/read/wait, agent
  dispatch/wait, and Autopilot run/wait.
- All created agent tasks carry `workflow_run_id`, the run's human principal, and
  `originator_source=workflow` without bypassing existing invocation gates.

- [ ] Write failing tests for operation-key replay, wrong step/run/daemon,
  cross-workspace targets, private-agent denial, quota/readiness denial, departed
  principal, issue origin linkage, and Autopilot attribution.
- [ ] Route mutations through existing issue/task/Autopilot services rather than
  duplicating admission logic.
- [ ] Make completed operation keys return their stored result and conflicting
  payloads return 409.
- [ ] Run `go test ./internal/service ./internal/handler -run WorkflowAction -count=1`;
  expect PASS.
- [ ] Commit as `feat(workflows): add idempotent Multica SDK actions`.

### Task 7: Add the CLI authoring and operations surface

**Files:**

- Create: `server/cmd/multica/cmd_workflow.go`
- Create: `server/cmd/multica/cmd_workflow_test.go`
- Modify: `server/cmd/multica/main.go`
- Modify: `server/internal/cli/client.go`
- Modify: `server/internal/cli/errors.go`
- Modify: `server/internal/service/builtin_skills/multica-platform/references/workflows.md`

**Interfaces:**

- Produces `multica workflow validate|create|update|get|list|archive`,
  `trigger add|list|remove|rotate-token`, and
  `run|runs|logs|replay|cancel`.
- Mutating commands support `--output json`; source is accepted through
  `--file`, never as an inline flag that leaks through shell history.

- [ ] Write command tests for validation failure, optimistic version conflict,
  agent task identity, secret redaction, JSON stability, bounded logs, replay, and
  cancel.
- [ ] Implement commands using shared client/error conventions and atomic file
  reads.
- [ ] Add the command to the built-in platform skill reference so agents know the
  Autopilot/Workflow boundary and trigger prohibition.
- [ ] Run `go test ./cmd/multica -run Workflow -count=1` from `server/`; expect PASS.
- [ ] Commit as `feat(cli): manage durable workflows`.

### Task 8: Add shared web/Desktop observability and verify durability

**Files:**

- Create: `packages/core/types/workflow.ts`
- Create: `packages/core/workflows/{index,queries,mutations}.ts`
- Modify: `packages/core/api/schema.ts`
- Modify: `packages/core/package.json`
- Create: `packages/views/workflows/`
- Modify: `packages/views/package.json`
- Modify: `packages/views/autopilots/components/autopilots-page.tsx`
- Modify: `packages/views/locales/{en,fr,ja,ko,zh-Hans}/autopilots.json`
- Create: `apps/web/app/[workspaceSlug]/(dashboard)/workflows/[id]/page.tsx`
- Create: `apps/web/app/[workspaceSlug]/(dashboard)/workflows/[id]/runs/[runId]/page.tsx`
- Create: `apps/desktop/src/renderer/src/pages/workflow-detail-page.tsx`
- Create: `apps/desktop/src/renderer/src/pages/workflow-run-page.tsx`
- Modify: `apps/desktop/src/renderer/src/routes.tsx`
- Create: `e2e/workflow-durability.spec.ts`

**Interfaces:**

- Produces shared list/detail/run components with platform route adapters only in
  app packages.
- Query keys are workspace-scoped and API payloads pass through Zod plus
  `parseWithFallback`.

- [ ] Write failing core schema tests for malformed revision, graph, step, log,
  principal, and runtime-availability payloads.
- [ ] Write component tests for empty/disabled/unrunnable states, stale revision,
  long names/logs, DAG status, pagination, cancel/replay permissions, and all
  supported translations.
- [ ] Implement the 工作流 tab and web/Desktop routes without browser editing or
  mobile scope.
- [ ] Add the durability E2E: create issue, durable-wait for closure, kill the
  daemon, restart it, close the issue, and assert one downstream agent dispatch.
- [ ] Run `pnpm typecheck`, `pnpm lint`, and `pnpm test`; expect PASS.
- [ ] Run `make test`; expect PASS.
- [ ] Run `pnpm exec playwright test e2e/workflow-durability.spec.ts`; expect PASS
  in the managed environment with Hatchet enabled.
- [ ] Commit as `feat(workflows): add run observability and durability coverage`.

## Deployment and rollback

1. Apply Multica migrations, provision the dedicated Hatchet database, and run
   Hatchet migrations before enabling the feature flag.
2. Deploy the server with Hatchet disabled; verify supervisor and tenant metrics,
   then enable it for an internal workspace.
3. Deploy the CLI/daemon bundle and confirm daemons advertise workflow capability
   before allowing trigger creation.
4. Deploy web/Desktop. Older clients remain able to use Autopilot; they simply do
   not expose Workflows.
5. Enable one workspace at a time. Watch queued-without-engine-ID age, reconcile
   lag, worker registrations, failed revision validations, and certificate expiry.

Application rollback first disables new fires and waits for queued submissions to
drain. Do not roll back Hatchet's database migration or regenerate its keyset.
Keep Multica's additive tables and columns so older application code can ignore
them while in-flight runs finish. Revoke tenant/runner tokens only when abandoning
those runs intentionally; record the resulting local runs as cancelled with an
operator reason.

Certificate renewal must complete before the one-year expiry and support overlap:
workers trust old and new CA material during the rotation window, reconnect with
new certificates, and only then remove the old chain.

## Completion criteria

- All focused and broad commands in Tasks 0-8 pass with their output recorded in
  the implementation handoff.
- The Review focus cases have named regression tests, not only manual notes.
- A security review confirms tenant isolation, human-principal resolution,
  runner-token binding, log redaction, and the arbitrary-code warning.
- The manual durability scenario passes after killing both daemon and engine at
  separate wait points.
- Operator documentation covers database provisioning, ports/TLS, backup,
  certificate rotation, engine upgrade, metrics, and rollback.
