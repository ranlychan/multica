# Pinned Hatchet compatibility contract

Pin: `v0.109.14` at `377de5d9642933b835990b1e2bea81d08f85c943`.
`VERSION` is the pin consumed by this package. The upstream Go module requests
Go 1.26.7. Multica must not import Hatchet's internal Go REST client.

**Status: source contract checked; release/runtime compatibility gate blocked.**
The four binaries could not be built because Go proxy networking failed
and required modules were not cached. The remote tag was verified successfully
after an earlier GitHub DNS failure. No real `--help` output or HTTP responses
were captured. The fixtures are upstream source excerpts, not simulated responses.
A passing offline test is not permission to claim engine compatibility.

## Checked manifest

All paths below are relative to the private Hatchet API origin. A tenant token
must match the tenant addressed by the request.

| Operation | Pinned interface | Evidence and limit |
| --- | --- | --- |
| Bootstrap | `hatchet-admin --config DIR`; `quickstart --cert-dir DIR --generated-config-dir DIR --skip STAGE --overwrite=false`; `seed` | CLI source only. Quickstart defaults `overwrite` to **true**. Never rerun it over existing keys/config. Seed is not a general tenant-create command. |
| Migration | `hatchet-migrate` with `DATABASE_URL` | CLI source pins `--version`, `--down`, `--up-to`, `--up-to-penultimate`; their presence is not a rollback guarantee. No `--config` flag. |
| API/engine | `hatchet-api --config DIR`, `hatchet-engine --config DIR` | Both declare `--version`; engine also declares `--no-graceful-shutdown`. Production should drain normally. |
| Tenant creation | `POST /api/v1/tenants` | JSON `name`, `slug`, optional `engineVersion`, `environment`, `onboardingData`. Requires user context and enabled tenant creation. Ordinary tenant bearer tokens are rejected on non-tenant endpoints. Bootstrap authentication is an unresolved integration requirement. |
| Token minting | `hatchet-admin token create --tenant-id UUID --name NAME --expiresIn DURATION --config DIR` | Emits a tenant JWT. REST `POST /api/v1/tenants/{tenant}/api-tokens` accepts `name`, optional `expiresIn`, and returns `token`. Neither is a global tenant-creation admin token. |
| Submission | `POST /api/v1/stable/tenants/{tenant}/workflow-runs/trigger` | JSON `workflowName`, `input`, optional `additionalMetadata`, `priority`, `return_only_id`. No run UUID, version selector, or idempotency header/field is exposed. |
| Registration | `v1.AdminService/PutWorkflow` | Protobuf `CreateWorkflowVersionRequest`: `name`, `version`, `tasks`, `idempotency`; response `id`, `workflow_id`. Worker SDK/RPC registration, not a guessed REST endpoint. |
| Durable sleep | TypeScript durable task `ctx.sleepFor(duration)` | SDK calls `waitFor`; `v1.V1Dispatcher/DurableTask` carries durable messages. Restart/resume behavior is unverified here. |
| Cancellation | `POST /api/v1/stable/tenants/{tenant}/tasks/cancel` | JSON `externalIds` accepts task or workflow-run UUIDs; optional `filter`. Repeated/terminal cancellation semantics need a runtime test. |
| Engine replay | `POST /api/v1/stable/tenants/{tenant}/tasks/replay` | JSON `externalIds` or `filter`; replays existing tasks. **Do not map Multica Replay to this endpoint.** |
| Run state | `GET /api/v1/stable/workflow-runs/{run}`, `.../{run}/status` | Details contain `run`, `tasks`, `shape`, `taskEvents`; run includes `metadata`, `status`, optional `workflowVersionId`. Status endpoint returns a JSON status string. REST states: QUEUED, RUNNING, COMPLETED, FAILED, CANCELLED. |
| Reconciliation | `GET /api/v1/stable/tenants/{tenant}/workflow-runs` | Filters include `idempotency_keys`, `additional_metadata`, `since`, `only_tasks`, `limit`, `offset`. List reads the OLAP view; a temporarily empty result is not proof of non-submission. |
| Logs | `GET /api/v1/stable/tasks/{task}/logs` | Bound with `limit`, `since`, `until`, `attempt`, `order_by_direction`; lines include `message`, `createdAt`, optional attempt/retry and task IDs. Workflow logs require walking its tasks. |

The exact upstream service name for durable dispatch is recorded in
`testdata/contract.json`; downstream code should use the pinned SDK rather than
hand-constructing the bidirectional durable protocol.

## Required adapter decisions and remaining gates

1. **Immutable revisions:** triggering by name selects the newest registered
   version (upstream SQL orders versions by descending `order`). The `version`
   field on registration does not add a version selector to submission. Register
   a distinct immutable name per Multica revision, and bind action/worker routing
   to that revision too. Never register changed code under a historical name.
   This is a source-supported adapter requirement; queued/sleeping revision
   isolation still requires an integration test.

2. **Idempotency:** upstream supports definition-level CEL expressions evaluated
   against input/additional metadata. Reserve an input envelope containing
   `multica_run_id` and user payload; register TTL idempotency with expression
   `input.multica_run_id`. The upstream CEL test demonstrates arbitrary input
   string fields, but executing it and the exact reserved-field scenario was
   blocked by missing dependencies. Do not claim that scenario was run.
   TTL expiry permits reuse, and STATUS mode releases keys after completion;
   neither supplies perpetual exactly-once submission. The adapter must have a
   bounded retry/reconciliation policy that never resubmits an ambiguous request
   after its protection window. That policy is not proven by this task.

3. **Collision recovery:** the RPC returns an AlreadyExists error with
   `existing_run_external_id`; the REST trigger handler has no explicit
   AlreadyExists response mapping. Do not assume a REST 409 or an existing run ID.
   The list API exposes `idempotency_keys` for reconciliation, subject to OLAP
   lag and retention. Prove ambiguous-timeout and duplicate scenarios before
   enabling dispatch.

4. **Product replay:** create a new local run UUID, retain the original input,
   choose the currently active immutable revision, and submit normally as the
   requesting human. Engine task replay does not implement that contract.

5. **Tenant bootstrap:** source confirms user-authenticated tenant creation and
   tenant-scoped minting separately. A usable bootstrap login/user session or
   supported user exchange-token flow remains to be proven. No global admin
   tenant-create capability is implied by `token create`.

## Verification

From `server/`:

```sh
go test ./internal/hatchet -run TestPinnedCompatibility -count=1 -v
HATCHET_UPSTREAM_DIR=/absolute/path/to/hatchet go test ./internal/hatchet -run TestPinnedCompatibility -count=1 -v
HATCHET_VERIFY_REMOTE_TAG=1 go test ./internal/hatchet -run TestPinnedCompatibility/remote_tag -count=1 -v
HATCHET_BIN_DIR=/absolute/path/to/binaries go test ./internal/hatchet -run TestPinnedCompatibility/built_binary_help -count=1 -v
```

The default test parses source-derived Go declarations/JSON tags, route
registrations and flags, plus protobuf wire declarations. The checkout check
resolves the tag and compares every whole-file SHA-256 and exact excerpt against
`git show PIN:path`; it does not alter the checkout. The remote check detects
moved tags. The binary check executes only help commands, never a service.
Opt-in checks report SKIP when their required environment variable is absent.
No test here substitutes for a running-engine integration gate.

Build in a clean, detached checkout of the pinned commit:

```sh
go build -o ./bin/ ./cmd/hatchet-admin ./cmd/hatchet-migrate ./cmd/hatchet-api ./cmd/hatchet-engine
```

A build target, supervisor and container integration are later tasks; this change
does not claim they already exist.

## Upgrade and rollback limits

Before changing the pin, resolve the tag to the intended full commit, build all
four binaries, capture real help and API fixtures, and repeat tenant isolation,
duplicate submission, cancellation, immutable revision and restart/sleep tests.
Review source changes and refresh the captured manifest explicitly.

Take a consistent database backup **and** preserve the matching configuration,
encryption/JWT keysets and TLS material before migration. Database migrations
are forward operations; reverting binaries does not undo schema or data changes.
A `--down` flag is not proof that an older engine can safely read migrated data.
Rollback requires a tested compatible schema or restoring the matching backup.

**Never regenerate existing keysets during upgrade, restart or rollback.** They
are needed to decrypt existing data and validate existing credentials. Preserve
them with the data they protect. Quickstart's overwrite default makes it
particularly unsuitable as an unconditional supervisor boot command.

## Provenance

`testdata/contract.json` contains exact source excerpts from
[the pinned upstream commit](https://github.com/hatchet-dev/hatchet/tree/377de5d9642933b835990b1e2bea81d08f85c943),
with repository paths, one-based inclusive line ranges and whole-file hashes.
Full files have no line range. Upstream's MIT license text is included as a
source entry. These are schema/source fixtures, **not captured HTTP responses or
help output**. No secrets or real tenant tokens are included.
