# Pinned Hatchet compatibility contract

Pin: `v0.109.14` at `377de5d9642933b835990b1e2bea81d08f85c943`.
`VERSION` is the pin consumed by this package. The upstream Go module requests
Go 1.26.7. Multica must not import Hatchet's internal Go REST client.

**Status: pinned build, help, source and live engine probes passed.**
All four binaries built on darwin/arm64 with Go 1.27.1. Dependency downloads used
`GOPROXY=https://goproxy.cn,direct` with normal Go checksum verification after the
default proxy failed. Local and remote tag checks matched the pin. Actual help
and HTTP-response fixtures are checked in. The opt-in live harness used disposable
Postgres 15.6, generated test certificates, gRPC TLS and the pinned public Go SDK.
The packaged TypeScript runner and Multica's dispatch policy remain later tasks.

## Checked manifest

All paths below are relative to the private Hatchet API origin. A tenant token
must match the tenant addressed by the request.

| Operation | Pinned interface | Evidence and limit |
| --- | --- | --- |
| Bootstrap | `hatchet-admin --config DIR`; `quickstart --cert-dir DIR --generated-config-dir DIR --skip STAGE --overwrite=false`; `seed` | Fresh config/certificates/keysets and seed were exercised. Quickstart defaults `overwrite` to **true**. Never rerun it over existing keys/config. Seed is not a general tenant-create command. |
| Migration | `hatchet-migrate` with `DATABASE_URL` | CLI source pins `--version`, `--down`, `--up-to`, `--up-to-penultimate`; their presence is not a rollback guarantee. No `--config` flag. |
| API/engine | `hatchet-api --config DIR`, `hatchet-engine --config DIR` | Both declare `--version`; engine also declares `--no-graceful-shutdown`. Production should drain normally. |
| Tenant creation | `POST /api/v1/users/login`, then `POST /api/v1/tenants` | Seeded-user login creates a cookie session; tenant JSON uses `name`, `slug`, optional `engineVersion`, `environment`, `onboardingData`. Two tenants were created. A tenant bearer token gets 403 when provisioning or accessing another tenant. |
| Token minting | `hatchet-admin token create --tenant-id UUID --name NAME --expiresIn DURATION --config DIR` | Emits a tenant JWT. REST `POST /api/v1/tenants/{tenant}/api-tokens` accepts `name`, optional `expiresIn`, and returns `token`. Neither is a global tenant-creation admin token. |
| Submission | `POST /api/v1/stable/tenants/{tenant}/workflow-runs/trigger` | JSON `workflowName`, `input`, optional `additionalMetadata`, `priority`, `return_only_id`. No run UUID, version selector, or idempotency header/field is exposed. |
| Registration | `v1.AdminService/PutWorkflow` | Protobuf `CreateWorkflowVersionRequest`: `name`, `version`, `tasks`, `idempotency`; response `id`, `workflow_id`. Worker SDK/RPC registration, not a guessed REST endpoint. |
| Durable sleep | SDK durable task sleep; `v1.V1Dispatcher/DurableTask` | Go SDK Memo + SleepFor survived SIGKILL of worker and engine, with one external memo effect. TypeScript `ctx.sleepFor` source delegates to waitFor; TypeScript packaging needs its own test. |
| Cancellation | `POST /api/v1/stable/tenants/{tenant}/tasks/cancel` | JSON `externalIds` accepts task or workflow-run UUIDs; optional `filter`. Two cancellation requests returned 200 and the run became CANCELLED. |
| Engine replay | `POST /api/v1/stable/tenants/{tenant}/tasks/replay` | JSON `externalIds` or `filter`; replays existing tasks. **Do not map Multica Replay to this endpoint.** |
| Run state | `GET /api/v1/stable/workflow-runs/{run}`, `.../{run}/status` | Details contain `run`, `tasks`, `shape`, `taskEvents`; run includes `metadata`, `status`, optional `workflowVersionId`. Status endpoint returns a JSON status string. REST states: QUEUED, RUNNING, COMPLETED, FAILED, CANCELLED. |
| Reconciliation | `GET /api/v1/stable/tenants/{tenant}/workflow-runs` | Filters include `idempotency_keys`, `additional_metadata`, `since`, `only_tasks`, `limit`, `offset`. List reads the OLAP view; a temporarily empty result is not proof of non-submission. |
| Logs | `GET /api/v1/stable/tasks/{task}/logs` | Bound with `limit`, `since`, `until`, `attempt`, `order_by_direction`; lines include `message`, `createdAt`, optional attempt/retry and task IDs. Workflow logs require walking its tasks. |
| Durable event evidence | `GET /api/v1/stable/tenants/{tenant}/durable-tasks/{task}` | After r2 registration, the probe requires r1 RUNNING and its own unsatisfied SLEEP with at least 30 seconds remaining, then SIGKILLs both worker and engine. |
| Registered workflows | `GET /api/v1/tenants/{tenant}/workflows` | The probe waits for each immutable revision name before submission. |

The exact upstream service name for durable dispatch is recorded in
`testdata/contract.json`; downstream code should use the pinned SDK rather than
hand-constructing the bidirectional durable protocol.

## Required adapter decisions and remaining gates

1. **Immutable revisions:** triggering by name selects the newest registered
   version (upstream SQL orders versions by descending `order`). The `version`
   field on registration does not add a version selector to submission. Register
   a distinct immutable name per Multica revision, and bind action/worker routing
   to that revision too. Never register changed code under a historical name.
   The live probe registered r2 while r1 was sleeping, crashed its worker and
   engine, and verified r1 completed with r1 output after restart. Its 120-second
   wait covers registration; a post-sleep completion record must be absent before
   the crash and appear with invocation >= 2 and a timestamp after restart.

2. **Idempotency:** upstream supports definition-level CEL expressions evaluated
   against input/additional metadata. Reserve an input envelope containing
   `multica_run_id` and user payload; register TTL idempotency with expression
   `input.multica_run_id`. Both the exact reserved-field CEL probe and real
   duplicate submission/reconciliation passed. The probe uses a one-hour TTL.
   TTL expiry permits reuse, and STATUS mode releases keys after completion;
   neither supplies perpetual exactly-once submission. The adapter must have a
   bounded retry/reconciliation policy that never resubmits an ambiguous request
   after its protection window. Task 4 must enforce that deadline in the local
   ledger: after it, only reconcile or require an explicit new run UUID. An empty
   lookup never authorizes a fresh submission outside the protected interval.

3. **Collision recovery:** the RPC returns an AlreadyExists error with
   `existing_run_external_id`; the REST trigger handler has no explicit
   AlreadyExists response mapping; the real duplicate response is **HTTP 500**.
   Do not assume a REST 409 or an existing run ID. The probe forwarded a real
   submission, dropped its accepted response, retried, and reconciled exactly one
   engine run through `idempotency_keys`. Poll this lookup within the protected
   window; account for OLAP lag and retention.

4. **Product replay:** create a new local run UUID, retain the original input,
   choose the currently active immutable revision, and submit normally as the
   requesting human. This fresh-run/current-revision adapter passed the live
   probe. Engine task replay does not implement that contract.

5. **Tenant bootstrap:** seed a private provisioning user, log in with email and
   password, retain its cookie session server-side, and create tenants through
   that user session. The live probe exercised this path and CLI tenant-token
   minting. No global admin capability is implied by `token create`.

6. **TLS and queue setup:** use generated/preserved TLS material and
   `SERVER_TLS_STRATEGY=tls` (or configured mTLS). The server rejects `none`, even
   though clients accept it. The live probe uses TLS with a generated test CA,
   `SERVER_GRPC_INSECURE=false`, and PostgreSQL message queue/pubsub via
   `SERVER_MSGQUEUE_KIND=postgres` and `SERVER_MSGQUEUE_PUBSUB_KIND=postgres`.

## Verification

From `server/`:

```sh
go test ./internal/hatchet -count=1 -v
HATCHET_UPSTREAM_DIR=/absolute/path/to/hatchet go test ./internal/hatchet -run TestPinnedCompatibility -count=1 -v
HATCHET_VERIFY_REMOTE_TAG=1 go test ./internal/hatchet -run TestPinnedCompatibility/remote_tag -count=1 -v
HATCHET_BIN_DIR=/absolute/path/to/binaries go test ./internal/hatchet -run TestPinnedCompatibility/built_binary_help -count=1 -v
```

The default tests parse source-derived Go declarations/JSON tags, route
registrations and flags, protobuf wire declarations, captured help, and real
HTTP fixtures. The checkout check
resolves the tag and compares every whole-file SHA-256 and exact excerpt against
`git show PIN:path`; it does not alter the checkout. The remote check detects
moved tags. The binary check executes only help commands, never a service.
Opt-in checks report SKIP when their required environment variable is absent.

Build in a clean, detached checkout of the pinned commit:

```sh
go build -o ./bin/ ./cmd/hatchet-admin ./cmd/hatchet-migrate ./cmd/hatchet-api ./cmd/hatchet-engine
```

To repeat the live probe, copy `testdata/probe-worker.go.txt` to
`cmd/multica-compat-probe/main.go` **inside that disposable upstream checkout**,
then build `go build -o ./bin/multica-compat-probe ./cmd/multica-compat-probe`.
From Multica's `server/`, with Docker available:

```sh
HATCHET_LIVE=1 HATCHET_BIN_DIR=/absolute/pinned/bin HATCHET_PROBE_WORKER=/absolute/pinned/bin/multica-compat-probe go test ./internal/hatchet -run TestPinnedLiveContract -count=1 -v -timeout 8m
```

The test creates a uniquely named disposable Postgres container and child
processes, then removes only those resources. It does not use `.env` or existing
services. Bootstrap and child processes use the disposable directory as cwd.
A bootstrap-only OpenSSL wrapper supplies an explicit CA serial path because
macOS LibreSSL otherwise truncates paths at a dotted ancestor; the test checks
that serial state remains inside its certificate directory. The pinned binary
and embedded certificate script are unchanged.
`HATCHET_CAPTURE_DIR` may point to a fresh private output directory to
retain diagnostics; it contains ephemeral key material, so never commit it.
The checked-in HTTP fixture omits credentials/cookies. Its `login` and
`ambiguous-response` entries are explicitly outcome markers; `restart-evidence`
records harness crash/restart times and the worker's post-sleep completion.
Other bodies are real responses from the successful TLS probe.

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
source entry. These are schema/source fixtures. `testdata/help/` contains actual
binary help and `testdata/live-responses.json` contains the successful probe's
HTTP evidence, with the diagnostic outcome markers described above. No secrets
or real tenant tokens are included.
