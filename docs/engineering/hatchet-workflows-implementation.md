# Workflow implementation and operator handoff

This handoff records the reviewed implementation through Task 7 and the Task 8
integration work. Historical receipts below were extracted from the implementation
reports; they are not fresh executions. Final branch review is a separate gate.

## Runtime and security contract

Hatchet is pinned to `v0.109.14`, commit
`377de5d9642933b835990b1e2bea81d08f85c943`. The only upstream patch is the two-line
private-listener patch at `server/internal/hatchet/private-listeners.patch`, SHA256
`92560c9627e5783e5aec865fd070ee4690cc0c909d13dab07c564542c48248d4`.
Build with `make build-hatchet`, retaining `PIN`, `PATCH_SHA256`, and
`BINARIES_SHA256`; verify the four binaries with `shasum -a256 -c BINARIES_SHA256`.
These are local build provenance receipts, not signed attestations.

The public npm dependency is exactly `@hatchet-dev/typescript-sdk@1.34.0-alpha.2`,
integrity `sha512-cM9esZxPaJjds4+Z2qm6g7BLiMxaNlDurlyOZsuuNGpFvvvDp3DrU0LnuubtvUXG8mRSW/vMTF04uXJapBo+ZA==`.
The artifact verifier checked the complete official tarball hash and eight
critical installed JavaScript files against the exact upstream compiler output.
It does not claim the entire transitive graph is identical to upstream's lockfile.

Definitions run on an explicitly enrolled daemon with Node 22 or newer, accepted
arbitrary-code risk, a workspace allowlist, a packaged runner and verified TLS
connectivity. They run with that OS user's file/network privileges. Import
validation and process ownership are not an OS sandbox. The application server
never executes authored workflow code. Ordinary agent execution remains available
when workflow readiness fails.

Each workspace has a distinct tenant. Provisioning and server credentials stay
server-side; the browser reads only bounded product endpoints. A daemon receives
only its workspace's control and generation credentials. Every SDK action checks
the current human principal, grants, exact engine assignment and local authority.
The engine snapshot is not a lease across the subsequent application transaction.

The stable mutation identity is `(workspace, run, step, authorKey)`. Native retry
attempt and durable invocation are audit data and **do not** enter that identity.
Reusing a key for a different operation or payload returns HTTP 409. Business rows,
quota, lineage and the receipt commit together; notifications follow commit and
are not promised exactly once. See [the SDK contract](../../packages/workflow-sdk/README.md).

## Deploy, maintain and roll back

1. Back up the application database and apply additive Multica migrations through
   605. Create a separate Hatchet database on an explicitly verified PostgreSQL
   connection, with a distinct database name even when clusters differ. For a
   trusted operator connection, inspect identity/ownership first:

   ```sh
   psql "$POSTGRES_ADMIN_URL" -v ON_ERROR_STOP=1 -c \
     'SELECT current_user, inet_server_addr(), inet_server_port(), pg_postmaster_start_time()'
   psql "$POSTGRES_ADMIN_URL" -v ON_ERROR_STOP=1 -c \
     "SELECT datname, pg_get_userbyid(datdba) FROM pg_database WHERE datname = 'workflow_engine'"
   # Only after confirming the intended instance, owner and unused target:
   psql "$POSTGRES_ADMIN_URL" -v ON_ERROR_STOP=1 -c 'CREATE DATABASE workflow_engine'
   ```

   Use secrets from the deployment's secret store, not shell history or this
   document. For development, `make up` provisions the app database through the
   configured connection; `make status` proves environment identity and
   `make down` preserves its data. Do not copy another checkout's environment.

2. Keep `MULTICA_HATCHET_ENABLED=false` until provisioning is ready. Enabled
   startup needs the dedicated DB URL, private REST origin, reachable gRPC
   address, TLS strategy, private state directory, verified binary directory,
   provisioning email/password and `MULTICA_HATCHET_SECRET_KEY`. The supervisor
   runs Hatchet migrations and bootstraps once. Preserve its config, CA, private
   keys and encryption/JWT keysets across restart; never regenerate them to repair
   an existing deployment. REST and health listeners stay private. Public worker
   gRPC requires mTLS; generated local material is restricted to its owner.

3. Back up both databases plus the Hatchet state directory and application
   encryption secret as one recoverable deployment. Test restoration privately
   before routing traffic. Do not restore a DB with a mismatched encryption
   keyset or expose backup credentials in logs. Renew certificates before their
   one-year expiry with old/new CA trust overlap, deploy new leaf certificates,
   reconnect workers, then remove the retired chain. Server mapping tokens also
   need renewal before 8760 hours; the checked procedure is
   [server credential renewal](../../server/internal/hatchet/TENANT-CREDENTIALS.md).
   No automatic renewal CLI is shipped.

4. Enable the server and selected daemons, then one workspace at a time. Observe
   supervisor readiness, tenant mapping/cleanup failures, registration and
   revision validation, age of queued runs without an engine ID, reconciliation
   lag, worker leases, unfinished projections and certificate/token expiry.
   Product history must show `workflow_final_logs_incomplete` after bounded final
   log recovery; recovered entries are not proof that logs are complete.

5. Upgrade only after checking the exact new binary and SDK contract: registration
   names/action aliases, idempotency envelope, operational SQL/schema, inline
   payload mode, TLS, logs, revocation, retry and separate daemon/engine recovery.
   The retiring-worker heartbeat uses an exported SDK surface annotated
   `@internal`; the exact pin is intentional. Back up before engine migrations.
   Migration rollback is not implied by the presence of a CLI `--down` flag.

For application rollback, stop new ingress first: disable schedule/API triggers
through `PATCH /api/workflows/{id}/triggers/{triggerId}` with `enabled:false`,
and block manual run/replay and bearer-trigger POSTs at ingress during drainage.
Keep the enabled engine and active workers available until active runs settle.
The global Hatchet flag is not a new-fires-only drain switch: disabling it also
removes active workflow machinery. Do not archive definitions or revoke active
generation credentials as a substitute for draining. After drainage, disable the
feature and deploy the older application while retaining additive tables,
Hatchet DB migrations and state/keysets. If runs are intentionally abandoned,
record explicit operator cancellation reasons and revoke only their credentials.

Credential revocation has limits: fresh authentication converges after the
engine's auth cache, and an already authenticated gRPC stream was observed to
receive work after token revocation. Independent product authorization and local
worker stop are required. A pre-602 generation with unknown engine token ID may
block replacement until its original one-hour deadline; do not claim another
generation was revoked. Physical Hatchet tenant deletion is unsupported by the
pin: deleted workspaces retain encrypted failed tombstones with
`tenant_deletion_unsupported` for operator handling, not a false deletion success.

## Reviewed historical requirements and receipts

All Go commands below run from `server/` with an explicit reachable disposable
`DATABASE_URL` and the prefix `../scripts/go-test-with-agent-cli-guard.sh`.
Root commands run from the checkout root. Use command-scoped
`PATH="$NODE22_BIN_DIR:$PATH"` for Node 22; no global runtime change is needed.

| Task / reviewed end commit | Requirement → named tests and command receipt |
| --- | --- |
| 0 / `4098270f8` | Exact pin, API, TLS, idempotency, immutable revision and actual durable sleep: `HATCHET_LIVE=1 HATCHET_BIN_DIR="$PINNED_HATCHET_BIN_DIR" HATCHET_PROBE_WORKER="$PROBE_WORKER" go test ./internal/hatchet -run TestPinnedLiveContract -count=1 -v -timeout 8m` passed 140.09s. Actual unsatisfied 120s wait, worker+engine SIGKILL, resumed invocation 2, one memo effect. `TestCrashProbeRejectsSatisfiedOrExpiredSleep` prevents the earlier weak harness. |
| 1a / `c75da1f81` | Immutable schema, stable author key, bounded submission window, no FK/cascade and concurrent indexes: `go test ./cmd/migrate -run 'Workflow\|EveryConcurrent' -count=1 -v` passed, 12 workflow subtests plus both index registries (0.540s; final nonverbose 0.560s). `make sqlc` passed. |
| 1b / `560fe0b4f` | Actual workspace deletion and rollback: `go test ./internal/handler -run '^(TestWorkspaceDeletionManifestCoversPublicSchema\|TestDeleteWorkspace_(CleansWorkflowHistoryAndQueuesTenant\|RollsBackWorkflowCleanupAndTenantQueue))$' -count=1 -v` passed three tests, 0.706s. Root `bash scripts/test-go.sh --race` passed 83 packages (historical). No engine deletion claim. |
| 2a / `002d506e7` | Strict dedicated-DB config, tenant isolation, targeted credential cleanup, tombstones: `go test ./internal/hatchet -run 'TestHatchet(Config\|WorkerTransient\|CleanupRevocation\|Tenant)' -count=1 -v` passed 0.500s with explicit scratch SQL fixture. HTTP/SQL/pinned-source proof, not live revocation. |
| 2b / `d54148bd6` | Patched listener receipts, preserved keysets, startup cleanup/signals: `go test -race ./internal/hatchet -run 'TestHatchet(Config\|SupervisorExplicitBindInternalAddress\|SupervisorHealthTimeoutAndPartialCleanup)' -count=1 -v` passed 4.796s. Actual mTLS IPv6 supervisor lifecycle passed 42.28s; named-file signal tests bypassed the unreachable-DB TestMain skip and passed 4.258s. Windows cross-build passed; enabled Windows startup deliberately fails closed. |
| 3 / `f257dee10` | Human-principal resolution, real task-token middleware, writer/collaborator rules, immutable cancel intent, input/log/token redaction: `go test -race ./internal/service ./internal/handler ./cmd/migrate ./internal/middleware -run 'Workflow\|RequestLogger' -count=1` passed 1.631/2.442/1.412/1.529s. `TestWorkflowCancelRetainsReconcilableStateAndLateAcknowledgement` covers queued/submitting/running intent. No actual engine cancellation was claimed by this phase. |
| 4 / `30f3dae21` | Local-first submit, schedule dedupe, principal removal, restart/ambiguity, fair progress and bounded logs: `go test -race ./cmd/server ./internal/service ./internal/handler ./cmd/migrate -run Workflow -count=1` passed 2.695/1.840/2.142/1.388s. `TestWorkflowScheduleDepartureDuringReadinessAfterBeginSkipsWithoutDiscovery` observed RED then GREEN with zero engine calls. `TestWorkflowReconcileOverlappingOldRunningPollCannotPublishTerminalLogs` fences stale projection; final recovery retains known outcome after three failures. Transport fixtures, not actual engine execution. |
| 5a / `b00a8dcd3` | Public SDK/packaged runner: package test/build/typecheck passed SDK12+runner9; exact npm artifact verifier passed. `MULTICA_NODE_WORKER_LIVE=1 ... go test ./internal/hatchet -run '^TestPackagedNodeWorkerLive$' -count=1 -v` passed 90.25s with actual mTLS, acknowledged evicted sleep recovery and native retry. Boundary-only final 26.00s checked hostile reflection/error/result/log redaction; it omitted the long durability matrix. |
| 5b / `0edfa9da0` | Private Go IPC/cache, human opt-in, staged current/retiring workers, target-only JWT cleanup and independent workspace renewal: `go test -race ./internal/handler ./internal/hatchet ./cmd/server -run 'Workflow\|HatchetTenant\|HatchetClient' -count=1` passed 3.395/1.673/2.693s. `TestWorkflowSharedTenantProvisioningCannotBlockCheckedPairLeases` proved blocked real-provider mint/revoke does not starve checked leases; RED12.23s → GREEN0.23s. Actual Node22 staged/JWT proof passed 71.96s. |
| 6a / `21d4e67d9` | Actual engine assignment, pre-ACK binding, current selectors and post-lock expiry: `TestWorkflowAssignmentPostLockWallClockExpiry` observed five-case RED7.76s → GREEN7.76s; `go test -race ./internal/handler ./cmd/server -run Workflow -count=1 -v` passed 10.607/3.032s. Actual staged proof passed 70.81s; long-hot proof passed 61.75s, original invocation still owned the 40s future. Read-only operational SQL is not a cross-DB lease. |
| 6b / `cbb68e806` | Atomic issue/agent/AP actions and receipts, quota scope, runtime consent, lineage and AP completion discrimination: `go test -race ./internal/handler -run '^TestWorkflowAction' -count=1 -v` passed 21 top-level tests, 6.112s. Final serial 87-test producer/consumer selection passed service1.894/handler6.695/server2.146s. Prior combined 300-test matrix passed; actual packaged issue/receipt proof passed 70.75s. AP modes have real DB/HTTP coverage, not live provider execution. |
| 7 / `6a3c5e8fd` | File-only CLI, explicit CAS, safe JSON/one-time secret disclosure, shipped references: `go test ./cmd/multica -run Workflow -count=1 -v` passed 17 top-level tests, 1.047s, with packaged Node check explicitly skipped by default. With `MULTICA_WORKFLOW_TEST_NODE="$NODE22_EXECUTABLE"`, `TestWorkflowLocalPackagedValidation` passed 0.47s. Three original shipped reference guards passed 0.286s; all three touched Go packages passed 2.210/2.602/6.674s. |

Historical limits remain visible. Initial Task 2 server package successes could
silently skip without DB access; later named executed tests supersede only those
specific gaps. Task 0's final remote-tag recheck failed on a GitHub timeout;
earlier remote checks passed, but the failed recheck is not rewritten as success.
Initial Task 1a broad deletion-manifest failures were resolved by 1b. Task 4's
later broad run failed nine handler tests (heartbeat/lease/rate-limit/seat-intent
cases) and three shipped-reference guards; its agent phase did not run. Later
focused passes did not constitute a whole-suite result. Task 7 fixed the three
reference guards.

The nine historical handler names were
`TestBuildDaemonWebSocketIdentitySeedsBatchRuntimeLeases`,
`TestSendCodeRateLimit`,
`TestBatchedHeartbeatScheduler_StopRecoveryUsesBoundedContextAndKnownWorkspace`,
`TestBatchedHeartbeatScheduler_RaceToOfflineSelfHeals`,
`TestRecordHeartbeat_NoopStoreAlwaysWritesDB`,
`TestRecordHeartbeat_DBFlushOnStaleRow`,
`TestRecordHeartbeat_TouchErrorFallsBackToDB`,
`TestRecordHeartbeat_SweeperRaceRecoversOnline`, and
`TestClaimNextDueSeatCapacityIntentIsExclusiveAcrossReplicas`. The recovered
Task 8 full run printed a passing handler package before its later compilation
failure; it did not complete the entire backend suite.

Task 5a's original frontend run failed missing generated web `.source` and nine
Node26 DOM-storage cases. Existing `pnpm --filter @multica/web mdx` generated the
source, and the same 78 core tests passed under Node22 or the documented Node26
webstorage flag without business-storage changes. Final frontend verification
uses Node22. Task 5a also had one unexplained 90s native-retry timeout followed by
a pass on unchanged code. Later native retries do not establish its cause or a
fix. Expected negative TLS/auth/lock-timeout logs and existing React warnings are
not called pristine output.

Some original RED logs were not retained after interruption (Task4 early fixes,
Task5a initial implementation); their later passing tests are not invented RED
receipts. The wait timeout currently counts requested sleep rather than transport
elapsed time; downtime policy and the runner's optional Vite/esbuild peer warning
remain review triage items. macOS process proofs and a Windows cross-build are
not Linux/Windows execution proofs. Default runs intentionally skip explicit
live/real-provider tests; no installed real agent was authorized or executed.

## Task 8 integration receipts

Task 8 adds shared core schemas/query keys, shared read-only list/detail/run
views, both platform route adapters and all five locales. Active views poll
bounded product endpoints; terminal run/log polling stops. Cancellation after a
persisted Begin but before the first submission attempt now atomically finalizes
locally, preserving cancellation/admission evidence and avoiding engine calls;
attempted ambiguity remains reconciliation-owned.

Task 8's initial disk/Docker blockage was recovered on 2026-10-19. Independent
Task 8 review and the controller's whole-branch review remain separate gates.
Commands used Node22.23.3/pnpm10.28.2 and Go1.27.1 on macOS arm64:

| Command / evidence | Observed result |
| --- | --- |
| `pnpm typecheck` | PASS12/12 tasks after correcting one nullable revision callback; saved TS2742 had already been resolved by explicit JSON typing before recovery. |
| `pnpm lint` | PASS7/7 tasks, existing warnings retained. |
| First recovered `pnpm test` | FAIL because a desktop configuration test imported an uninstalled Electron binary. A test-only Electron boundary mock keeps explicit fixture paths and rejects host-profile discovery. Its four tests then passed. |
| Next `pnpm test` | PASS8/8 tasks: SDK12, runner18, docs16, UI-lab46, web279, core2201, desktop666, views5961. This predates the final schema-fixture/typography refinement and is historical relative to the next row. |
| Disk-blocked `pnpm test` | FAIL1, ENOSPC opening Vite temporary files in `agents/manual-draft-store.test.ts`, `issues/cache-helpers.test.ts` and `paths/route-icons.test.ts`; Turbo aborted. |
| `pnpm --filter @multica/core exec vitest run workflows/queries.test.ts` | Final focused PASS7/7, 132ms. Valid base payloads isolate malformed revision, graph, step, log, principal and runtime fields; older projections keep authorization closed. |
| `pnpm --filter @multica/views exec vitest run workflows/workflows.test.tsx` | Final focused PASS11/11, 940ms: five locale empty states, disabled/unrunnable, long names/logs, pagination, stale revision/live attempts/incomplete logs, reader controls and replay navigation. |
| Guarded `go test -p 1 ./cmd/server ./internal/handler -run 'TestWorkflowCancelAfterBeginBeforeAttempt\|TestWorkflowReconcileCancellationDuringSubmission\|TestWorkflowReconcileOutageExpiryAndCancellation\|TestWorkflowRead' -count=1 -v` | Server PASS1.196s with actual named DB tests, including both direct submit and reconciliation after Begin. Handler matched no tests and is not coverage. The interrupted implementer's original RED receipt was not retained; no replacement historical log is invented. |
| `GOFLAGS=-p=1 make test DATABASE_URL="$DISPOSABLE_UNIT_DATABASE_URL" POSTGRES_DB=multica_task3 MULTICA_HATCHET_ENABLED=false` | FAIL2, ENOSPC at Telegram compilation and remaining builds. Earlier server9.908s, daemon35.078s, handler46.949s and Hatchet5.576s passed. Internal guards remained enabled. Agent half was not reached. An earlier parallel focused build also failed linking from ENOSPC. |
| Guarded `MULTICA_WORKFLOW_DURABILITY=1 pnpm exec playwright test e2e/workflow-durability.spec.ts` | FAIL1 in beforeAll: configured app and engine DB connections refused on port55510; second test did not run. Docker diagnostics return `Docker Desktop is unable to start`. No workflow scenario or fake agent ran. |
| Four pinned binary checksum checks; `git diff --check` | PASS; all four binary checks printed OK. |

The original private keyset was lost during user disk cleanup. Its engine DB was
preserved unchanged, not bootstrapped with replacement keys and not claimed as
restored history. After verifying that the managed app had no tenant mappings or
dependent workflow data, recovery used a new, distinct task-owned engine DB and
new private state. The retained PostgreSQL container/data volume was restarted,
not recreated; its actual published port was inspected and all owned connection
paths reconciled. Pinned binaries were rebuilt using the existing audited builder
and all four checksums verified. No shared cache or database was deleted.

Fresh recovery typecheck passed12/12 and lint7/7 with existing warnings. The first
recovery full frontend run failed one unchanged MCP test at5000ms; its29-test file
then passed unchanged in isolation. That focused pass is not a whole-suite pass.
Fresh real-DB cancellation mutation testing observed RED for both Begin-before-
attempt cases, then GREEN after restoring the fix. The named current projection
test also actually executed all disabled/unrunnable/author/reader cases. `make sqlc`
passed with no further generated-query change.

The enabled managed durability proof subsequently passed **both separate crash
scenarios**: daemon1.1m, engine2.3m, total `2 passed (3.6m)`. Run it explicitly:

```sh
PATH="$OWNED_PSQL_ADAPTER_DIR:$NODE22_BIN_DIR:$PATH" \
  MULTICA_WORKFLOW_DURABILITY=1 MULTICA_WORKFLOW_BROWSER_CHANNEL=chrome \
  scripts/go-test-with-agent-cli-guard.sh \
  pnpm exec playwright test e2e/workflow-durability.spec.ts
```

Load the owned managed environment into this process first, without printing its
secrets. Each scenario asserted an actual unsatisfied, evicted engine WAIT_FOR
with unexpired SLEEP before its SIGKILL. Following managed restart and issue
completion, it asserted one issue, a native retry, one downstream completed fake
task, and list/detail/source/run browser behavior. Provider discovery was fenced
to explicit missing executables except the owned fake Claude; its config was
isolated. No real provider ran. The default Playwright browser was missing;
installed Chrome155.0.8059.80 ran with an isolated temporary profile. Two
NO_COLOR/FORCE_COLOR warnings remain. Earlier setup/browser/parser/teardown
failures are retained as failed receipts, not rewritten as product passes. The
later diagnostic-only JSON-file persistence helper was parser-checked separately;
its additional files are not claimed as part of that already-loaded passing run.

The final frontend command `PATH="$NODE22_BIN_DIR:$PATH" pnpm test --concurrency=1`
passed8/8 tasks (7cached),3m23.655s: core2202,views5961,desktop666,web279,
SDK12,runner18,docs16,UI-lab46. Only package concurrency was bounded; no tests,
assertions or timeouts were reduced. This is the current complete frontend
result, not just the earlier focused MCP pass. Existing React/query/i18n warnings
remain. Current Playwright `--list` also parsed/discovered both tests successfully.

The first post-recovery complete backend invocation finished its regular phase
with one existing test-fixture cleanup race: `TestStartDshProfileProvision_RunsAtMostOnce`
observed its fake invocation before the owned background profile writer finished,
so TempDir removal raced a write. Its agent phase was not reached. The smallest
test-only join waits on the existing atomic in-flight flag before environment/
directory cleanup, including early failures, within the same10s deadline. All
four named DSH provisioning tests then passed1.810s; the single unchanged focused
baseline had also passed, so this is an intermittent cleanup failure, not a
claimed deterministic reproduction. No DSH production behavior changed and no
existing assertion was weakened. The final complete command passed:

```sh
PATH="$OWNED_PSQL_ADAPTER_DIR:$NODE22_BIN_DIR:$PATH" \
  GOPROXY=https://goproxy.cn,direct GOFLAGS=-p=1 \
  make test DATABASE_URL="$DISPOSABLE_UNIT_DATABASE_URL" \
  POSTGRES_DB=multica_task3 MULTICA_HATCHET_ENABLED=false
```

The disposable unit DB was explicitly verified reachable on the retained
container's inspected published listener, with migration605 applied. Both phases
of `scripts/test-go.sh --race` completed with their internal agent-CLI guards:
regular packages passed (daemon32.039s; some others cache-valid), then the full
`pkg/agent` phase passed115.539s with its existing `-p 2 -parallel 2` limits.
GOFLAGS only serialized package compilation; no tests were selected out. The
earlier named `-count=1 -v` server/HTTP regression executions establish actual DB
test execution, not an unreachable-DB TestMain skip. No unexpected agent CLI
invocation was reported. Explicit live/provider cases remain opt-in, not newly
claimed coverage. Independent reviews remain required before branch acceptance.

The owned managed environment was stopped with `make down`, keeping its app DB,
profile and slot. Its recovery engine keyset/TLS state was moved intact out of
scratch reports to ignored `.multica/hatchet-task8-state`; the owned ignored
`.env` now references that location and is mode0600. Keep both with the recovery
engine DB, including during report cleanup. State remains0700 and private keys
0600; no key regeneration or long proof rerun was needed for a same-filesystem
move. `make status` with the real, command-scoped PostgreSQL adapter confirmed
all managed components stopped and the app database present. All original and
recovery databases and the retained container/data volume remain intact.
The command-scoped real PostgreSQL adapter is retained in ignored
`.multica/task8-bin`, not the scratch-report directory. It rejects any connection
outside its verified listener; inspect the retained container's actual published
port again before a future restart rather than assuming it is stable.

## Task 8 review fix: final log snapshot

I1 identified that changing the same log query's interval from5000 to false did
not fetch its final projection, leaving old entries mounted indefinitely. Log
keys now append an active/terminal snapshot phase after the unchanged workspace,
workflow, run and page prefix. Queries wait for observed run status; transition
fetches the requested terminal page once, without periodic polling. An older
in-flight active response cannot overwrite it. Other pages remain bounded and
on-demand; prefix invalidation and ordinary cache GC cover both phases.

With the real app QueryClient (including infinite stale time) and only transport
mocked, the final regression version observed RED7fail/11pass against the original
production code, then GREEN18/18 in952ms after restoring the fix. It covers all
four terminal states, old active entries, an in-flight active request, deferred
already-terminal mount, cached page navigation and no further periodic requests.
Node22 verification commands and results:

- `pnpm --filter @multica/views exec vitest run workflows/workflows.test.tsx`:
  PASS18; core `workflows/queries.test.ts` also PASS7.
- `pnpm test --concurrency=1`: PASS8/8 tasks (3cached),2m46.93s; views5968,
  core2202,desktop666,web279 and the other four packages pass. No assertions or
  timeouts were reduced.
- `pnpm typecheck`: PASS12/12 (7cached),1m4.075s; `pnpm lint`: PASS7/7
  (5cached),13.13s, existing warnings retained.

This UI/cache-only fix does not claim new Go/live-engine execution; the earlier
both-phase backend and actual durability2/2 receipts remain unchanged. Minor JSX
formatting and warning triage are deferred review-ledger items, not suppressed.

## Final review fix wave: trigger reads and generation authority

The three Important findings from the completed whole-branch review are addressed
in one consolidated fix wave. Acceptance still requires the controller's single
scoped rereview; this handoff does not declare the branch ready to merge.

Public trigger create/update/list projections now apply the same bounded,
recognizable-field/bearer sanitizer as run input. The original private stored
template remains available for execution. Trigger-token rotation continues to
return only its newly issued token. The actual HTTP/member-read regression
checks nested fields/arrays/bearers and structurally unchanged stored JSON.

A failed final ACK retains original per-revision publication success and
CAS/authoring-failure evidence, then returns409 before any generation handover or
lease change. The healthy current worker retains its phase, lease and checked
registrations; its failed prepared candidate remains candidate for daemon
stop/retire. A winning newly published revision can remain unavailable until
replacement succeeds. Both competing proposals and an editor losing authority
are covered through candidate retirement, old-worker renewal, a successful new
rotation/ACK and queued admission of the actual current revision. A separate
owned Node IPC test verifies that HTTP409 keeps the manager's current process,
does not create a retiring owner, retires only the failed candidate and renews
the healthy process's lease.

Rotation's two preliminary commits now separately check the database wall clock
after their blocking writes. Final credential delivery locks and checks the
original generation reservation before extending its deadline, then checks both
control and generation expiry after the token write and before commit. Final
ACK/prepare check those expiries after publication/demotion/lease locks; an
evidence-only failed ACK has the same final fence. Expiry rolls back that
transaction and invokes existing exact-token cleanup for an abandoned mint.
Workspace → daemon → generation → workflow lock order and the separate existing
action fence are retained. No engine I/O is moved under application row locks.

Actual unchanged-row lock regressions observe the exact blocking backend PIDs,
wait for PostgreSQL `clock_timestamp()` to cross expiry, then unlock. They cover
24 ACK/prepare/evidence-only cases (including demotion of the old current), six
final-mint cases, and five preliminary rotation cases. Assertions require no
publication/phase/map/lease mutation, no delivered token and exact abandoned-token
revocation without revoking the healthy generation. No new control framework,
SDK timeout policy, CLI projection or JSX change is included.

Fresh fix-wave receipts use the same inspected retained PostgreSQL container/data
volume and published51106 listener, explicit reachable `multica_task3` at
migration605, Node22.23.3, `GOPROXY=https://goproxy.cn,direct`, and the repository
agent-CLI guard. Container/DB ownership and migration/counts were reverified on
resumption; no database/state/key reset or service restart occurred.

| Check | Result |
| --- | --- |
| `TestWorkflowTriggerPublicTemplateRedactionPreservesStoredInput` | Actual RED: create/update/member list exposed nested credentials; token-only rotation safe. GREEN covers all four responses and unchanged stored input. |
| Exact I2/I3 handler selector, `-count=1 -v` | RED43.900s: both failed-ACK handovers and28 expiry cases reproduced. First GREEN44.008s. |
| Demotion-lock mutation, removing only final post-lease fence | RED3.586s for both control/generation expiry; fence restored before race verification. |
| Preliminary-rotation mutation, removing only its two final control fences | RED8.180s for all five cases; both fences restored. |
| `go test -race -p 1 ./internal/handler -run '^TestWorkflowControlRotationInitialStagesPostLockExpiry$\|^TestWorkflowControlRotationPostLockExpiryRevokesOnlyUndeliveredJWT$' -count=1 -v` | GREEN19.109s, all11 cases, exact token-ID cleanup assertions. |
| `go test -race -p 1 ./internal/handler ./internal/daemon ./internal/hatchet ./cmd/migrate -run 'Workflow\|HatchetTenant\|HatchetClient' -count=1 -v` | PASS handler62.182s, daemon100.379s, Hatchet1.366s, migrate1.482s. These completed saved outputs were recovered after interruption; they were not regenerated. Explicit live engine and separate destructive tenant SQL fixture tests remained gated. New authority SQL was exercised by the actual handler DB tests. |
| `make sqlc`; four pinned binary checksums | PASS; only expected control queries regenerated, all four retained binaries verified `OK`. |

The one complete backend invocation for this fix wave completed on October26:

```sh
PATH="$OWNED_PSQL_ADAPTER_DIR:$NODE22_BIN_DIR:$PATH" \
  GOPROXY=https://goproxy.cn,direct GOFLAGS=-p=1 \
  MULTICA_WORKFLOW_TEST_NODE="$NODE22_EXECUTABLE" MULTICA_RUN_REAL_AGENT_SMOKE=0 \
  make test DATABASE_URL="$DISPOSABLE_UNIT_DATABASE_URL" \
  POSTGRES_DB=multica_task3 MULTICA_HATCHET_ENABLED=false
```

PASS exit0:69 regular packages followed by the complete guarded `pkg/agent`
phase,70 passing packages total, zero cached results,14 packages without tests.
Representative fresh results: server10.063s, daemon129.414s, handler103.508s,
Hatchet5.569s, service9.280s, migration15.252s and agent120.572s. No unexpected
agent CLI invocation was reported. The explicit live/provider tests remained
opt-in; these are actual local fixture/race results, not new engine/provider
proof. The existing owned-env preflight reported the retained app DB; the
explicit make-exported unit DB URL controlled migrations and Go tests. All
migrations were already applied. Final database counts remained0workflows/1run,
the managed services remained stopped, `.env` stayed0600 and engine state0700.
`gofmt -l` and `git diff --check` passed.

Frontend and long-engine durability proof are unchanged and were not repeated.
M1 SDK sleep-only timeout, M2 CLI transport/run reason overwrite and M3
compressed shared JSX remain unchanged, nonblocking items for explicit controller
adjudication. The historical unexplained retry timeout, engine revocation cache/
stream limits, platform/runtime proof gaps and existing warnings above remain
disclosed; no broader guarantees are inferred from these focused fixes.

## Final acceptance and accepted costs

The single authorized scoped rereview of implementation commit
`4a3cec143ab5545c3a95be9d7deb9ab3ebf47ab8` closed all three Important findings:
I1 public trigger-template credential projection, I2 failed-ACK generation
handover, and I3 post-lock control/generation expiry. It found zero new Critical,
Important or Minor findings. The controller accepted the result with zero
blocking residuals; no second fix or rereview wave was required.

The controller explicitly accepted these unchanged nonblocking costs:

- M1: SDK `timeoutMs` counts durable requested sleeps, so transport and downtime
  can make the caller's wait exceed the nominal value. Replay-aware elapsed-time
  and deadline semantics remain future work; independent request/step bounds
  remain in force.
- M2: the CLI can overwrite the log-transport reason with the run reason. The
  availability and incomplete-final-log flags remain accurate, but operators may
  need to inspect the endpoint and run separately to recover diagnostic detail.
- M3: compressed new shared JSX increases future UI maintenance and review
  effort. No behavioral defect was identified; an additional formatting sweep
  was not included in final acceptance.

Acceptance does not expand the guarantees recorded above. The historical retry
timeout remains unexplained; existing warnings, platform/runtime and Docker
packaging gaps, provider proof limits, OS-user arbitrary-code authority, engine
revocation cache/stream limits, snapshot-not-lease assignment checks and
post-commit notification limits remain disclosed. Historical failed or cached
receipts retain their original labels. This closing record changes only the
handoff; it adds no code, tests, migrations, suites or resource operations.

## Durable decisions appendix

These 32 ledger `Ruling:` records are retained verbatim in their original order,
including their rationale and accepted costs. They record the decision at that
point in the work; historical ports, task splits and verification restrictions
are not instructions to replace the current runtime/operator guidance above.
No private fixture DSN or secret occurs in these records.

```text
Ruling: work in the current checkout — the native worktree tool requires an explicit user request for a worktree, and the feature branch already contains the untracked Hatchet/migration drafts the approved plan revises — if wrong, isolation is weaker and unrelated local edits would need extra care, but no main/master branch is touched.
Ruling: treat Task 0 as a mandatory compatibility gate, not permission to rewrite the product contract around unproven Hatchet behavior — if the pinned release lacks a required primitive, record the exact gap and implement the smallest adapter where possible; if no secure adapter exists, that is a load-bearing blocker.
Ruling: do not modify `pnpm-workspace.yaml` in Task 5 merely to satisfy the file list because it already includes `packages/*` — change it only if the implementation genuinely requires workspace configuration; if wrong, the review will flag the missing integration.
Ruling: proceed with focused tests until a managed database environment exists — generating an `.env` or starting shared services would change machine-level development state beyond what the user explicitly requested; if wrong, integration failures may be discovered later by the broad verification rather than at task start.
Ruling: use reachable GOPROXY=https://goproxy.cn,direct with checksum verification retained and disposable Docker/Postgres probes — user authorized autonomous resolution; if wrong, mirror availability could hinder reproduction, while source identity remains pinned.
Ruling: register immutable engine names per revision and reserve input.multica_run_id for TTL CEL idempotency — pinned REST submits by latest name and has no perpetual idempotency field; if wrong, old runs may route to wrong code or duplicate outside the protection window, so Task 4 must enforce a persisted deadline and reconcile-only afterward.
Ruling: SDK operation keys exclude engine attempt number and use a stable author-supplied operation key within run/step — including attempt would duplicate mutations on retry; if wrong, distinct operations with reused keys conflict and must return 409.
Ruling: split Task 1 into 1a schema/behavioral database tests/sqlc interfaces and 1b workspace cleanup wiring — both receive review before Task 1 completes; if wrong, extra review overhead but smaller independently verifiable changes.
Ruling: split Task 2 into 2a configuration/client/tenant and 2b bootstrap/supervision/packaging, each independently reviewed — narrow implementation context after an unproductive dispatch — if wrong, extra review overhead and integration interfaces need explicit handoff.
Ruling: pinned upstream has no public tenant DELETE route or matching admin command; cleanup must revoke source-proven Multica-issued tenant credentials and retain a fenced failed tombstone with tenant_deletion_unsupported plus operator guidance, never claim physical deletion or mutate upstream internal tables — secure supported API is preferable to guessed deletion — if wrong, tenant data/storage remain until operator cleanup or a future supported upstream lifecycle interface; this limitation must appear in final handoff.
Ruling: require enabled Hatchet database name to differ from the application database name, regardless of host aliases/fallbacks, as a conservative fail-closed isolation boundary — parsed primary-host equality cannot prove separation — if wrong, operators using identically named databases on distinct clusters must choose a differently named dedicated database; avoids DNS/fallback routing into the application database.
Ruling: apply a minimal audited packaging patch to force REST and health listeners to127.0.0.1 while retaining configurable TLS/mTLS gRPC — private URL configuration cannot make an all-interface listener private — if wrong, patched build identity differs from pristine upstream and upgrades must revalidate patch targets; original full pin, patch digest/receipt and runtime listener tests must be retained, supervisor rejects unpatched binaries.
Ruling: Task4 may leave listeners.go unchanged because its new workflow endpoints/jobs publish no event-bus payload; live views in Task8 will poll the bounded product ledger/projection rather than introduce an unrequested realtime protocol — the product contract requires live state, not WebSocket transport — if wrong, UI polling introduces bounded refresh lag and a future event integration may be needed; this file-map discrepancy must remain visible to Task4 and final reviewers.
Ruling: defer schedules without a published immutable revision while continuing other published triggers; do not fabricate revision/hash or historical skipped snapshot — no eligible code exists to snapshot — if wrong, an overdue pre-publication planned occurrence may fire after publication rather than creating unavailable history before publication; verify and document the chosen behavior.
Ruling: bound final-log recovery to3 durable incomplete attempts while preserving an observed sanitized terminal outcome on the active row, then finalize with retained known logs and public workflow_final_logs_incomplete — terminal row immutability prevents late changes and engine outage must not erase known state or indefinitely hide a known outcome — if wrong, terminal visibility is delayed during retries and final logs can remain incomplete after prolonged outage; the stable warning must reach CLI/UI/operator docs.
Ruling: split Task5 into5a packaged TypeScript SDK/runner and5b Go daemon/control/credentials/ack/capability integration, each reviewed and Task5 complete only after both plus integration proof — coherent language/test boundaries reduce broad implementation context while preserving contracts — if wrong, extra review overhead and cross-language IPC/HTTP contract drift must be caught by5b actual packaged-worker tests.
Ruling: preserve WorkflowActionName as the existing logical task alias and use it as Node task.name; Node's workflow:task fullaction then exactly matches reconciler's HatchetName+":"+WorkflowActionName — concrete consumer source disproves inferred helper-as-full-action mismatch — if wrong, verbose duplicate revision prefix may hit upstream length bounds, which real registration proof must catch; no needless Go action rewrite or SDK patch.
Ruling: workflow control enrollment uses separate hashed expiring wdc_ credentials accepted only by dedicated workflow handlers, not new generic mdt privileges; operators configure reachable advertisedgRPC andruntime-localCA/clientTLSfiles, controlresponses neverdistributeserverkeys/CAprivatepaths — currentgenericdaemonregistrationhasnoreusablescopedidentity andtaskmdtprivilegesmustnotexpand — ifwrong, operatorsmustprovisionruntimeTLSmaterialandrenewaseparatecontrolcredential; extraenrollmentstate/cleanupmustbereviewedanddocumented ratherthansilentlybroadenexistingdaemonauth.
Ruling: replace single-live-generation credential constraint with explicit bounded staged/current+retiring pair and one current publisher per workspace/daemon, using standalone concurrent-index migrations; validate/cache before disturbing old, grant checked candidate action lease before it consumes queued old work, activate only after actualready, retain old assigned-work authorization until safe retirement then targetedrevoke; global removal/revoke/shutdown remains immediate boundedforce-stop — normal version edit must not kill/revoke valid immutable old execution and public dynamic-action update is unproved — ifwrong, more schema/IPC/lifecycle complexity and further updates may wait for old work to drain; no future revoked_at audit-timestamp hack, no routine arbitrary1s/2s/30s kill, cap retained pair via step/credential deadlines and current-human/engine-assignment checks. Full5breview must check this explicit Task1single-token constraint correction.
Ruling: Task6 implements narrow server-only READ-ONLY exact-pin Hatchet operational-DB assignment verifier using alreadyserver-helddedicatedDBURL, no upstream writes/schemachanges/daemonDBsurface; publicv1OLAPtasksummary lacksworkeridentity andhistoricaleventsare notcurrentassignmentproof — requiredwrong-daemon/step authorizationcannotbebasedonstaleevents — ifwrong, tightercouplingtopinnedengineoperationalschemaandupgradesmustrevalidatejoins/predicates; explicitread-onlyconnections/schemaidentity/tenant-isolatedactualassignment testsrequired. Task5b retainsrun/revision/step/frozenhumanfoundation withunboundActionhookfailclosed; exactassignmentcheckisloadbearing6work,notwaived.
Ruling: split Task6 into6a independently verified current engine assignment/private selectors and6b atomic idempotent business actions, with separate task reviews and combined proof before whole Task6 completion — exact-pin read-only assignment and existing multi-transaction business admission each require distinct design/testing attention and the latter consumes the former — if wrong, extra report/review overhead and interface drift;6b must consume reviewed6a and prove real assignment plus business behavior rather than relying on separate mocks. Prepared briefs only; no implementation starts before5b review passes.
Ruling: staged pair bound includes successfully delivered tenant engine JWTs as well as product runner credentials; persist each generation's non-secret engine-token identity and target revocation/retry on candidate replacement, retirement, global revoke and fresh authority replacement, with durable cleanup/backpressure against unbounded issuance — product410/localstop alone does not stop a discarded JWT opening a fresh engine stream for its remaining one-hour lifetime — if wrong, more metadata/cleanup and rotation may wait during engine outage. Known upstream new-auth cache delay and established-stream behavior remain documented bounds, not claims of immediate upstream cessation; product fencing/localstop stay immediate and independent. Never tenant-wide-revoke another valid current/retiring worker or hold app row locks across engine calls.
Ruling: pre602 unexpired generations without retained exact engine-token identity get explicit unknown-issuance/cleanup state, distinct from known-unissued new reservations; unknown rows cannot be marked engine-revoked and block new mint until original expiry unless exact identity can be safely recovered — migration must not manufacture proof of credential revocation or use a fake token-ID sentinel/tenant-wide revoke — if wrong, upgrades with such rows can delay replacement up to the original one-hour deadline and need feature-disabled rollout/wait guidance. No runtime legacy adapter or dualwrite is introduced; new delivered generations always retain real non-secret IDs.
Ruling: mark a new reservation's issuance ambiguous durably before calling external engine mint; an error/crash without returned token ID cannot be called known-unissued, and mint backpressure clears only on exact-identity cleanup/genuine no-issuance proof/conservative expiry covering latest bounded mint time plus JWT TTL — no transaction spans application DB and engine token creation — if wrong, rare mint failures can delay replacement roughly one hour plus issuance budget; document that availability cost instead of silently allowing credential accumulation. Delivered rows retain actual JWT expiry and exact returned revocation-error receipts may be persisted/retried immediately.
Ruling: allow first app-ledger engine-ID CAS from server-verified INLINE TASK_INPUT reserved local UUID plus exact immutable registration/current assignment only while original55-minute submission deadline is future and attempts>0, with fresh appTX authority/cancel/state fencing; thereafter engine-ID equality is permanent and conflicting late Submit/Discover ACK cannot overwrite it — valid steps can call SDK before localHTTPack, including accepted-response loss, but an expired TTL must not authorize arbitrary later runs with reused UUID — if wrong, tighter payload/schema and dual-producer CAS coupling; offloaded/missing input cannot prove identity and needs bounded trusted-ACK recovery or demonstrated owned-config inline coverage. Afterdeadline unbound stays trustedDiscover-owned; already-bound long-running/descendant steps must not be rejected by first-binding-only insertion-time rules. Snapshot remains notcrossDBlease; no secondSubmit/business6b sideeffect shortcut.
Ruling: during routine retirement only, continue the exact registered worker's native tenant-scoped gRPC heartbeat in the live owned Node process while public worker.stop awaits its active futures; validate captured typed Register tenant/name/workerID, bound and serialize heartbeat calls, stop/abort continuation when stop settles or process/credential owner ends; preserve strict current-assignment checks and existing forced-stop deadlines — exact-pin stop prematurely ends native heartbeat, whereas dispatcher Heartbeat deliberately writes liveness before inactive-stream status9 and independently gates new assignment on isActive; server pause plus runtime-empty snapshot is not a scheduler commit barrier, and retaining idle pairs for a full hour would unnecessarily block edits — if wrong, tighter dependency on SDK dispatcher.client's exported but @internal typed gRPC surface and exact-pin status9 post-write semantics, plus network failures can still cause normal reassignment; upgrades require this long-hot continuity regression. No private worker fields/reflection, upstream SQL writes/source patch, server ghost heartbeat, or worker REST exception.
Ruling: v1 agents.dispatch is explicit issue-backed dispatch with agent_id+issue_id, not an invented standalone prompt/chat/quick-create task; issue dispatch/assignments and AP create_issue retain existing Waitable-offline policy, AP run_only requires Ready; full receipt transaction owns real issue/task/APrun/links/quota mutations via minimal reused InTx/finalize seams, preserving AP-origin issue links plus separate workflow lineage and frozen-human attribution at insertion — current producers have no generic standalone task contract and admission-only AP rows lack durable downstream recovery — if wrong, authors must create/select an issue before agent dispatch and future standalone semantics require an explicit extension; transaction-aware refactors and AP lineage columns add implementation/review surface. Public existing handlers keep their behavior, no parallel workflow-specific admission framework/storage.
Ruling: use a task-owned ignored .env pointing to verified disposable PostgreSQL55510; managed make up provisions the application DB, while explicitly provisioning a distinct task-owned Hatchet DB through that same verified connection after ownership/name checks — managed scripts do not provision the dedicated engine DB and the repository forbids assumed-instance/manual application provisioning, not verified isolated engine prerequisites — if wrong, local environment state and an extra disposable engine DB require cleanup; preserve multica_task3, never copy user env/secrets or touch shared databases, and document portable provisioning.
Ruling: after external cleanup removed temporary pinned binaries and engine keyset, rebuild the exact pinned four binaries and audited patch using the existing script into ignored server/bin/hatchet; preserve the original engine DB untouched and provision a fresh uniquely named task-owned engine DB/private state/keyset for acceptance — old encrypted history must never be reopened with invented replacement keys — if wrong, extra rebuild/provision time and another disposable DB; reuse managed appDB only after proving no tenant mappings/runs/generations, otherwise provision new managed appDB too without resets. Old history recovery requires original keyset; no claim of recovered engine history or Task0 revalidation.
Ruling: accept final Minor M1 sleep-only timeout accounting as disclosed v1 semantic debt, without a replay/deadline redesign — independent step/request bounds remain and the approved contract does not define durable wall-clock downtime semantics — if wrong, callers may wait longer than timeoutMs including transport/downtime and need explicit deadline semantics in a future change.
Ruling: accept final Minor M2 CLI log transport reason overwrite while preserving available=false and final-log-incomplete warnings — it loses diagnostic specificity, not authorization, execution or completeness flags — if wrong, operators must inspect the endpoint/run separately to distinguish unavailable transport from run admission failure.
Ruling: accept final Minor M3 compressed new JSX without a formatting-only final code sweep — no behavioral defect was found and another broad diff adds review overhead — if wrong, future UI maintenance/review takes more effort until ordinary formatting cleanup.
```
