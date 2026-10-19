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
