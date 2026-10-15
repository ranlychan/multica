# Task 5b — Go daemon/control and packaged worker integration

2026-10-15. Recovered the pending Task5b implementation on
`feat/hatchet-workflows`, base `b00a8dcd35fe5d6f1550f581dbad3373aa146576`.
Preserved the saved source; did not reset it, redo5a, change the sibling Hatchet
checkout, run real provider/agent CLIs, edit `.env`, push, merge, or start shared
services. No subagents/reviewers were dispatched. Controller owns independent
review. This report records fresh evidence, not inherited PASS claims.

## Implemented boundaries

`internal/workflowrunner` mirrors the reviewed JSON IPC types, source SHA256 and
graph digest. Concrete independent Node-generated digest vectors cover emoji vs
BMP UTF16 order, `<`, `&`, U+2028/U+2029, nil-vs-empty parent lists. Go disables
HTML escaping and reproduces JavaScript JSON.stringify's separator handling.

The Go runner invokes the absolute packaged `dist/cli.js` with Node22+, private
socket fd3 and NODE_CHANNEL_FD/NODE_CHANNEL_SERIALIZATION_MODE. Credentials and
descriptors are never argv or public stdout/stderr. Child environment is an
allowlist; stdout/stderr are discarded, IPC scanning is bounded2MiB. Validation
checks source/hash/graph in a distinct credential-free authored-code child.
Cache directories must be real/private0700 directories; immutable content-addressed
source files are0600 and replaced using synced temporary files plus atomic rename.
Hash rejection leaves the old cache entry untouched.

The daemon manager is runtime opt-in plus an explicit watched-workspace allowlist.
One current publisher per workspace/tenant contains current/pending and
queued/running immutable revisions. A bounded pair supports current+candidate
during preparation, then current+retiring during completion. Changed source sets
or45-minute renewal validate/cache candidates while the healthy current worker
continues. Invalid drafts preserve that worker. Gated candidate registration is
checked before assignments start, then publication/eligibility changes atomically.
The old worker stops new assignments and retires after active work finishes or
durable eviction is acknowledged; routine edits have no1s/2s/30s force-kill budget.
Both leases renew while retiring. Further edits/refresh are deferred until the
retiring slot clears; the old original1hour credential expiry is the final bound.
Explicit removal/revoke/shutdown still use bounded force-stop. A failed current
is stopped/revoked alone while valid retiring work continues; after the old worker
exits/expires, exact retirement clears the pair and replacement can recover.
Crash/IPC/control failures back off2–64seconds. The10-second reconcile loop renews
checked45-second leases. Old sleeping revisions remain fetched through active
product run rows; no mutable revision alias is substituted.

`WorkflowService.Eligible` is wired to checked registration lease queries, current
membership/control capability, tenant mapping and unrevoked/unexpired generation,
independent of ordinary daemon health. Runtime/Node/TLS/runner failures keep normal
agent registration available. Workflow-only hosts can register without an agent
runtime after a successful probe.

## Configuration and warning

Safe defaults: server `MULTICA_HATCHET_ENABLED=false`; daemon workflow manager is
absent unless `MULTICA_WORKFLOW_ENABLED=true`. A runtime also requires
`MULTICA_WORKFLOW_ACCEPT_CODE_RISK=true` and comma-separated workspace UUIDs in
`MULTICA_WORKFLOW_WORKSPACES`, intersected with the daemon's watched workspaces.
Before probing/advertising, daemon logs the exact warning:

> Workflows execute arbitrary code with this runtime's OS-user privileges, including access to local files and network credentials. Enable only for trusted workspace authors.

Task8 owns UI presentation before first enablement. Runtime-local configuration:

| Variable | Meaning |
| --- | --- |
| MULTICA_WORKFLOW_NODE | Absolute Node22+ executable |
| MULTICA_WORKFLOW_RUNNER | Absolute packaged workflow-runner/dist/cli.js |
| MULTICA_WORKFLOW_ENGINE_ADDRESS | Operator-advertised reachable host:port; wildcard rejected |
| MULTICA_WORKFLOW_TLS_STRATEGY | tls or mtls; public addresses require mtls |
| MULTICA_WORKFLOW_TLS_SERVER_NAME | Expected certificate identity |
| MULTICA_WORKFLOW_TLS_CA | Runtime-local public CA certificate path |
| MULTICA_WORKFLOW_TLS_CERT / MULTICA_WORKFLOW_TLS_KEY | Runtime-local mTLS client identity paths |

Cache is `<workspaces-root>/.workflow-cache`. Probe checks actual Node version,
regular packaged entrypoint, TLS material/identity, TLS gRPC reachability and actual
packaged validation. No engine admin/server token or CA private key is delivered.
The operator provisions the advertised gRPC endpoint and runtime-local TLS files;
HTTP control never sends filesystem paths. The production Node adapter retains
5a's denied engine REST adapter and unusable127.0.0.1:1 REST origin; only gRPC is
used for registration/execution. Fresh5b proof does not add5a's test-only wire
counter instrumentation, so wire0 is5a evidence, not a new5b measurement.

## Authentication, schema and acknowledgement

Ordinary authenticated human daemon registration may opt into workflows and returns
a dedicated `wdc_` control capability with no-store. Current membership is read
again; task/cloud machine identities and mdt credentials cannot enroll new workflow
authority. Another human cannot overwrite an existing daemon ID's owner. Failure
of this optional enrollment does not reject a valid ordinary-agent registration.
No generic mdt privilege was added. The ordinary Client credential is unchanged.

Control credentials are SHA256-hashed at rest, expire24hours, and renew through
ordinary human-authenticated registration after12hours. Routine registration
proves possession in private workflow_control_token and renews the same hash/token,
preserving active generations. Fresh enrollment without that proof replaces the
hash and fences old generations. Control requests require current workspace,
daemon owner membership, capable flag and nonexpired selected-workspace credential
on every fetch/rotate/prepare/ack/retire/revoke/validation-failed call. Body identity is not trusted.

Dedicated paths are GET `/api/daemon/workflows/revisions` and POST sibling
`rotate`, `prepare`, `ack`, `retire`, `revoke`, `validation-failed`. Rotation returns
workspace/tenant, generation UUID, exact expires_at, worker-only tenant JWT and
`wrt_` product token. The token is hashed, expires1hour and is bound to
daemon/control hash/tenant. Rotation replaces stale unprepared candidates, while
the checked current remains valid. A live retiring generation blocks a third
generation with409 workflow_retirement_pending. Exact-target retire revokes a
finished/failed generation without revoking another generation's active work.
`revoke` revokes product generations; the still-valid human-enrolled control may
later request a fresh generation. Removing membership/workspace, setting capability
false, control expiry or re-enrollment denies that control itself.

Worker mint/tenant Ensure/actual engine version reads run outside SQL row locks.
After mint, control is checked again under workspace→daemon lock order before
token persistence/delivery. If that fails, only the just-minted token is targeted
via RevokeWorkerCredentials / RetryWorkerTokenRevocation and its receipt mechanism;
no tenant-wide revoke occurs. Pending cleanup receipts are in memory; a process
crash can lose them, but undisclosed tokens have the bounded1hour TTL and reserved
discoverable worker token name inherited from TenantService.

Ack must match protocol/version/Node/workspace/tenant/generation and immutable
source SHA256, graph digest, logical task names and full action aliases. The
server-private tenant client reads the actual exact engine workflow/version and
checks name, graph/parent mappings, absent cron/event triggers and TTL/CEL
idempotency (`input.multica_run_id`, TTL3600000). Caller-supplied IDs/graphs alone
are insufficient. Private IPC activation_gate lets Node register definitions
without starting assignment streams. prepare verifies all evidence and grants
only a candidate action lease: no publication/Eligible. Go then sends activate;
only after Node confirms active does final ack promote candidate→current and old
current→retiring, publishing/renewing eligibility. Retiring ack can renew only its
exact already-checked registration map and cannot publish new revisions.
Final control/generation recheck, original service author/CAS
gate, activation and registration lease share one SQL transaction. All control
writes lock workspace→daemon, then definitions; workspace deletion takes workspace
first. The original expected-current-version is retained. Authority/CAS failures
remain visible as invalid revisions, are excluded from leased evidence and leave
current unchanged. A failed lease write rolls activation back.

Migrations597/598 add workflow_daemon (composite workspace/daemon primary key,
owner, hashed expiring control, capability/version fields), generation control hash,
tenant ID, registration JSON and lease_until. The sole new nonprimary unique hash
index is its own CONCURRENTLY migration; retry cleanup is registered. No FKs or
cascades. Workspace deletion explicitly deletes workflow_daemon and the existing
runner tokens; manifest updated. Isolated up/down contract now includes597/598.
The retained application DB already had597/598 when recovery inspected it.
Staged continuity adds599 phase(candidate/current/retiring),600 drops the previous
single-generation partial unique index,601 creates a per-phase partial unique
index CONCURRENTLY. The locked handler enforces at most two live rows; only one
can be current/publish.600 rollback's unique-index rebuild has a down-direction
invalid-index cleanup hook;601 has its up-direction hook. Rollback to the old
single-generation index requires first retiring the overlap (otherwise the unique
build fails closed). Fresh isolated-schema tests include599–601 up/down and no
FKs; application DB was advanced through601 without resetting it.

## Task6 handoff — authenticated action foundation only

POST `/api/workflow-runs/{id}/actions` accepts the5a exact JSON contract:
`{protocol:1,run_id,step_id,attempt,operation,operation_key?,payload}`. Headers are
Bearer wrt_, X-Workspace-ID, X-Multica-Workflow-Generation and lowercase hex
X-Multica-Workflow-Signature. HMAC-SHA256 key is the product token; signed bytes:
`POST\nESCAPED_PATH\nWORKSPACE_UUID\nGENERATION_UUID\nEXACT_JSON_BODY`.

Each request independently checks current control/owner membership/tenant mapping,
generation expiry/revocation/live lease, run workspace/state/cancellation, immutable
run-revision/hash registration and step membership, then current frozen-human
workflow authority. Caller-supplied principal is never used. Status401 is invalid
credential/signature,403 is forbidden step/revision/human,404 is missing run,410 is
expired/revoked/cancelled capability;503 is unavailable. `WorkflowControl.Action`
receives WorkflowActionAuthority{Run,Revision,Runner,Principal} plus the bounded
request and returns JSON; absent implementation returns503. It is intentionally
unimplemented for business actions. Task6 MUST additionally enforce exact current
engine assignment with the approved narrow server-only read-only exact-pin Hatchet
operational-DB adapter (tenant/task/run/worker/retry/invocation as applicable), using
the server-held dedicated DB URL. It must handle pre-local-submission-ack binding.
Public v1 task/run REST uses OLAP and has no authoritative current assigned worker;
worker REST returns empty recent assignments. Historical OLAP events are not
assignment proof, nor is a worker's paused status grounds to deny already-assigned
retiring work. See task-6-assignment-contract-research.md. The5b purpose-specific
Action test hook proves this foundation, NOT exact assignment or business effects.
Default Action remains nil/503 until Task6 supplies this load-bearing verification.
Task6 also supplies operation allowlisting,
resource-specific authorization, atomic receipt/payload conflict semantics and
proper domain-error mapping (current callback error maps409). Existing receipt
schema/SDK deterministic operation keys remain unchanged. SDK returns `{result}`
and maps fixed statuses as documented in5a; do not bypass this authentication seam.

## Observed RED/GREEN and root causes

All commands below ran in this recovery. Earlier agent RED receipts were not
available and are not asserted. From `server/`, shared environment for Go commands:

```sh
export DATABASE_URL='postgres://postgres:task3_disposable@127.0.0.1:55510/multica_task3?sslmode=disable'
export MULTICA_WORKFLOW_TEST_NODE='/Users/ranlychan/Library/Caches/pnpm/dlx/82782474caa34688d1e0c2131af20406/mv95m2v4-1b6h/node_modules/.pacquet/node@22.23.3/node_modules/node/bin/node'
# Prefix every go test below with ../scripts/go-test-with-agent-cli-guard.sh
```

1. `go test ./internal/daemon -run '^TestWorkflowValidationDescendantExitsOnTimeoutAndShutdown$' -count=1 -v`
   RED6.315s: both timeout and shutdown reported authored validator PID76949/76952
   survived Go cancellation. Test cleanup killed those exact newly owned PIDs.
   Root cause: validateSource detached its validator into a new PGID while Go
   owned only the CLI PGID; CLI's early stop lost the JS timeout owner. Minimal5a
   lifecycle change: CLI validation joins Go's group; standalone validation keeps
   its separate bounded group; process-exit cleanup also kills the validator.
   Build then same test GREEN1.894s (1.50s timeout /0.09s shutdown). Final race
   rerun likewise observed validator exit. The controller had already removed
   original orphan63177/63853/64043 before dispatch; no other old resources removed.
2. First focused Go compile found saved live fixture using UUID `.String` method
   values as strings. Fixed test fixture to uuidToString; no product API change.
3. Actual engine rotate503 traced to POST `/api/v1/users/login`302 with an existing
   valid session cookie. `TestWorkflowWorkerProvisioningRenewsCookieLoginWithoutRedirect`
   RED `credential login1: hatchet: HTTP302`; reset only the TenantService-owned
   private Client/http.Client jar for explicit credential login. All production
   accesses hold s.mu; no shared-client concurrency or redirect policy relaxed.
   Same test GREEN; actual real SQLTenantService/mTLS engine proof then PASS25.46s.
4. `go test ./internal/handler -run '^TestWorkflowEnrollmentRejectsMachineIdentityAndPreservesAgentRegistration' -count=1 -v`
   RED expected400/got200 on task identity enrollment. Added machine identity gate
   and kept successful ordinary mdt runtime registration200 with no control token.
   GREEN in focused and final race matrices.
5. `go test ./internal/handler -run '^TestWorkflowRevocationDuringEngineCheck' -count=1 -v`
   RED both rotate/revoke paths: old generation activated version1 before rejected
   lease update. Added final serialized generation recheck. Then
   `-run '^TestWorkflowLeaseFailure'` RED `failed lease committed activation:
   version=1`; extracted service AcknowledgeRevisionTx and made activation+lease
   atomic. Both regressions GREEN; injected failed lease leaves version0.
6. `-run '^TestWorkflowConflictingAcknowledgement'` RED `conflict not visible:
   pending`; preserve invalid CAS result and exclude it from leased registrations
   within that same transaction. GREEN, current competing revision unchanged.
7. `go test ./internal/daemon -run '^TestWorkflowManagerStopsWorkerError' -count=1 -v`
   RED `failed worker renewed registration lease`; consume actual IPC error/closed
   channel before renewal and stop/back off the owned process. Same command GREEN.

Fixture corrections, not product bugs: private t.TempDir needed a nested0700 cache;
workflow_daemon has no single id column and uses explicit fixture Exec/cleanup;
TLS probe fixture needed HTTP2 ALPN enabled; running run fixture needs immutable
submission timestamps; principal is immutable (use a separate collaborator and
remove its membership); cancellation requires paired immutable actor/timestamp.
These unsuccessful test-development runs were corrected before final GREEN.

## Initial focused verification receipts (before staged continuity correction)

From repository root with command-local PATH pointing at the Node22 directory:

```text
pnpm --filter @multica/workflow-sdk --filter @multica/workflow-runner test
  SDK12 passed; runner9 passed (5 files/21 tests), exit0
  runner script also builds SDK and runner via tsc, exit0
pnpm --filter @multica/workflow-sdk --filter @multica/workflow-runner typecheck
  both tsc --noEmit exit0
make sqlc
  first proxy.golang.org request timed out; exit2
GOPROXY=https://goproxy.cn,direct make sqlc
  generated successfully, exit0
git diff --check
  no output, exit0
```

Guarded commands from `server/` with the explicit environment above:

```text
go test ./internal/daemon ./internal/handler ./internal/workflowrunner ./internal/hatchet -run Workflow -count=1 -v
  daemon3.940s; handler1.321s; workflowrunner0.467s; hatchet0.244s PASS
  opt-in live fixture skipped here; it executed separately below
go test ./internal/service ./cmd/server -run Workflow -count=1 -v
  service0.348s; cmd/server1.320s PASS, actual named DB-backed execution
go test ./cmd/migrate -run 'Workflow|ConcurrentIndexCleanups|EveryConcurrent' -count=1 -v
  isolated WorkflowDurableContracts up/down + no-FKs and concurrent-index checks
  all PASS0.463s
go test -race ./internal/daemon ./internal/hatchet ./internal/workflowrunner -run 'Workflow|HatchetTenant|HatchetClient' -count=1 -v
  daemon4.818s; hatchet1.581s; workflowrunner1.804s PASS; no race reports
  TestHatchetTenantSQLFences skipped (its separate destructive scratch DB unset)
go test ./cmd/server -run '^TestWorkflowControlRoutesAreDedicatedAndFailClosed$' -count=1 -v
  PASS0.432s; six workflow paths503 disabled, generic /api/workspaces rejects wdc401
go test ./internal/handler -run 'TestWorkspaceDeletionManifestCoversPublicSchema|TestDaemonRegister_WithDaemonToken|TestDaemonRegister_RecordsRuntimeProfileRegistrationFailure' -count=1 -v
  four named tests PASS0.514s
go test ./internal/daemon -run 'TestRegisterRuntimes_PiAndOmpBothReachTheServer|TestConcurrentRegisters_SameWorkspaceSerializedSoRecordMatchesServer|TestRefreshAgentVersions_DoesNotDisturbTheRuntimeSet' -count=1 -v
  three ordinary registration/version-refresh tests PASS0.340s
```

The initial handler race+live command (same explicit DB/Node and guard; the final
real-manager test was subsequently moved into internal/daemon):

```sh
MULTICA_WORKFLOW_CONTROL_LIVE=1 HATCHET_BIN_DIR=/tmp/multica-task5a-hatchet-bin \
../scripts/go-test-with-agent-cli-guard.sh go test -race ./internal/handler -run '^TestWorkflow' -count=1 -v
```

```text
actual Go packaged IPC runtime Node22.23.3
engine initial run GET404 (55c8d4b4-9f17-4d00-b528-0f368e8dcf20), then bounded polling succeeds
completed revision=1 output={"value":{"attempt":0,"revision":1}}
completed revision=2 output={"value":{"attempt":1,"revision":2}}
cross-workspace fetch401; revoked ack401; revoked product action410
TestWorkflowGoPackagedControlLive PASS22.86s
all additional named Workflow handler regressions PASS; no race reports
ok .../internal/handler24.701s
```

That initial live test used production SQLTenantService, tenant worker-only JWTs, actual Go
IPC and actual packaged Node22 mTLS registration. Server independently reads real
registered versions before ack/publication/eligibility. A queued R1 survives
replacement with R1+R2; both execute native durable sleep, and R2 deliberately fails
attempt0 before completing attempt1. Exact output values are assertions. It then
revokes the product generation, checks immediate eligibility/action denial and
stops the actual worker with bounded drain. Manager removal/disable/denial/error
paths separately use observable real Node IPC processes, including a worker that
ignores both drain and SIGTERM; forced stop waits for exit (~1.07s).

One earlier race+live run failed15.36s on the first immediate GET404 after accepted
submission, run `d330cdde-5132-456e-9d8a-9a83470d24c8`. It failed before any retry
scenario, not a90-second retry timeout. Test-only polling now retries exactly404
within the same90-second deadline; all other errors fail. The final run exercised
this branch then succeeded. Previous nonrace integrated live also passed25.59s.
5a's separate unexplained90-second deliberate-retry timeout remains unresolved;
this report does not label it fixed.5b timeout failures retain the last engine
detail under `/tmp/multica-5b-retry-failure-*.json`; none occurred in these completed
retry runs.5a's more detailed task/event/wait timeout diagnostics remain available.

## Staged continuity correction and final evidence

Controller review correctly rejected the initial stop-before-validation design:
keeping old revision source in a replacement descriptor did not preserve an
already-running worker/action. The final staged pair/prepare protocol above
replaces that design. Additional test-first/mutation receipts from server/ (same
guard and explicit DB/Node environment):

```text
go test ./internal/daemon -run '^TestWorkflowInvalidDraftPreservesHealthyOldGeneration$' -count=1 -v
  RED0.31s: invalid draft stopped healthy immutable old generation
  GREEN after validating/caching before replacement
go test ./internal/handler -run '^TestWorkflow' -count=1 -v
  staged rotation RED: previous checked generation ack401, expected200
  GREEN: current+candidate, then current+retiring, only one current publisher
go test ./internal/handler -run '^TestWorkflowAuthenticatedControlRenewalPreservesActiveGeneration$' -count=1 -v
  RED: authenticated routine enrollment changed active control authority
  GREEN: possession-backed renewal preserves hash/token and checked generation
go test ./internal/daemon -run '^TestWorkflowCurrentCrashDoesNotForceStopValidRetiringWork$' -count=1 -v
  RED3.391s: current crash force-stopped valid retiring work
  GREEN1.379s: stop/revoke only failed current, retain retiring and back off
go test ./internal/handler -run '^TestWorkflowFailedCurrentRecoveryWaitsForRetiringGeneration$' -count=1 -v
  GREEN: failed-current exact retire; retiring ack200; replacement rotate409;
  old exact retire frees pair; subsequent rotation/ack becomes eligible
go test ./internal/daemon -run '^TestWorkflowCandidateCacheFailurePreservesHealthyCurrentAndLease$' -count=1 -v
  final source review RED0.22s: candidate cache failure stopped healthy current
  deterministic regular-file cache target prevents private directory creation
  fix: candidate-local preparation backoff, not global capability-loss stop
go test -race ./internal/daemon -run '^TestWorkflow(CandidateCacheFailurePreservesHealthyCurrentAndLease|InvalidDraftPreservesHealthyOldGeneration|RetirementUsesCredentialDeadlineWithoutStoppingCurrent|CurrentCrashDoesNotForceStopValidRetiringWork|ManagerStopsWorkerErrorBeforeRenewingEligibility)$' -count=1 -v
  all five PASS4.046s, no race reports; cache regression PASS0.14s
  existing checked current stays alive, renews its lease once, no rotation,
  candidate stays unpublished/ineligible and preparation backs off
```

The final race run also executes
`TestWorkflowPreparedCandidateDoesNotPublishOrBecomeEligible`,
`TestWorkflowRetiringGenerationCannotPublishNewRevision`, and
`TestWorkflowRetirementUsesCredentialDeadlineWithoutStoppingCurrent`. Preparing
does not change current revision/Eligible; retiring cannot add evidence or publish;
the original credential expiry bounds retirement and releases backpressure without
stopping current. If current has failed while retiring remains, the failed current
is independently retired; no third generation is minted until retiring exits or
reaches that original deadline. Recovery is then allowed, not permanently wedged.
No arbitrary normal-edit drain timer was substituted for this bound.

Actual gated-manager negative mutation (ActivationGate false instead of true) ran
the live test and failed initial-manager-generation readiness after26.36s. Restored
the gate. The live test's prepare wrapper separately inspects the real server-only
worker list and refuses a candidate worker before its checked action lease. This
receipt is a readiness failure, not a claim of independently observing a denied
business operation under that mutation.

The final `TestWorkflowGoPackagedControlLive` is in internal/daemon and drives the
real workflowManager rather than manually composing replacement. It uses the
production SQLTenantService, exact-pin engine, tenant worker-only JWTs, packaged
Node22.23.3, Go private IPC and mTLS. It admits R1, confirms native durable sleep
is actually evicted, holds another R1 inside a real SDK HTTP action, introduces an
invalid draft and checks the old worker stays alive, then stages valid R2. A
45-minute refresh is deferred while the pair is occupied. After switch, the hot
R1's next action is authorized on the same retiring generation; sleeping R1 resumes
on the replacement and its action is authorized there. Exact R1 output remains
revision1/attempt0 in both cases; R2 deliberately retries to revision2/attempt1.
The purpose-specific Action hook checks only the5b foundation, not Task6's pending
exact-current-assignment/business binding. Revocation then immediately denies
eligibility/ack/actions and manager cleanup observes worker exit.

Failed sleep-proof receipts are intentionally retained:

- The12-second sleep fixture failed waiting for acknowledged eviction after24.52s,
  engine run1053355b-4309-4905-b958-3b8e95d6648b. The fixture now uses45-second sleep
  and180-second step timeout and asserts actual eviction before transition.
- Later45-second runs failed the final FIFO observation assertion after67.86s and
  69.18s even though all asserted outputs completed. Diagnostic engine sleep run
  e8ca0597-cecd-49de-8a63-0ed6bc9290a3; old generation
  484a2836-4a07-4aef-a8a5-8389c13733e7; current generation
  88df16b1-26d7-4e8f-9e7c-462858feaec0; hot product run
  1f8f01b5-1c43-44ee-9890-8e39b87bd463; sleeping product run
  c5d139cd-3bd4-47d3-9b18-b41bfaea6221. The observed action had the correct new
  generation but product run40bc6915-290f-4fcf-a11e-b4dd84ea5cff, which was R2.
  Source inspection confirmed AdmitRun auto-submits when Engine is configured, so
  R2's action can arrive before sleeping R1 irrespective of polling order. The
  fixture now collects observations by exact product-run ID AND generation; it
  does not relax eviction, authorization or output assertions. GREEN71.02s.

Final scoped race command from server/:

```sh
DATABASE_URL='postgres://postgres:task3_disposable@127.0.0.1:55510/multica_task3?sslmode=disable' \
MULTICA_WORKFLOW_CONTROL_LIVE=1 HATCHET_BIN_DIR=/tmp/multica-task5a-hatchet-bin \
MULTICA_WORKFLOW_TEST_NODE='/Users/ranlychan/Library/Caches/pnpm/dlx/82782474caa34688d1e0c2131af20406/mv95m2v4-1b6h/node_modules/.pacquet/node@22.23.3/node_modules/node/bin/node' \
../scripts/go-test-with-agent-cli-guard.sh go test -race ./internal/daemon ./internal/handler ./internal/hatchet ./internal/workflowrunner ./internal/service ./cmd/server -run 'Workflow|HatchetTenant|HatchetClient' -count=1 -v
```

```text
PASS daemon77.355s; handler3.089s; hatchet1.276s; workflowrunner1.489s;
     service1.960s; cmd/server3.904s; exit0; no race reports
actual staged live PASS69.91s, Node22.23.3
sleep initial engine GET4045d1c1f30-858e-43c5-bdaf-22f17da8d9ca, bounded poll succeeds
sleep acknowledged evicted before replacement
old generation00c7afa3-73a5-49e6-b4f2-958e32c66720
new generation616f420d-4212-4f74-b24d-63cddd3ba008
hot product runcec70f43-f575-4669-b285-88c976d93e7c: after action on old generation
sleep product run057718d9-0dfc-4a91-a68e-6008f940cf2a: after action on new generation
R2 product run47a3cdd6-1526-4ebb-85ba-c4d8079953d6 arrives before sleeping after action
R1 outputs {"value":{"attempt":0,"revision":1}} twice
R2 output {"value":{"attempt":1,"revision":2}}
cross-workspace401; revoked ack401; revoked action410; owned workers exited
eight dedicated routes503 when disabled; generic /api/workspaces wdc401
HatchetTenantSQLFences skipped: separate destructive scratch DB environment unset
```

Latest other covering checks:

```text
go test ./cmd/migrate -run 'Workflow|ConcurrentIndexCleanups|EveryConcurrent' -count=1
  PASS0.525s: actual isolated up/down597–601, no FKs, both-direction index hooks
  earlier RED caught600 hook incorrectly in up-cleanups; moved to down-cleanups
PATH=<explicit Node22 directory>:$PATH pnpm --filter @multica/workflow-sdk --filter @multica/workflow-runner test
  SDK12 + runner9 =21 tests PASS, SDK/runner tsc builds exit0
PATH=<explicit Node22 directory>:$PATH pnpm --filter @multica/workflow-sdk --filter @multica/workflow-runner typecheck
  both tsc --noEmit exit0 after final staged TS changes
go test -race ./internal/daemon -run Workflow -count=1 -v
  amended cache-routing code: PASS7.485s, no race reports
  cache regression0.15s; current-crash1.07s; validator descendant1.59s
  opt-in live skipped in this local-only rerun; unchanged staged live69.91s above
```

## Self-review, resource state and limits

Reviewed final schema/control/query bindings, lock order, no engine network under
row locks, targeted undisclosed-token cleanup, unchanged author/CAS gate,
activation/lease atomicity, generation/tenant/source/graph/action evidence, HMAC
body binding, frozen human authority and current per-call membership, local owned
process shutdown and source-cache permissions. Fresh regression evidence covers
the discovered leak, login and acknowledgement races, phase/activation-gate
boundary, routine retirement vs explicit force-stop, failed-current recovery and
candidate-local cache failure. No unrelated hardening or
business-action implementation was added.

Exact engine pin is377de5d9642933b835990b1e2bea81d08f85c943. Fresh
`(cd /tmp/multica-task5a-hatchet-bin && LC_ALL=C shasum -a256 -c BINARIES_SHA256)`:
hatchet-admin/migrate/api/engine all OK; existing PIN/PATCH_SHA256 receipts retained.
Each live invocation created only its own disposable PostgreSQL15.6 container and
supervisor children and removed them on exit. Final audit finds no workflow CLI,
validator or Hatchet child; only existing `multica-task3-pg`127.0.0.1:55510 remains.
Application DB data/migrations and the exact binaries/temporary Node22 are retained.
No real agent/provider command passed the guard.

Limitations: Windows private IPC deliberately fails closed until a named-pipe
owner is implemented; this proof ran macOS. Arbitrary workflow code retains OS-user
privileges and can deliberately escape normal process ownership by its own OS
actions; this is not an OS sandbox or arbitrary-process cessation guarantee.
Task2b's5-second new-auth cache / observed7-second rejection does not force-close
established engine streams. Product authorization and local generation stopping
remain independent. The final live proof drives real manager staged transitions,
old in-flight actions and evicted sleeping revision resumption using actual
packaged mTLS workers. It does not run the complete Daemon.Run/main loop or an
engine-crash/restart E2E. Full daemon/engine crash coverage and broader UI
durability are Task8. Product action business durability is Task6. Root frontend
suite failures disclosed by5a were not rerun or claimed fixed; broad suites remain
Task8. No whole-root test claim is made here.
