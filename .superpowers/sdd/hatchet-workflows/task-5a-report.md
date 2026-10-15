# Task5a — TypeScript SDK and packaged worker

2026-10-15. Stage5a only; Go daemon/handler integration remains5b, product action
authorization/business logic6, authoring CLI7, UI8. Recovered the original agent's
saved uncommitted package implementation after its capacity error. No reset,
replacement implementation, sibling checkout mutation, real agent/provider CLI,
shared service write, `.env`, push or merge. Controller reviews independently.

## Deliverables and current checks

- `packages/workflow-sdk`: typed JSON/context/definition API, graph validation,
  stable author operation keys, bounded waits/logging, redaction, tests and docs.
- `packages/workflow-runner`: credential-free validation child, packaged IPC CLI,
  graph/source verification, tenant TLS/mTLS worker, REST deny adapter, product
  action transport/signing, actual registration collector, tests and docs.
- `server/internal/hatchet/node_worker_live_test.go`: opt-in test-only engine
  bridge; no Go production daemon/handler/action code changed.
- `pnpm-lock.yaml`: exact SDK/esbuild/acorn and new transitive resolutions.
  Existing importer/snapshot dependencies were restored after detecting pnpm
  re-resolution churn. Final diff362 insertions/2 deletions: the deletions replace
  semver7.8.5's optional-only snapshot with a nonoptional snapshot now used by SDK.
  Frozen offline install passed. No `pnpm-workspace.yaml` change needed: its
  existing package glob already includes both packages.

Fresh focused verification from repository root:

```text
pnpm --filter @multica/workflow-sdk --filter @multica/workflow-runner test
  workflow-sdk: 1 test file, 8 tests passed
  workflow-runner: builds SDK and runner, 4 test files, 9 tests passed
pnpm --filter @multica/workflow-sdk --filter @multica/workflow-runner typecheck
  both tsc --noEmit exit0
pnpm --filter @multica/workflow-sdk --filter @multica/workflow-runner build
  both tsc exit0 (also exercised by runner test script)
pnpm install --offline --frozen-lockfile --ignore-scripts --filter @multica/workflow-runner
  Lockfile is up to date, resolution step is skipped; exit0
git diff --check
  no output, exit0
```

Runtime: Node26.7.0; pnpm command delegates to repository packageManager10.28.2.
Required engine floor is Node22. This proof did not separately execute Node22.

## RED/GREEN evidence from recovery

Original pre-recovery RED receipts were not saved in the supplied files; do not
infer or claim their commands/results. New behavioral changes used observed RED:

1. Added `src/packaged.test.ts`, ran
   `pnpm --filter @multica/workflow-runner exec vitest run src/packaged.test.ts`:
   FAIL `packaged CLI exited before response`. Distributable API validation
   already passed; the declared `dist/cli.js` was missing. Added private IPC CLI,
   then the test passed. An initial suspected validator path issue was discarded:
   `../dist/child.js` resolves correctly from both source and compiled directory.
2. Fresh package build failed TS2322/TS2769 for `${number}ms` Duration arguments;
   then TS2353 for unsupported inline durableTask `evictionPolicy`. Public
   DurationObject `{seconds: ms/1000}` preserves milliseconds; the public returned
   durable task declaration accepts its typed evictionPolicy field. Build/typecheck
   became GREEN. No SDK patch/private import or Go action rename was needed.
3. Added computed-import regression and ran
   `pnpm --filter @multica/workflow-runner exec vitest run src/validate.test.ts`:
   FAIL `promise resolved ... instead of rejecting`. esbuild preserves computed
   imports inside deferred callbacks without warning. Added emitted-JS AST import
   checking using pinned acorn; GREEN. Import policy is not an OS sandbox.
4. Added missing mutation-key regression; `pnpm --filter @multica/workflow-sdk test`
   failed `promise resolved null instead of rejecting`. Mutating calls now reject
   an undefined author key before transport; GREEN.
5. Controller supplied previously omitted exact public API requirements from
   `docs/engineering/hatchet-workflows.md:226`. Added desired-status/approved-name
   regression; SDK test failed `ctx.issues.waitForStatus is not a function`.
   Renamed the unreleased API, implemented selected issue status waits and removed
   unused list/update/comment endpoints; GREEN. No compatibility aliases shipped.

## Exact SDK artifact and engine provenance

Engine pin `377de5d9642933b835990b1e2bea81d08f85c943` (v0.109.14). Only binaries in
`/tmp/multica-task5a-hatchet-bin` were used. `PIN`, `PATCH_SHA256`, and all four
`BINARIES_SHA256` receipts passed supervisor verification and independent
`LC_ALL=C shasum -a256 -c BINARIES_SHA256` (all four OK). Patch SHA256:
`92560c9627e5783e5aec865fd070ee4690cc0c909d13dab07c564542c48248d4`.

```text
hatchet-admin   bb71677fb596291e8c1bf409b83d6a8c6afb114b0aaec35a44fe53bfe1cf62cb
hatchet-migrate 8dbf7e790488aea2e5c2e8e70029e40c5894e07b82b543ba0006a283785b90de
hatchet-api     94c1a623f5203fc4f1569420f2157c2c3863ac4688181ab4dda5804d6937c6c2
hatchet-engine  cf4613e1ca2b7c768393cf061a78deced3982cd7c744dcb31d6805089083bc73
```

SDK exact npm pin `@hatchet-dev/typescript-sdk@1.34.0-alpha.2`; official registry
metadata (`pnpm view ... --registry=https://registry.npmjs.org --json`) agrees with
lock integrity `sha512-cM9esZxPaJjds4+Z2qm6g7BLiMxaNlDurlyOZsuuNGpFvvvDp3DrU0LnuubtvUXG8mRSW/vMTF04uXJapBo+ZA==`,
SHA1 `46c1ee0106939f5ae935c4bfb62282f372f34df5`. Reproducible artifact command:

```text
LC_ALL=C node packages/workflow-runner/test/verify-sdk-artifact.mjs /Users/ranlychan/Projects/multicaflow/hatchet
```

Downloads the official tarball, verifies the entire SHA512, byte-compares eight
installed critical JS files with that archive, then compares normalized compiler
emit from exact-pin `git show` source after the pinned publish command's alias
rewriting. All eight MATCH: client, declaration, worker context, worker lifecycle,
REST factory, Node transport, dispatcher action names, duration implementation.
This is scoped source correspondence, not a claim that every tarball file was
rebuilt byte-for-byte. An initial unrewritten transpilation differed only in
documented alias requires; alias normalization produced eight MATCH. Initial tar
invocation emitted locale warnings; LC_ALL=C removes those, hashes unchanged.

## Real packaged Node proof

```text
cd server
MULTICA_NODE_WORKER_LIVE=1 HATCHET_BIN_DIR=/tmp/multica-task5a-hatchet-bin GOPROXY=https://goproxy.cn,direct go test ./internal/hatchet -run '^TestPackagedNodeWorkerLive$' -count=1 -v
--- PASS: TestPackagedNodeWorkerLive (87.20s)
ok github.com/multica-ai/multica/server/internal/hatchet 87.429s
```

Own disposable PostgreSQL15.6 container and supervisor children only; all removed
on exit. Existing `multica-task3-pg` port55510/database596 was retained untouched.
Test-only Go provisioning uses cookie tenant/token APIs to mint separate tenant
worker/server tokens. Only the tenant worker token enters private Node IPC; no
provisioning password/cookie or server token enters the packaged worker.

Actual mTLS unary registration and streaming task/log/durable operations succeeded.
Wrong serverName and absent client certificate were rejected. Worker descriptor
uses reachable127.0.0.1:dynamic-port, certificate identity `cluster`, explicit CA,
worker certificate/key paths. Wildcard dial addresses fail descriptor validation.

Configured engine REST `http://127.0.0.1:1` is unreachable. Test preload observes
the actual SDK Axios adapter and blocks every http/https request before wire;
gRPC uses HTTP/2. Each ready worker observed constructor probe denied1/wire0;
after actual task/log/sleep/retry/drain, final counter was denied2/wire0. Both
counter wrapping and wire guard are test-only, not a private REST proxy or fake
engine response. This is process-level REST denial, not network-namespace OS
isolation; authored code remains trusted OS-user code.

Native workflow/task naming was checked both before gRPC PutWorkflow and against
actual returned engine task actions. Go WorkflowActionName is logical alias:
`<immutableName>-step-<sha256(stepID)[0:16]>`; native SDK adds workflow prefix to
produce `<immutableName>:<logicalAlias>`, matching Go reconciler full action IDs.
Actual registry responses yielded distinct workflow/version IDs for both revisions,
and re-registering old revision yielded its same IDs. Collector also enforced
exact parent aliases, empty cron/event triggers, and TTL/CEL registration.

Final old run `afc3fd1f-741c-4664-8b82-5e95a971d286`, first task
`9f64b8c5-d2eb-407f-b5f7-b4be021a3aa4`. Pre-crash persisted WAIT_FOR at
`2026-10-15T14:05:31.313349+08:00`, isSatisfied=false, SLEEP45000ms. Harness
waited for actual task isEvicted=true (10s cap), rechecked RUNNING and remaining
sleep >20s, SIGKILLed old worker and awaited its exit, then started replacement.
No old-revision worker overlapped the outage. The independent new-only worker
registered revision2 while revision1 slept; replacement registered both, then
new-only worker was killed. Old run resumed/replayed in replacement with parent
`{attempt:0, revision:1, run:<reservedMulticaRunId>}` and final revision1. Engine
contained two corresponding gRPC log records. New revision and deliberate retry
both completed with parent revision2/attempt1, proving `retryCount()` audit mapping
is zero-based. Task result parents are wrapped/unwrapped as `{value: JSONValue}`.

Observed exact engine idempotency is `{expression:"input.multica_run_id",
method:"TTL",ttlMs:3600000}`. Reserved envelope has nested authored `input`.
Duplicate submission returned500, matching pinned native behavior; product must
reconcile ambiguous/duplicate submissions through the local ledger, not expose
this engine response as product semantics. A clean drain acknowledgement followed.

Earlier findings retained: first overlap-start proof passed73.64s but did not prove
strict outage. A kill before durable eviction acknowledgement timed out193.49s;
persisted sleep alone did not imply released worker ownership and a later task
was RUNNING/retry1. The final proof intentionally covers an acknowledged suspended
sleep; arbitrary crash intervals/engine restarts are not newly proved here. An
intermediate improved proof recovered old run then failed on immediate retry GET
before run visibility; final bounded polling retries404 only and fails other
non200 statuses. No product fix was made for these harness observations.

## Public API and5b/6 handoff

SDK exact surface and example are in package README/contracts. Desired issue
status is any nonempty workspace status key up to128UTF8 bytes; it is compared
exactly to `issues.get` result.status. Internal operations are issues.create/get,
agents.run/get, autopilots.invoke/get. Public agents.dispatch/autopilots.run map
to those internal names; Task6 should normalize run statuses to the documented
completed/done/succeeded success or failed/cancelled failure values.

Author mutation key identity is SHA256(JSON.stringify([runId,stepId,authorKey])),
excluding retry/invocation. Attempt is audit-only. Stable request/error mapping
and409 behavior are tested against a purpose-specific fake HTTP product server;
Task6 owns atomic payload/operation mismatch conflict and per-call authorization.

IPC: launch `node <absolute dist/cli.js>` with a private Node JSON IPC channel,
not descriptor argv/stdin. Node fork uses fd3; Go can supply private fd3/socket
with NODE_CHANNEL_FD=3 and NODE_CHANNEL_SERIALIZATION_MODE=json, newline-framed
JSON. ParentMessage/ChildMessage contracts are in runner contracts.ts. Only
validated/ready/registration/error/drained metadata is returned. Go5b must bound
messages, verify child identity/source/hash/graph/engine evidence, maintain live
generation eligibility, independently kill/back off children and stop removed or
revoked workspace workers. Stdout/stderr must be discarded/bounded, never
forwarded as product logs. IPC framing was exercised with native Node fork here;
actual Go pipe/socket launcher integration is5b's responsibility.

Secret descriptor contains tenant token/TLS paths and product runner token,
generation/workspace, immutable revisions/source cache paths/graphs. Readiness
binds generation/workspace/tenant + source hash/graph digest/action set + engine
workflow/version IDs. It is not itself trusted publication authority. Graph digest
canonicalization is compact JSON with IDs and parent lists sorted by JavaScript
UTF16 comparison (Go must match this for non-ASCII IDs).

Product transport: POST `/api/workflow-runs/{run_id}/actions`, body
`{protocol:1,run_id,step_id,attempt,operation,operation_key?,payload}`. Headers:
Authorization bearer runner token, X-Workspace-ID,
X-Multica-Workflow-Generation, X-Multica-Workflow-Signature. Signature lowercase
hex HMAC-SHA256(token, `POST\nPATH\nWORKSPACE\nGENERATION\nEXACT_JSON_BODY`).
Success `{result:JSONValue}`. Stable status mappings401 unauthenticated,
403 forbidden,404 not_found,409 operation_conflict,410 capability_revoked,
429 rate_limited,503 unavailable; prefix each with `workflow_`. Each action must
revalidate product authorization irrespective of existing engine streams. Task2b
engine cache default5s/new authentication rejected after7s; established streams
can still receive tasks after revocation. Do not use token revoke as force-close.

## Self-review, limits, and broader-suite result

Reviewed source/hash/graph binding before credentials register, public SDK-only
engine construction/transport, exact fullaction naming, tenant JWT subject check,
TLS-only descriptor, fail-closed REST, mutation key behavior, redacted typed errors,
JSON/source/graph/log limits matching Go exported constants, and distributable
entrypoints. Definition validation runs with only LANG inherited, independently
timed/outbound output bounded; it never constructs a credentialed engine client.
Worker source remains trusted code and can access OS resources by other means.

Default/maximum drain10/30s, CLI hard deadline31s; final live proves clean drain,
not a separate stuck-task forced-kill/unregister matrix. Go5b must test force kill,
crash backoff, missing Node, stale cache/ack, rotated/removed capability, independent
registration authentication and preserved normal-agent health. No Task5b Go tests
were claimed. The16–17 focused tests are not exhaustive policy combinations.

`pnpm test` was also launched before controller's narrower-check instruction.
It failed (Turbo exit1,26.444s), so repository-wide verification is NOT GREEN.
Actual logs: apps/web/.turbo/turbo-test.log and packages/core/.turbo/turbo-test.log.
Web failed suite `lib/use-case-locale-fallback.test.ts`: Cannot find package
`@/.source`, imported by lib/use-cases-source.ts. Web274 tests passed/1 suite failed.
Core displayed these nine failures before Turbo stopped (not silently exempted):

- realtime/use-realtime-sync-ws-instance.test.tsx: ignores the event for a delete
  this client initiated; still cleans up for a delete initiated elsewhere; destroys
  the local search copy when this user is removed.
- workspace/mutations.test.tsx: leaves list cache untouched while DELETE pending;
  invalidates workspace list after successful delete; clears deleted slug storage;
  leaves storage/cache untouched when DELETE fails; keeps self-initiated marker
  after success and lifts it after failure.
- issues/mutations.test.tsx: applies normal issue-create cache coordination.

Also observed existing frontend localStorage/act/Node26 warnings. No unrelated
production fixes or reruns were made; controller/Task8 retains these outstanding
root-suite issues. Dependency install warned about existing Vite/Next peers and
new runner's Vite optional esbuild peer mismatch; package builds/tests and actual
esbuild execution passed with pinned0.25.12. Exact transitive lock differs from
upstream source lock (e.g. grpc-js1.14.6/protobuf2.16.0); tested installed graph is
the committed resolution, not an assertion of upstream lock equivalence.
