# Task4 implementation report

Status: DONE_WITH_CONCERNS. Scoped implementation and supplemental fairness/publication fixes are complete; broad-suite failures remain for final integration. Base: f257dee10 on feat/hatchet-workflows. Initial commit: `17a86796b feat(workflows): dispatch and reconcile durable runs`.

## Implemented behavior and integration

- `WorkflowService.ClaimScheduledRuns` locks due workspace-scoped triggers with SKIP LOCKED, rechecks/locks the active workflow, snapshots immutable current revision/hash/input/human principal, inserts the local UUID ledger, and advances the schedule in one transaction. Existing `(trigger_id,planned_at)` rows are reused. Concurrent delivery/lease reentry cannot create a second local run. Missed occurrences collapse to the stored planned occurrence and next cron occurrence after the supplied DB time. Missing membership produces skipped `workflow_principal_departed`; changed rights produce skipped `workflow_forbidden`; invalid registration produces skipped `workflow_unrunnable`; absent health/eligibility produces skipped `workflow_engine_unavailable`. Paused trigger and archived/paused workflow do not fire.
- `SubmitRun(ctx, RunIntent{WorkspaceID,RunID})` loads only committed local intent. Manual/replay/API `AdmitRun` commits before invoking it when Engine is configured. Scheduled runs are submitted by the persistent reconciliation scan, recovering a crash between schedule commit and submission.
- The immutable submission start/deadline are persisted before network calls using PostgreSQL time. `service.WorkflowEngineIdempotencyTTL = time.Hour` is the registration TTL required from Task5. The submission window is 55 minutes, requests are limited to 30 seconds, and attempt claims require more than 30 seconds remaining. Five durable attempts maximum, 45-second durable retry leases. The request context starts before claiming, preventing a suspended process from awakening after the lease with a fresh network budget. Retries never renew the original window; after deadline or attempt exhaustion only discovery continues.
- Discovery precedes every retry. HTTP/network error, response loss, discovery failure or eventual OLAP absence never fabricates a failed/completed run. Accepted response loss remains submitting until engine identity is discovered. A late old lease owner's direct response is fenced by attempt count and unexpired lease in `AcknowledgeWorkflowSubmissionAttempt`; discovery can still acknowledge the accepted run. Terminal CAS and existing database guards preserve immutable terminal states and snapshots.
- Durable cancellation requests survive submission races. Never-attempted queued intent can become cancelled locally via an atomic guard. Ambiguous/submitting intent remains active until discovery attaches the late engine ID, cancellation is sent, and Status confirms terminal outcome. An engine cancel call alone never finalizes the local run.
- `workflowHatchetEngine` adapts existing pinned Client Submit/Reconcile/Status/Detail/Logs/Cancel methods. Only server tenant credentials from Ensure are used. Immutable revision HatchetName, local run UUID, and nested user input are supplied unchanged. Discovery filters matching idempotency key and tenant, rejects invalid/conflicting identities. Logs map action names back to known authored step IDs, normalize severity and redact/bound entries; output uses existing recursive product sanitization. Step projection retains only bounded id/status fields. Raw engine JSON/credentials do not cross the service boundary.
- `main.go` wires supervisor health, tenant transport and persisted log reader; registers 30-second `workflow_schedule_dispatch` and `workflow_run_reconcile` jobs on the existing scheduler. Each workspace is an independent scope. Scheduler stale reentry is safe because local transactional deduplication and durable submission attempt fences remain authoritative.
- **Task5 seam:** `WorkflowService.Eligible func(context.Context, pgtype.UUID /*workspace*/, pgtype.UUID /*revision*/) bool`. Nil is fail closed. Task5 must inject it only after verified workspace-specific daemon capability and immutable registration, matching `WorkflowEngineIdempotencyTTL`. Main sets Ready and Engine but deliberately leaves Eligible nil. A connected daemon or healthy engine alone does not enable runs.
- **listeners.go file-map discrepancy:** unchanged by controller ruling. Workflow handlers currently publish no workflow bus events. Existing workflowRunView only returns local product fields; detail input and log endpoints sanitize bounded product projections. Task8 will poll the local ledger/projections for live views, with bounded refresh lag. There is no new raw-engine event requiring listener projection. No cosmetic listener edit was made.

## Files

Production: server/internal/service/workflow.go; new server/internal/service/workflow_runtime.go; new server/cmd/server/workflow_schedule_job.go and workflow_reconciler.go; server/cmd/server/main.go.

Storage: server/migrations/593_workflow_runtime_projection.{up,down}.sql plus supplemental migrations 594–595; server/pkg/db/queries/workflow.sql; regenerated server/pkg/db/generated/{models.go,workflow.sql.go}; migration-contract upper bound advanced through 595. Migration 593 adds submission_attempts/submission_lease_until/log_projection/step_projection; 594 adds durable scan progress, and 595 separately creates its partial index concurrently. No foreign keys were added.

Tests: new server/cmd/server/workflow_schedule_job_test.go and workflow_reconciler_test.go; migration contract bound in server/cmd/migrate/workflow_contract_test.go. Existing service/handler Workflow tests, including HTTP replay/human-authority coverage, were run unchanged.

## Exact verification environment

All Go commands ran with:

`DATABASE_URL='postgres://postgres:task3_disposable@127.0.0.1:55510/multica_task3?sslmode=disable' GOPROXY=https://goproxy.cn,direct`

Disposable `multica-task3-pg` database retained and advanced through migration595. Explicit DB was reachable: actual cmd/server verbose output lists all tests and PASS (no silent TestMain skip). No .env loaded or changed. No live authored workflow, provider CLI, SDK worker, real engine or existing shared container was run/modified. Guard wrapper used for final tests. No temporary engine resources were created.

## RED evidence

From `server`, `go test ./cmd/server -run 'WorkflowSchedule|WorkflowReconcile' -count=1`:

First invocation failed to build due missing runtime interfaces/methods. Minimal runtime interface and no-op method scaffolding allowed behavioral RED. One test initially indexed an empty claim slice and panicked; fixture guard fixed before meaningful RED. No behavior was implemented for that harness error.

Behavioral RED exited1:

```
FAIL TestWorkflowReconcileCommitCrashAmbiguousAndTerminalMonotonicity: claim <nil>
FAIL TestWorkflowReconcileOutageExpiryAndCancellation/{outage,expired,cancel_queued,cancel_ambiguous}: expected one committed run, got 0: <nil>
FAIL TestWorkflowScheduleDuplicateDeliveryAndDepartedPrincipal/{duplicate,departed}: exactly one committed run: 0 <nil>
FAIL TestWorkflowScheduleDisabledAndPaused/{disabled_engine,no_daemon}: unavailable schedule not visible
```

`go test ./cmd/server -run WorkflowScheduleManual -count=1` exited1 before eligibility admission fix: `unverified daemon admitted`, status queued, error nil. Fixed to visible skipped/unavailable.

`go test ./cmd/server -run WorkflowReconcileStolen -count=1` exited1 before acknowledgement fence: `stolen owner attached late response: running <nil>`. Fixed with attempt/lease CAS; discovery still recovers the late accepted identity.

Pinned adapter/job test first failed to compile on missing workflowHatchetEngine/workflowScheduleJob/workflowReconcilerJob; then real transport/job behavior passed. Core red tests preceded functional production implementation. Supplemental race test initially modeled immediate OLAP visibility while expecting retry exhaustion; actual discovery correctly attached running identity. Fixture changed to explicit eventually-invisible OLAP, preserving accepted engine identity; production unchanged for that fixture correction.

## GREEN evidence

`GOPROXY=https://goproxy.cn,direct make sqlc` exited0. Initial default-proxy make sqlc timed out proxy.golang.org; explicit configured proxy succeeded. `go run ./cmd/migrate up` exited0, `up 593_workflow_runtime_projection`, `Done.`

From `server`, required test with guard and verbose actual-execution evidence:

`../scripts/go-test-with-agent-cli-guard.sh -- go test ./cmd/server -run 'WorkflowSchedule|WorkflowReconcile' -count=1 -v`

```
PASS TestWorkflowReconcilePinnedTransportAndJobWiring
PASS TestWorkflowReconcileStolenSubmissionLeaseFencesLateResponse
PASS TestWorkflowReconcileCommitCrashAmbiguousAndTerminalMonotonicity
PASS TestWorkflowReconcileRetryBudgetAndConcurrentReplicas
PASS TestWorkflowReconcileCancellationDuringSubmission
PASS TestWorkflowReconcileProjectionBounds
PASS TestWorkflowReconcileOutageExpiryAndCancellation/{outage,expired,cancel_queued,cancel_ambiguous}
PASS TestWorkflowScheduleDuplicateDeliveryAndDepartedPrincipal/{duplicate,departed}
PASS TestWorkflowScheduleDisabledAndPaused/{paused_trigger,archived_workflow,disabled_engine,no_daemon}
PASS TestWorkflowScheduleManualRequiresWorkspaceEligibility
PASS
ok github.com/multica-ai/multica/server/cmd/server 0.682s
```

Pinned transport test loads `internal/hatchet/testdata/live-responses.json` submit and recovered-run captures, drives the actual production scheduler job handlers and Client HTTP paths, verifies tenant Authorization, immutable revision engine name, correct user payload/local UUID, and queries the database during submission to prove the committed submitting ledger already exists.

Initial all-focused command `go test ./cmd/server ./internal/service ./internal/handler ./cmd/migrate -run Workflow -count=1`: four packages PASS (1.020s/1.112s/0.857s/1.428s).

Guarded focused race command:

`../scripts/go-test-with-agent-cli-guard.sh -- go test -race ./cmd/server ./internal/service ./internal/handler ./cmd/migrate -run Workflow -count=1`

Earlier complete run: four packages PASS (2.864s/2.668s/2.031s/2.388s). Final run after request-budget self-review: cmd/server PASS2.570s, service PASS1.587s, handler PASS1.909s, cmd/migrate PASS1.957s. `git diff --check` passed.

## Broad-suite findings requiring follow-up

TDD skill requests the normal suite; root `bash scripts/test-go.sh --race` was run with explicit DB and guard (no real agents). First broad output was truncated by the tool, so repeated into stable log `/tmp/multica-task4-suite.log` to recover every failure. Final broad regular phase exited1; cmd/server PASS18.878s, migrations and hatchet PASS. Agent half was not reached because regular phase failed.

Handler failures (no Task4 production changes in these areas; timing causes unverified):

- TestBuildDaemonWebSocketIdentitySeedsBatchRuntimeLeases: last_seen_at unexpectedly stale.
- TestSendCodeRateLimit: second SendCode expected429 got200.
- TestBatchedHeartbeatScheduler_StopRecoveryUsesBoundedContextAndKnownWorkspace: sweeper rows0 want1.
- TestBatchedHeartbeatScheduler_RaceToOfflineSelfHeals: sweeper rows0 want1.
- TestRecordHeartbeat_NoopStoreAlwaysWritesDB: DB timestamp did not bump (before20:13:09, after20:05:30).
- TestRecordHeartbeat_DBFlushOnStaleRow: stale DB timestamp not flushed.
- TestRecordHeartbeat_TouchErrorFallsBackToDB: touch failure did not bump DB timestamp.
- TestRecordHeartbeat_SweeperRaceRecoversOnline: last_seen_at7m38.5s ago.
- TestClaimNextDueSeatCapacityIntentIsExclusiveAcrossReplicas: claimed0 empty2 want1/1.

Service workflow-document integration failures introduced in Task3 and routed by controller to Task7, **not waived**:

- TestBuiltinSkillPayloadCarriesNoSourceReferences/multica-platform: references/workflows.md leaks `service/workflow.go`; reader has no repository source.
- TestPlatformSkillRoutingTableMatchesItsReferences: SKILL.md routing table never names references/workflows.md.
- TestPlatformSkillDescriptionNamesEveryDomain: references/workflows.md has no trigger word in skill description.

No unrelated production/timing/documentation fixes made. Initial clock-skew inference is unverified; controller later sampled aligned clocks. Exact broad assertions remain in the stable log.

## Self-review and limits

Reviewed commit-before-network path, immutable snapshot and deadline guards, tenant filtering, response-loss discovery, lease fencing, request budget starting before claim, cancellation guard/CAS races, output/log/step bounds, terminal update predicates, nil eligibility, scheduler scope separation and main wiring. Server never interprets or runs definition source. Historical revision identity remains fixed during dispatch even when a newer workflow revision is published. Existing replay handler uses current requesting human via the shared AdmitRun path; replay-specific authority/input tests remain in handler package.

No live patched Hatchet engine smoke was added; production adapter verification is real Client HTTP against pinned captured forms. Task5 registration must validate that task action IDs match the namespaced mapping. Persisted step projections exist for Task8 and public run detail now exposes their bounded product fields; logs use the persisted reader. Existing sanitizer protects known credential patterns, not arbitrary secrets unknown to the server.

Workspace scans cap at100 active rows. Supplemental migrations594–595 and atomic scan progress now rotate older active rows so later intents can advance. A schedule with no published current revision is deferred without advancing its watermark or inventing a run; trusted publication in Task5 is required before it becomes runnable. Engine discovery is eventually consistent, so expired/exhausted ambiguous intent may remain submitting indefinitely if no identity becomes discoverable; this is intentional fail-safe preservation, not proof of rejection.

Database/container retained for next task. No push/merge. Temporary broad-suite log retained for controller diagnostics.

## Supplemental pre-review follow-up

Supplemental commit: `8d248fc54 fix(workflows): rotate reconciliation and defer unpublished schedules`.

The original follow-up observed two actual-DB behavioral RED cases before the pending production edits: `TestWorkflowReconcileFairProgressBeyondOnePageAcrossRestartAndReplicas` left a later queued intent behind 105 older running rows across restart/concurrent scans; `TestWorkflowScheduleDefersUnpublishedWithoutPoisoningPublishedTrigger` returned zero rows and an error when an unpublished due trigger preceded a published one. The exact invocation from that earlier run was not retained in this handoff. This follow-up also ran a fresh exact RED for the missing concurrent-index cleanup registry:

`(cd server && GOPROXY=https://goproxy.cn,direct go test ./cmd/migrate -run TestEveryConcurrentUpBuildHasCleanup -count=1)` exited 1 with `595_workflow_reconciliation_progress_index: builds "idx_workflow_run_reconciliation_progress" concurrently on up but has no up cleanup`. After registering 595 in `concurrentIndexCleanups`, the same command exited 0 (`ok .../cmd/migrate 0.339s`).

Reconciliation now claims up to 100 active rows per workspace in `last_reconciled_at,id` order with `FOR UPDATE SKIP LOCKED`, atomically advancing the durable marker before any network request. The partial index lives in standalone concurrent migration 595 with the retry cleanup hook. The DB regression covers 105 long-running rows, a later queued intent, a new service instance, and concurrent replicas: the queued intent reaches running and all 105 older rows retain log progress. Terminal statuses are excluded from the claim; existing terminal CAS/immutability tests still pass.

The due-trigger query now selects only a valid published current revision, and the locked workflow/revision are rechecked before creating a run. An unpublished trigger keeps its original `next_run_at`, has no `last_fired_at` and no run snapshot, while another published trigger in the workspace fires. The regression then publishes the deferred workflow and proves the overdue pre-publication occurrence fires once with its original planned time and the newly published revision. This is the chosen overdue behavior, without a fabricated skipped run.

Run detail now returns `steps: [{id,status,attempt,started_at?,finished_at?}]` as a bounded product projection. `id` is the authored graph step ID: pinned `tasks[].actionId` is matched to `revision.HatchetName + ":" + WorkflowActionName(revision, step.ID)` from the immutable revision graph. Status is restricted to `QUEUED|RUNNING|COMPLETED|FAILED|CANCELLED` (unknown maps to `QUEUED`); negative attempts clamp to zero; optional timestamps serialize as RFC3339. A projection above 128 KiB, above 256 steps, or with invalid step IDs becomes `[]`. Engine `stepId`, `taskExternalId`, `actionId`, raw JSON extras, credentials, and paths are not public fields. List rows remain summaries without steps. The pinned transport regression verifies action mapping, attempt/timestamps and no engine UUID/action in persisted steps; the HTTP regression verifies exact exposed keys, removal of historical raw extras, and the 256-step boundary.

Final focused verification on the disposable DB used `DATABASE_URL='postgres://postgres:task3_disposable@127.0.0.1:55510/multica_task3?sslmode=disable'` and `GOPROXY=https://goproxy.cn,direct`:

- `go run ./cmd/migrate up` exited 0 and reported migrations 594–595 already applied; the disposable schema is current.
- `(cd server && ../scripts/go-test-with-agent-cli-guard.sh -- go test ./cmd/migrate -run 'TestWorkflowDurableContracts|TestEveryConcurrentUpBuildHasCleanup' -count=1 -v)` exited 0, both named tests PASS; the contract test applied through 595 in its private schema.
- `(cd server && ../scripts/go-test-with-agent-cli-guard.sh -- go test -race ./cmd/server ./internal/service ./internal/handler ./cmd/migrate -run Workflow -count=1)` exited 0 on the final test edit: cmd/server 2.745s, service 1.564s, handler 2.153s, cmd/migrate 1.420s.
- `(cd server && ../scripts/go-test-with-agent-cli-guard.sh -- go test ./cmd/server -run 'WorkflowSchedule|WorkflowReconcile' -count=1 -v)` exited 0 with the two new named tests PASS and package `ok .../cmd/server 0.752s`; verbose output confirms tests ran.
- Root `GOPROXY=https://goproxy.cn,direct make sqlc` exited 0; `git diff --check` exited 0.

No broad suite rerun was made for this focused supplement. The named broad failures and Task7 documentation follow-up above remain outstanding. `listeners.go` remains unchanged by controller ruling; Task8 polls these bounded projections with bounded refresh lag.

## Review fix round 1 (after 8d248fc54)

Three review defects were reproduced against the disposable PostgreSQL database before the saved production edits: a committed queued intent submitted after its frozen human left; three cancelled inspections advanced all 100 reconciliation markers; and terminal HTTP 503 from the log endpoint replaced previously projected logs with `[]` before an immutable terminal transition. Those RED observations were made by the prior implementation turn; its exact command output was not retained. During this continuation, `TestWorkflowScheduleDepartureDuringSubmissionClaimSkipsAtomically` independently failed with a still-queued run after the creator left between the precheck and submission claim. Its diagnostic showed zero remaining memberships, zero submissions, and a successful manual skip. The cause was assigning the zero-value `BeginWorkflowRunSubmission` result over the original run on `pgx.ErrNoRows`; preserving the original run ID made the atomic skip reachable. The same focused test then passed.

The queued submission path now atomically transitions an unattempted departed-principal intent to `skipped` with stable `workflow_principal_departed`, before any engine discovery or submission. Both the first submission transition and each durable attempt claim require current membership. An already attempted ambiguous submission stays active: discovery still runs after the human leaves, but no fresh submit attempt is claimed. The regression checks zero Discover and Submit contacts for unattempted departed intents and later discovery of an already accepted ambiguous run.

Reconciliation now claims and marks one active row immediately before each actual attempt, checking cancellation before every claim. The database-time cutoff prevents repeatedly claiming the same row within one invocation; the persisted `last_reconciled_at,id` ordering still rotates old active rows fairly across restarts and replicas. A regression inserts 105 running rows plus a later queued intent, cancels after three inspections on each of three restarts, verifies exactly 3/6/9 markers advanced, then verifies the later intent reaches running. Terminal rows remain outside this scan and immutable.

`workflowHatchetEngine.Inspect` now distinguishes incomplete log reads from a complete empty result. Incomplete reads preserve the last successful log projection. Observed terminal status and sanitized output are staged durably while the run remains active; successful log recovery finalizes them. Migration 596 adds `pending_terminal_status`, `pending_terminal_output`, and `final_projection_attempts` to `workflow_run` without an index, foreign key, or cascade. After three incomplete attempts, including a full later engine outage, the run finalizes with the staged terminal outcome, retained known logs, and public stable `reason_code=workflow_final_logs_incomplete`. The HTTP transport regression covers prior logs, terminal log 503, restart recovery, restart exhaustion, preserved sanitized output, and terminal immutability. The public run detail/list view already exposes `reason_code`; this round added no response fields.

Final scoped verification used `DATABASE_URL='postgres://postgres:task3_disposable@127.0.0.1:55510/multica_task3?sslmode=disable'` and `GOPROXY=https://goproxy.cn,direct`. `go run ./cmd/migrate up` exited 0 and reported migration 596 already applied in that disposable database. Root `make sqlc` exited 0. From `server`, guarded `go test ./cmd/server -run 'WorkflowSchedule|WorkflowReconcile' -count=1 -v` exited 0 with every named test actually executed and `ok .../cmd/server 1.043s`; guarded `go test -race ./cmd/server ./internal/service ./internal/handler ./cmd/migrate -run Workflow -count=1` exited 0 (2.808s, 1.452s, 2.636s, 2.513s). Guarded `go test ./cmd/migrate -run 'TestWorkflowDurableContracts|TestEveryConcurrentUpBuildHasCleanup' -count=1 -v` exited 0 with both named tests PASS. `git diff --check` exited 0 before report append.

Self-review checked principal preflight and claim predicates, the zero-value `pgx.ErrNoRows` branch, discovery without departed-authority retries, one-row fair scan and cancellation budget, pending-terminal persistence, log preservation, output sanitization, terminal CAS, and public reason visibility. The finite recovery budget can finalize with an incomplete-log reason if the engine stays unavailable; this is explicit in the public run and retains the last known logs. No live Hatchet engine or real agent was run. The broad-suite failures already listed above were not rerun in this scoped fix round; Task7 owns the workflow-document defects.

## Review fix round 2 (after 845b0e9d1)

Round 1's claim that every departed, unattempted intent was skipped was incomplete. A process could commit `BeginWorkflowRunSubmission` (`status=submitting`, immutable start/deadline, `submission_attempts=0`), then lose the human member before its attempt claim. The membership-gated attempt would do nothing, and both direct `SubmitRun` and restarted reconciliation would call Discover on the unattempted run. A separate race let an older RUNNING poll overwrite known logs with `[]` and finalize a newer poll's staged terminal outcome without a matching terminal/log observation. The fair one-row scan from round 1 was left intact.

Fresh actual-DB RED, using the disposable DSN and guarded server test command `go test ./cmd/server -run 'TestWorkflowScheduleDepartureAfterBeginBeforeFirstAttemptSkipsWithoutEngineContact|TestWorkflowReconcileOverlappingOldRunningPollCannotPublishTerminalLogs' -count=1 -v`, exited 1. The relevant output was `old running poll overwrote terminal recovery: status=completed pending=completed logs=[] reason=`, `direct_submit ... status=submitting reason= attempts=0 discoveries=1 submissions=0`, and `restarted_reconciler ... status=submitting reason= attempts=0 discoveries=2 submissions=0`. The overlapping HTTP transport test blocked an older RUNNING log response, let another poll stage COMPLETED with log 503, then released the older empty result.

`SkipDepartedUnsubmittedWorkflowRun` now atomically accepts queued intents and submitting intents with zero durable attempts and no engine ID; it keeps the immutable submission start/deadline. `SubmitRun` checks this state before any discovery and after a failed first-attempt claim. Reconciliation checks it before its own discovery, including after restart. Rows with one or more attempts retain discovery and cannot be skipped by this query; departed principals still cannot claim another Submit attempt.

The existing `last_reconciled_at` claim now advances strictly per row (`GREATEST(clock_timestamp(), old+1 microsecond)`), and each projection update requires the exact claimed marker. The reconciler retains its own marker even when a submitting row is reloaded or acknowledged, so a later claim cannot be adopted by an older poll. A stale poll cannot update logs, steps, staged terminal metadata, retry count, or terminal status after another poll has claimed the row. Nonterminal log projection requires no pending terminal status. A complete terminal observation atomically writes sanitized logs/steps/output and the matching terminal status in one CAS; it cannot finalize an incompatible pending status. Incomplete-log exhaustion uses a separate marker-fenced CAS, retains known logs, and still publishes `workflow_final_logs_incomplete` after three durable attempts. No schema migration or index was needed; migration 596 remains unchanged.

The exact RED command above exited 0 after the fix: both named tests and the two principal subcases PASS (`ok .../cmd/server 0.542s`). After the final marker-retention edit, guarded `go test ./cmd/server -run 'WorkflowSchedule|WorkflowReconcile' -count=1 -v` exited 0 with all named tests actually run (`ok .../cmd/server 1.378s`). Final guarded `go test -race ./cmd/server ./internal/service ./internal/handler ./cmd/migrate -run Workflow -count=1` exited 0 (cmd/server 2.706s; service 1.870s; handler 2.220s; migrate 1.427s). Guarded migration `go test ./cmd/migrate -run 'TestWorkflowDurableContracts|TestEveryConcurrentUpBuildHasCleanup' -count=1 -v` exited 0, both named tests PASS (`ok .../cmd/migrate 0.451s`). Root `GOPROXY=https://goproxy.cn,direct make sqlc` exited 0 and regenerated the changed queries. All DB-backed commands used `DATABASE_URL='postgres://postgres:task3_disposable@127.0.0.1:55510/multica_task3?sslmode=disable'` and `GOPROXY=https://goproxy.cn,direct`.

Self-review checked the zero-attempt/attempted-ambiguous distinction, direct and restarted paths, immutable start/deadline, claim-marker monotonicity, stale projection writes, matching terminal status, atomic complete publication, bounded incomplete recovery, and terminal row immutability. The new transport test uses real production `workflowHatchetEngine` HTTP calls with a blocked older response and checks retained logs before later complete finalization. No broad suite rerun, real agent, live Hatchet engine, shared container change, or push/merge was performed; the prior broad failures and Task7 documentation follow-up above remain outstanding.
