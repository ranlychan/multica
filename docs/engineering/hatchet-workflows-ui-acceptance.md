# Workflow detail UX acceptance

This increment follows the existing Autopilot detail interaction model. Workflow source remains read-only; definitions are still authored and published through the CLI.

## Delivered

- Shared web/Desktop breadcrumb header preserves the workflows tab on return.
- Enable/pause switch blocks new admission without cancelling existing runs.
- Name/description editing uses the observed timestamp for optimistic concurrency; failed saves retain the draft.
- Manual JSON input runs the current revision and opens its inspection dialog after successful admission.
- Properties, static dependencies, read-only revision/source disclosure, triggers and paginated history use existing Multica components.
- History opens run inspection without leaving the detail page. Standalone run links remain available.
- Shared run inspection preserves frozen revisions, live attempts, bounded final logs, cancellation requests and replay using the current revision.
- All five existing UI locales include the new controls.

## Verification

- Core package: 172 test files / 2206 tests passed on Node 22.
- Workflow view suite: 30 tests passed on the final tree.
- Workflow mutation API suite: 4 tests passed on the final tree.
- Shared views TypeScript check and workflow ESLint check passed.
- Actual browser: metadata save, manual submission to completion, final logs, dialog focus handoff/restoration, pause and re-enable verified on the local acceptance fixture.
- Full frontend test and aggregate typecheck commands were interrupted due to local resource contention. They are not claimed as passing. No backend or mobile code changed; their suites were not rerun for this UI increment.

## Deliberate boundaries and follow-ups

- Continue in the current feature checkout so the existing acceptance service reflects changes directly; no new worktree, merge or push.
- Source authoring and trigger CRUD stay CLI-driven. This avoids expanding the foundational UI increment; browser source editing was explicitly excluded by the user.
- No engine changes: a wrong new manual-run client path was corrected to the existing `/run` endpoint with a regression test.
- Verification concurrency was reduced, then aggregate runs were stopped to prioritize the manual acceptance environment. Generated Webpack cache was moved to a recoverable local backup, without resetting application data or keys.
- Deferred minor: client name/description length validation counts UTF-16 units while the server limits UTF-8 bytes; long non-ASCII input may receive a generic validation error.
- Deferred minor: standalone run inspection disables replay when paused/unavailable without a dedicated explanation beside that action.
- Actual device/viewport coverage is limited to the local browser; exhaustive responsive and packaged Desktop testing is not claimed.
