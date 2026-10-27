# Workflow Detail UX Implementation Plan

> **For agentic workers:** Use superpowers:executing-plans. Track each task below; the user explicitly requested agile execution without mockups or another approval round.

**Goal:** Bring the workflow detail and run inspection experience into Multica's existing Autopilot interaction language.

**Architecture:** Reuse BreadcrumbHeader, shared Dialog/Button/Switch primitives, and existing workflow HTTP endpoints. Extract reusable run content for modal and standalone routes. Keep all UI in shared views and server data/mutations in core.

**Tech Stack:** React, TanStack Query, existing zod API parsing, existing Multica UI and i18n.

**Spec:** User walkthrough feedback in this conversation: Autopilot-style header, status/edit/manual-run actions, properties/triggers/history, run inspection in a dialog. Explicit clarification: source remains read-only. User requests direct agile implementation and will inspect afterward.

## Global Constraints

- Source remains read-only; CLI owns definition validation/publication. Editing changes name/description only in this increment.
- Preserve existing server authorization, revision pinning, generation authority and replay/cancel semantics; do not change engine internals.
- Workflow pause prevents new admission, not execution cancellation. Runtime availability is distinct from workflow enabled state.
- Metadata updates include the observed `expected_updated_at`; conflicts preserve the form and require refresh, never silently overwrite.
- Shared web/Desktop UI; use navigation adapter, semantic tokens and all five existing locales. No new dependencies or mobile changes.
- Reuse current feature checkout and managed fake-provider test service; never commit local credentials or database fixtures.

## Review Focus

- Reader or unavailable/paused workflow: mutations unavailable with visible context, no accidental run.
- Concurrent metadata changes: conflict leaves input intact and offers deliberate refresh/retry.
- Modal close/reopen and replay: correct run selected, polling unmounts, original history retained.
- Historical revision, incomplete logs and cancellation request: do not misrepresent current code or successful termination.
- Narrow viewport, long names/source/logs: reachable actions, scrollable content, keyboard focus restored.

## Task 1: Core mutations and typed API contracts

Files: packages/core/api/client.ts; packages/core/workflows/mutations.ts; colocated API tests.
Interfaces: `updateWorkflow(id, {name?,description?,status?,expectedUpdatedAt})` performs PATCH, no misleading canWrite synthesis; `createWorkflowRun(id,input)` performs POST /run and returns parsed WorkflowRun. Hooks invalidate workspace workflow keys and expose pending/errors.

- [ ] Write failing tests for request CAS mapping and malformed response rejection for manual run.
- [ ] Run targeted core tests and confirm RED.
- [ ] Implement parsed API methods and hooks; run core tests/typecheck to GREEN.

## Task 2: Autopilot-style workflow detail

Files: packages/views/workflows/workflow-detail-page.tsx; focused dialogs/history components; locales/*/autopilots.json; colocated tests.
Interfaces: consume Task 1 hooks; manual-run success selects returned run in Task 3 dialog. Edit only name/description. Status uses current observed updatedAt.

- [ ] Test header return link retains workflows tab, source read-only, reader guards, metadata conflict and JSON input validation.
- [ ] Observe RED; implement fixed header, compact properties, static flow/revision/source, styled triggers and clickable history.
- [ ] Manual run validates JSON before submission, preserves input on error, prevents double submission, opens returned run only on success.
- [ ] Keep trigger data presentation in scope; do not add trigger CRUD or secret/token UI in this increment.
- [ ] Run targeted views tests and typecheck to GREEN.

## Task 3: Reusable run inspection dialog and verification

Files: packages/views/workflows/workflow-run-page.tsx; workflow-run-dialog.tsx; common.tsx; colocated tests.
Interfaces: shared `WorkflowRunContent({workflowId,runId,onRunCreated?})`; modal switches to replay result while standalone route navigates. Run revision remains frozen.

- [ ] Test history opens run detail without leaving page, modal replay selects new ID, readers cannot mutate, focus restoration and existing final-log behavior remain intact.
- [ ] Observe RED; compose accessible bounded-height Dialog; retain standalone deep link, cancellation pending/error states, immutable run metadata, DAG attempt states and bounded logs.
- [ ] Show only server-observed steps/attempts; do not fabricate full Hatchet event history or task links absent from API.
- [ ] Run views/core tests, frontend typecheck/lint and inspect actual browser flow on existing managed service.
- [ ] Perform one fresh scoped review of this increment, fix important findings, and hand off the running test service. No automatic push.
