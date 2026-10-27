# Default agent workflow operations

**Goal:** Ordinary Multica agents, including those with no custom instructions or bound workspace skills, can discover and operate the complete existing workflow surface like Autopilot.

**Architecture:** Preserve the universal multica-platform skill and human-originator authorization. Complete the CLI surface over existing HTTP contracts; add a small universal runtime-brief pointer and forward only safe local authoring configuration into task environments. No new authorization bypass, daemon credential forwarding, model provider call, schema migration or engine redesign.

User requested agile implementation and will inspect afterward. Continue the current feature branch, preserving all pending workflow UI changes; no automatic commit/push. Tests use isolated fake providers and the existing dedicated test database only. Run Go verification serially with `GOFLAGS=-p=1`, `GOPROXY=https://goproxy.cn,direct`; avoid frontend recompilation and broad parallel tests that contend with the acceptance service.

## Task 1 — Complete agent CLI operations

- [ ] Test missing commands against controlled HTTP fixtures first; verify RED.
- [ ] Implement workflow delete, revision list, trigger update, collaborator list/add/remove, and explicit read-only source retrieval (`get --revision ... --show-source`) while retaining default source/credential redaction.
- [ ] Preserve CAS metadata/source requirements, UUID validation, bounded input/pages and shared task authentication. Never print secrets by default; new token operations already use explicit private-output opt-in.
- [ ] Verify targeted CLI tests GREEN, including invalid input and server error propagation. Do not modify authorization services or default delete semantics.

## Task 2 — Default runtime discovery and authoring environment

- [ ] Inspect empty-instruction task briefs and generated child environment; write failing behavior tests for missing default workflow discovery and safe Node/runner propagation.
- [ ] Add a compact workflow CLI/reference pointer for all task kinds independently of custom instructions or bound skills.
- [ ] Propagate only validated, non-secret local Node/packaged runner configuration required by credential-free source validation. Do not forward daemon tokens, engine TLS keys, enrollment/enable flags, workspace grants, or change security opt-ins.
- [ ] Update the universal workflow reference with exact commands after Task 1 supplies names; no new user-required skill binding or custom agent definition.
- [ ] Verify runtime/execenv tests with fake executables only.

## Task 3 — Default-agent integration and handoff

- [ ] Verify an agent with empty instructions and no bound workspace skills receives the universal workflow reference through actual skill bundle loading.
- [ ] Exercise task-authenticated workflow CRUD/trigger/collaborator/run operations through existing HTTP handlers with a human originator; verify outsider/missing-originator/cross-workspace denials remain.
- [ ] Run relevant backend package tests with serialized compiler/test resources; inspect changes, request one fresh scoped review, and fix important findings only.
- [ ] Rebuild/restart only required managed components using the existing fake-provider environment; preserve DBs, certificates and manual fixtures. Hand off actual verified capability and remaining infrastructure prerequisites without claiming a real-model evaluation.
