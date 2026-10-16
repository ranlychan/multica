# Workflow SDK

Node.js 22 or later. Workflow code runs with the daemon OS user's privileges.
Validation is an import policy and resource bound, not a security sandbox.

```ts
import { defineWorkflow } from '@multica/workflow-sdk';

export default defineWorkflow({ steps: [
  { id: 'create', run: async ctx => {
    const issue = await ctx.issues.create('create-ticket', { title: 'Review build' });
    return issue;
  } },
  { id: 'follow-up', parents: ['create'], run: async ctx => {
    await ctx.log('info', 'Created ticket', ctx.parents.create);
    await ctx.sleep(1000);
    return { ready: true };
  } },
] });
```

The public context includes `issues.create(key, payload)`, `issues.get(id)`,
`issues.waitForStatus(id, status, { timeoutMs? })`, `agents.dispatch(key, payload)`,
`agents.waitForRun(id, options?)`, `autopilots.run(key, payload)`, and
`autopilots.waitForRun(id, options?)`. Issue status is the workspace's status key,
including custom keys. Run waits expect `completed`, `done`, or `succeeded`, and
reject `failed`/`cancelled`. Each poll uses durable sleep with 1/2/4/8/16/30-second
backoff, capped at 30 seconds. Wait timeout defaults to one hour (maximum one day).

The v1 business payloads are:

- `issues.create(key, { title, description?, status?, priority?, assignee_type?, assignee_id?, parent_issue_id?, project_id? })`. Assignee type/ID must be paired; types are `member`, `agent`, or `squad`. Status defaults to `todo`, priority to `none`. Responses contain `id` and the exact workspace `status` key.
- `agents.dispatch(key, { agent_id, issue_id })` dispatches that agent on an existing workspace issue. Standalone prompt/chat dispatch is unsupported and returns `workflow_invalid_payload`; it does not create a quick-create job. Responses contain task `id`, `status`, `agent_id`, and `issue_id`.
- `autopilots.run(key, { autopilot_id, payload? })` invokes the existing active Autopilot as the workflow's frozen human. Responses contain run `id`, `status`, `autopilot_id`, `issue_id`, and `task_id` (nullable links).

Issue-backed work may queue for an offline but usable runtime. Autopilot
`create_issue` follows that policy; `run_only` requires an online ready runtime.
Every action, wait poll, and cached receipt rechecks the current workflow,
membership, resource and agent invocation grants. Responses intentionally omit
raw task results, runtime configuration, execution paths, credentials, Autopilot
payloads, and failure text. Business mutations and operation receipts commit in
one application transaction; notifications and daemon wakeups follow commit.

`input`, `parents`, `runId`, `stepId`, and zero-based engine `attempt` are available.
Parents are keyed by authored step ID. Return values and inputs must be JSON.
Mutations require a nonempty author key, stable across retries and durable replay.
The effect identity is SHA256 of JSON `[runId, stepId, authorKey]`; attempt is audit
data only. Reuse with a different operation/payload is an HTTP409 server contract.

Limits mirror the Go workflow service: source256KiB, input/result64KiB, graph128KiB,
256 steps, depth32, logs64KiB/200 entries. Retries are0–10; task timeouts and sleeps
are1ms–24h. No cron/event/webhook declarations or arbitrary SDK imports are allowed.
Errors use stable `workflow_*` codes; SDK responses and structured logs redact
credential fields, bearer tokens, and recognized private filesystem paths.
