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
