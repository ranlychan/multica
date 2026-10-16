// @vitest-environment node
import { createServer } from 'node:http';
import { createHmac } from 'node:crypto';
import { afterEach, expect, it } from 'vitest';
import { assignedTransport, createProductTransport, denyEngineREST } from './transport.js';
const servers: ReturnType<typeof createServer>[] = [];
it('adds native per-call assignment selectors without changing public operation identity', async () => {
  const requests: unknown[] = [];
  let invocationCount = 1;
  const transport = assignedTransport(async request => { requests.push(request); return null; }, {
    workflowRunId: () => 'engine-run', taskRunExternalId: () => 'engine-task', get invocationCount() { return invocationCount; },
  });
  const request = { protocol: 1 as const, run_id: 'local', step_id: 'step', attempt: 2, operation: 'issues.create' as const, operation_key: 'stable-key', payload: {} };
  await transport(request); invocationCount = 3; await transport(request);
  expect(requests).toEqual([1, 3].map(count => ({ ...request, engine_run_id: 'engine-run', engine_task_id: 'engine-task', invocation_count: count })));
  expect(request).not.toHaveProperty('engine_run_id');
});
afterEach(async () => { await Promise.all(servers.splice(0).map(s => new Promise<void>(r => s.close(() => r())))); });
it('signs exact run-bound requests and maps conflict without upstream data', async () => {
  const bodies: string[] = [];
  const server = createServer(async (req, res) => {
    let body = ''; for await (const part of req) body += part;
    bodies.push(body);
    expect(req.headers.authorization).toBe('Bearer fake-product-token');
    expect(req.headers['x-multica-workflow-signature']).toBe(createHmac('sha256', 'fake-product-token').update(`POST\n/api/workflow-runs/run/actions\nws\ngen\n${body}`).digest('hex'));
    res.setHeader('content-type', 'application/json');
    if (bodies.length > 1) { res.statusCode = 409; res.end(JSON.stringify({ error: { code: '/Users/private token=secret' } })); }
    else res.end(JSON.stringify({ result: { id: 'created', token: 'upstream-secret' } }));
  });
  servers.push(server); await new Promise<void>(r => server.listen(0, '127.0.0.1', r));
  const address = server.address() as { port: number };
  const transport = createProductTransport({ origin: `http://127.0.0.1:${address.port}`, runner_token: 'fake-product-token' }, 'ws', 'gen');
  const request = { protocol: 1 as const, run_id: 'run', step_id: 'step', attempt: 0, operation: 'issues.create' as const, operation_key: 'key', payload: { title: 'test' } };
  expect(await transport(request)).toEqual({ id: 'created', token: '[redacted]' });
  await expect(transport({ ...request, attempt: 1, payload: { title: 'different' } })).rejects.toThrow('workflow_operation_conflict');
  expect(JSON.parse(bodies[1]!).operation_key).toBe('key');
});
it('denies every engine REST call without retaining Axios config secrets', async () => {
  await expect(denyEngineREST({ url: '/anything', headers: { Authorization: 'secret' } } as never)).rejects.toThrow('workflow_engine_rest_denied');
  try { await denyEngineREST({} as never); } catch (e) { expect(JSON.stringify(e)).not.toContain('secret'); }
});
