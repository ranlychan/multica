// @vitest-environment node
import { describe, expect, it } from 'vitest';
import { createWorkflowContext, defineWorkflow, operationKey, validateJSON, redactJSON, WorkflowError } from './index.js';

describe('JSON boundary', () => {
  it('rejects lossy values, oversized UTF8 and excessive depth', () => {
    for (const value of [undefined, NaN, Infinity, { x: undefined }, new Date(), 1n, '好'.repeat(23000)]) expect(() => validateJSON(value)).toThrow('workflow_invalid_payload');
    let nested: unknown = 0;
    for (let i = 0; i < 32; i++) nested = [nested];
    expect(() => validateJSON(nested)).toThrow('workflow_invalid_payload');
    expect(validateJSON({ ok: [null, true, 1, 'hello'] })).toEqual({ ok: [null, true, 1, 'hello'] });
  });
  it('redacts credentials, private paths and secret text recursively', () => {
    expect(redactJSON({ token: 'opaque', nested: { cert_file: '/Users/alice/private.pem', text: 'Bearer abc token=xyz' }, value: 42 })).toEqual({ token: '[redacted]', nested: { cert_file: '[redacted]', text: '[redacted] [redacted]' }, value: 42 });
  });
});
describe('definition', () => {
  const run = async () => null;
  it('keeps parent order and rejects malformed graphs and trigger settings', () => {
    expect(defineWorkflow({ steps: [{ id: 'a', run }, { id: 'b', parents: ['a'], run }] }).graph).toEqual({ steps: [{ id: 'a', parents: [] }, { id: 'b', parents: ['a'] }] });
    for (const definition of [
      { steps: [{ id: 'a', parents: ['b'], run }] },
      { steps: [{ id: 'a', parents: ['a'], run }] },
      { steps: [{ id: 'a', run }, { id: 'a', run }] },
      { steps: [{ id: 'a', retries: -1, run }] },
      { steps: [{ id: 'a', timeoutMs: Infinity, run }] },
      { steps: [{ id: 'a', run }], cron: '* * * * *' },
    ]) expect(() => defineWorkflow(definition as never)).toThrow('workflow_invalid_graph');
  });
});
describe('run-scoped operations', () => {
  it('maps an upstream authored WorkflowError to a fixed transport code', async () => {
    const malicious = new WorkflowError('/Users/private token=leaked');
    const ctx = createWorkflowContext({ runId: 'r', stepId: 's', attempt: 0, input: null, parents: {}, transport: async () => { throw malicious; }, sleep: async () => {}, log: async () => {} });
    await expect(ctx.issues.get('i')).rejects.toEqual(new WorkflowError('workflow_transport_unavailable'));
    const known = new WorkflowError('workflow_operation_conflict');
    known.message = '/srv/private token=leaked';
    known.stack = '/srv/runner/private.ts';
    const conflict = createWorkflowContext({ runId: 'r', stepId: 's', attempt: 0, input: null, parents: {}, transport: async () => { throw known; }, sleep: async () => {}, log: async () => {} });
    await expect(conflict.issues.get('i')).rejects.toEqual(new WorkflowError('workflow_operation_conflict'));
  });
  it('redacts private paths and raw worker credentials through actual response and log boundaries', async () => {
    const jwt = 'eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ0ZW5hbnQifQ.c2lnbmF0dXJl';
    const pem = '-----BEGIN PRIVATE KEY-----\ncHJpdmF0ZS1rZXk=\n-----END PRIVATE KEY-----';
    const secret = { unix: '/srv/workspace/private.pem', windows: 'C:\\workspace\\private.pem', unc: '\\\\host\\share\\private.pem', raw: jwt, pem, runner_token: 'opaque-runner-credential' };
    const logs: unknown[] = [];
    const ctx = createWorkflowContext({ runId: 'r', stepId: 's', attempt: 0, input: null, parents: {}, transport: async () => secret, sleep: async () => {}, log: async entry => { logs.push(entry); } });
    expect(await ctx.issues.get('i')).toEqual({ unix: '[redacted]', windows: '[redacted]', unc: '[redacted]', raw: '[redacted]', pem: '[redacted]', runner_token: '[redacted]' });
    await ctx.log('info', `private /srv/workspace/private.pem ${jwt} ${pem}`, secret);
    expect(logs).toEqual([{ level: 'info', message: 'private [redacted] [redacted] [redacted]', fields: { unix: '[redacted]', windows: '[redacted]', unc: '[redacted]', raw: '[redacted]', pem: '[redacted]', runner_token: '[redacted]' } }]);
  });
  it('waits for the author-selected issue status and exposes approved dispatch/run names', async () => {
    let polls = 0;
    const ctx = createWorkflowContext({ runId: 'r', stepId: 's', attempt: 0, input: {}, parents: {}, transport: async () => ({ status: ++polls === 1 ? 'in_progress' : 'review' }), sleep: async () => {}, log: async () => {} });
    expect(await ctx.issues.waitForStatus('i', 'review')).toEqual({ status: 'review' });
    expect(typeof ctx.agents.dispatch).toBe('function');
    expect(typeof ctx.agents.waitForRun).toBe('function');
    expect(typeof ctx.autopilots.run).toBe('function');
    expect(typeof ctx.autopilots.waitForRun).toBe('function');
  });
  it('rejects a missing author key before sending a mutation', async () => {
    let requests = 0;
    const ctx = createWorkflowContext({ runId: 'r', stepId: 's', attempt: 0, input: {}, parents: {}, transport: async () => { requests++; return null; }, sleep: async () => {}, log: async () => {} });
    await expect(ctx.issues.create(undefined as never, {})).rejects.toThrow('workflow_invalid_operation_key');
    expect(requests).toBe(0);
  });
  it('uses author key independent of attempts and unambiguous run/step boundaries', () => {
    expect(operationKey('run', 'step', 'create')).toBe(operationKey('run', 'step', 'create'));
    expect(operationKey('run', 'step', 'create')).not.toBe(operationKey('run', 'step2', 'create'));
    expect(operationKey('a:b', 'c', 'd')).not.toBe(operationKey('a', 'b:c', 'd'));
    expect(() => operationKey('run', 'step', '')).toThrow('workflow_invalid_operation_key');
  });
  it('polls through durable sleep with backoff capped at thirty seconds', async () => {
    const sleeps: number[] = [];
    let polls = 0;
    const ctx = createWorkflowContext({ runId: 'run', stepId: 'step', attempt: 7, input: {}, parents: {}, transport: async () => (++polls > 9 ? { status: 'completed', id: 'i' } : { status: 'running', id: 'i' }), sleep: async ms => { sleeps.push(ms); }, log: async () => {} });
    expect(await ctx.issues.waitForStatus('i', 'completed', { timeoutMs: 300000 })).toEqual({ status: 'completed', id: 'i' });
    expect(sleeps).toEqual([1000, 2000, 4000, 8000, 16000, 30000, 30000, 30000, 30000]);
  });
  it('does not leak upstream errors, results or log credentials', async () => {
    const logs: unknown[] = [];
    const ctx = createWorkflowContext({ runId: 'run', stepId: 'step', attempt: 0, input: {}, parents: {}, transport: async () => ({ id: 'ok', hatchet_token: 'secret' }), sleep: async () => {}, log: async value => { logs.push(value); } });
    expect(await ctx.issues.get('i')).toEqual({ id: 'ok', hatchet_token: '[redacted]' });
    await ctx.log('info', 'Bearer abc /tmp/private.pem', { password: 'xyz' });
    expect(JSON.stringify(logs)).not.toContain('abc');
    expect(JSON.stringify(logs)).not.toContain('/tmp/');
    const broken = createWorkflowContext({ runId: 'r', stepId: 's', attempt: 0, input: null, parents: {}, transport: async () => { throw new Error('/tmp/key token=leaked'); }, sleep: async () => {}, log: async () => {} });
    await expect(broken.issues.get('i')).rejects.toEqual(new WorkflowError('workflow_transport_unavailable'));
  });
});
