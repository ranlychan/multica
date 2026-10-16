// @vitest-environment node
import { expect, it } from 'vitest';
import type { DispatcherClient } from '@hatchet-dev/typescript-sdk/protoc/dispatcher';
import { captureWorkerRegistration } from './registration.js';

it('captures only this worker native registration, preserving options and result', async () => {
  const result = { tenantId: 'tenant', workerName: 'multica-generation', workerId: 'a0d7e410-ab33-404a-a7be-d0626f9f7a22' };
  const request = { workerName: 'multica-generation', actions: ['workflow:task'] };
  const options = { signal: new AbortController().signal };
  const captured: string[] = [];
  const rpc: Pick<DispatcherClient, 'register'> = { register: async function (actualRequest, actualOptions) { expect(this).toBe(rpc); expect(actualRequest).toBe(request); expect(actualOptions).toBe(options); return result; } };
  captureWorkerRegistration(rpc, 'tenant', 'multica-generation', id => captured.push(id));
  expect(await rpc.register(request, options)).toBe(result);
  expect(captured).toEqual(['a0d7e410-ab33-404a-a7be-d0626f9f7a22']);
});

for (const mismatch of [{ tenantId: 'foreign' }, { workerName: 'another-generation' }, { workerId: 'bad' }]) {
  it(`refuses mismatched native registration ${JSON.stringify(mismatch)}`, async () => {
    const captured: string[] = [];
    const rpc: Pick<DispatcherClient, 'register'> = { register: async () => ({ tenantId: 'tenant', workerName: 'multica-generation', workerId: 'a0d7e410-ab33-404a-a7be-d0626f9f7a22', ...mismatch }) };
    captureWorkerRegistration(rpc, 'tenant', 'multica-generation', id => captured.push(id));
    await expect(rpc.register({ workerName: 'multica-generation' })).rejects.toThrow('workflow_registration_mismatch');
    expect(captured).toEqual([]);
  });
}
