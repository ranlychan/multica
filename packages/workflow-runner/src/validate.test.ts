// @vitest-environment node
import { expect, it } from 'vitest';
import { validateSource } from './validate.js';
const source = `import { defineWorkflow } from '@multica/workflow-sdk'; export default defineWorkflow({steps:[{id:'first',run:async()=>({ok:true})}]});`;
it('extracts a credential-free graph and hashes the exact source', async () => {
  process.env.HATCHET_CLIENT_TOKEN = 'private-token';
  try {
    const result = await validateSource(source.replace('({ok:true})', '({ok:true})'));
    expect(result.graph).toEqual({ steps: [{ id: 'first', parents: [] }] });
    expect(result.hash).toMatch(/^[a-f0-9]{64}$/);
    await expect(validateSource(`if(process.env.HATCHET_CLIENT_TOKEN) throw Error('credential'); ${source}`)).resolves.toHaveProperty('graph');
  } finally { delete process.env.HATCHET_CLIENT_TOKEN; }
});
it('rejects imports, malformed declarations and bounds executable validation', async () => {
  await expect(validateSource(`import fs from 'node:fs'; ${source}`)).rejects.toThrow('workflow_invalid_import');
  await expect(validateSource('export default { steps: [] }')).rejects.toThrow('workflow_invalid_graph');
  await expect(validateSource(`while(true){}; ${source}`, { timeoutMs: 200 })).rejects.toThrow('workflow_validation_timeout');
  await expect(validateSource(`throw Error('/Users/private token=abc'); ${source}`)).rejects.toThrow('workflow_invalid_source');
});
it('rejects computed imports hidden in a deferred step', async () => {
  await expect(validateSource(`import {defineWorkflow} from '@multica/workflow-sdk'; export default defineWorkflow({steps:[{id:'a',run:async ctx=>{const moduleName=ctx.input; await import(moduleName); return null;}}]});`)).rejects.toThrow('workflow_invalid_import');
});
