// @vitest-environment node
import { fork } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { expect, it } from 'vitest';
const source = `import {defineWorkflow} from '@multica/workflow-sdk'; export default defineWorkflow({steps:[{id:'a',run:async()=>null}]});`;
it('runs validation through the distributable API and declared CLI over private IPC', async () => {
  const api = await import('../dist/index.js');
  await expect(api.validateSource(source)).resolves.toHaveProperty('graph.steps.0.id', 'a');
  const child = fork(fileURLToPath(new URL('../dist/cli.js', import.meta.url)), [], { silent: true, env: { LANG: 'C' } });
  try {
    const reply = new Promise<any>((resolve, reject) => {
      child.once('message', resolve);
      child.once('exit', () => reject(new Error('packaged CLI exited before response')));
    });
    child.send({ mode: 'graph', source });
    expect(await reply).toMatchObject({ type: 'validated', result: { graph: { steps: [{ id: 'a', parents: [] }] } } });
  } finally { child.kill('SIGKILL'); }
});
