#!/usr/bin/env node
import { validateSource } from './validate.js';
import type { ChildMessage, ParentMessage } from './contracts.js';
import { WorkflowError } from '@multica/workflow-sdk';

const send = (message: ChildMessage) => process.send?.(message);
let drain: (() => Promise<void>) | undefined;
let activate: (() => Promise<void>) | undefined;
let retire: (() => Promise<void>) | undefined;
let stopping = false;
async function stop() {
  if (stopping) return;
  stopping = true;
  const deadline = setTimeout(() => process.exit(1), 31000);
  try { await drain?.(); process.exit(0); }
  catch { send({ type: 'error', code: 'workflow_drain_timeout' }); process.exit(1); }
  finally { clearTimeout(deadline); }
}
process.on('SIGTERM', stop);
process.on('SIGINT', stop);
process.on('disconnect', stop);
process.once('message', async (message: ParentMessage) => {
  try {
    if (Number(process.versions.node.split('.')[0]) < 22) throw new WorkflowError('workflow_node_unavailable');
    if (message.mode === 'validate' || message.mode === 'graph') {
      send({ type: 'validated', result: await validateSource(message.source, { processGroup: false }) });
      process.disconnect?.();
    } else if (message.mode === 'worker') {
      const { startWorker } = await import('./worker.js');
      ({ drain, activate, retire } = await startWorker(message.descriptor, send));
      process.on('message', (next: ParentMessage) => {
        if (next.mode === 'drain') void stop();
        if (next.mode === 'activate') void activate?.().catch(() => { send({type:'error',code:'workflow_worker_failed'}); process.exit(1); });
        if (next.mode === 'retire') void retire?.().then(() => process.exit(0), () => { send({type:'error',code:'workflow_worker_failed'});process.exit(1); });
      });
    } else throw new WorkflowError('workflow_invalid_descriptor');
  } catch (error) {
    send({ type: 'error', code: error instanceof WorkflowError ? error.code : 'workflow_worker_failed' });
    process.exitCode = 1;
    process.disconnect?.();
    setTimeout(() => process.exit(1), 10);
  }
});
