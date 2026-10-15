import { fork } from 'node:child_process';
import { createHash } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { limits, WorkflowError, validateGraph, type WorkflowGraph } from '@multica/workflow-sdk';
import type { ChildMessage, ValidationResult } from './contracts.js';

export const sourceHash = (source: string): string => createHash('sha256').update(source).digest('hex');
/** Canonical graph hash: ID/parent sorting makes source order irrelevant. */
export function graphDigest(graph: WorkflowGraph): string {
  validateGraph(graph);
  return sourceHash(JSON.stringify({ steps: graph.steps.map(s => ({ id: s.id, parents: [...s.parents].sort() })).sort((a, b) => a.id < b.id ? -1 : a.id > b.id ? 1 : 0) }));
}
export function validateSource(source: string, options: { timeoutMs?: number } = {}): Promise<ValidationResult> {
  if (typeof source !== 'string' || !source.length || Buffer.byteLength(source) > limits.sourceBytes) return Promise.reject(new WorkflowError('workflow_invalid_source'));
  const timeout = options.timeoutMs ?? 5000;
  if (!Number.isSafeInteger(timeout) || timeout < 1 || timeout > 30000) return Promise.reject(new WorkflowError('workflow_invalid_duration'));
  return new Promise((resolve, reject) => {
    // Allowlist, not credential-name filtering. No HOME, NODE_OPTIONS, proxies,
    // cloud/engine/product credentials, or inherited configuration files.
    const child = fork(fileURLToPath(new URL('../dist/child.js', import.meta.url)), [], { env: { LANG: 'C.UTF-8' }, execArgv: [], silent: true, detached: process.platform !== 'win32' });
    let settled = false, bytes = 0;
    const finish = (result?: ValidationResult, code?: string) => {
      if (settled) return;
      settled = true; clearTimeout(timer);
      if (child.pid) {
        try { if (process.platform !== 'win32') process.kill(-child.pid, 'SIGKILL'); else child.kill('SIGKILL'); } catch { /* Already exited. */ }
      }
      if (result) resolve(result); else reject(new WorkflowError(code ?? 'workflow_invalid_source'));
    };
    const timer = setTimeout(() => finish(undefined, 'workflow_validation_timeout'), timeout);
    const output = (data: Buffer) => { bytes += data.length; if (bytes > limits.graphBytes) finish(undefined, 'workflow_validation_output_limit'); };
    child.stdout?.on('data', output); child.stderr?.on('data', output);
    child.on('error', () => finish()); child.on('exit', () => finish());
    child.on('message', (message: ChildMessage) => {
      if (!message || Buffer.byteLength(JSON.stringify(message)) > limits.graphBytes + 1024) return finish();
      if (message.type === 'validated') {
        try {
          if (message.result.hash !== sourceHash(source) || message.result.graph_digest !== graphDigest(message.result.graph)) return finish(undefined, 'workflow_hash_mismatch');
          finish(message.result);
        } catch { finish(undefined, 'workflow_invalid_graph'); }
      } else if (message.type === 'error') {
        finish(undefined, ['workflow_invalid_source', 'workflow_invalid_graph', 'workflow_invalid_import'].includes(message.code) ? message.code : 'workflow_invalid_source');
      }
    });
    child.send({ mode: 'validate', source });
  });
}
