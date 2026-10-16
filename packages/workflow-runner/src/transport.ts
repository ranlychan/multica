import { createHmac } from 'node:crypto';
import { limits, validateJSON, redactJSON, WorkflowError, type ActionTransport } from '@multica/workflow-sdk';
import type { WorkerDescriptor } from './contracts.js';

/** Private native selectors are read for each call, outside the author API. */
export function assignedTransport(product: ActionTransport, context: { workflowRunId(): string; taskRunExternalId(): string; readonly invocationCount: number }): ActionTransport {
  return request => product({ ...request, engine_run_id: context.workflowRunId(), engine_task_id: context.taskRunExternalId(), invocation_count: context.invocationCount } as Parameters<ActionTransport>[0]);
}

/** Axios adapter rejects before network dispatch, including constructor tenant probe. */
export async function denyEngineREST(_config?: unknown): Promise<never> { throw new WorkflowError('workflow_engine_rest_denied'); }

export function validateProductOrigin(origin: string): URL {
  const url = new URL(origin);
  if ((url.protocol !== 'https:' && !(url.protocol === 'http:' && ['localhost', '127.0.0.1', '[::1]'].includes(url.hostname))) || url.username || url.password || url.pathname !== '/' || url.search || url.hash) throw new WorkflowError('workflow_invalid_descriptor');
  return url;
}
export function createProductTransport(product: WorkerDescriptor['product'], workspace: string, generation: string): ActionTransport {
  const origin = validateProductOrigin(product.origin).origin;
  return async request => {
    const path = `/api/workflow-runs/${encodeURIComponent(request.run_id)}/actions`;
    const body = JSON.stringify(request);
    if (Buffer.byteLength(body) > limits.payloadBytes + 2048) throw new WorkflowError('workflow_invalid_payload');
    const signature = createHmac('sha256', product.runner_token).update(`POST\n${path}\n${workspace}\n${generation}\n${body}`).digest('hex');
    try {
      const response = await fetch(origin + path, { method: 'POST', redirect: 'error', signal: AbortSignal.timeout(10000), headers: { 'content-type': 'application/json', authorization: `Bearer ${product.runner_token}`, 'x-workspace-id': workspace, 'x-multica-workflow-generation': generation, 'x-multica-workflow-signature': signature }, body });
      if (!response.ok) {
        await response.body?.cancel();
        throw new WorkflowError(({ 401: 'workflow_unauthenticated', 403: 'workflow_forbidden', 404: 'workflow_not_found', 409: 'workflow_operation_conflict', 410: 'workflow_capability_revoked', 429: 'workflow_rate_limited', 503: 'workflow_unavailable' } as Record<number, string>)[response.status] ?? 'workflow_action_failed');
      }
      const reader = response.body?.getReader();
      if (!reader) throw new WorkflowError('workflow_invalid_response');
      const parts: Uint8Array[] = []; let bytes = 0;
      for (;;) {
        const { done, value } = await reader.read(); if (done) break;
        bytes += value.byteLength;
        if (bytes > limits.payloadBytes + 32) { await reader.cancel(); throw new WorkflowError('workflow_invalid_response'); }
        parts.push(value);
      }
      const data = JSON.parse(Buffer.concat(parts).toString('utf8'));
      if (!data || typeof data !== 'object' || !Object.hasOwn(data, 'result')) throw new WorkflowError('workflow_invalid_response');
      return redactJSON(validateJSON(data.result));
    } catch (error) {
      if (error instanceof WorkflowError) throw error;
      throw new WorkflowError('workflow_transport_unavailable');
    }
  };
}
