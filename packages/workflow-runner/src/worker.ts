import { readFile } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import { Hatchet, CreateWorkflow, type CreateWorkflowDurableTaskOpts } from '@hatchet-dev/typescript-sdk';
import { createNodeTransport, type Transport } from '@hatchet-dev/typescript-sdk/clients/transport';
import type { ClientConfig } from '@hatchet-dev/typescript-sdk/clients/hatchet-client';
import { createWorkflowContext, validateJSON, redactJSON, safeWorkflowErrorCode, WorkflowError, type JSONValue, type WorkflowDefinition } from '@multica/workflow-sdk';
import { loadDefinition } from './load.js';
import { graphDigest, sourceHash } from './validate.js';
import { createProductTransport, denyEngineREST, validateProductOrigin } from './transport.js';
import { RUNNER_VERSION, type WorkerDescriptor, type RevisionDescriptor, type RegistrationEvidence, type ChildMessage } from './contracts.js';

export function actionNames(name: string, step: string) {
  const task_name = `${name}-step-${createHash('sha256').update(step).digest('hex').slice(0, 16)}`;
  return { task_name, action_id: `${name}:${task_name}` };
}
export function validateDescriptor(d: WorkerDescriptor): void {
  try {
    if (d.protocol !== 1 || !d.workspace_id || !d.tenant_id || !d.generation || !Array.isArray(d.revisions) || d.revisions.length > 256 || !d.product.runner_token) throw Error();
    const host = new URL(`https://${d.engine.host_port}`);
    if (!host.port || host.username || host.password || host.pathname !== '/' || host.search || host.hash || ['0.0.0.0', '[::]', '*'].includes(host.hostname)) throw Error();
    const tls = d.engine.tls;
    if (!['tls', 'mtls'].includes(tls.strategy) || !tls.server_name || !tls.ca_file || (tls.strategy === 'mtls' && (!tls.cert_file || !tls.key_file))) throw Error();
    // This validates descriptor consistency, not JWT authenticity. Engine verifies it.
    const claims = JSON.parse(Buffer.from(d.engine.token.split('.')[1]!, 'base64url').toString());
    if (claims.sub !== d.tenant_id) throw Error();
    validateProductOrigin(d.product.origin);
    if (d.drain_timeout_ms !== undefined && (!Number.isInteger(d.drain_timeout_ms) || d.drain_timeout_ms < 1 || d.drain_timeout_ms > 30000)) throw Error();
    if (new Set(d.revisions.map(r => r.engine_workflow_name)).size !== d.revisions.length) throw Error();
  } catch { throw new WorkflowError('workflow_invalid_descriptor'); }
}
export async function verifyRevision(source: string, revision: RevisionDescriptor): Promise<WorkflowDefinition> {
  if (sourceHash(source) !== revision.hash || !/^[a-f0-9]{64}$/.test(revision.hash)) throw new WorkflowError('workflow_hash_mismatch');
  if (!revision.workflow_id || !revision.revision_id || !Number.isInteger(revision.version) || revision.version < 1 || revision.engine_workflow_name !== `multica-${revision.workflow_id}-v${revision.version}-${revision.hash.slice(0, 12)}` || revision.engine_workflow_name !== revision.engine_workflow_name.toLowerCase()) throw new WorkflowError('workflow_invalid_descriptor');
  const definition = await loadDefinition(source);
  if (graphDigest(definition.graph) !== revision.graph_digest || graphDigest(revision.graph) !== revision.graph_digest) throw new WorkflowError('workflow_graph_mismatch');
  return definition;
}

/** Only called inside the daemon-owned credentialed child. */
export async function startWorker(descriptor: WorkerDescriptor, send: (message: ChildMessage) => void) {
  validateDescriptor(descriptor);
  const definitions = await Promise.all(descriptor.revisions.map(async revision => ({ revision, definition: await verifyRevision(await readFile(revision.source_path, 'utf8'), revision) })));
  // No upstream logs are forwarded: errors may contain TLS paths, tokens or input.
  const silentLogger = () => ({ debug() {}, info() {}, green() {}, warn() {}, error() {}, util() {} });
  const config: ClientConfig = {
    token: descriptor.engine.token, tenant_id: descriptor.tenant_id, host_port: descriptor.engine.host_port,
    api_url: 'http://127.0.0.1:1', namespace: '', tls_config: { tls_strategy: descriptor.engine.tls.strategy, ca_file: descriptor.engine.tls.ca_file, cert_file: descriptor.engine.tls.cert_file, key_file: descriptor.engine.tls.key_file, server_name: descriptor.engine.tls.server_name },
    logger: silentLogger, log_level: 'OFF', healthcheck: { enabled: false, port: 0 }, retrier: { maxAttempts: 1, initialInterval: 100, maxJitter: 0 },
    cancellation_grace_period: 1, cancellation_warning_threshold: 1, grpc_max_recv_message_length: 4 << 20, grpc_max_send_message_length: 4 << 20,
  };
  const registrations = new Map<string, RegistrationEvidence>();
  const base = createNodeTransport(config);
  const transport: Transport = { ...base, unary: async (...args) => {
    const [method, , , , input] = args;
    let revision: RevisionDescriptor | undefined;
    if (method.name === 'PutWorkflow') {
      const request = input as { name?: string; tasks?: { readableId?: string; action?: string; parents?: string[] }[]; eventTriggers?: unknown[]; cronTriggers?: unknown[]; idempotency?: { expression?: string; ttlMs?: unknown; method?: number } };
      revision = descriptor.revisions.find(r => r.engine_workflow_name === request.name);
      if (!revision || request.eventTriggers?.length || request.cronTriggers?.length || request.idempotency?.expression !== 'input.multica_run_id' || String(request.idempotency.ttlMs) !== '3600000' || request.idempotency.method !== 0) throw new WorkflowError('workflow_registration_mismatch');
      const expected = revision.graph.steps.map(s => ({ readableId: actionNames(revision!.engine_workflow_name, s.id).task_name, action: actionNames(revision!.engine_workflow_name, s.id).action_id, parents: s.parents.map(p => actionNames(revision!.engine_workflow_name, p).task_name).sort() })).sort((a, b) => a.action.localeCompare(b.action));
      const actual = (request.tasks ?? []).map(t => ({ readableId: t.readableId, action: t.action, parents: [...(t.parents ?? [])].sort() })).sort((a, b) => (a.action ?? '').localeCompare(b.action ?? ''));
      if (JSON.stringify(expected) !== JSON.stringify(actual)) throw new WorkflowError('workflow_registration_mismatch');
    }
    // Bound unary RPCs even when upstream omitted a deadline.
    args[2] = Math.min(args[2] ?? 10000, 10000);
    const response = await base.unary(...args);
    if (revision) {
      const result = response.message as unknown as { id: string; workflowId: string };
      if (!result.id || !result.workflowId) throw new WorkflowError('workflow_registration_mismatch');
      const { source_path: _private, ...publicRevision } = revision;
      registrations.set(revision.engine_workflow_name, { ...publicRevision, engine_workflow_id: result.workflowId, engine_version_id: result.id, actions: revision.graph.steps.map(s => ({ step_id: s.id, ...actionNames(revision!.engine_workflow_name, s.id) })) });
    }
    return response;
  } };
  const client = Hatchet.init(config, { transport }, { adapter: denyEngineREST });
  const product = createProductTransport(descriptor.product, descriptor.workspace_id, descriptor.generation);
  const workflows = definitions.map(({ revision, definition }) => {
    const workflow = CreateWorkflow<{ multica_run_id: string; input: JSONValue }, Record<string, { value: JSONValue }>>({ name: revision.engine_workflow_name, idempotency: { strategy: 'ttl', expression: 'input.multica_run_id', ttlMs: 3600000 } });
    const tasks = new Map<string, CreateWorkflowDurableTaskOpts<any, any>>();
    const add = (id: string): CreateWorkflowDurableTaskOpts<any, any> => {
      if (tasks.has(id)) return tasks.get(id)!;
      const step = definition.steps.find(s => s.id === id)!;
      const parents = (step.parents ?? []).map(add);
      const task = workflow.durableTask({ name: actionNames(revision.engine_workflow_name, id).task_name, parents, retries: step.retries ?? 0, executionTimeout: { seconds: (step.timeoutMs ?? 60000) / 1000 }, fn: async (envelope, engineContext) => {
        try {
          if (typeof envelope.multica_run_id !== 'string' || !envelope.multica_run_id) throw new WorkflowError('workflow_invalid_payload');
          const parentValues: Record<string, JSONValue> = Object.create(null);
          for (const parent of step.parents ?? []) {
            const output = await engineContext.parentOutput<{ value: JSONValue }>(tasks.get(parent)!);
            parentValues[parent] = validateJSON(output.value);
          }
          const context = createWorkflowContext({ runId: envelope.multica_run_id, stepId: id, attempt: engineContext.retryCount(), input: envelope.input, parents: parentValues, transport: product, sleep: async ms => { await engineContext.sleepFor({ seconds: ms / 1000 }); }, log: async entry => {
            // Upstream silently drops >1000-character logs; keep a valid bounded record.
            const text = JSON.stringify(entry);
            await engineContext.log(text.length <= 950 ? text : JSON.stringify({ level: entry.level, message: entry.message.slice(0, 500), truncated: true }));
          } });
          return { value: redactJSON(validateJSON(await step.run(context))) };
        } catch (error) {
          // Authored/upstream errors are never serialized back to engine or product.
          const failure = new Error(safeWorkflowErrorCode(error, 'workflow_step_failed'));
          // The SDK serializes Error.stack into the engine failure payload.
          failure.stack = undefined;
          throw failure;
        }
      } });
      task.evictionPolicy = { ttl: '1s', allowCapacityEviction: true };
      tasks.set(id, task); return task;
    };
    for (const step of definition.steps) add(step.id);
    return workflow;
  });
  const worker = await client.worker(`multica-${descriptor.generation}`, { workflows, slots: 10, durableSlots: 10, handleKill: false });
  const running = worker.start();
  running.catch(() => send({ type: 'error', code: 'workflow_worker_failed' }));
  await worker.waitUntilReady(10000);
  if (registrations.size !== descriptor.revisions.length) throw new WorkflowError('workflow_registration_mismatch');
  send({ type: 'ready', protocol: 1, runner_version: RUNNER_VERSION, node_version: process.versions.node, workspace_id: descriptor.workspace_id, tenant_id: descriptor.tenant_id, generation: descriptor.generation, registrations: [...registrations.values()] });
  return { drain: async () => {
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      await Promise.race([worker.stop(), new Promise<never>((_, reject) => { timer = setTimeout(() => reject(new WorkflowError('workflow_drain_timeout')), descriptor.drain_timeout_ms ?? 10000); })]);
      send({ type: 'drained', generation: descriptor.generation });
    } finally { clearTimeout(timer); }
  } };
}
