import { createHash } from 'node:crypto';
import { limits, type JSONValue, type JSONObject, type ActionOperation, type RuntimeContext, type WorkflowContext, type WorkflowDefinition, type StepDefinition, type WorkflowGraph, type WaitOptions } from './contracts.js';
export * from './contracts.js';

export class WorkflowError extends Error {
  constructor(readonly code: string) { super(code); this.name = 'WorkflowError'; }
}

// The public error class is author-constructible. Its contents are not trusted
// transport metadata; only these fixed codes may cross execution boundaries.
const safeErrorCodes = new Set([
  'workflow_action_failed', 'workflow_capability_revoked', 'workflow_drain_timeout',
  'workflow_engine_rest_denied', 'workflow_forbidden', 'workflow_graph_mismatch',
  'workflow_hash_mismatch', 'workflow_invalid_descriptor', 'workflow_invalid_duration',
  'workflow_invalid_graph', 'workflow_invalid_import', 'workflow_invalid_log',
  'workflow_invalid_operation_key', 'workflow_invalid_payload', 'workflow_invalid_response',
  'workflow_invalid_source', 'workflow_invalid_status', 'workflow_log_limit',
  'workflow_node_unavailable', 'workflow_not_found', 'workflow_operation_conflict',
  'workflow_rate_limited', 'workflow_registration_mismatch', 'workflow_step_failed',
  'workflow_transport_unavailable', 'workflow_unauthenticated', 'workflow_unavailable',
  'workflow_validation_output_limit', 'workflow_validation_timeout',
  'workflow_wait_failed', 'workflow_wait_timeout', 'workflow_worker_failed',
]);
export function safeWorkflowErrorCode(error: unknown, fallback: 'workflow_transport_unavailable' | 'workflow_step_failed'): string {
  try {
    const code = error instanceof WorkflowError ? Object.getOwnPropertyDescriptor(error, 'code')?.value : undefined;
    return typeof code === 'string' && safeErrorCodes.has(code) ? code : fallback;
  } catch {
    return fallback;
  }
}

export function validateJSON(value: unknown): JSONValue {
  const seen = new Set<object>();
  function visit(v: unknown, depth: number): void {
    if (depth > limits.depth) throw new WorkflowError('workflow_invalid_payload');
    if (v === null || typeof v === 'string' || typeof v === 'boolean' || (typeof v === 'number' && Number.isFinite(v))) return;
    if (typeof v !== 'object' || seen.has(v) || (!Array.isArray(v) && Object.getPrototypeOf(v) !== Object.prototype && Object.getPrototypeOf(v) !== null)) throw new WorkflowError('workflow_invalid_payload');
    seen.add(v);
    if (Reflect.ownKeys(v).some(k => typeof k === 'symbol')) throw new WorkflowError('workflow_invalid_payload');
    if (Array.isArray(v)) { for (const child of v) visit(child, depth + 1); }
    else { for (const child of Object.values(v)) visit(child, depth + 1); }
    seen.delete(v);
  }
  visit(value, 1);
  if (Buffer.byteLength(JSON.stringify(value)) > limits.payloadBytes) throw new WorkflowError('workflow_invalid_payload');
  return value as JSONValue;
}

export function redactText(text: string): string {
  return text.replace(/-----BEGIN ([A-Z ]*(?:PRIVATE KEY|CERTIFICATE))-----[\s\S]*?-----END \1-----/g, '[redacted]')
    .replace(/(?<![A-Za-z0-9_-])(?:eyJ[A-Za-z0-9_-]*|e30)\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+(?![A-Za-z0-9_-])/g, '[redacted]')
    .replace(/Bearer\s+[^\s"<>]+|(?:wft_|mul_|mat_)[A-Za-z0-9_-]+|(?:password|secret|token|api[_-]?key)["']?\s*[:=]\s*["']?[^\s,"'<>]+/gi, '[redacted]')
    .replace(/(?:[A-Za-z]:[\\/]|\\\\|(?<![\w:/])\/(?!\/))[^\s"'<>]*/g, '[redacted]');
}
export function redactJSON(value: JSONValue): JSONValue {
  if (typeof value === 'string') return redactText(value);
  if (Array.isArray(value)) return value.map(redactJSON);
  if (value && typeof value === 'object') return Object.fromEntries(Object.entries(value).map(([key, child]) => [redactText(key), /password|secret|token|credential|authorization|cookie|privatekey|apikey|hatchet|tenantid|certfile|keyfile|cafile/i.test(key.replace(/[_ -]/g, '')) ? '[redacted]' : redactJSON(child)]));
  return value;
}

export function validateGraph(graph: WorkflowGraph): void {
  const fail = () => { throw new WorkflowError('workflow_invalid_graph'); };
  if (!graph || !Array.isArray(graph.steps) || !graph.steps.length || graph.steps.length > limits.steps || Buffer.byteLength(JSON.stringify(graph)) > limits.graphBytes) fail();
  const nodes = new Map<string, string[]>();
  for (const step of graph.steps) {
    if (!step || typeof step.id !== 'string' || !step.id || Buffer.byteLength(step.id) > 128 || nodes.has(step.id) || !Array.isArray(step.parents) || step.parents.some(p => typeof p !== 'string') || new Set(step.parents).size !== step.parents.length) fail();
    nodes.set(step.id, step.parents);
  }
  const visiting = new Set<string>(), depths = new Map<string, number>();
  function visit(id: string): number {
    if (visiting.has(id) || !nodes.has(id)) return fail();
    if (depths.has(id)) return depths.get(id)!;
    visiting.add(id);
    const depth = 1 + Math.max(0, ...nodes.get(id)!.map(visit));
    if (depth > limits.depth) fail();
    visiting.delete(id); depths.set(id, depth); return depth;
  }
  for (const id of nodes.keys()) visit(id);
}

export function defineWorkflow(config: { steps: StepDefinition[] }): WorkflowDefinition {
  const fail = () => { throw new WorkflowError('workflow_invalid_graph'); };
  if (!config || Object.keys(config).some(k => k !== 'steps') || !Array.isArray(config.steps)) fail();
  for (const s of config.steps) {
    if (!s || Object.keys(s).some(k => !['id', 'parents', 'retries', 'timeoutMs', 'run'].includes(k)) || typeof s.run !== 'function') fail();
    if (s.retries !== undefined && (!Number.isInteger(s.retries) || s.retries < 0 || s.retries > 10)) fail();
    if (s.timeoutMs !== undefined && (!Number.isInteger(s.timeoutMs) || s.timeoutMs < 1 || s.timeoutMs > 86400000)) fail();
  }
  const graph = { steps: config.steps.map(s => ({ id: s.id, parents: [...(s.parents ?? [])] })) };
  validateGraph(graph);
  return Object.freeze({ steps: Object.freeze(config.steps.map(s => Object.freeze({ ...s, parents: Object.freeze([...(s.parents ?? [])]) as unknown as string[] }))), graph });
}

export function operationKey(runId: string, stepId: string, authorKey: string): string {
  if (![runId, stepId, authorKey].every(v => typeof v === 'string' && v.length > 0 && Buffer.byteLength(v) <= 128)) throw new WorkflowError('workflow_invalid_operation_key');
  return createHash('sha256').update(JSON.stringify([runId, stepId, authorKey])).digest('hex');
}

export function createWorkflowContext(runtime: RuntimeContext): WorkflowContext {
  const input = validateJSON(runtime.input);
  const parents = validateJSON(runtime.parents) as Record<string, JSONValue>;
  const call = async (operation: ActionOperation, payload: JSONObject, key?: string): Promise<JSONValue> => {
    validateJSON(payload);
    if (['issues.create', 'agents.run', 'autopilots.invoke'].includes(operation) && key === undefined) throw new WorkflowError('workflow_invalid_operation_key');
    try {
      return redactJSON(validateJSON(await runtime.transport({ protocol: 1, run_id: runtime.runId, step_id: runtime.stepId, attempt: runtime.attempt, operation, ...(key === undefined ? {} : { operation_key: operationKey(runtime.runId, runtime.stepId, key) }), payload })));
    } catch (err) {
      throw new WorkflowError(safeWorkflowErrorCode(err, 'workflow_transport_unavailable'));
    }
  };
  const sleep = async (ms: number) => {
    if (!Number.isSafeInteger(ms) || ms < 1 || ms > 86400000) throw new WorkflowError('workflow_invalid_duration');
    await runtime.sleep(ms);
  };
  const wait = async (operation: 'issues.get' | 'agents.get' | 'autopilots.get', id: string, options: WaitOptions = {}, desiredStatus?: string): Promise<JSONValue> => {
    const timeout = options.timeoutMs ?? 3600000;
    if (!Number.isSafeInteger(timeout) || timeout < 1 || timeout > 86400000) throw new WorkflowError('workflow_invalid_duration');
    let elapsed = 0, delay = 1000;
    for (;;) {
      const value = await call(operation, { id });
      const status = value && typeof value === 'object' && !Array.isArray(value) ? value.status : undefined;
      if (desiredStatus === undefined ? status === 'completed' || status === 'done' || status === 'succeeded' : status === desiredStatus) return value;
      if (desiredStatus === undefined && (status === 'failed' || status === 'cancelled')) throw new WorkflowError('workflow_wait_failed');
      if (elapsed >= timeout) throw new WorkflowError('workflow_wait_timeout');
      const duration = Math.min(delay, timeout - elapsed);
      await sleep(duration); elapsed += duration; delay = Math.min(30000, delay * 2);
    }
  };
  let logEntries = 0, logBytes = 2;
  return Object.freeze({ input, parents, runId: runtime.runId, stepId: runtime.stepId, attempt: runtime.attempt, sleep,
    log: async (level, message, fields = null) => {
      if (!['debug', 'info', 'warn', 'error'].includes(level) || typeof message !== 'string') throw new WorkflowError('workflow_invalid_log');
      const entry = { level, message: redactText(message).slice(0, 4096), fields: redactJSON(validateJSON(fields)) };
      const bytes = Buffer.byteLength(JSON.stringify(entry));
      if (++logEntries > limits.logEntries || (logBytes += bytes + 1) > limits.logBytes) throw new WorkflowError('workflow_log_limit');
      await runtime.log(entry);
    },
    issues: {
      get: id => call('issues.get', { id }),
      create: (key, payload) => call('issues.create', payload, key),
      waitForStatus: async (id, status, options) => {
        if (typeof status !== 'string' || !status || Buffer.byteLength(status) > 128) throw new WorkflowError('workflow_invalid_status');
        return wait('issues.get', id, options, status);
      },
    },
    agents: { dispatch: (key, payload) => call('agents.run', payload, key), waitForRun: (id, options) => wait('agents.get', id, options) },
    autopilots: { run: (key, payload) => call('autopilots.invoke', payload, key), waitForRun: (id, options) => wait('autopilots.get', id, options) },
  } satisfies WorkflowContext);
}
