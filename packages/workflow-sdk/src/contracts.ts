export type JSONValue = null | boolean | number | string | JSONValue[] | { [key: string]: JSONValue };
export type JSONObject = { [key: string]: JSONValue };
// These limits mirror server/internal/service/workflow.go. A JSON leaf has depth 1.
export const limits = Object.freeze({ sourceBytes: 256 << 10, payloadBytes: 64 << 10, graphBytes: 128 << 10, steps: 256, depth: 32, pageSize: 100, logBytes: 64 << 10, logEntries: 200 });
export type WorkflowGraph = { steps: { id: string; parents: string[] }[] };
export type ActionOperation = 'issues.get' | 'issues.create' | 'agents.run' | 'agents.get' | 'autopilots.invoke' | 'autopilots.get';
/** Internal HTTP body. Server must authorize each request, verify run/step/generation,
 * and persist operation_key + operation/payload atomically (mismatch: HTTP 409).
 * attempt is audit-only; it never participates in effect identity. */
export interface ActionRequest {
  protocol: 1;
  run_id: string;
  step_id: string;
  attempt: number;
  operation: ActionOperation;
  operation_key?: string;
  payload: JSONObject;
}
export type ActionResponse = { result: JSONValue } | { error: { code: string } };
export type ActionTransport = (request: ActionRequest) => Promise<JSONValue>;
export type LogEntry = { level: 'debug' | 'info' | 'warn' | 'error'; message: string; fields: JSONValue };
export interface RuntimeContext {
  runId: string;
  stepId: string;
  attempt: number;
  input: JSONValue;
  parents: Record<string, JSONValue>;
  transport: ActionTransport;
  sleep: (ms: number) => Promise<void>;
  log: (entry: LogEntry) => Promise<void>;
}
export interface WaitOptions { timeoutMs?: number }
export interface WorkflowContext {
  readonly input: JSONValue;
  readonly parents: Readonly<Record<string, JSONValue>>;
  readonly runId: string;
  readonly stepId: string;
  readonly attempt: number;
  sleep(ms: number): Promise<void>;
  log(level: LogEntry['level'], message: string, fields?: JSONValue): Promise<void>;
  issues: {
    get(id: string): Promise<JSONValue>;
    create(key: string, payload: JSONObject): Promise<JSONValue>;
    waitForStatus(id: string, status: string, options?: WaitOptions): Promise<JSONValue>;
  };
  agents: { dispatch(key: string, payload: JSONObject): Promise<JSONValue>; waitForRun(id: string, options?: WaitOptions): Promise<JSONValue> };
  autopilots: { run(key: string, payload: JSONObject): Promise<JSONValue>; waitForRun(id: string, options?: WaitOptions): Promise<JSONValue> };
}
export interface StepDefinition {
  id: string;
  parents?: string[];
  retries?: number;
  timeoutMs?: number;
  run: (context: WorkflowContext) => JSONValue | Promise<JSONValue>;
}
export interface WorkflowDefinition { readonly steps: readonly StepDefinition[]; readonly graph: WorkflowGraph }
