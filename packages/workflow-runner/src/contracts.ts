import type { WorkflowGraph } from '@multica/workflow-sdk';
export const RUNNER_VERSION = '0.1.0';
export interface ValidationResult { hash: string; graph: WorkflowGraph; graph_digest: string }
export interface RevisionDescriptor {
  workflow_id: string;
  revision_id: string;
  version: number;
  hash: string;
  engine_workflow_name: string;
  source_path: string;
  graph: WorkflowGraph;
  graph_digest: string;
}
/** Private IPC only. Never serialize this descriptor into public APIs/logs/argv. */
export interface WorkerDescriptor {
  protocol: 1;
  workspace_id: string;
  tenant_id: string;
  generation: string;
  engine: {
    token: string;
    host_port: string;
    tls: { strategy: 'tls' | 'mtls'; server_name: string; ca_file: string; cert_file?: string; key_file?: string };
  };
  product: { origin: string; runner_token: string };
  revisions: RevisionDescriptor[];
  drain_timeout_ms?: number;
  activation_gate?: boolean;
}
export interface RegistrationEvidence {
  workflow_id: string;
  revision_id: string;
  version: number;
  hash: string;
  graph: WorkflowGraph;
  graph_digest: string;
  engine_workflow_name: string;
  engine_workflow_id: string;
  engine_version_id: string;
  actions: { step_id: string; task_name: string; action_id: string }[];
}
/** Readiness is evidence for daemon verification, never eligibility by itself. */
export type ChildMessage =
  | { type: 'validated'; result: ValidationResult }
  | { type: 'ready'; protocol: 1; runner_version: string; node_version: string; workspace_id: string; tenant_id: string; generation: string; registrations: RegistrationEvidence[] }
  | { type: 'drained'; generation: string }
  | { type: 'active'; generation: string }
  | { type: 'error'; code: string };
export type ParentMessage = { mode: 'validate' | 'graph'; source: string } | { mode: 'worker'; descriptor: WorkerDescriptor } | { mode: 'drain' | 'activate' | 'retire' };
