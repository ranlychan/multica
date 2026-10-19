import { queryOptions } from "@tanstack/react-query";
import { api } from "../api";

export const WORKFLOW_PAGE_SIZE = 25;
export const workflowKeys = {
  all: (wsId: string) => ["workflows", wsId] as const,
  list: (wsId: string, offset: number) => ["workflows", wsId, "list", offset] as const,
  detail: (wsId: string, id: string) => ["workflows", wsId, id] as const,
};
export const isWorkflowRunActive = (status: string) => ["queued", "submitting", "running"].includes(status);
export function workflowListOptions(wsId: string, offset = 0) {
  return queryOptions({ queryKey: workflowKeys.list(wsId, offset), queryFn: () => api.listWorkflows(offset), enabled: !!wsId, refetchInterval: 10_000 });
}
export function workflowDetailOptions(wsId: string, id: string) {
  return queryOptions({ queryKey: workflowKeys.detail(wsId, id), queryFn: () => api.getWorkflow(id), enabled: !!wsId && !!id, refetchInterval: 10_000 });
}
export function workflowRevisionsOptions(wsId: string, id: string, offset = 0) {
  return queryOptions({ queryKey: [...workflowKeys.detail(wsId, id), "revisions", offset], queryFn: () => api.listWorkflowRevisions(id, offset), enabled: !!wsId && !!id });
}
export function workflowRevisionOptions(wsId: string, id: string, revisionId: string) {
  return queryOptions({ queryKey: [...workflowKeys.detail(wsId, id), "revision", revisionId], queryFn: () => api.getWorkflowRevision(id, revisionId), enabled: !!wsId && !!revisionId, staleTime: Infinity });
}
export function workflowTriggersOptions(wsId: string, id: string) {
  return queryOptions({ queryKey: [...workflowKeys.detail(wsId, id), "triggers"], queryFn: () => api.listWorkflowTriggers(id), enabled: !!wsId && !!id, refetchInterval: 10_000 });
}
export function workflowRunsOptions(wsId: string, id: string, offset = 0) {
  return queryOptions({ queryKey: [...workflowKeys.detail(wsId, id), "runs", offset], queryFn: () => api.listWorkflowRuns(id, offset), enabled: !!wsId && !!id, refetchInterval: 5_000 });
}
export function workflowRunOptions(wsId: string, id: string, runId: string) {
  return queryOptions({ queryKey: [...workflowKeys.detail(wsId, id), "run", runId], queryFn: () => api.getWorkflowRun(id, runId), enabled: !!wsId && !!id && !!runId,
    refetchInterval: (query) => query.state.data && isWorkflowRunActive(query.state.data.status) ? 5_000 : false });
}
export function workflowLogsOptions(wsId: string, id: string, runId: string, offset = 0, status?: string) {
  const active = status !== undefined && isWorkflowRunActive(status);
  // A terminal snapshot must not reuse (or race) a page fetched before final projection.
  return queryOptions({ queryKey: [...workflowKeys.detail(wsId, id), "logs", runId, offset, active ? "active" : "terminal"], queryFn: () => api.getWorkflowLogs(id, runId, offset), enabled: !!wsId && !!id && !!runId && status !== undefined, refetchInterval: active ? 5_000 : false });
}
