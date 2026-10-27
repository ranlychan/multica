import { useMutation, useQueryClient } from "@tanstack/react-query";
import { api } from "../api";
import { workflowKeys } from "./queries";
import type { WorkflowRun } from "../types/workflow";

export function useUpdateWorkflow(wsId: string, id: string) {
  const queryClient = useQueryClient();
  return useMutation({ mutationFn: (data: Parameters<typeof api.updateWorkflow>[1]) => api.updateWorkflow(id, data),
    onSettled: () => queryClient.invalidateQueries({ queryKey: workflowKeys.all(wsId) }) });
}

export function useCreateWorkflowRun(wsId: string, id: string) {
  const queryClient = useQueryClient();
  return useMutation({ mutationFn: (input: WorkflowRun["input"]) => api.createWorkflowRun(id, input),
    onSettled: () => queryClient.invalidateQueries({ queryKey: workflowKeys.all(wsId) }) });
}

export function useWorkflowRunAction(wsId: string, id: string, action: "cancel" | "replay") {
  const queryClient = useQueryClient();
  return useMutation({ mutationFn: (runId: string) => api.workflowRunAction(id, runId, action),
    onSettled: () => queryClient.invalidateQueries({ queryKey: workflowKeys.all(wsId) }) });
}
