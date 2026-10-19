import { useMutation, useQueryClient } from "@tanstack/react-query";
import { api } from "../api";
import { workflowKeys } from "./queries";

export function useWorkflowRunAction(wsId: string, id: string, action: "cancel" | "replay") {
  const queryClient = useQueryClient();
  return useMutation({ mutationFn: (runId: string) => api.workflowRunAction(id, runId, action),
    onSettled: () => queryClient.invalidateQueries({ queryKey: workflowKeys.all(wsId) }) });
}
