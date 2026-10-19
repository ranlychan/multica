import { useParams } from "react-router-dom";
import { WorkflowRunPage as WorkflowRun } from "@multica/views/workflows";
export function WorkflowRunPage() {
  const { id, runId } = useParams<{ id: string; runId: string }>();
  return id && runId ? <WorkflowRun key={runId} workflowId={id} runId={runId} /> : null;
}
