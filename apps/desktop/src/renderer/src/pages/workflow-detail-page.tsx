import { useParams } from "react-router-dom";
import { WorkflowDetailPage as WorkflowDetail } from "@multica/views/workflows";
export function WorkflowDetailPage() {
  const { id } = useParams<{ id: string }>();
  return id ? <WorkflowDetail key={id} workflowId={id} /> : null;
}
