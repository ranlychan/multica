"use client";
import { use } from "react";
import { WorkflowRunPage } from "@multica/views/workflows";
export default function Page({ params }: { params: Promise<{ id: string; runId: string }> }) {
  const { id, runId } = use(params);
  return <WorkflowRunPage key={runId} workflowId={id} runId={runId} />;
}
