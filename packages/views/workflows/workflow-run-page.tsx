"use client";
import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { useWorkspaceId } from "@multica/core/hooks";
import { useWorkspaceSlug } from "@multica/core/paths";
import { isWorkflowRunActive, useWorkflowRunAction, workflowDetailOptions, workflowRevisionOptions, workflowRunOptions, workflowLogsOptions } from "@multica/core/workflows";
import { Button } from "@multica/ui/components/ui/button";
import { AppLink, useNavigation } from "../navigation";
import { useT } from "../i18n";
import { WorkflowGraph, WorkflowLoadState, WorkflowPagination, WorkflowTime } from "./common";

export function WorkflowRunPage({ workflowId, runId }: { workflowId: string; runId: string }) {
  const wsId = useWorkspaceId(); const slug = useWorkspaceSlug(); const { t } = useT("autopilots"); const navigation = useNavigation();
  const [offset, setOffset] = useState(0);
  const workflow = useQuery({ ...workflowDetailOptions(wsId, workflowId), refetchInterval: false });
  const run = useQuery(workflowRunOptions(wsId, workflowId, runId));
  const active = !!run.data && isWorkflowRunActive(run.data.status);
  const revision = useQuery(workflowRevisionOptions(wsId, workflowId, run.data?.revisionId ?? ""));
  const logs = useQuery(workflowLogsOptions(wsId, workflowId, runId, offset, run.data?.status));
  const cancel = useWorkflowRunAction(wsId, workflowId, "cancel"); const replay = useWorkflowRunAction(wsId, workflowId, "replay");
  if (run.isPending || workflow.isPending || !run.data || !workflow.data) return <WorkflowLoadState loading={run.isPending || workflow.isPending} />;
  const canWrite = workflow.data.canWrite;
  return <main className="flex-1 min-h-0 space-y-5 overflow-auto p-4 text-body">
    <AppLink href={`/${slug}/workflows/${workflowId}`} className="break-all hover:underline">{workflow.data.name}</AppLink>
    <h1 className="break-all text-title font-semibold">{run.data.id}</h1>
    <p>{run.data.status}{run.data.reasonCode && <> · {run.data.reasonCode}</>}</p>
    <div className="flex flex-wrap gap-2"><Button variant="outline" disabled={!canWrite || !active || !!run.data.cancelRequestedAt || cancel.isPending} aria-busy={cancel.isPending} onClick={() => cancel.mutate(runId)}>{t(($) => $.workflows.cancel)}</Button><Button disabled={!canWrite || workflow.data.runtimeAvailability.state !== "ready" || replay.isPending} aria-busy={replay.isPending} onClick={() => replay.mutate(runId, { onSuccess: (next) => navigation.push(`/${slug}/workflows/${workflowId}/runs/${next.id}`) })}>{t(($) => $.workflows.replay)}</Button></div>
    {!canWrite && <p>{t(($) => $.workflows.permission)}</p>}
    <p className="text-muted-foreground">{t(($) => $.workflows.replayHelp)}</p>
    {(cancel.isError || replay.isError) && <p role="alert">{t(($) => $.workflows.actionError)}</p>}
    {run.data.cancelRequestedAt && <p role="status">{t(($) => $.workflows.cancelPending)}</p>}
    {run.data.revisionId !== workflow.data.currentRevisionId && <p>{t(($) => $.workflows.stale)}</p>}
    <dl className="grid grid-cols-[auto_1fr] gap-x-4 gap-y-2"><dt>{t(($) => $.workflows.principal)}</dt><dd className="break-all">{run.data.principalUserId}</dd><dt>{t(($) => $.workflows.revision)}</dt><dd className="break-all">{revision.data?.version ?? run.data.revisionId}</dd><dt>{t(($) => $.workflows.hash)}</dt><dd className="break-all">{revision.data?.definitionHash ?? "—"}</dd><dt>{t(($) => $.workflows.started)}</dt><dd><WorkflowTime value={run.data.startedAt} /></dd><dt>{t(($) => $.workflows.finished)}</dt><dd><WorkflowTime value={run.data.finishedAt} /></dd></dl>
    <section><h2 className="font-medium">{t(($) => $.workflows.input)}</h2><pre className="max-h-72 overflow-auto whitespace-pre-wrap break-all rounded border p-3">{JSON.stringify(run.data.input, null, 2)}</pre></section>
    {revision.data ? <WorkflowGraph revision={revision.data} run={run.data} /> : <WorkflowLoadState loading={revision.isPending} />}
    <section><h2 className="font-medium">{t(($) => $.workflows.logs)}</h2>
      {run.data.reasonCode === "workflow_final_logs_incomplete" && <p role="alert">{t(($) => $.workflows.incomplete)}</p>}
      {logs.isPending || logs.isError || !logs.data ? <WorkflowLoadState loading={logs.isPending} /> : !logs.data.available ? <p>{t(($) => $.workflows.logsPending)}</p> : !logs.data.entries.length ? <p>{t(($) => $.workflows.noLogs)}</p> : <ol className="max-h-96 space-y-2 overflow-auto rounded border p-3">{logs.data.entries.map((entry, index) => <li key={`${offset}-${index}`}><span className="text-muted-foreground">{entry.createdAt} · {entry.stepId} · {entry.level}</span><pre className="whitespace-pre-wrap break-all">{entry.message}</pre></li>)}</ol>}
      <WorkflowPagination offset={offset} count={logs.data?.entries.length ?? 0} pending={logs.isFetching} onChange={setOffset} />
    </section>
  </main>;
}
