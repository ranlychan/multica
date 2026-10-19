"use client";
import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { useWorkspaceId } from "@multica/core/hooks";
import { useWorkspaceSlug } from "@multica/core/paths";
import { workflowDetailOptions, workflowRevisionOptions, workflowRevisionsOptions, workflowRunsOptions, workflowTriggersOptions } from "@multica/core/workflows";
import { AppLink } from "../navigation";
import { useT } from "../i18n";
import { WorkflowAvailability, WorkflowGraph, WorkflowLoadState, WorkflowPagination, WorkflowTime } from "./common";

export function WorkflowDetailPage({ workflowId }: { workflowId: string }) {
  const wsId = useWorkspaceId(); const slug = useWorkspaceSlug(); const { t } = useT("autopilots");
  const [selected, setSelected] = useState(""); const [offset, setOffset] = useState(0); const [revisionOffset, setRevisionOffset] = useState(0);
  const workflow = useQuery(workflowDetailOptions(wsId, workflowId));
  const revisions = useQuery(workflowRevisionsOptions(wsId, workflowId, revisionOffset));
  const revisionId = selected || workflow.data?.currentRevisionId || "";
  const revision = useQuery(workflowRevisionOptions(wsId, workflowId, revisionId));
  const triggers = useQuery(workflowTriggersOptions(wsId, workflowId));
  const runs = useQuery(workflowRunsOptions(wsId, workflowId, offset));
  if (workflow.isPending || !workflow.data) return <WorkflowLoadState loading={workflow.isPending} />;
  return <main className="flex-1 min-h-0 space-y-5 overflow-auto p-4 text-body">
    <AppLink href={`/${slug}/autopilots?tab=workflows`} className="hover:underline">{t(($) => $.workflows.back)}</AppLink>
    <h1 className="break-all text-title font-semibold">{workflow.data.name}</h1>
    {workflow.data.description && <p className="break-words">{workflow.data.description}</p>}
    <WorkflowAvailability state={workflow.data.runtimeAvailability.state} />
    <p className="text-muted-foreground">{t(($) => $.workflows.trust)}</p>
    <section><label className="flex flex-wrap items-center gap-2">{t(($) => $.workflows.revision)}<select className="rounded border bg-background p-2" value={revisionId} onChange={(event) => setSelected(event.target.value)}>
      {revision.data && !revisions.data?.some((r) => r.id === revision.data?.id) && <option value={revision.data.id}>{revision.data.version}</option>}
      {revisions.data?.map((r) => <option key={r.id} value={r.id}>{r.version} · {r.validationStatus}</option>)}
    </select></label><WorkflowPagination offset={revisionOffset} count={revisions.data?.length ?? 0} pending={revisions.isFetching} onChange={setRevisionOffset} />
    {revisions.isError && <WorkflowLoadState loading={false} />}
    {!revisionId ? <p>{t(($) => $.workflows.noRevision)}</p> : revision.data ? <><p className="break-all">{t(($) => $.workflows.hash)}: {revision.data.definitionHash}</p><WorkflowGraph revision={revision.data} /><h2 className="mt-4 font-medium">{t(($) => $.workflows.source)}</h2><pre className="max-h-96 overflow-auto rounded border p-3"><code>{revision.data.definition}</code></pre></> : <WorkflowLoadState loading={revision.isPending} />}
    </section>
    <section><h2 className="font-medium">{t(($) => $.workflows.triggers)}</h2>{triggers.isError ? <WorkflowLoadState loading={false} /> : !triggers.data?.length ? <p>{t(($) => $.workflows.noTriggers)}</p> : <ul className="space-y-2">{triggers.data.map((trigger) => <li key={trigger.id} className="break-all">{trigger.kind} · {trigger.enabled ? t(($) => $.workflows.enabled) : t(($) => $.workflows.disabledTrigger)} · {trigger.cronExpression} {trigger.timezone} · <WorkflowTime value={trigger.nextRunAt} /></li>)}</ul>}</section>
    <section><h2 className="font-medium">{t(($) => $.workflows.history)}</h2>{runs.isError ? <WorkflowLoadState loading={false} /> : !runs.data?.length ? <p>{t(($) => $.workflows.noRuns)}</p> : <ul className="space-y-2">{runs.data.map((run) => <li className="break-all" key={run.id}><AppLink className="hover:underline" href={`/${slug}/workflows/${workflowId}/runs/${run.id}`}>{run.status} · <WorkflowTime value={run.triggeredAt} /> · {run.id}</AppLink></li>)}</ul>}<WorkflowPagination offset={offset} count={runs.data?.length ?? 0} pending={runs.isFetching} onChange={setOffset} /></section>
  </main>;
}
