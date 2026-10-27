"use client";

import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { ChevronRight, Clock, Webhook } from "lucide-react";
import { useWorkspaceId } from "@multica/core/hooks";
import { useWorkspaceSlug } from "@multica/core/paths";
import { useUpdateWorkflow, workflowDetailOptions, workflowRevisionOptions, workflowRevisionsOptions, workflowRunsOptions, workflowTriggersOptions } from "@multica/core/workflows";
import { Switch } from "@multica/ui/components/ui/switch";
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle, DialogTrigger } from "@multica/ui/components/ui/dialog";
import { AppLink } from "../navigation";
import { BreadcrumbHeader } from "../layout/breadcrumb-header";
import { PAGE_GUTTER } from "../layout/page-header";
import { useT } from "../i18n";
import { WorkflowAvailability, WorkflowGraph, WorkflowLoadState, WorkflowPagination, WorkflowTime, WorkflowRunStatus, WorkflowDuration, WorkflowRunSource } from "./common";
import { WorkflowEditDialog, WorkflowManualRunDialog } from "./workflow-action-dialogs";
import { WorkflowRunContent } from "./workflow-run-page";

export function WorkflowDetailPage({ workflowId }: { workflowId: string }) {
  const wsId = useWorkspaceId();
  const slug = useWorkspaceSlug();
  const { t } = useT("autopilots");
  const [selected, setSelected] = useState("");
  const [runId, setRunId] = useState<string | null>(null);
  const [offset, setOffset] = useState(0);
  const [revisionOffset, setRevisionOffset] = useState(0);
  const workflow = useQuery(workflowDetailOptions(wsId, workflowId));
  const revisions = useQuery(workflowRevisionsOptions(wsId, workflowId, revisionOffset));
  const revisionId = selected || workflow.data?.currentRevisionId || "";
  const revision = useQuery(workflowRevisionOptions(wsId, workflowId, revisionId));
  const triggers = useQuery(workflowTriggersOptions(wsId, workflowId));
  const runs = useQuery(workflowRunsOptions(wsId, workflowId, offset));
  const update = useUpdateWorkflow(wsId, workflowId);
  if (workflow.isPending || !workflow.data) return <WorkflowLoadState loading={workflow.isPending} />;
  const flow = workflow.data;
  const enabled = flow.status === "active";
  const status = flow.status === "active" || flow.status === "paused" || flow.status === "archived" ? flow.status : "archived";
  return <div className="flex h-full min-h-0 flex-col">
    <BreadcrumbHeader segments={[{ href: `/${slug}/autopilots?tab=workflows`, label: t(($) => $.page.title) }]} leaf={<>
      <h1 className="min-w-0 truncate text-body font-medium">{flow.name}</h1>
      <div className="ml-1 flex shrink-0 items-center gap-1.5">
        <Switch size="sm" checked={enabled} disabled={!flow.canWrite || !flow.updatedAt || flow.status === "archived" || update.isPending} aria-label={enabled ? t(($) => $.workflows.detailUi.pause) : t(($) => $.workflows.detailUi.enable)} onCheckedChange={(checked) => {
          if (flow.updatedAt) update.mutate({ status: checked ? "active" : "paused", expectedUpdatedAt: flow.updatedAt });
        }} />
        <span className="hidden text-caption text-muted-foreground sm:inline">{t(($) => $.status[status])}</span>
      </div>
    </>} actions={flow.canWrite ? <><WorkflowEditDialog key={workflowId} workflow={flow} wsId={wsId} refresh={async () => (await workflow.refetch()).data ?? null} /><WorkflowManualRunDialog key={`run-${workflowId}`} workflow={flow} wsId={wsId} onCreated={(run) => setRunId(run.id)} /></> : undefined} />
    <main className={`min-h-0 flex-1 overflow-auto ${PAGE_GUTTER}`}>
      <div className="mx-auto max-w-4xl space-y-8 py-6 text-body">
        {update.isError && <p role="alert" className="text-destructive">{t(($) => $.workflows.actionError)}</p>}
        <section className="space-y-4">
          <h2 className="text-title font-semibold">{t(($) => $.workflows.detailUi.properties)}</h2>
          {flow.description && <p className="whitespace-pre-wrap break-words">{flow.description}</p>}
          <dl className="grid grid-cols-[minmax(0,8rem)_minmax(0,1fr)] gap-x-4 gap-y-3">
            <dt className="text-muted-foreground">{t(($) => $.workflows.revision)}</dt><dd>{`v${flow.definitionVersion}`}</dd>
            <dt className="text-muted-foreground">{t(($) => $.workflows.runtime)}</dt><dd><WorkflowAvailability state={flow.runtimeAvailability.state} /></dd>
            <dt className="text-muted-foreground">{t(($) => $.workflows.nextRun)}</dt><dd><WorkflowTime value={flow.nextRunAt} /></dd>
          </dl>
          <p className="text-caption text-muted-foreground">{t(($) => $.workflows.detailUi.pauseHelp)}</p>
          {!flow.canWrite && <p className="text-caption text-muted-foreground">{t(($) => $.workflows.permission)}</p>}
        </section>
        <section className="space-y-3">
          {revisionId ? revision.data ? <WorkflowGraph revision={revision.data} /> : <WorkflowLoadState loading={revision.isPending} /> : <p>{t(($) => $.workflows.noRevision)}</p>}
          <details className="rounded-md border p-4"><summary className="cursor-pointer font-medium">{t(($) => $.workflows.detailUi.advanced)}</summary>
            <div className="mt-4 space-y-3">
              <label className="flex flex-wrap items-center gap-2">{t(($) => $.workflows.revision)}<select className="rounded-md border bg-background p-2" value={revisionId} onChange={(event) => setSelected(event.target.value)}>
                {revision.data && !revisions.data?.some((r) => r.id === revision.data?.id) && <option value={revision.data.id}>{`v${revision.data.version}`}</option>}
                {revisions.data?.map((r) => <option key={r.id} value={r.id}>{`v${r.version} · ${r.validationStatus}`}</option>)}
              </select></label>
              <WorkflowPagination offset={revisionOffset} count={revisions.data?.length ?? 0} pending={revisions.isFetching} onChange={setRevisionOffset} />
              {revisions.isError && <WorkflowLoadState loading={false} />}
              <p className="text-caption text-muted-foreground">{t(($) => $.workflows.detailUi.editHelp)}</p>
              {revision.data && <><p className="break-all text-caption text-muted-foreground">{t(($) => $.workflows.hash)}: {revision.data.definitionHash}</p><pre className="max-h-96 overflow-auto rounded-md bg-muted/30 p-4 text-caption"><code>{revision.data.definition}</code></pre></>}
            </div>
          </details>
        </section>
        <section className="space-y-3"><h2 className="text-title font-semibold">{t(($) => $.workflows.triggers)}</h2>
          {triggers.isPending || triggers.isError ? <WorkflowLoadState loading={triggers.isPending} /> : !triggers.data?.length ? <p className="text-muted-foreground">{t(($) => $.workflows.noTriggers)}</p> : <ul className="divide-y rounded-md border">{triggers.data.map((trigger) => {
            const Icon = trigger.kind === "schedule" ? Clock : Webhook;
            return <li key={trigger.id} className="flex flex-wrap items-center gap-3 p-4"><Icon className="size-4 shrink-0 text-muted-foreground" /><span className="font-medium">{trigger.kind}</span><span className="text-caption text-muted-foreground">{trigger.enabled ? t(($) => $.workflows.enabled) : t(($) => $.workflows.disabledTrigger)}</span>{trigger.cronExpression && <code className="text-caption">{trigger.cronExpression}</code>}<span className="text-caption text-muted-foreground">{trigger.timezone}</span><span className="ml-auto text-caption text-muted-foreground"><WorkflowTime value={trigger.enabled ? trigger.nextRunAt : null} /></span></li>;
          })}</ul>}
        </section>
        <Dialog open={!!runId} onOpenChange={(open) => { if (!open) setRunId(null); }}>
          <section className="space-y-3"><h2 className="text-title font-semibold">{t(($) => $.workflows.history)}</h2>
            {runs.isPending || runs.isError ? <WorkflowLoadState loading={runs.isPending} /> : !runs.data?.length ? <p className="text-muted-foreground">{t(($) => $.workflows.noRuns)}</p> : <ul className="divide-y overflow-hidden rounded-md border">{runs.data.map((run) => <li key={run.id}>
              <DialogTrigger render={<button type="button" className="flex w-full flex-wrap items-center gap-3 px-4 py-3 text-left transition-colors hover:bg-accent/30 focus-visible:outline-2 focus-visible:outline-ring" />} onClick={() => setRunId(run.id)} aria-label={`${run.id} · ${run.status}`}>
                <WorkflowRunStatus status={run.status} /><span className="text-caption text-muted-foreground"><WorkflowRunSource source={run.source} /></span><span className="min-w-0 flex-1 truncate font-mono text-caption text-muted-foreground">{run.id.slice(0, 8)}</span><span className="text-caption text-muted-foreground"><WorkflowDuration start={run.startedAt} end={run.finishedAt} /></span><span className="text-caption text-muted-foreground"><WorkflowTime value={run.triggeredAt} /></span><ChevronRight className="size-3.5 text-muted-foreground" />
              </DialogTrigger>
            </li>)}</ul>}
            <WorkflowPagination offset={offset} count={runs.data?.length ?? 0} pending={runs.isFetching} onChange={setOffset} />
          </section>
          <DialogContent className="flex h-[85dvh] max-h-[calc(100dvh-2rem)] flex-col overflow-hidden sm:max-w-4xl">
            <DialogHeader className="shrink-0 pr-8"><DialogTitle>{t(($) => $.workflows.detailUi.runDetails)}</DialogTitle><DialogDescription className="break-all">{flow.name}</DialogDescription></DialogHeader>
            {runId && <><AppLink className="w-fit shrink-0 text-caption text-muted-foreground hover:underline" href={`/${slug}/workflows/${workflowId}/runs/${runId}`}>{t(($) => $.workflows.detailUi.openPage)}</AppLink><div className="min-h-0 flex-1 overflow-auto"><WorkflowRunContent key={runId} workflowId={workflowId} runId={runId} onRunCreated={(run) => setRunId(run.id)} /></div></>}
          </DialogContent>
        </Dialog>
      </div>
    </main>
  </div>;
}
