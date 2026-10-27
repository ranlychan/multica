"use client";
import { Button } from "@multica/ui/components/ui/button";
import type { WorkflowRevision, WorkflowRun } from "@multica/core/types/workflow";
import { WORKFLOW_PAGE_SIZE } from "@multica/core/workflows";
import { useT } from "../i18n";
import { CheckCircle2, XCircle, Loader2, Clock, Ban } from "lucide-react";
import { formatInTimeZone } from "../common/format-in-time-zone";

export function WorkflowRunStatus({ status }: { status: string }) {
  const { t } = useT("autopilots");
  const Icon = status === "completed" ? CheckCircle2 : status === "failed" ? XCircle : status === "running" ? Loader2 : status === "cancelled" || status === "skipped" ? Ban : Clock;
  const color = status === "completed" ? "text-emerald-500" : status === "failed" ? "text-destructive" : status === "running" ? "text-blue-500" : "text-muted-foreground";
  const label = status === "completed" || status === "failed" || status === "running" || status === "skipped" ? t(($) => $.run_status[status]) : status === "queued" || status === "submitting" || status === "cancelled" ? t(($) => $.workflows.detailUi[status]) : status;
  return <span className={`inline-flex shrink-0 items-center gap-2 text-caption font-medium ${color}`}><Icon className={`size-4 ${status === "running" ? "animate-spin" : ""}`} />{label}</span>;
}

export function WorkflowRunSource({ source }: { source: string }) {
  const { t } = useT("autopilots");
  return <>{source === "manual" || source === "schedule" || source === "api" || source === "webhook" ? t(($) => $.run_source[source]) : source}</>;
}

export function WorkflowDuration({ start, end }: { start?: string | null; end?: string | null }) {
  const duration = start && end ? Date.parse(end) - Date.parse(start) : NaN;
  return <span className="tabular-nums">{Number.isFinite(duration) && duration >= 0 ? `${Math.round(duration / 1000)}s` : "—"}</span>;
}

export function WorkflowAvailability({ state }: { state: string }) {
  const { t } = useT("autopilots");
  const key = state === "ready" || state === "disabled" || state === "unavailable" || state === "unrunnable" ? state : "unknown";
  return <span className="text-muted-foreground">{t(($) => $.workflows.availability[key])}</span>;
}
export function WorkflowPagination({ offset, count, pending, onChange }: { offset: number; count: number; pending: boolean; onChange: (offset: number) => void }) {
  const { t } = useT("autopilots");
  return <div className="flex gap-2 py-3"><Button variant="outline" size="sm" disabled={pending || offset === 0} onClick={() => onChange(offset - WORKFLOW_PAGE_SIZE)}>{t(($) => $.workflows.previous)}</Button><Button variant="outline" size="sm" disabled={pending || count < WORKFLOW_PAGE_SIZE || offset >= 100000 - WORKFLOW_PAGE_SIZE} onClick={() => onChange(offset + WORKFLOW_PAGE_SIZE)}>{t(($) => $.workflows.next)}</Button></div>;
}
export function WorkflowGraph({ revision, run }: { revision: WorkflowRevision; run?: WorkflowRun }) {
  const { t } = useT("autopilots");
  const headings = [t(($) => $.workflows.name), t(($) => $.workflows.parents)];
  if (run) headings.push(t(($) => $.workflows.status), t(($) => $.workflows.attempt), t(($) => $.workflows.started), t(($) => $.workflows.finished));
  return <section className="min-w-0 space-y-3">
    <h2 className="font-medium">{t(($) => $.workflows.graph)}</h2>
    <div className="overflow-x-auto rounded-md border"><table className="w-full text-left text-caption">
      <thead className="bg-muted/30 text-muted-foreground"><tr>{headings.map((label) => <th className="whitespace-nowrap p-3 font-medium" key={label}>{label}</th>)}</tr></thead>
      <tbody className="divide-y">{revision.graph.steps.map((step) => {
        const live = run?.steps.find((s) => s.id === step.id);
        return <tr key={step.id}>
          <th scope="row" className="max-w-48 break-all p-3 font-mono font-normal">{step.id}</th>
          <td className="max-w-48 break-all p-3 font-mono text-muted-foreground">{step.parents.join(", ") || "—"}</td>
          {run && <><td className="p-3">{live?.status ?? t(($) => $.workflows.unobserved)}</td><td className="p-3 tabular-nums">{live?.attempt ?? "—"}</td><td className="whitespace-nowrap p-3"><WorkflowTime value={live?.startedAt} /></td><td className="whitespace-nowrap p-3"><WorkflowTime value={live?.finishedAt} /></td></>}
        </tr>;
      })}</tbody>
    </table></div>
  </section>;
}
export function WorkflowTime({ value }: { value?: string | null }) {
  const { i18n } = useT("autopilots");
  return value && Number.isFinite(Date.parse(value)) ? <time dateTime={value} title={value}>{formatInTimeZone(value, undefined, i18n.language)}</time> : <span>—</span>;
}
export function WorkflowLoadState({ loading }: { loading: boolean }) {
  const { t } = useT("autopilots");
  return <p role={loading ? "status" : "alert"} className="p-4">{loading ? t(($) => $.workflows.loading) : t(($) => $.workflows.error)}</p>;
}
