"use client";
import { Button } from "@multica/ui/components/ui/button";
import type { WorkflowRevision, WorkflowRun } from "@multica/core/types/workflow";
import { WORKFLOW_PAGE_SIZE } from "@multica/core/workflows";
import { useT } from "../i18n";

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
  return <section className="min-w-0"><h2 className="font-medium">{t(($) => $.workflows.graph)}</h2><div className="overflow-x-auto"><table className="w-full text-left text-body"><thead><tr>{[t(($) => $.workflows.name), t(($) => $.workflows.parents), t(($) => $.workflows.status), t(($) => $.workflows.attempt), t(($) => $.workflows.started), t(($) => $.workflows.finished)].map((label) => <th className="p-2" key={label}>{label}</th>)}</tr></thead><tbody>{revision.graph.steps.map((step) => { const live = run?.steps.find((s) => s.id === step.id); return <tr key={step.id}><th scope="row" className="max-w-48 break-all p-2 font-normal">{step.id}</th><td className="max-w-48 break-all p-2">{step.parents.join(", ") || "—"}</td><td className="p-2">{live?.status ?? t(($) => $.workflows.unobserved)}</td><td className="p-2">{live?.attempt ?? "—"}</td><td className="p-2"><WorkflowTime value={live?.startedAt} /></td><td className="p-2"><WorkflowTime value={live?.finishedAt} /></td></tr>; })}</tbody></table></div></section>;
}
export function WorkflowTime({ value }: { value?: string | null }) {
  return value ? <time dateTime={value}>{value}</time> : <span>—</span>;
}
export function WorkflowLoadState({ loading }: { loading: boolean }) {
  const { t } = useT("autopilots");
  return <p role={loading ? "status" : "alert"} className="p-4">{loading ? t(($) => $.workflows.loading) : t(($) => $.workflows.error)}</p>;
}
