"use client";
import { useState } from "react";
import { useCreateWorkflowRun, useUpdateWorkflow } from "@multica/core/workflows";
import type { Workflow, WorkflowRun } from "@multica/core/types/workflow";
import { Button } from "@multica/ui/components/ui/button";
import { Input } from "@multica/ui/components/ui/input";
import { Textarea } from "@multica/ui/components/ui/textarea";
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle, DialogFooter, DialogClose, DialogTrigger } from "@multica/ui/components/ui/dialog";
import { Pencil, Play, Loader2 } from "lucide-react";
import { useT } from "../i18n";

export function WorkflowEditDialog({ workflow, wsId, refresh }: { workflow: Workflow; wsId: string; refresh: () => Promise<Workflow | null> }) {
  const { t } = useT("autopilots");
  const [open, setOpen] = useState(false);
  const [name, setName] = useState("");
  const [description, setDescription] = useState("");
  const [version, setVersion] = useState<string | null>(null);
  const [refreshing, setRefreshing] = useState(false);
  const update = useUpdateWorkflow(wsId, workflow.id);
  const conflict = update.error instanceof Error && (("status" in update.error && update.error.status === 409) || update.error.message === "workflow_version_conflict");
  return <Dialog open={open} onOpenChange={(value) => {
    if (update.isPending || refreshing) return;
    if (value) { setName(workflow.name); setDescription(workflow.description); setVersion(workflow.updatedAt); update.reset(); }
    setOpen(value);
  }}>
    <DialogTrigger render={<Button size="sm" variant="outline" disabled={!workflow.updatedAt} aria-label={t(($) => $.workflows.detailUi.edit)} />}><Pencil className="size-3.5" /><span className="hidden sm:inline">{t(($) => $.workflows.detailUi.edit)}</span></DialogTrigger>
    <DialogContent>
      <form className="space-y-4" onSubmit={(event) => {
        event.preventDefault();
        if (!version || update.isPending || refreshing || !name.trim()) return;
        update.mutate({ name: name.trim(), description, expectedUpdatedAt: version }, { onSuccess: () => setOpen(false) });
      }}>
        <DialogHeader><DialogTitle>{t(($) => $.workflows.detailUi.editTitle)}</DialogTitle><DialogDescription>{t(($) => $.workflows.detailUi.editHelp)}</DialogDescription></DialogHeader>
        <label className="block space-y-2"><span>{t(($) => $.workflows.name)}</span><Input value={name} onChange={(e) => setName(e.target.value)} required maxLength={200} disabled={update.isPending} /></label>
        <label className="block space-y-2"><span>{t(($) => $.workflows.detailUi.description)}</span><Textarea value={description} onChange={(e) => setDescription(e.target.value)} maxLength={4096} disabled={update.isPending} /></label>
        {update.isError && <div className="space-y-2"><p role="alert" className="text-destructive">{conflict ? t(($) => $.workflows.detailUi.conflict) : t(($) => $.workflows.actionError)}</p><Button type="button" variant="outline" size="sm" disabled={refreshing} onClick={async () => {
          setRefreshing(true);
          try { const next = await refresh(); if (next?.updatedAt) { setVersion(next.updatedAt); update.reset(); } } finally { setRefreshing(false); }
        }}>{t(($) => $.workflows.detailUi.refresh)}</Button></div>}
        <DialogFooter><DialogClose render={<Button type="button" variant="outline" disabled={update.isPending || refreshing} />}>{t(($) => $.workflows.detailUi.close)}</DialogClose><Button type="submit" disabled={update.isPending || refreshing || !version || !name.trim()} aria-busy={update.isPending}>{update.isPending && <Loader2 className="size-4 animate-spin" />}{t(($) => $.workflows.detailUi.save)}</Button></DialogFooter>
      </form>
    </DialogContent>
  </Dialog>;
}

export function WorkflowManualRunDialog({ workflow, wsId, onCreated }: { workflow: Workflow; wsId: string; onCreated: (run: WorkflowRun) => void }) {
  const { t } = useT("autopilots");
  const [open, setOpen] = useState(false);
  const [input, setInput] = useState("{}");
  const [invalid, setInvalid] = useState(false);
  const create = useCreateWorkflowRun(wsId, workflow.id);
  const allowed = workflow.canWrite && workflow.status === "active" && workflow.runtimeAvailability.state === "ready";
  return <Dialog open={open} onOpenChange={(value) => { if (!create.isPending) { setOpen(value); setInvalid(false); create.reset(); } }}>
    <DialogTrigger render={<Button size="sm" disabled={!allowed} aria-label={t(($) => $.workflows.detailUi.run)} />}><Play className="size-3.5" /><span className="hidden sm:inline">{t(($) => $.workflows.detailUi.run)}</span></DialogTrigger>
    <DialogContent>
      <form className="space-y-4" onSubmit={(event) => {
        event.preventDefault();
        if (!allowed || create.isPending) return;
        let parsed: WorkflowRun["input"];
        try { parsed = JSON.parse(input); } catch { setInvalid(true); return; }
        setInvalid(false);
        create.mutate(parsed, { onSuccess: (run) => { setOpen(false); onCreated(run); } });
      }}>
        <DialogHeader><DialogTitle>{t(($) => $.workflows.detailUi.runTitle)}</DialogTitle><DialogDescription>{t(($) => $.workflows.detailUi.runHelp)}</DialogDescription></DialogHeader>
        <label className="block space-y-2"><span>{t(($) => $.workflows.detailUi.json)}</span><Textarea className="min-h-40 font-mono" value={input} onChange={(e) => setInput(e.target.value)} disabled={create.isPending} aria-invalid={invalid} /></label>
        {(invalid || create.isError) && <p role="alert" className="text-destructive">{invalid ? t(($) => $.workflows.detailUi.invalidJson) : t(($) => $.workflows.actionError)}</p>}
        <DialogFooter><DialogClose render={<Button type="button" variant="outline" disabled={create.isPending} />}>{t(($) => $.workflows.detailUi.close)}</DialogClose><Button type="submit" disabled={!allowed || create.isPending} aria-busy={create.isPending}>{create.isPending && <Loader2 className="size-4 animate-spin" />}{t(($) => $.workflows.detailUi.start)}</Button></DialogFooter>
      </form>
    </DialogContent>
  </Dialog>;
}
