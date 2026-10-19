"use client";
import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { useWorkspaceId } from "@multica/core/hooks";
import { useWorkspaceSlug } from "@multica/core/paths";
import { workflowListOptions } from "@multica/core/workflows";
import { AppLink } from "../navigation";
import { useT } from "../i18n";
import { WorkflowAvailability, WorkflowLoadState, WorkflowPagination, WorkflowTime } from "./common";

export function WorkflowList() {
  const wsId = useWorkspaceId();
  const slug = useWorkspaceSlug();
  const { t } = useT("autopilots");
  const [offset, setOffset] = useState(0);
  const query = useQuery(workflowListOptions(wsId, offset));
  if (query.isPending || query.isError) return <WorkflowLoadState loading={query.isPending} />;
  return <div className="min-h-0 overflow-auto p-4 text-body">
    <p className="mb-4 text-muted-foreground">{t(($) => $.workflows.authoring)}</p>
    {!query.data.length ? <p>{t(($) => $.workflows.empty)}</p> : <div className="overflow-x-auto"><table className="w-full text-left"><thead><tr>{[t(($) => $.workflows.name), t(($) => $.workflows.revision), t(($) => $.workflows.triggers), t(($) => $.workflows.status), t(($) => $.workflows.lastRun), t(($) => $.workflows.nextRun), t(($) => $.workflows.runtime)].map((label) => <th className="p-2" key={label}>{label}</th>)}</tr></thead><tbody>{query.data.map((workflow) => <tr className="border-b" key={workflow.id}><td className="min-w-48 max-w-80 p-2"><AppLink className="break-all font-medium hover:underline" href={`/${slug}/workflows/${workflow.id}`}>{workflow.name}</AppLink>{workflow.description && <p className="break-words text-muted-foreground">{workflow.description}</p>}</td><td className="p-2">{workflow.definitionVersion || "—"}</td><td className="p-2">{workflow.triggerKinds.join(", ") || "—"}</td><td className="p-2">{workflow.status}</td><td className="p-2"><WorkflowTime value={workflow.lastRunAt} /></td><td className="p-2"><WorkflowTime value={workflow.nextRunAt} /></td><td className="min-w-40 p-2"><WorkflowAvailability state={workflow.runtimeAvailability.state} /></td></tr>)}</tbody></table></div>}
    <WorkflowPagination offset={offset} count={query.data.length} pending={query.isFetching} onChange={setOffset} />
  </div>;
}
