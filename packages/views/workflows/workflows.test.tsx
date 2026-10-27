import { render, screen, fireEvent, waitFor, cleanup, act, within } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { QueryClientProvider } from "@tanstack/react-query";
import { createQueryClient } from "@multica/core/query-client";
import { I18nProvider } from "@multica/core/i18n/react";
import { api } from "@multica/core/api";
import { workflowKeys } from "@multica/core/workflows";
import { NavigationProvider } from "../navigation";
import { WorkflowList, WorkflowRunPage, WorkflowDetailPage } from "./index";
import en from "../locales/en/autopilots.json";
import fr from "../locales/fr/autopilots.json";
import ja from "../locales/ja/autopilots.json";
import ko from "../locales/ko/autopilots.json";
import zh from "../locales/zh-Hans/autopilots.json";

vi.mock("@multica/core/hooks", () => ({ useWorkspaceId: () => "ws" }));
vi.mock("@multica/core/paths", () => ({ useWorkspaceSlug: () => "team" }));
vi.mock("@multica/core/api", () => ({ api: { listWorkflows: vi.fn(), getWorkflow: vi.fn(), getWorkflowRun: vi.fn(), getWorkflowRevision: vi.fn(), getWorkflowLogs: vi.fn(), workflowRunAction: vi.fn(), listWorkflowRevisions: vi.fn(), listWorkflowTriggers: vi.fn(), listWorkflowRuns: vi.fn(), updateWorkflow: vi.fn(), createWorkflowRun: vi.fn() } }));
const push = vi.fn();
const workflow = { id: "w", workspaceId: "ws", name: "A long workflow " + "x".repeat(180), slug: "flow", description: "Read only", status: "active", currentRevisionId: "new", definitionVersion: 2, updatedAt: "2026-10-26T00:00:00Z", lastRunAt: null, nextRunAt: null, triggerKinds: ["schedule"], canWrite: true, runtimeAvailability: { state: "ready" } };
const run = { id: "run", workflowId: "w", revisionId: "old", principalUserId: "human", source: "manual", status: "running", reasonCode: "workflow_final_logs_incomplete", triggeredAt: "2026-10-01T00:00:00Z", startedAt: null, finishedAt: null, cancelRequestedAt: null, input: { ticket: 42 }, steps: [{ id: "wait", status: "RUNNING", attempt: 2, startedAt: null, finishedAt: null }] };
const resources = { en: { autopilots: en }, fr: { autopilots: fr }, ja: { autopilots: ja }, ko: { autopilots: ko }, "zh-Hans": { autopilots: zh } };
function mount(ui: React.ReactNode, locale: "en" | "fr" | "ja" | "ko" | "zh-Hans" = "en") {
  const client = createQueryClient();
  render(<I18nProvider locale={locale} resources={resources}><QueryClientProvider client={client}><NavigationProvider value={{ push, replace: vi.fn(), back: vi.fn(), pathname: "/team/workflows/w", searchParams: new URLSearchParams(), hash: "", getShareableUrl: (p) => p }}>{ui}</NavigationProvider></QueryClientProvider></I18nProvider>);
  return client;
}
beforeEach(() => {
  vi.clearAllMocks();
  vi.mocked(api.listWorkflows).mockResolvedValue([]);
  vi.mocked(api.getWorkflow).mockResolvedValue(workflow);
  vi.mocked(api.listWorkflowRevisions).mockResolvedValue([]);
  vi.mocked(api.listWorkflowTriggers).mockResolvedValue([]);
  vi.mocked(api.listWorkflowRuns).mockResolvedValue([run]);
  vi.mocked(api.getWorkflowRun).mockResolvedValue(run);
  vi.mocked(api.getWorkflowRevision).mockResolvedValue({ id: "old", workflowId: "w", version: 1, definition: "export default {}", definitionHash: "frozen", graph: { steps: [{ id: "wait", parents: [] }] }, validationStatus: "valid", validationError: null, publishedAt: "2026-10-01T00:00:00Z" });
  vi.mocked(api.getWorkflowLogs).mockResolvedValue({ runId: "run", available: true, reasonCode: null, entries: [{ stepId: "wait", message: "long " + "log ".repeat(500), level: "info", createdAt: "2026-10-01T00:00:00Z" }] });
});
afterEach(() => { cleanup(); vi.useRealTimers(); });

function logPage(message: string, count = 1) {
  return { runId: "run", available: true, reasonCode: null, entries: Array.from({ length: count }, (_, i) => ({ stepId: "wait", message: `${message} ${i}`, level: "info", createdAt: "2026-10-01T00:00:00Z" })) };
}

async function advance(ms: number) {
  await act(async () => { await vi.advanceTimersByTimeAsync(ms); });
}
describe("workflow shared observability", () => {
  it("opens history in a run dialog and keeps the workflows tab in its breadcrumb", async () => {
    mount(<WorkflowDetailPage workflowId="w" />);
    expect(await screen.findByRole("link", { name: "Autopilot" })).toHaveAttribute("href", "/team/autopilots?tab=workflows");
    fireEvent.click(await screen.findByRole("button", { name: /run ·/ }));
    const dialog = await screen.findByRole("dialog", { name: "Run details" });
    expect(await within(dialog).findByText("frozen")).toBeInTheDocument();
    expect(push).not.toHaveBeenCalled();
    expect(within(dialog).getByRole("link", { name: "Open full page" })).toHaveAttribute("href", "/team/workflows/w/runs/run");
  });

  it("shows static dependencies without misleading live execution columns", async () => {
    mount(<WorkflowDetailPage workflowId="w" />);
    const table = await screen.findByRole("table");
    expect(within(table).getByRole("columnheader", { name: "Depends on" })).toBeInTheDocument();
    expect(within(table).queryByRole("columnheader", { name: "Attempt" })).not.toBeInTheDocument();
  });

  it("restores focus to the history opener when closing run inspection", async () => {
    mount(<WorkflowDetailPage workflowId="w" />);
    const opener = await screen.findByRole("button", { name: /run ·/ });
    opener.focus();
    fireEvent.click(opener);
    const dialog = await screen.findByRole("dialog", { name: "Run details" });
    fireEvent.click(within(dialog).getByRole("button", { name: "Close" }));
    await waitFor(() => expect(screen.queryByRole("dialog")).not.toBeInTheDocument());
    await waitFor(() => expect(opener).toHaveFocus());
  });

  it("keeps source read-only while editing properties with the observed CAS timestamp", async () => {
    vi.mocked(api.updateWorkflow).mockResolvedValue();
    mount(<WorkflowDetailPage workflowId="w" />);
    fireEvent.click(await screen.findByRole("button", { name: "Edit" }));
    const dialog = screen.getByRole("dialog", { name: "Edit workflow" });
    fireEvent.change(within(dialog).getByLabelText("Name"), { target: { value: "Renamed flow" } });
    expect(within(dialog).queryByLabelText("Source")).not.toBeInTheDocument();
    fireEvent.click(within(dialog).getByRole("button", { name: "Save" }));
    await waitFor(() => expect(api.updateWorkflow).toHaveBeenCalledWith("w", { name: "Renamed flow", description: "Read only", expectedUpdatedAt: workflow.updatedAt }));
    await waitFor(() => expect(screen.queryByRole("dialog")).not.toBeInTheDocument());
  });

  it("preserves edited input on a failed metadata update", async () => {
    vi.mocked(api.updateWorkflow).mockRejectedValue(new Error("workflow_version_conflict"));
    mount(<WorkflowDetailPage workflowId="w" />);
    fireEvent.click(await screen.findByRole("button", { name: "Edit" }));
    fireEvent.change(screen.getByLabelText("Name"), { target: { value: "My draft" } });
    fireEvent.click(screen.getByRole("button", { name: "Save" }));
    expect(await screen.findByRole("alert")).toBeInTheDocument();
    expect(screen.getByLabelText("Name")).toHaveValue("My draft");
  });

  it("validates manual input and opens the admitted run after success", async () => {
    vi.mocked(api.createWorkflowRun).mockResolvedValue({ ...run, id: "admitted" });
    mount(<WorkflowDetailPage workflowId="w" />);
    fireEvent.click(await screen.findByRole("button", { name: "Run now" }));
    fireEvent.change(screen.getByLabelText("Input (JSON)"), { target: { value: "broken" } });
    fireEvent.click(screen.getByRole("button", { name: "Start run" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("Enter valid JSON.");
    expect(api.createWorkflowRun).not.toHaveBeenCalled();
    fireEvent.change(screen.getByLabelText("Input (JSON)"), { target: { value: '{"ticket":42}' } });
    fireEvent.click(screen.getByRole("button", { name: "Start run" }));
    await waitFor(() => expect(api.createWorkflowRun).toHaveBeenCalledWith("w", { ticket: 42 }));
    expect(await screen.findByRole("dialog", { name: "Run details" })).toBeInTheDocument();
    expect(api.getWorkflowRun).toHaveBeenCalledWith("w", "admitted");
  });

  it("pauses admission without sending a cancel action", async () => {
    vi.mocked(api.updateWorkflow).mockResolvedValue();
    mount(<WorkflowDetailPage workflowId="w" />);
    fireEvent.click(await screen.findByRole("switch", { name: "Pause workflow" }));
    await waitFor(() => expect(api.updateWorkflow).toHaveBeenCalledWith("w", { status: "paused", expectedUpdatedAt: workflow.updatedAt }));
    expect(api.workflowRunAction).not.toHaveBeenCalled();
  });

  it("does not expose edit or run controls to readers", async () => {
    vi.mocked(api.getWorkflow).mockResolvedValue({ ...workflow, canWrite: false });
    mount(<WorkflowDetailPage workflowId="w" />);
    await screen.findByText(workflow.name);
    expect(screen.queryByRole("button", { name: "Edit" })).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Run now" })).not.toBeInTheDocument();
    expect(screen.getByRole("switch")).toHaveAttribute("aria-disabled", "true");
    fireEvent.click(screen.getByRole("switch"));
    expect(api.updateWorkflow).not.toHaveBeenCalled();
  });

  it("switches the run dialog to the replay result without navigating", async () => {
    vi.mocked(api.workflowRunAction).mockResolvedValue({ ...run, id: "replayed" });
    mount(<WorkflowDetailPage workflowId="w" />);
    fireEvent.click(await screen.findByRole("button", { name: /run ·/ }));
    const dialog = await screen.findByRole("dialog", { name: "Run details" });
    fireEvent.click(await within(dialog).findByRole("button", { name: "Replay" }));
    await waitFor(() => expect(api.getWorkflowRun).toHaveBeenCalledWith("w", "replayed"));
    expect(within(dialog).getByRole("link", { name: "Open full page" })).toHaveAttribute("href", "/team/workflows/w/runs/replayed");
    expect(push).not.toHaveBeenCalled();
  });

  it.each([{ status: "paused", state: "ready" }, { status: "active", state: "unavailable" }])("does not admit manual runs for $status/$state", async ({ status, state }) => {
    vi.mocked(api.getWorkflow).mockResolvedValue({ ...workflow, status, runtimeAvailability: { state } });
    mount(<WorkflowDetailPage workflowId="w" />);
    expect(await screen.findByRole("button", { name: "Run now" })).toBeDisabled();
    fireEvent.click(screen.getByRole("button", { name: "Run now" }));
    expect(api.createWorkflowRun).not.toHaveBeenCalled();
  });

  it("refreshes a conflicted version without replacing the draft", async () => {
    vi.mocked(api.updateWorkflow).mockRejectedValueOnce(new Error("workflow_version_conflict")).mockResolvedValueOnce();
    mount(<WorkflowDetailPage workflowId="w" />);
    fireEvent.click(await screen.findByRole("button", { name: "Edit" }));
    fireEvent.change(screen.getByLabelText("Name"), { target: { value: "My draft" } });
    fireEvent.click(screen.getByRole("button", { name: "Save" }));
    await screen.findByRole("alert");
    vi.mocked(api.getWorkflow).mockResolvedValue({ ...workflow, name: "Other edit", updatedAt: "2026-10-26T01:00:00Z" });
    fireEvent.click(screen.getByRole("button", { name: "Refresh version" }));
    await waitFor(() => expect(screen.queryByRole("alert")).not.toBeInTheDocument());
    expect(screen.getByLabelText("Name")).toHaveValue("My draft");
    fireEvent.click(screen.getByRole("button", { name: "Save" }));
    await waitFor(() => expect(api.updateWorkflow).toHaveBeenLastCalledWith("w", { name: "My draft", description: "Read only", expectedUpdatedAt: "2026-10-26T01:00:00Z" }));
  });
  it.each(["en", "fr", "ja", "ko", "zh-Hans"] as const)("renders translated empty state in %s", async (locale) => {
    mount(<WorkflowList />, locale);
    expect(await screen.findByText(resources[locale].autopilots.workflows.empty)).toBeInTheDocument();
    expect(screen.queryByText(/workflows\.empty/)).not.toBeInTheDocument();
  });
  it.each(["disabled", "unrunnable"])("shows %s runtime and a long name without dropping its link", async (state) => {
    vi.mocked(api.listWorkflows).mockResolvedValue([{ ...workflow, runtimeAvailability: { state } }]);
    mount(<WorkflowList />);
    expect(await screen.findByRole("link", { name: workflow.name })).toHaveAttribute("href", "/team/workflows/w");
    expect(screen.getByText(state === "disabled" ? "Workflows are disabled on this server." : "No eligible runtime has registered this revision.")).toBeInTheDocument();
  });
  it("requests the next bounded workflow page", async () => {
    vi.mocked(api.listWorkflows).mockResolvedValue(Array.from({ length: 25 }, (_, i) => ({ ...workflow, id: `w${i}` })));
    mount(<WorkflowList />);
    await screen.findAllByRole("link", { name: workflow.name });
    fireEvent.click(screen.getByRole("button", { name: "Next" }));
    await waitFor(() => expect(api.listWorkflows).toHaveBeenLastCalledWith(25));
  });
  it("shows frozen revision, live DAG attempts and incomplete bounded logs", async () => {
    mount(<WorkflowRunPage workflowId="w" runId="run" />);
    expect(await screen.findByText("This run uses an older revision.")).toBeInTheDocument();
    expect(await screen.findByText("frozen")).toBeInTheDocument();
    expect(screen.getByText("RUNNING")).toBeInTheDocument();
    expect(screen.getByText("2")).toBeInTheDocument();
    expect(screen.getByText("Final logs are incomplete. Previously recovered entries are shown.")).toBeInTheDocument();
    expect(await screen.findByText(/^long log/)).toHaveClass("whitespace-pre-wrap", "break-all");
  });
  it("denies cancel and replay for readers", async () => {
    vi.mocked(api.getWorkflow).mockResolvedValue({ ...workflow, canWrite: false });
    mount(<WorkflowRunPage workflowId="w" runId="run" />);
    expect(await screen.findByRole("button", { name: "Cancel run" })).toBeDisabled();
    expect(screen.getByRole("button", { name: "Replay" })).toBeDisabled();
  });
  it("awaits replay and navigates to the new run", async () => {
    vi.mocked(api.workflowRunAction).mockResolvedValue({ ...run, id: "new-run" });
    mount(<WorkflowRunPage workflowId="w" runId="run" />);
    fireEvent.click(await screen.findByRole("button", { name: "Replay" }));
    await waitFor(() => expect(push).toHaveBeenCalledWith("/team/workflows/w/runs/new-run"));
    expect(api.workflowRunAction).toHaveBeenCalledWith("w", "run", "replay");
  });

  it.each(["completed", "failed", "cancelled", "skipped"])("reads final logs once when a running run becomes %s, then stops polling", async (status) => {
    vi.useFakeTimers({ toFake: ["setInterval", "clearInterval"] });
    vi.mocked(api.getWorkflowLogs).mockResolvedValue(logPage("Earlier entry"));
    const client = mount(<WorkflowRunPage workflowId="w" runId="run" />);
    try {
      expect(await screen.findByText("Earlier entry 0")).toBeInTheDocument();
      vi.mocked(api.getWorkflowLogs).mockResolvedValue(logPage("Final entry"));
      await act(async () => {
        client.setQueryData([...workflowKeys.detail("ws", "w"), "run", "run"], { ...run, status });
      });
      expect(await screen.findByText("Final entry 0")).toBeInTheDocument();
      expect(screen.queryByText("Earlier entry 0")).not.toBeInTheDocument();
      await advance(20_000);
      expect(api.getWorkflowLogs).toHaveBeenCalledTimes(2);
      expect(api.getWorkflowRun).toHaveBeenCalledTimes(1);
    } finally { client.clear(); }
  });

  it("does not let an in-flight active log page overwrite terminal logs", async () => {
    vi.useFakeTimers({ toFake: ["setInterval", "clearInterval"] });
    let resolveActive!: (value: Awaited<ReturnType<typeof api.getWorkflowLogs>>) => void;
    vi.mocked(api.getWorkflowLogs)
      .mockImplementationOnce(() => new Promise((resolve) => { resolveActive = resolve; }))
      .mockResolvedValue(logPage("Final entry"));
    const client = mount(<WorkflowRunPage workflowId="w" runId="run" />);
    try {
      await waitFor(() => expect(api.getWorkflowLogs).toHaveBeenCalledTimes(1));
      await act(async () => {
        client.setQueryData([...workflowKeys.detail("ws", "w"), "run", "run"], { ...run, status: "completed" });
      });
      expect(await screen.findByText("Final entry 0")).toBeInTheDocument();
      await act(async () => { resolveActive(logPage("Stale active entry")); });
      await waitFor(() => expect(client.isFetching()).toBe(0));
      expect(screen.queryByText("Stale active entry 0")).not.toBeInTheDocument();
      expect(screen.getByText("Final entry 0")).toBeInTheDocument();
      await advance(20_000);
      expect(api.getWorkflowLogs).toHaveBeenCalledTimes(2);
    } finally { client.clear(); }
  });

  it("reads only requested terminal pages even when their active pages were cached", async () => {
    vi.useFakeTimers({ toFake: ["setInterval", "clearInterval"] });
    vi.mocked(api.getWorkflowLogs).mockImplementation(async (_workflow, _run, offset) => logPage(`Earlier page ${offset}`, 25));
    const client = mount(<WorkflowRunPage workflowId="w" runId="run" />);
    try {
      await screen.findByText("Earlier page 0 0");
      fireEvent.click(screen.getByRole("button", { name: "Next" }));
      expect(await screen.findByText("Earlier page 25 0")).toBeInTheDocument();
      vi.mocked(api.getWorkflowLogs).mockImplementation(async (_workflow, _run, offset) => logPage(`Final page ${offset}`, 25));
      await act(async () => {
        client.setQueryData([...workflowKeys.detail("ws", "w"), "run", "run"], { ...run, status: "completed" });
      });
      expect(await screen.findByText("Final page 25 0")).toBeInTheDocument();
      expect(api.getWorkflowLogs).toHaveBeenCalledTimes(3);
      fireEvent.click(screen.getByRole("button", { name: "Previous" }));
      expect(await screen.findByText("Final page 0 0")).toBeInTheDocument();
      expect(vi.mocked(api.getWorkflowLogs).mock.calls.map((args) => args[2])).toEqual([0, 25, 25, 0]);
      await advance(20_000);
      expect(api.getWorkflowLogs).toHaveBeenCalledTimes(4);
    } finally { client.clear(); }
  });

  it("loads an already-terminal run once without starting periodic log requests", async () => {
    vi.useFakeTimers({ toFake: ["setInterval", "clearInterval"] });
    let resolveRun!: (value: Awaited<ReturnType<typeof api.getWorkflowRun>>) => void;
    vi.mocked(api.getWorkflowRun).mockImplementationOnce(() => new Promise((resolve) => { resolveRun = resolve; }));
    vi.mocked(api.getWorkflowLogs).mockResolvedValue(logPage("Final entry"));
    const client = mount(<WorkflowRunPage workflowId="w" runId="run" />);
    try {
      await waitFor(() => expect(api.getWorkflowRun).toHaveBeenCalledTimes(1));
      expect(api.getWorkflowLogs).not.toHaveBeenCalled();
      await act(async () => { resolveRun({ ...run, status: "completed" }); });
      expect(await screen.findByText("Final entry 0")).toBeInTheDocument();
      await advance(20_000);
      expect(api.getWorkflowLogs).toHaveBeenCalledTimes(1);
      expect(api.getWorkflowRun).toHaveBeenCalledTimes(1);
    } finally { client.clear(); }
  });
});
