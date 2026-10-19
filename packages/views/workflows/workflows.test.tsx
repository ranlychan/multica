import { render, screen, fireEvent, waitFor, cleanup } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { I18nProvider } from "@multica/core/i18n/react";
import { api } from "@multica/core/api";
import { NavigationProvider } from "../navigation";
import { WorkflowList, WorkflowRunPage } from "./index";
import en from "../locales/en/autopilots.json";
import fr from "../locales/fr/autopilots.json";
import ja from "../locales/ja/autopilots.json";
import ko from "../locales/ko/autopilots.json";
import zh from "../locales/zh-Hans/autopilots.json";

vi.mock("@multica/core/hooks", () => ({ useWorkspaceId: () => "ws" }));
vi.mock("@multica/core/paths", () => ({ useWorkspaceSlug: () => "team" }));
vi.mock("@multica/core/api", () => ({ api: { listWorkflows: vi.fn(), getWorkflow: vi.fn(), getWorkflowRun: vi.fn(), getWorkflowRevision: vi.fn(), getWorkflowLogs: vi.fn(), workflowRunAction: vi.fn() } }));
const push = vi.fn();
const workflow = { id: "w", workspaceId: "ws", name: "A long workflow " + "x".repeat(180), slug: "flow", description: "Read only", status: "active", currentRevisionId: "new", definitionVersion: 2, lastRunAt: null, nextRunAt: null, triggerKinds: ["schedule"], canWrite: true, runtimeAvailability: { state: "ready" } };
const run = { id: "run", workflowId: "w", revisionId: "old", principalUserId: "human", source: "manual", status: "running", reasonCode: "workflow_final_logs_incomplete", triggeredAt: "2026-10-01T00:00:00Z", startedAt: null, finishedAt: null, cancelRequestedAt: null, input: { ticket: 42 }, steps: [{ id: "wait", status: "RUNNING", attempt: 2, startedAt: null, finishedAt: null }] };
const resources = { en: { autopilots: en }, fr: { autopilots: fr }, ja: { autopilots: ja }, ko: { autopilots: ko }, "zh-Hans": { autopilots: zh } };
function mount(ui: React.ReactNode, locale: "en" | "fr" | "ja" | "ko" | "zh-Hans" = "en") {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
  render(<I18nProvider locale={locale} resources={resources}><QueryClientProvider client={client}><NavigationProvider value={{ push, replace: vi.fn(), back: vi.fn(), pathname: "/team/workflows/w", searchParams: new URLSearchParams(), hash: "", getShareableUrl: (p) => p }}>{ui}</NavigationProvider></QueryClientProvider></I18nProvider>);
}
beforeEach(() => {
  vi.clearAllMocks();
  vi.mocked(api.listWorkflows).mockResolvedValue([]);
  vi.mocked(api.getWorkflow).mockResolvedValue(workflow);
  vi.mocked(api.getWorkflowRun).mockResolvedValue(run);
  vi.mocked(api.getWorkflowRevision).mockResolvedValue({ id: "old", workflowId: "w", version: 1, definition: "export default {}", definitionHash: "frozen", graph: { steps: [{ id: "wait", parents: [] }] }, validationStatus: "valid", validationError: null, publishedAt: "2026-10-01T00:00:00Z" });
  vi.mocked(api.getWorkflowLogs).mockResolvedValue({ runId: "run", available: true, reasonCode: null, entries: [{ stepId: "wait", message: "long " + "log ".repeat(500), level: "info", createdAt: "2026-10-01T00:00:00Z" }] });
});
afterEach(cleanup);
describe("workflow shared observability", () => {
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
});
