// @vitest-environment node
import { afterEach, expect, it, vi } from "vitest";
import { ApiClient } from "./client";

const client = new ApiClient("https://api.example.test");
const workflow = { id: "w", workspace_id: "ws", name: "Report", slug: "report", status: "active", definition_version: 1, updated_at: "2026-10-26T00:00:00.123456Z" };
const run = { id: "r", workflow_id: "w", revision_id: "v1", principal_user_id: "u", source: "manual", status: "queued" };
afterEach(() => vi.unstubAllGlobals());

it("retains exact observed timestamp and maps a metadata CAS patch", async () => {
  const fetcher = vi.fn().mockImplementation(async () => new Response(JSON.stringify(workflow)));
  vi.stubGlobal("fetch", fetcher);
  expect((await client.getWorkflow("w"))?.updatedAt).toBe("2026-10-26T00:00:00.123456Z");
  await client.updateWorkflow("w", { name: "Renamed", expectedUpdatedAt: "2026-10-26T00:00:00.123456Z" });
  expect(fetcher.mock.calls[1]?.[1]).toMatchObject({ method: "PATCH", body: JSON.stringify({ name: "Renamed", expected_updated_at: "2026-10-26T00:00:00.123456Z" }) });
});

it("submits manual JSON input and parses the admitted run", async () => {
  const fetcher = vi.fn().mockResolvedValue(new Response(JSON.stringify(run), { status: 202 }));
  vi.stubGlobal("fetch", fetcher);
  await expect(client.createWorkflowRun("w", { ticket: 42 })).resolves.toMatchObject({ id: "r", revisionId: "v1", status: "queued" });
  expect(fetcher.mock.calls[0]?.[0]).toBe("https://api.example.test/api/workflows/w/run");
  expect(fetcher.mock.calls[0]?.[1]).toMatchObject({ method: "POST", body: '{"input":{"ticket":42}}' });
});

it("rejects malformed admission rather than reporting a run was created", async () => {
  vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response('{"id":"r"}')));
  await expect(client.createWorkflowRun("w", {})).rejects.toThrow("workflow_invalid_response");
});

it("does not swallow metadata conflicts", async () => {
  vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response('{"error":"workflow_version_conflict"}', { status: 409 })));
  await expect(client.updateWorkflow("w", { status: "paused", expectedUpdatedAt: workflow.updated_at })).rejects.toThrow();
});
