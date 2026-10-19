// @vitest-environment node
import { describe, expect, it } from "vitest";
import { parseWithFallback } from "../api/schema";
import * as schemas from "../api/schema";

const revision = { id: "r", workflow_id: "w", version: 1, definition: "export default {}", definition_hash: "hash", definition_graph: { steps: [{ id: "a", parents: [] }] }, validation_status: "valid" };
const run = { id: "r", workflow_id: "w", revision_id: "v", principal_user_id: "human", source: "manual", status: "running" };

describe("workflow API projections", () => {
  it.each([
    ["WorkflowRevisionSchema", { ...revision, version: -1 }],
    ["WorkflowGraphSchema", { steps: [{ id: "a", parents: 7 }] }],
    ["WorkflowStepSchema", { id: "a", status: "RUNNING", attempt: "two" }],
    ["WorkflowLogsSchema", { run_id: "r", available: true, entries: [{ step_id: "a", level: "info", message: {}, created_at: "2026-10-18T00:00:00Z" }] }],
    ["WorkflowRunSchema", { ...run, principal_user_id: { token: "never-display" } }],
    ["WorkflowAvailabilitySchema", { state: true }],
  ])("rejects malformed %s without returning unvalidated payload", (name, payload) => {
    const schema = Reflect.get(schemas, name);
    expect(schema, "public workflow schema must exist").toBeDefined();
    expect(parseWithFallback(payload, schema, null, { endpoint: name })).toBeNull();
  });

  it("accepts older projections while keeping unknown runtime authorization closed", () => {
    expect(parseWithFallback({ id: "w", workspace_id: "ws", name: "Workflow", slug: "workflow", status: "active", definition_version: 1 }, schemas.WorkflowSchema, null, { endpoint: "workflow" })).toMatchObject({ canWrite: false, runtimeAvailability: { state: "unknown" } });
    expect(parseWithFallback(revision, schemas.WorkflowRevisionSchema, null, { endpoint: "revision" })).toMatchObject({ version: 1, graph: { steps: [{ id: "a", parents: [] }] } });
    expect(parseWithFallback(run, schemas.WorkflowRunSchema, null, { endpoint: "run" })).toMatchObject({ principalUserId: "human", input: null, steps: [] });
  });
});
