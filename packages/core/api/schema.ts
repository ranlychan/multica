import { z, type ZodType } from "zod";
import { type Logger, noopLogger } from "../logger";

// Module-level logger for schema warnings. Defaults to no-op so test
// runs don't spam stderr; the platform layer wires a real logger via
// `setSchemaLogger` at app boot.
let schemaLogger: Logger = noopLogger;

export function setSchemaLogger(logger: Logger): void {
  schemaLogger = logger;
}

export interface ParseOptions {
  /** Endpoint identifier used in the warning log so we can grep for which
   *  contract drifted in production telemetry. */
  endpoint: string;
}

/**
 * Validate a JSON value parsed from an API response against a zod schema,
 * returning the parsed value on success or `fallback` on failure.
 *
 * On failure we log a warning with the endpoint and zod's structured error,
 * but never throw — the UI layer must keep rendering. This is the boundary
 * defense that turns "API contract drifted" from a white-screen incident
 * into a degraded-but-rendering page.
 *
 * The return type is anchored to `T` (inferred from `fallback`), not to the
 * schema's `z.infer` type. Schemas are intentionally **lenient** — string
 * enums kept as `z.string()` so an unknown enum value still parses, etc. —
 * so the parsed runtime value can be wider than the strict TS type at the
 * call site. The caller asserts compatibility by typing the fallback to the
 * expected `T`; downstream code is already responsible for handling unknown
 * enum values via `default`-bearing switches and optional chaining.
 *
 * See CLAUDE.md "API Response Compatibility" for when to reach for this.
 */
export function parseWithFallback<T>(
  data: unknown,
  schema: ZodType,
  fallback: T,
  opts: ParseOptions,
): T {
  const result = schema.safeParse(data);
  if (result.success) return result.data as T;
  schemaLogger.warn(
    `API response failed schema validation: ${opts.endpoint}`,
    {
      endpoint: opts.endpoint,
      issues: result.error.issues,
      received: data,
    },
  );
  return fallback;
}

const workflowId = z.string().min(1);
const workflowTime = z.string().nullable().optional().transform((v) => v ?? null);
export const WorkflowAvailabilitySchema = z.object({
  state: z.string().transform((v) => ["ready", "disabled", "unavailable", "unrunnable"].includes(v) ? v : "unknown"),
});
export const WorkflowSchema = z.object({
  id: workflowId, workspace_id: workflowId, name: z.string(), slug: z.string(),
  description: z.string().nullable().optional(), status: z.string(),
  current_revision_id: workflowId.nullable().optional(), definition_version: z.number().int().nonnegative(),
  last_run_at: workflowTime, next_run_at: workflowTime,
  trigger_kinds: z.array(z.string()).optional().default([]),
  can_write: z.boolean().optional().default(false),
  runtime_availability: WorkflowAvailabilitySchema.optional().default({ state: "unknown" }),
}).transform((v) => ({ id: v.id, workspaceId: v.workspace_id, name: v.name, slug: v.slug,
  description: v.description ?? "", status: v.status, currentRevisionId: v.current_revision_id ?? null,
  definitionVersion: v.definition_version, lastRunAt: v.last_run_at, nextRunAt: v.next_run_at,
  triggerKinds: v.trigger_kinds, canWrite: v.can_write, runtimeAvailability: v.runtime_availability }));
export const WorkflowGraphSchema = z.object({ steps: z.array(z.object({
  id: z.string().min(1).max(128), parents: z.array(z.string()).nullish().transform((v) => v ?? []),
})).max(256) });
export const WorkflowRevisionSchema = z.object({
  id: workflowId, workflow_id: workflowId, version: z.number().int().positive(),
  definition: z.string().max(256 * 1024), definition_hash: z.string(),
  definition_graph: WorkflowGraphSchema, validation_status: z.string(),
  validation_error: z.string().nullable().optional(), published_at: workflowTime,
}).transform((v) => ({ id: v.id, workflowId: v.workflow_id, version: v.version,
  definition: v.definition, definitionHash: v.definition_hash, graph: v.definition_graph,
  validationStatus: v.validation_status, validationError: v.validation_error ?? null, publishedAt: v.published_at }));
export const WorkflowStepSchema = z.object({
  id: z.string().max(128), status: z.string(), attempt: z.number().int().nonnegative(),
  started_at: workflowTime, finished_at: workflowTime,
}).transform((v) => ({ id: v.id, status: v.status, attempt: v.attempt, startedAt: v.started_at, finishedAt: v.finished_at }));
export const WorkflowRunSchema = z.object({
  id: workflowId, workflow_id: workflowId, revision_id: workflowId,
  principal_user_id: workflowId, source: z.string(), status: z.string(),
  reason_code: z.string().nullable().optional(), triggered_at: workflowTime,
  started_at: workflowTime, finished_at: workflowTime, cancel_requested_at: workflowTime,
  input: z.json().optional().default(null), steps: z.array(WorkflowStepSchema).max(256).optional().default([]),
}).transform((v) => ({ id: v.id, workflowId: v.workflow_id, revisionId: v.revision_id,
  principalUserId: v.principal_user_id, source: v.source, status: v.status, reasonCode: v.reason_code ?? null,
  triggeredAt: v.triggered_at, startedAt: v.started_at, finishedAt: v.finished_at,
  cancelRequestedAt: v.cancel_requested_at, input: v.input, steps: v.steps }));
export const WorkflowLogsSchema = z.object({
  run_id: workflowId, available: z.boolean(), reason_code: z.string().optional(),
  entries: z.array(z.object({ step_id: z.string().max(128), level: z.string(),
    message: z.string().max(4096), created_at: z.string(), })).max(200),
}).transform((v) => ({ runId: v.run_id, available: v.available, reasonCode: v.reason_code ?? null,
  entries: v.entries.map((e) => ({ stepId: e.step_id, level: e.level, message: e.message, createdAt: e.created_at })) }));
export const WorkflowTriggerSchema = z.object({
  id: workflowId, kind: z.string(), enabled: z.boolean(), principal_user_id: workflowId,
  cron_expression: z.string().nullable().optional(), timezone: z.string().nullable().optional(), next_run_at: workflowTime,
}).transform((v) => ({ id: v.id, kind: v.kind, enabled: v.enabled, principalUserId: v.principal_user_id,
  cronExpression: v.cron_expression ?? null, timezone: v.timezone ?? null, nextRunAt: v.next_run_at }));
