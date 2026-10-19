import type { z } from "zod";
import type { WorkflowSchema, WorkflowRevisionSchema, WorkflowRunSchema, WorkflowLogsSchema, WorkflowTriggerSchema } from "../api/schema";

export type Workflow = z.infer<typeof WorkflowSchema>;
export type WorkflowRevision = z.infer<typeof WorkflowRevisionSchema>;
export type WorkflowJsonValue = null | boolean | number | string | WorkflowJsonValue[] | { [key: string]: WorkflowJsonValue };
export type WorkflowRun = Omit<z.infer<typeof WorkflowRunSchema>, "input"> & { input: WorkflowJsonValue };
export type WorkflowLogs = z.infer<typeof WorkflowLogsSchema>;
export type WorkflowTrigger = z.infer<typeof WorkflowTriggerSchema>;
