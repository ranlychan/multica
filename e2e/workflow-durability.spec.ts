import { test, expect, type TestInfo } from "@playwright/test";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { mkdtemp, mkdir, writeFile, readFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import pg from "pg";
import { TestApiClient } from "./fixtures";

const exec = promisify(execFile);
const root = resolve(process.cwd());
const providers = ["CLAUDE", "CODEX", "OPENCODE", "CODEARTS", "DEVECO", "OPENCLAW", "HERMES", "PI", "OMP", "CURSOR", "COPILOT", "KIMI", "REASONIX", "DSH", "KIRO", "CODEBUDDY", "ANTIGRAVITY", "QODER", "QODERCLICN", "TRAECLI", "GROK", "QWEN", "QWENPAW", "DIM", "MCODE", "ZEROCLAW"];
test.use({ channel: process.env.MULTICA_WORKFLOW_BROWSER_CHANNEL });

test.describe("managed workflow durability", () => {
  test.skip(process.env.MULTICA_WORKFLOW_DURABILITY !== "1", "Explicit managed Hatchet/fake-agent proof only");
  test.describe.configure({ mode: "serial", timeout: 360_000 });
  const api = new TestApiClient();
  let workspace: { id: string; slug: string };
  let fixtureDir: string;
  let daemonEnv: NodeJS.ProcessEnv;
  let profile: string;
  let app: pg.Client;
  let engine: pg.Client;

  async function managed(args: string[]) {
    return exec("make", args, { cwd: root, env: daemonEnv, timeout: 120_000, maxBuffer: 2 << 20 });
  }
  async function request(path: string, method = "GET", body?: unknown) {
    return api.requestJson(path, { method, body: body === undefined ? undefined : JSON.stringify(body) });
  }
  async function snapshot(runId: string) {
    const run = (await app.query("SELECT id,status,reason_code,hatchet_run_id,submission_attempts FROM workflow_run WHERE id=$1", [runId])).rows;
    const tasks = (await app.query("SELECT id,status,workflow_run_id,originator_user_id,originator_source FROM agent_task_queue WHERE workflow_run_id=$1", [runId])).rows;
    const waits = run[0]?.hatchet_run_id ? (await engine.query(`SELECT e.kind,e.is_satisfied,e.inserted_at,e.wait_data,f.latest_invocation_count,t.retry_count,rt.evicted_at
      FROM v1_task t JOIN v1_durable_event_log_entry e ON (e.durable_task_id,e.durable_task_inserted_at)=(t.id,t.inserted_at)
      JOIN v1_durable_event_log_file f ON (f.durable_task_id,f.durable_task_inserted_at)=(t.id,t.inserted_at)
      JOIN v1_task_runtime rt ON (rt.task_id,rt.task_inserted_at,rt.retry_count)=(t.id,t.inserted_at,t.retry_count)
      WHERE t.workflow_run_id=$1 ORDER BY e.id`, [run[0].hatchet_run_id])).rows : [];
    return { run, tasks, waits };
  }
  async function pendingSleep(runId: string) {
    const state = await snapshot(runId);
    return state.run[0]?.status === "running" && state.waits.some((wait) => wait.kind === "WAIT_FOR" && !wait.is_satisfied && wait.evicted_at &&
      wait.wait_data.conditions?.some((condition: { kind: string; sleepDurationMs?: number }) => condition.kind === "SLEEP" &&
        new Date(wait.inserted_at).getTime() + (condition.sleepDurationMs ?? 0) - Date.now() > 8_000));
  }
  async function evidence(info: TestInfo, name: string, state: unknown) {
    const path = join(fixtureDir, `${name}.json`);
    await writeFile(path, JSON.stringify(state, null, 2), { mode: 0o600 });
    await info.attach(name, { path, contentType: "application/json" });
    console.info(`[workflow-durability] ${name}: ${path}`);
  }
  async function killDaemonTree() {
    const { stdout } = await exec(join(root, "server/bin/multica"), ["daemon", "status", "--profile", profile, "--output", "json"], { env: daemonEnv });
    const pid = JSON.parse(stdout).pid as number;
    const { stdout: processes } = await exec("ps", ["-axo", "pid=,ppid=,command="]);
    const rows = processes.trim().split("\n").map((line) => line.trim().match(/^(\d+)\s+(\d+)\s+(.+)$/)).filter((row) => row !== null);
    const owner = rows.find((row) => Number(row[1]) === pid);
    expect(owner?.[3]).toContain(profile);
    const owned = new Set([pid]);
    for (let i = 0; i < rows.length; i++) for (const row of rows) if (owned.has(Number(row[2]))) owned.add(Number(row[1]));
    for (const child of [...owned].reverse()) process.kill(child, "SIGKILL");
    return [...owned];
  }

  test.beforeAll(async () => {
    expect(process.env.MULTICA_HATCHET_ENABLED).toBe("true");
    expect(process.env.MULTICA_WORKFLOW_NODE).toBeTruthy();
    const status = await exec("make", ["status", "ARGS=--json"], { cwd: root });
    const environment = JSON.parse(status.stdout);
    expect(resolve(environment.dir)).toBe(root);
    profile = environment.profile;
    // Only the disposable database names explicitly configured for this proof.
    expect(new URL(process.env.DATABASE_URL!).pathname).toMatch(/^\/multica_task8_/);
    expect(new URL(process.env.MULTICA_HATCHET_DATABASE_URL!).pathname).toMatch(/^\/multica_task8_/);
    expect(new URL(process.env.MULTICA_HATCHET_DATABASE_URL!).pathname).not.toBe(new URL(process.env.DATABASE_URL!).pathname);
    app = new pg.Client(process.env.DATABASE_URL);
    engine = new pg.Client(process.env.MULTICA_HATCHET_DATABASE_URL);
    await app.connect(); await engine.connect();
    await api.login(process.env.MULTICA_DEV_EMAIL!, "Workflow durability");
    workspace = await api.ensureWorkspace("Workflow durability", process.env.MULTICA_DEV_WORKSPACE_SLUG!);
    await api.markUserOnboarded();
    fixtureDir = await mkdtemp(join(tmpdir(), "multica-workflow-e2e-"));
    const fake = join(fixtureDir, "fake-claude");
    await writeFile(fake, `#!${process.env.MULTICA_WORKFLOW_NODE}\nconst fs = require('node:fs');
if(process.argv.includes('--version')) { console.log('2.1.0 (Claude Code)'); process.exit(0); }
if(process.argv.includes('--help')) { console.log('Owned workflow durability fixture'); process.exit(0); }
process.stdin.once('data', () => {
 fs.appendFileSync(${JSON.stringify(join(fixtureDir, "dispatches"))}, 'dispatch\\n');
 console.log(JSON.stringify({type:'system',session_id:'workflow-fake-session'}));
 console.log(JSON.stringify({type:'result',subtype:'success',is_error:false,session_id:'workflow-fake-session',result:'done'}));
 process.exit(0);
});\n`, { mode: 0o755 });
    const claudeConfig = join(fixtureDir, "claude-config");
    await mkdir(claudeConfig, { mode: 0o700 });
    daemonEnv = { ...process.env, GOFLAGS: "-p=1", MULTICA_WORKFLOW_WORKSPACES: workspace.id, CLAUDE_CONFIG_DIR: claudeConfig };
    for (const provider of providers) daemonEnv[`MULTICA_${provider}_PATH`] = join(fixtureDir, `missing-${provider.toLowerCase()}`);
    daemonEnv.MULTICA_CLAUDE_PATH = fake;
    await managed(["up", "C=daemon"]);
  });

  test.afterAll(async () => {
    if (daemonEnv) await managed(["down", "ARGS=--components daemon"]);
    await app?.end(); await engine?.end();
  });

  for (const crash of ["daemon", "engine"] as const) test(`resumes after ${crash} dies during an issue wait with one downstream fake dispatch`, async ({ page }, info) => {
    const nonce = `${crash}-${Date.now()}`;
    let workflowId = "", runId = "", agentId = "", issueId = "";
    let passed = false;
    try {
      const runtimes = await request("/api/runtimes");
      const runtime = runtimes.find((item: { provider: string; status: string }) => item.provider === "claude" && item.status === "online");
      expect(runtime).toBeTruthy();
      const agent = await request("/api/agents", "POST", { name: `Workflow fake ${nonce}`, runtime_id: runtime.id, visibility: "workspace", max_concurrent_tasks: 1 });
      agentId = agent.id;
      const workflow = await request("/api/workflows", "POST", { name: `Durable ${nonce}`, slug: `durable-${nonce}` });
      workflowId = workflow.id;
      const definition = `import {defineWorkflow} from '@multica/workflow-sdk';
export default defineWorkflow({steps:[
 {id:'create',retries:1,run:async ctx=>{const issue=await ctx.issues.create('create-ticket',{title:'Durable ${nonce}'});if(ctx.attempt===0)throw new Error('intentional retry');return issue;}},
 {id:'wait',parents:['create'],timeoutMs:300000,run:async ctx=>{await ctx.issues.waitForStatus(ctx.parents.create.id,'done',{timeoutMs:300000});return ctx.parents.create;}},
 {id:'dispatch',parents:['wait'],run:async ctx=>ctx.agents.dispatch('follow-up',{agent_id:'${agentId}',issue_id:ctx.parents.wait.id})}
]});`;
      await request(`/api/workflows/${workflowId}/revisions`, "POST", { expected_version: 0, definition, definition_graph: { steps: [{ id: "create", parents: [] }, { id: "wait", parents: ["create"] }, { id: "dispatch", parents: ["wait"] }] } });
      await expect.poll(async () => (await request(`/api/workflows/${workflowId}`)).runtime_availability.state, { timeout: 90_000 }).toBe("ready");
      runId = (await request(`/api/workflows/${workflowId}/run`, "POST", { input: { crash } })).id;
      await expect.poll(async () => {
        const issues = (await app.query("SELECT id FROM issue WHERE workspace_id=$1 AND title=$2", [workspace.id, `Durable ${nonce}`])).rows;
        if (issues.length === 1) issueId = issues[0].id;
        return issues.length;
      }, { timeout: 60_000 }).toBe(1);
      await expect.poll(() => pendingSleep(runId), { timeout: 60_000 }).toBe(true);
      const before = await snapshot(runId);
      expect(before.tasks).toHaveLength(0);
      await evidence(info, `before-${crash}-${runId}`, before);
      if (crash === "daemon") {
        const killed = await killDaemonTree();
        await evidence(info, `killed-daemon-${runId}`, killed);
        await managed(["up", "C=daemon"]);
      } else {
        const port = new URL(`http://${process.env.MULTICA_HATCHET_GRPC_ADDRESS}`).port;
        const { stdout } = await exec("lsof", ["-t", `-iTCP:${port}`, "-sTCP:LISTEN"]);
        const pid = Number(stdout.trim());
        const { stdout: command } = await exec("ps", ["-p", String(pid), "-o", "comm="]);
        expect(command.trim()).toBe(join(process.env.MULTICA_HATCHET_BINARY_DIR!, "hatchet-engine"));
        process.kill(pid, "SIGKILL");
        await managed(["down", "ARGS=--components api"]);
        await managed(["up", "C=api"]);
      }
      // The business wait remains unsatisfied through the restart.
      expect((await snapshot(runId)).tasks).toHaveLength(0);
      await api.updateIssue(issueId, { status: "done" });
      await expect.poll(async () => (await request(`/api/workflows/${workflowId}/runs/${runId}`)).status, { timeout: 120_000 }).toBe("completed");
      await expect.poll(async () => (await snapshot(runId)).tasks[0]?.status, { timeout: 90_000 }).toBe("completed");
      const final = await snapshot(runId);
      expect(final.tasks).toHaveLength(1);
      expect(final.tasks[0]).toMatchObject({ originator_source: "workflow" });
      expect((await app.query("SELECT count(*)::int AS count FROM issue WHERE workspace_id=$1 AND title=$2", [workspace.id, `Durable ${nonce}`])).rows[0].count).toBe(1);
      const retry = (await engine.query("SELECT retry_count FROM v1_task WHERE workflow_run_id=$1", [final.run[0].hatchet_run_id])).rows;
      expect(retry.some((item) => item.retry_count >= 1)).toBe(true);
      await evidence(info, `after-${crash}-${runId}`, { ...final, retry });
      await page.addInitScript((token) => localStorage.setItem("multica_token", token!), api.getToken());
      await page.goto(`/${workspace.slug}/autopilots?tab=workflows`);
      await expect(page.getByRole("link", { name: workflow.name, exact: true })).toBeVisible({ timeout: 30_000 });
      await page.getByRole("link", { name: workflow.name, exact: true }).click();
      await expect(page.getByRole("heading", { name: "Source", exact: true })).toBeVisible();
      await page.goto(`/${workspace.slug}/workflows/${workflowId}/runs/${runId}`);
      await expect(page.getByRole("heading", { name: runId, exact: true })).toBeVisible({ timeout: 30_000 });
      await expect(page.getByRole("button", { name: "Cancel run" })).toBeDisabled();
      const dispatches = (await readFile(join(fixtureDir, "dispatches"), "utf8")).trim().split("\n");
      expect(dispatches).toHaveLength(crash === "daemon" ? 1 : 2);
      passed = true;
    } finally {
      if (runId) await evidence(info, `final-${crash}-${runId}`, await snapshot(runId));
      // Failed fixtures remain intact for inspecting ledger/task/wait evidence.
      if (passed) {
        await request(`/api/workflows/${workflowId}`, "DELETE");
        await request(`/api/agents/${agentId}/archive`, "POST");
        await api.deleteIssue(issueId);
      }
    }
  });
});
