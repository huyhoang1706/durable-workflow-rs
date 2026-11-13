// Crash/resume chaos on real Postgres + Redis, with workers in separate Node
// processes (`test/chaos/worker.ts`) that the test kills.
//
// Runs only when DURABLE_TEST_DATABASE_URL and DURABLE_TEST_REDIS_URL are set
// (`npm run test:servers` starts both). Redis is required: the processes share
// work through its queue.

import { spawn, type ChildProcess } from "node:child_process";
import { mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

import { afterEach, describe, expect, test } from "vitest";

import { Backend, Client, replay, type Event } from "../ts/index.ts";
import { chaosWorkflows, expected } from "./chaos/workflows.ts";

const DATABASE_URL = process.env.DURABLE_TEST_DATABASE_URL;
const REDIS_URL = process.env.DURABLE_TEST_REDIS_URL;

const WORKER = fileURLToPath(new URL("./chaos/worker.ts", import.meta.url));
const schema = `ts_chaos_${process.pid}_${Date.now().toString(36)}`;
let seq = 0;

const children = new Set<ChildProcess>();

afterEach(() => {
  for (const c of children) c.kill("SIGKILL");
  children.clear();
});

interface Env {
  backend: Backend;
  client: Client;
  /** Spawn a worker process; resolves once it is claiming runs. */
  spawnWorker(): Promise<ChildProcess>;
  /** The worker processes' log, parsed. */
  log(): { execs: { pid: number; run: string; key: string; attempt: number }[]; violations: string[] };
}

async function setup(): Promise<Env> {
  const namespace = `chaos_${seq++}`;
  // Migrate once here; the workers connect with `migrate: false`.
  const backend = await Backend.connect(DATABASE_URL!, { schema, namespace, redisUrl: REDIS_URL! });
  const logPath = join(mkdtempSync(join(tmpdir(), "durable-chaos-")), "steps.log");
  writeFileSync(logPath, "");
  const env = {
    ...process.env,
    CHAOS_LOG: logPath,
    CHAOS_SCHEMA: schema,
    CHAOS_NAMESPACE: namespace,
  };

  function spawnWorker(): Promise<ChildProcess> {
    const child = spawn(process.execPath, [WORKER], { env, stdio: ["ignore", "pipe", "inherit"] });
    children.add(child);
    child.once("exit", () => children.delete(child));
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error("a worker process did not start in time")), 20_000);
      child.stdout!.setEncoding("utf8");
      child.stdout!.on("data", (d: string) => {
        if (d.includes("ready")) {
          clearTimeout(timer);
          resolve(child);
        }
      });
      child.once("exit", (code, signal) => {
        clearTimeout(timer);
        reject(new Error(`a worker process exited before it was ready (${code ?? signal})`));
      });
    });
  }

  function log() {
    const lines = readFileSync(logPath, "utf8").split("\n").filter(Boolean);
    const execs = [];
    const violations = [];
    for (const line of lines) {
      const [what, pid, run, key, attempt] = line.split(" ") as [string, string, string, string, string?];
      if (what === "exec") execs.push({ pid: Number(pid), run, key, attempt: Number(attempt) });
      else violations.push(`${run}:${key}`);
    }
    return { execs, violations };
  }

  return { backend, client: new Client(backend), spawnWorker, log };
}

/** Stop a process; resolves with its exit code, or the signal that ended it. */
function stop(child: ChildProcess, signal: NodeJS.Signals): Promise<number | string> {
  return new Promise((resolve) => {
    if (child.exitCode !== null) return resolve(child.exitCode);
    child.once("exit", (code, sig) => resolve(code ?? sig ?? "unknown"));
    child.kill(signal);
  });
}

const pause = (ms: number) => new Promise((r) => setTimeout(r, ms));

type Run = { id: string; workflow: string; n: number };
type RunRow = { status: string; awaiting: string[]; output: unknown; last_error: string | null };

async function startRuns(client: Client, count: number, workflows: string[]): Promise<Run[]> {
  const runs: Run[] = [];
  for (let n = 0; n < count; n++) {
    const workflow = workflows[n % workflows.length]!;
    runs.push({ id: (await client.start(workflow, n)).id, workflow, n });
  }
  return runs;
}

/** Signal `go` to each run as it reaches the wait, until every run has finished. */
async function signalAndFinish(client: Client, runs: Run[], go: number, ms: number): Promise<Map<string, RunRow>> {
  const deadline = Date.now() + ms;
  const signalled = new Set<string>();
  const done = new Map<string, RunRow>();
  while (done.size < runs.length) {
    for (const r of runs) {
      if (done.has(r.id)) continue;
      const row = (await client.getRun(r.id)) as unknown as RunRow;
      if (["succeeded", "failed", "cancelled"].includes(row.status)) done.set(r.id, row);
      else if (!signalled.has(r.id) && row.awaiting.includes("go")) {
        await client.signal(r.id, "go", go);
        signalled.add(r.id);
      }
    }
    if (Date.now() > deadline) {
      const stuck = runs.filter((r) => !done.has(r.id)).slice(0, 3);
      const rows = await Promise.all(stuck.map((r) => client.getRun(r.id)));
      throw new Error(`${runs.length - done.size} runs unfinished; e.g. ${JSON.stringify(rows)}`);
    }
    await pause(100);
  }
  return done;
}

const stepKeys = (workflow: string) => (workflow === "chaos.seq" ? ["s0", "s1", "s2"] : ["work[0]", "work[1]", "work[2]"]);

/** Every run succeeded with the right output, recorded each step once, and replays to the same result. */
async function checkRuns(client: Client, runs: Run[], rows: Map<string, RunRow>, go: number): Promise<void> {
  const definitions = new Map(chaosWorkflows(async () => {}).map((w) => [w.name, w]));
  for (const r of runs) {
    const row = rows.get(r.id)!;
    expect({ run: r, status: row.status, output: row.output }).toStrictEqual({
      run: r,
      status: "succeeded",
      output: expected(r.n, go),
    });
    const history: Event[] = await client.events(r.id);
    const succeeded = history.filter((e) => e.kind.type === "step_succeeded").map((e) => e.kind.key);
    expect(succeeded.sort()).toStrictEqual(stepKeys(r.workflow));
    // The history a crash-ridden run left behind is one a single clean run could have written.
    const again = await replay(definitions.get(r.workflow)!, r.id, r.n, history);
    expect({ fault: again.fault, result: again.result }).toStrictEqual({ fault: null, result: { ok: expected(r.n, go) } });
  }
}

describe.skipIf(!DATABASE_URL || !REDIS_URL)("chaos: worker processes", () => {
  test("SIGKILLed workers lose no run and rerun no committed step", { timeout: 180_000 }, async () => {
    const env = await setup();
    const workers = await Promise.all([env.spawnWorker(), env.spawnWorker(), env.spawnWorker()]);
    const runs = await startRuns(env.client, 60, ["chaos.seq", "chaos.map"]);

    // Kill a worker every 300 ms while steps are in flight and runs sleep,
    // and start a fresh process in its place. The step work (60 runs x 3
    // steps of 150 ms on 3 x 4 slots, plus retries) outlasts the kills.
    const killed = new Set<number>();
    const chaos = (async () => {
      for (let k = 0; k < 6; k++) {
        await pause(k === 0 ? 150 : 300);
        const victim = workers[k % 3]!;
        killed.add(victim.pid!);
        expect(await stop(victim, "SIGKILL")).toBe("SIGKILL");
        workers[k % 3] = await env.spawnWorker();
      }
    })();
    const rows = await signalAndFinish(env.client, runs, 1, 120_000);
    await chaos;

    await checkRuns(env.client, runs, rows, 1);
    const { execs, violations } = env.log();
    expect(violations, "committed steps ran again").toStrictEqual([]);
    // The kills hit work in flight: some attempts died with a killed process
    // and the step ran again elsewhere.
    const pidsByStep = new Map<string, Set<number>>();
    for (const e of execs) {
      const k = `${e.run}:${e.key}`;
      pidsByStep.set(k, (pidsByStep.get(k) ?? new Set()).add(e.pid));
    }
    const moved = [...pidsByStep.values()].filter((pids) => pids.size > 1 && [...pids].some((p) => killed.has(p)));
    const stats = `${execs.length} attempts of ${pidsByStep.size} steps, ${killed.size} kills`;
    expect(moved.length, `no step attempt was interrupted by a kill (${stats})`).toBeGreaterThan(0);
  });

  test("SIGTERM drains: in-flight steps finish, and no step runs twice", { timeout: 120_000 }, async () => {
    const env = await setup();
    const [leaving] = await Promise.all([env.spawnWorker(), env.spawnWorker()]);
    const runs = await startRuns(env.client, 20, ["chaos.seq"]);

    await pause(300);
    expect(await stop(leaving!, "SIGTERM")).toBe(0);
    const rows = await signalAndFinish(env.client, runs, 2, 60_000);

    await checkRuns(env.client, runs, rows, 2);
    const { execs, violations } = env.log();
    expect(violations).toStrictEqual([]);
    expect(execs.some((e) => e.pid === leaving!.pid), "the leaving worker ran no step").toBe(true);
    // `chaos.seq` never fails a step: one attempt per step, whoever ran it.
    const counts = new Map<string, number>();
    for (const e of execs) counts.set(`${e.run}:${e.key}`, (counts.get(`${e.run}:${e.key}`) ?? 0) + 1);
    expect([...counts.values()].every((c) => c === 1), "a step ran more than once").toBe(true);
    expect(counts.size).toBe(runs.length * 3);
  });
});
