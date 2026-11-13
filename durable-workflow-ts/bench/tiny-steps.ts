// How fast TypeScript workflows run many tiny steps, and what each step costs
// in calls between the engine (tokio threads) and JavaScript (the main
// thread). Build the release addon first:
//
//   npm run build:native:release && npm run bench
//
// The memory backend is the default, so the numbers are the engine plus the
// FFI. Set BENCH_DATABASE_URL (and BENCH_REDIS_URL) to run on Postgres.
// `cargo run --release -p durable-workflow --example tiny_steps` runs the same
// workloads with Rust workflows, for comparison.

import { performance } from "node:perf_hooks";

import { Backend, Client, Worker, workflow, type Workflow } from "../ts/index.ts";

const num = (name: string, fallback: number) => Number(process.env[name] ?? fallback);
const SEQ_STEPS = num("BENCH_SEQ_STEPS", 2_000);
const RUNS = num("BENCH_RUNS", 500);
const STEPS_PER_RUN = num("BENCH_STEPS_PER_RUN", 20);
const FAN_OUT = num("BENCH_FAN_OUT", 2_000);
const CONCURRENCY = num("BENCH_CONCURRENCY", 64);

const sequential = workflow("bench.sequential", async (ctx, n: number) => {
  let sum = 0;
  for (let i = 0; i < n; i++) sum += await ctx.step("s", () => i);
  return sum;
});

const fanOut = workflow("bench.fan_out", async (ctx, n: number) => {
  const parts = await ctx.map("m", Array.from({ length: n }, (_, i) => i), (i) => i);
  return parts.reduce((a, b) => a + b, 0);
});

async function backend(): Promise<Backend> {
  const url = process.env.BENCH_DATABASE_URL;
  if (!url) return Backend.memory();
  return Backend.connect(url, {
    redisUrl: process.env.BENCH_REDIS_URL,
    schema: `bench_${process.pid}`,
    namespace: `bench_${Date.now().toString(36)}`,
  });
}

interface Row {
  scenario: string;
  steps: number;
  ms: number;
  activations: number;
  stepCalls: number;
  elu: number;
}

async function measure(
  scenario: string,
  steps: number,
  workflows: Workflow<number, number>[],
  start: (client: Client) => Promise<Promise<unknown>[]>,
): Promise<Row> {
  const b = await backend();
  const worker = new Worker(b, workflows, { concurrency: CONCURRENCY, pollIntervalMs: 5 });
  await worker.start();
  const client = new Client(b);
  const elu0 = performance.eventLoopUtilization();
  const t0 = performance.now();
  await Promise.all(await start(client));
  const ms = performance.now() - t0;
  const elu = performance.eventLoopUtilization(elu0).utilization;
  const { activations, steps: stepCalls } = worker.stats;
  await worker.shutdown();
  return { scenario, steps, ms, activations, stepCalls, elu };
}

const rows: Row[] = [];
rows.push(
  await measure(`1 run x ${SEQ_STEPS} sequential steps`, SEQ_STEPS, [sequential], async (c) => [
    (await c.start(sequential, SEQ_STEPS)).result(),
  ]),
);
rows.push(
  await measure(`${RUNS} runs x ${STEPS_PER_RUN} steps, ${CONCURRENCY} at once`, RUNS * STEPS_PER_RUN, [sequential], async (c) => {
    const handles = [];
    for (let i = 0; i < RUNS; i++) handles.push(await c.start(sequential, STEPS_PER_RUN));
    return handles.map((h) => h.result());
  }),
);
rows.push(
  await measure(`1 run, ctx.map over ${FAN_OUT} items`, FAN_OUT, [fanOut], async (c) => [(await c.start(fanOut, FAN_OUT)).result()]),
);

console.log(`backend: ${process.env.BENCH_DATABASE_URL ? (process.env.BENCH_REDIS_URL ? "postgres + redis" : "postgres") : "memory"}, node ${process.version}`);
console.table(
  rows.map((r) => ({
    scenario: r.scenario,
    "steps/s": Math.round((r.steps / r.ms) * 1000),
    "µs/step": Math.round((r.ms * 1000) / r.steps),
    "activations/step": +(r.activations / r.steps).toFixed(2),
    "step calls/step": +(r.stepCalls / r.steps).toFixed(2),
    "main thread busy": `${Math.round(r.elu * 100)}%`,
  })),
);
