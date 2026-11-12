// One chaos worker process. The test starts several and kills them with
// SIGKILL (a crash: nothing is acked, no lease is handed back) or SIGTERM (a
// graceful stop: `shutdown()` drains, then the process exits).
//
// Every step attempt appends a line to $CHAOS_LOG:
//   exec <pid> <run_id> <key> <attempt>
//   violation <pid> <run_id> <key>      the step ran although its result was already in history
// Lines are short single `write`s to a file opened for appending, so lines
// from concurrent processes do not interleave.

import { appendFileSync } from "node:fs";

import { Backend, Client, Worker, type StepContext } from "../../ts/index.ts";
import { chaosWorkflows } from "./workflows.ts";

const env = (name: string): string => {
  const v = process.env[name];
  if (!v) throw new Error(`${name} is not set`);
  return v;
};

const log = env("CHAOS_LOG");
const backend = await Backend.connect(env("DURABLE_TEST_DATABASE_URL"), {
  redisUrl: env("DURABLE_TEST_REDIS_URL"),
  schema: env("CHAOS_SCHEMA"),
  namespace: env("CHAOS_NAMESPACE"),
  migrate: false,
});
const client = new Client(backend);

async function guard(s: StepContext): Promise<void> {
  const [runId] = s.idempotencyKey.split(":") as [string];
  appendFileSync(log, `exec ${process.pid} ${runId} ${s.key} ${s.attempt}\n`);
  const history = await client.events(runId);
  if (history.some((e) => e.kind.type === "step_succeeded" && e.kind.key === s.key)) {
    appendFileSync(log, `violation ${process.pid} ${runId} ${s.key}\n`);
  }
}

const worker = new Worker(backend, chaosWorkflows(guard), {
  concurrency: 4,
  leaseMs: 1_500,
  pollIntervalMs: 50,
  reconcileIntervalMs: 200,
  // Timers longer than this suspend the run, so a crash finds runs both
  // mid-step and asleep.
  inlineRetryThresholdMs: 100,
  drainTimeoutMs: 5_000,
  workerId: `chaos-${process.pid}`,
});
await worker.start();

process.once("SIGTERM", () => {
  void worker.shutdown().then(() => process.exit(0));
});
process.stdout.write("ready\n");
