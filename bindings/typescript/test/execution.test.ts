// The host loop's step side: what the engine will call in S2.

import assert from "node:assert/strict";
import { test } from "node:test";

import { Execution, StepError, workflow, type Ctx } from "../ts/index.ts";
import { native } from "../ts/native.ts";
import type { Event } from "../ts/wire.ts";

const runId = "01a1bb5e-f50d-757f-ab41-c75d52238109";
const started: Event = { run_id: runId, seq: 1, at: "2030-01-01T00:00:00Z", kind: { type: "run_started", input: null } };

/** Start `body` on a fresh run and let it issue its first commands. */
async function start(body: (ctx: Ctx) => PromiseLike<unknown>): Promise<Execution> {
  const r = new native.Replayer(runId, [started]);
  const exec = new Execution(workflow("t", body), r, null);
  assert.equal(r.advance(), "run_code");
  await exec.activate(r.takeResolved());
  return exec;
}

const info = { idempotencyKey: `${runId}:k` };

test("a step body runs on demand, as often as the engine asks", async () => {
  let calls = 0;
  const exec = await start((ctx) => ctx.step("k", (s) => ({ attempt: s.attempt, calls: ++calls, at: new Date(0) })));
  assert.deepEqual(await exec.runStep("k", 1, info), { ok: { attempt: 1, calls: 1, at: "1970-01-01T00:00:00.000Z" } });
  assert.deepEqual(await exec.runStep("k", 2, info), { ok: { attempt: 2, calls: 2, at: "1970-01-01T00:00:00.000Z" } });
});

test("thrown values pick the failure class", async () => {
  const exec = await start(async (ctx) => {
    await Promise.all([
      ctx.step("fatal", () => {
        throw StepError.fatal("no");
      }),
      ctx.step("later", () => {
        throw StepError.retryAfter(5_000, "busy");
      }),
      ctx.step("plain", () => {
        throw new Error("boom");
      }),
      ctx.step("bigint", () => 1n),
    ]);
  });
  assert.deepEqual(await exec.runStep("fatal", 1, info), { err: { message: "no", class: { type: "fatal" } } });
  assert.deepEqual(await exec.runStep("later", 1, info), {
    err: { message: "busy", class: { type: "retry_after", ms: 5_000 } },
  });
  assert.deepEqual(await exec.runStep("plain", 1, info), { err: { message: "boom", class: { type: "retryable" } } });
  const big = await exec.runStep("bigint", 1, info);
  assert.ok("err" in big && big.err.class.type === "fatal", JSON.stringify(big));
});

test("abortStep aborts the attempt's signal", async () => {
  let signal: AbortSignal | undefined;
  const exec = await start((ctx) =>
    ctx.step("slow", (s) => {
      signal = s.signal;
      return new Promise((resolve) => s.signal.addEventListener("abort", () => resolve("aborted")));
    }),
  );
  const outcome = exec.runStep("slow", 1, info);
  exec.abortStep("slow", 1);
  assert.equal(signal?.aborted, true);
  assert.deepEqual(await outcome, { ok: "aborted" });
});

test("a call cannot be configured once awaited", async () => {
  let error: unknown;
  await start(async (ctx) => {
    const call = ctx.step("k", () => 1);
    void call.then(() => {});
    try {
      call.retry({ maxAttempts: 9 });
    } catch (e) {
      error = e;
    }
  });
  assert.match(String(error), /cannot be configured after it has been awaited/);
});

test("a body that awaits nothing durable finishes in its first activation", async () => {
  const r = new native.Replayer(runId, [started]);
  const exec = new Execution(workflow("t", async () => ({ n: 1, skip: undefined })), r, null);
  r.advance();
  assert.deepEqual(await exec.activate(r.takeResolved()), { status: "finished", result: { ok: { n: 1 } } });
});
