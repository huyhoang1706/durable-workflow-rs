// End to end on the real engine (memory backend): TypeScript workflows run
// by the Rust worker through the threadsafe-function host.

import { afterEach, expect, test } from "vitest";
import { z } from "zod";

import {
  Backend,
  Client,
  RunCancelledError,
  RunFailedError,
  StepError,
  StepFailedError,
  Worker,
  retry,
  workflow,
  type Workflow,
  type WorkerOptions,
} from "../ts/index.ts";

const workers: Worker[] = [];

afterEach(async () => {
  await Promise.all(workers.splice(0).map((w) => w.shutdown()));
});

/** A started worker and a client on a fresh memory backend. */
async function setup(workflows: Workflow<any, any>[], options: WorkerOptions = {}) {
  const backend = Backend.memory();
  const worker = new Worker(backend, workflows, { pollIntervalMs: 20, drainTimeoutMs: 1_000, ...options });
  workers.push(worker);
  await worker.start();
  return { backend, worker, client: new Client(backend) };
}

async function waitFor<T>(what: string, f: () => Promise<T | null | undefined | false>, ms = 5_000): Promise<T> {
  const deadline = Date.now() + ms;
  for (;;) {
    const v = await f();
    if (v) return v;
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${what}`);
    await new Promise((r) => setTimeout(r, 10));
  }
}

type RunRow = { status: string; awaiting: string[]; last_error: string | null };

async function run(client: Client, id: string): Promise<RunRow> {
  return (await client.getRun(id)) as unknown as RunRow;
}

test("steps, a timer and a signal", async () => {
  const calls: string[] = [];
  const onboarding = workflow("onboarding", async (ctx, user: { email: string }) => {
    const id = await ctx.step("create", { output: z.string(), execute: () => (calls.push("create"), `u-${user.email}`) });
    await ctx.sleep("cool-off", 30);
    const verified = await ctx.waitForSignal("verified", { payload: z.boolean() });
    await ctx.step("welcome", () => void calls.push("welcome"));
    return { id, verified };
  });
  const { client } = await setup([onboarding]);

  const h = await client.start(onboarding, { email: "a@b.c" });
  expect(h.existing).toBe(false);
  await waitFor("the wait", async () => (await run(client, h.id)).awaiting.includes("verified"));
  await client.signal(h.id, "verified", true);

  expect(await h.result()).toStrictEqual({ id: "u-a@b.c", verified: true });
  expect(calls).toStrictEqual(["create", "welcome"]);
  const kinds = (await client.events(h.id)).map((e) => e.kind.type);
  expect(kinds).toContain("timer_fired");
  expect(kinds).toContain("signal_received");
  expect(kinds.at(-1)).toBe("run_succeeded");
});

test("a step is retried by the engine with its policy, and sees its attempt", async () => {
  const attempts: number[] = [];
  const flaky = workflow("flaky", async (ctx) =>
    ctx.step("call", (s) => {
      attempts.push(s.attempt);
      if (s.attempt < 3) throw new Error(`attempt ${s.attempt} failed`);
      return { attempt: s.attempt, key: s.idempotencyKey };
    }).retry(retry.fixed(5, 10)),
  );
  const { client } = await setup([flaky]);
  const h = await client.start(flaky, null);
  expect(await h.result()).toStrictEqual({ attempt: 3, key: `${h.id}:call` });
  expect(attempts).toStrictEqual([1, 2, 3]);
});

test("a fatal step failure fails the run unless the body handles it", async () => {
  const strict = workflow("strict", async (ctx) => {
    await ctx.step("charge", () => {
      throw StepError.fatal("card declined");
    });
  });
  const lenient = workflow("lenient", async (ctx) => {
    try {
      await ctx.step("charge", () => {
        throw StepError.fatal("card declined");
      });
      return "charged";
    } catch (e) {
      return e instanceof StepFailedError ? `fallback: ${e.message}` : "?";
    }
  });
  const { client } = await setup([strict, lenient]);

  const s = await client.start(strict, null);
  await expect(s.result()).rejects.toThrow(RunFailedError);
  await expect(s.result()).rejects.toThrow(/card declined/);
  expect(await (await client.start(lenient, null)).result()).toBe("fallback: step failed: card declined");
});

test("a run that suspends resumes by replay, without running finished steps again", async () => {
  let runs = 0;
  let bodies = 0;
  const later = workflow("later", async (ctx) => {
    bodies++;
    const n = await ctx.step("once", () => ++runs);
    await ctx.sleep("nap", 100);
    const m = await ctx.step("after", () => ++runs);
    return [n, m, ctx.isReplaying()];
  });
  // Nothing is waited out in-process: the sleep releases the lease.
  const { client } = await setup([later], { inlineRetryThresholdMs: 0 });
  const h = await client.start(later, null);
  expect(await h.result()).toStrictEqual([1, 2, false]);
  expect(runs).toBe(2);
  // Two executions: the first suspended on the sleep, the second replayed.
  expect(bodies).toBe(2);
});

test("after a crash, another worker resumes the run and the lost attempt is aborted", async () => {
  const started: number[] = [];
  let abortedOnCrash = false;
  let release!: () => void;
  const gate = new Promise<void>((r) => (release = r));
  const job = workflow("job", async (ctx) => {
    const a = await ctx.step("first", () => "a");
    const b = await ctx.step("slow", async (s) => {
      started.push(s.attempt);
      if (started.length === 1) {
        // The first worker crashes while this attempt runs.
        await new Promise((_, reject) =>
          s.signal.addEventListener("abort", () => {
            abortedOnCrash = true;
            reject(new Error("aborted"));
          }),
        );
      }
      await gate;
      return "b";
    });
    return a + b;
  });
  const opts: WorkerOptions = { leaseMs: 200, reconcileIntervalMs: 50 };
  const { backend, worker, client } = await setup([job], opts);
  const h = await client.start(job, null);
  await waitFor("the first attempt", async () => started.length === 1);

  worker.abort();
  await waitFor("the lost attempt's abort", async () => abortedOnCrash);
  const second = new Worker(backend, [job], { pollIntervalMs: 20, drainTimeoutMs: 1_000, ...opts });
  workers.push(second);
  await second.start();
  await waitFor("the resumed attempt", async () => started.length === 2);
  release();

  expect(await h.result()).toBe("ab");
  const kinds = (await client.events(h.id)).map((e) => e.kind.type);
  expect(kinds.filter((k) => k === "step_succeeded")).toHaveLength(2);
});

test("events with a filter, and hooks", async () => {
  const order = workflow("order", async (ctx, id: string) => {
    const paid = await ctx.waitForEvent("paid", { payload: z.object({ order: z.string(), amount: z.number() }) }).filter({
      order: id,
    });
    const hook = await ctx.createHook("shipping", { token: `ship-${id}` });
    const label = await hook.recv();
    return { amount: paid.amount, label };
  });
  const { client } = await setup([order]);
  const h = await client.start(order, "o1");

  await waitFor("the event wait", async () => (await run(client, h.id)).awaiting.length > 0);
  expect(await client.sendEvent("paid", { order: "o2", amount: 1 })).toBe(0);
  expect(await client.sendEvent("paid", { order: "o1", amount: 42 })).toBe(1);
  await waitFor("the hook", async () => {
    try {
      await client.resumeHook("ship-o1", "DHL-1");
      return true;
    } catch {
      return false;
    }
  });
  expect(await h.result()).toStrictEqual({ amount: 42, label: "DHL-1" });
});

test("cancelling a waiting run", async () => {
  const waits = workflow("waits", async (ctx) => ctx.waitForSignal("never"));
  const { client } = await setup([waits]);
  const h = await client.start(waits, null);
  await waitFor("the wait", async () => (await run(client, h.id)).awaiting.includes("never"));
  await client.cancel(h.id, "no longer needed");
  await expect(h.result()).rejects.toThrow(RunCancelledError);
});

test("a timed-out attempt aborts the step's AbortSignal", async () => {
  let aborted = false;
  const slow = workflow("slow", async (ctx) =>
    ctx.step("hang", (s) => new Promise((_, reject) => s.signal.addEventListener("abort", () => ((aborted = true), reject(new Error("aborted"))))))
      .timeout(50)
      .retry(retry.none()),
  );
  const { client } = await setup([slow]);
  const h = await client.start(slow, null);
  await expect(h.result()).rejects.toThrow(/timed out/);
  await waitFor("the abort", async () => aborted);
});

test("idempotency keys return the existing run", async () => {
  const once = workflow("once", async (_ctx, n: number) => n * 2);
  const { client } = await setup([once]);
  const a = await client.start(once, 21, { idempotencyKey: "k" });
  const b = await client.start(once, 99, { idempotencyKey: "k" });
  expect(b.id).toBe(a.id);
  expect(b.existing).toBe(true);
  expect(await b.result()).toBe(42);
});

test("deterministic helpers survive replay", async () => {
  const det = workflow("det", async (ctx) => {
    const first = { uuid: ctx.uuid(), random: ctx.random() };
    await ctx.sleep("nap", 50);
    return { first, again: { uuid: ctx.uuid(), random: ctx.random() } };
  });
  const { client, worker } = await setup([det], { inlineRetryThresholdMs: 0 });
  const h = await client.start(det, null);
  const out = (await h.result()) as { first: { uuid: string } };
  // Replaying the stored history yields the values the live run recorded.
  const replayed = await worker.replay(det, h.id, null, await client.events(h.id));
  expect(replayed.fault).toBeNull();
  expect(replayed.result).toStrictEqual({ ok: out });
});

test("awaiting something that is not a ctx call faults the run", async () => {
  const bad = workflow("bad", async (ctx) => {
    await ctx.step("ok", () => 1);
    await new Promise((r) => setTimeout(r, 1_000));
    return "unreachable";
  });
  const { client } = await setup([bad]);
  const h = await client.start(bad, null);
  const row = await waitFor("the fault", async () => {
    const r = await run(client, h.id);
    return r.last_error ? r : null;
  });
  expect(row.status).not.toBe("succeeded");
  expect(row.last_error).toMatch(/frontier|non-durable|awaited/i);
});

test("finished executions are released", async () => {
  const quick = workflow("quick", async (ctx, n: number) => ctx.step("double", () => n * 2));
  const { client, worker } = await setup([quick]);
  const handles = await Promise.all([1, 2, 3, 4, 5].map((n) => client.start(quick, n)));
  expect(await Promise.all(handles.map((h) => h.result()))).toStrictEqual([2, 4, 6, 8, 10]);
  await waitFor("released executions", async () => worker.executions === 0);
});

test("unknown workflows and duplicate names are rejected", async () => {
  const a = workflow("a", async () => 1);
  expect(() => new Worker(Backend.memory(), [a, a])).toThrow(/registered twice/);
  await expect(new Worker(Backend.memory(), [a]).replay("nope", "01a1bb5e-f50d-757f-ab41-c75d52238109", null, [])).rejects.toThrow(
    /unknown workflow/,
  );
});
