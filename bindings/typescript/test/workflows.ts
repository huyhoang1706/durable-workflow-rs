// TypeScript ports of the corpus workflows (`crates/durable-runtime/tests/replay_corpus.rs`),
// by case name. For the non-determinism cases this is the *changed* version
// that replays the history.

import { StepError, StepFailedError, retry, workflow, type Workflow } from "../ts/index.ts";
import type { Json } from "../ts/wire.ts";

/** RFC 3339 as chrono serializes it: no fractional seconds when there are none. */
const rfc3339 = (d: Date) => d.toISOString().replace(".000Z", "Z");

const integer = (v: Json): number => {
  if (typeof v !== "number" || !Number.isInteger(v)) throw new TypeError(`expected an integer, got ${JSON.stringify(v)}`);
  return v;
};

const race = workflow("race", async (ctx) => {
  const winner = await ctx.select({
    signal: ctx.waitForSignal("approval"),
    timeout: ctx.sleep("deadline", 60_000),
  });
  return winner.key === "signal" ? `signal ${JSON.stringify(winner.value)}` : "timeout";
});

export const ports: Record<string, Workflow<any, any>> = {
  sequential_steps: workflow("add", async (ctx, n: number) => {
    const a = await ctx.step("double", () => n * 2);
    return ctx.step("inc", () => a + 1);
  }),

  parallel_steps: workflow("fan-out", async (ctx) => {
    const [a, b, c] = await Promise.all([ctx.step("a", () => 1), ctx.step("b", () => 2), ctx.step("c", () => 3)]);
    return a + b + c;
  }),

  repeated_names: workflow("loop", async (ctx) => {
    const out = [];
    for (let i = 0; i < 3; i++) out.push(await ctx.step("item", () => i * 10));
    return out;
  }),

  step_retry: workflow("retry", (ctx) =>
    ctx
      .step("flaky", (s) => {
        if (s.attempt < 3) throw StepError.retryable(`attempt ${s.attempt} failed`);
        return s.attempt;
      })
      .retry(retry.fixed(5, 0)),
  ),

  step_failure_recovered: workflow("recover", async (ctx) => {
    try {
      await ctx.step("bad", (): number => {
        throw StepError.retryable("always");
      }, { retry: retry.none() });
      throw new Error("unexpected success");
    } catch (e) {
      if (e instanceof StepFailedError) return `recovered from ${e.message.replace(/^step failed: /, "")}`;
      throw e;
    }
  }),

  step_failure_fails_run: workflow("fatal", async (ctx) => {
    await ctx.step("bad", () => {
      throw StepError.fatal("nope");
    });
    return null;
  }),

  sleep: workflow("sleepy", async (ctx) => {
    const before = ctx.now();
    await ctx.sleep("nap", 90_000);
    return (ctx.now().getTime() - before.getTime()) / 1000;
  }),

  signal: workflow("approve", (ctx) => ctx.waitForSignal("approval")),

  signal_timeout: workflow("timeout-wait", (ctx) =>
    ctx.waitForSignal("approval", { decode: integer }).timeout(300_000),
  ),

  race_timer_wins: race,
  race_signal_wins: race,

  hook: workflow("hooked", async (ctx) => {
    const hook = await ctx.createHook("callback", { token: "corpus-hook", decode: integer });
    const first = await hook.recv();
    const second = await hook.recv();
    await hook.dispose();
    return first + second;
  }),

  hook_minted_token: workflow("inbox", async (ctx) => {
    const hook = await ctx.createHook("inbox");
    return hook.recv();
  }),

  event_filter: workflow("order", async (ctx, id: number) => {
    const paid = await ctx.waitForEvent<{ amount: Json }>("order.paid").filter({ order_id: id });
    return paid.amount;
  }),

  map: workflow("squares", (ctx) => ctx.map("square", [1, 2, 3], (x) => x * x)),

  deterministic_helpers: workflow("det", async (ctx) => {
    const random = ctx.randomU64();
    const first = ctx.uuid();
    await ctx.sleep("nap", 10_000);
    return { random: random.toString(), first, second: ctx.uuid(), now: rfc3339(ctx.now()) };
  }),

  // Rust's sync workflow; TypeScript has only async bodies.
  sync_workflow: workflow("sync-sum", async (ctx, n: number) => {
    const [a, b] = await Promise.all([ctx.step("a", () => n + 1), ctx.step("b", () => n * 2)]);
    await ctx.sleep("cool-down", 60_000);
    return ctx.step("sum", () => a + b);
  }),

  waiting_on_signal: workflow("pending", async (ctx) => {
    await ctx.step("prepare", () => "ready");
    return ctx.waitForSignal("approval");
  }),

  invalid_name: workflow("bad-name", (ctx) => ctx.step("bad#name", () => 1)),

  nondeterminism_kind_mismatch: workflow("evolve", (ctx) => ctx.step("wait", () => 2)),

  nondeterminism_missing_command: workflow("renamed", async (ctx) => {
    await ctx.step("a", () => 1);
    await ctx.step("c", () => 2);
    return null;
  }),

  nondeterminism_output_type: workflow("typed", (ctx) => ctx.step("name", () => 1, { decode: integer })),
};
