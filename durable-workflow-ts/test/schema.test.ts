// Schemas on ctx calls: `output` on steps, `payload` on waits and hooks.

import { describe, expect, expectTypeOf, test } from "vitest";
import { z } from "zod";

import { Execution, replay, workflow, type Ctx, type Hook, type StandardSchemaV1 } from "../ts/index.ts";
import { native } from "../ts/native.ts";
import type { Event, Json } from "../ts/wire.ts";

const runId = "01a1bb5e-f50d-757f-ab41-c75d52238109";
const at = "2030-01-01T00:00:00Z";
const ev = (seq: number, kind: Event["kind"]): Event => ({ run_id: runId, seq, at, kind });
const started = ev(1, { type: "run_started", input: null });
const info = { idempotencyKey: `${runId}:k` };

async function start(body: (ctx: Ctx) => PromiseLike<unknown>): Promise<Execution> {
  const r = new native.Replayer(runId, [started]);
  const exec = new Execution(workflow("t", body), r, null);
  r.advance();
  await exec.activate(r.takeResolved());
  return exec;
}

/** Replay a body over one recorded step output. */
const replayStep = (body: (ctx: Ctx) => PromiseLike<unknown>, output: Json) =>
  replay(workflow("t", body), runId, null, [started, ev(2, { type: "step_succeeded", key: "k", output })]);

describe("step output", () => {
  test("a recorded output goes through the schema", async () => {
    const out = await replayStep(
      (ctx) => ctx.step("k", { output: z.string().transform((s) => s.length), execute: () => "abc" }),
      "abcd",
    );
    expect(out.result).toStrictEqual({ ok: 4 });
  });

  test("a recorded output that no longer fits faults the run", async () => {
    const out = await replayStep((ctx) => ctx.step("k", { output: z.number(), execute: () => 1 }), "ann");
    expect(out.fault).toMatch(/^non-determinism: `k` no longer deserializes into the requested type: /);
    expect(out.result).toMatchObject({ err: { kind: "fault" } });
  });

  test("a live result that does not fit fails the attempt for good", async () => {
    const exec = await start((ctx) =>
      ctx.step("k", { output: z.object({ n: z.number() }), execute: () => ({ n: "one" }) as unknown as { n: number } }),
    );
    expect(await exec.runStep("k", 1, info)).toMatchObject({
      err: { message: expect.stringMatching(/^step output does not match its schema: n: /), class: { type: "fatal" } },
    });
  });

  test("a live attempt records what execute returned, not what the schema made of it", async () => {
    const exec = await start((ctx) => ctx.step("k", { output: z.string().transform((s) => s.length), execute: () => "abc" }));
    expect(await exec.runStep("k", 1, info)).toStrictEqual({ ok: "abc" });
  });

  test("an asynchronous schema is a fault, not non-determinism", async () => {
    const slow: StandardSchemaV1<number> = {
      "~standard": { version: 1, vendor: "test", validate: async (value) => ({ value: value as number }) },
    };
    const out = await replayStep((ctx) => ctx.step("k", { output: slow, execute: () => 1 }), 1);
    expect(out.fault).toMatch(/validates asynchronously/);
  });

  test("map items take the same options", async () => {
    const out = await replay(
      workflow("t", (ctx) => ctx.map("sq", [1, 2], { output: z.number(), execute: (x) => x * x })),
      runId,
      null,
      [started, ev(2, { type: "step_succeeded", key: "sq[0]", output: 1 }), ev(3, { type: "step_succeeded", key: "sq[1]", output: "4" })],
    );
    expect(out.fault).toMatch(/`sq\[1\]` no longer deserializes/);
  });
});

describe("wait payload", () => {
  test("a signal payload goes through the schema; a timeout is still null", async () => {
    const body = (ctx: Ctx) => ctx.waitForSignal("s", { payload: z.object({ by: z.string() }) }).timeout(1_000);
    const waiting = ev(2, { type: "wait_started", key: "s", kind: "signal", name: "s", filter: null, timeout_at: "2030-01-01T00:00:01Z" });
    const signalled = await replay(workflow("t", body), runId, null, [
      started,
      waiting,
      ev(3, { type: "signal_received", key: "s", signal_id: "01a1bb5e-f488-76a4-9e9f-87663a837e20", payload: { by: "ann", extra: 1 } }),
    ]);
    expect(signalled.result).toStrictEqual({ ok: { by: "ann" } });
    const timedOut = await replay(workflow("t", body), runId, null, [started, waiting, ev(3, { type: "wait_timed_out", key: "s" })]);
    expect(timedOut.result).toStrictEqual({ ok: null });
  });
});

test("types follow the schema", () => {
  // Compile-time only: tsc checks these when it checks the tests.
  const types = (ctx: Ctx) => {
    expectTypeOf(ctx.step("a", () => 1)).resolves.toEqualTypeOf<number>();
    expectTypeOf(ctx.step("b", { execute: () => "x" })).resolves.toEqualTypeOf<string>();
    expectTypeOf(ctx.step("c", { output: z.string().transform(Number), execute: () => "1" })).resolves.toEqualTypeOf<number>();
    // @ts-expect-error execute must return what `output` accepts
    ctx.step("d", { output: z.number(), execute: () => "x" });
    expectTypeOf(ctx.waitForSignal("e")).resolves.toEqualTypeOf<Json>();
    expectTypeOf(ctx.waitForSignal("f", { payload: z.number() })).resolves.toEqualTypeOf<number>();
    expectTypeOf(ctx.waitForSignal("g", { payload: z.number() }).timeout(1)).resolves.toEqualTypeOf<number | null>();
    expectTypeOf(ctx.createHook("h", { payload: z.boolean() })).resolves.toEqualTypeOf<Hook<boolean>>();
    expectTypeOf(ctx.map("i", [1], { output: z.number(), execute: (x) => x })).resolves.toEqualTypeOf<number[]>();
  };
  expect(types).toBeTypeOf("function");
});
