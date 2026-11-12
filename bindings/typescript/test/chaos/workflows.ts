// The chaos workload, shared by the worker processes (`worker.ts`) and the
// test, which replays finished histories through the same definitions.

import { retry, workflow, type StepContext, type Workflow } from "../../ts/index.ts";

/** Called at the start of every step attempt. */
export type Guard = (step: StepContext) => Promise<void>;

const pause = (ms: number) => new Promise((r) => setTimeout(r, ms));

/**
 * - `chaos.seq`: three steps in a row, a timer long enough to suspend, a signal.
 * - `chaos.map`: a fan-out of three steps whose first attempts fail, a timer, a signal.
 */
export function chaosWorkflows(guard: Guard): Workflow<number, number>[] {
  const seq = workflow("chaos.seq", async (ctx, n: number) => {
    let total = 0;
    for (let i = 0; i < 3; i++) {
      total += await ctx.step(`s${i}`, async (s) => {
        await guard(s);
        await pause(150);
        return n + i;
      });
    }
    await ctx.sleep("nap", 600);
    const got = await ctx.waitForSignal("go");
    return total + Number(got);
  });

  const map = workflow("chaos.map", async (ctx, n: number) => {
    const parts = await ctx.map("work", [n, n + 1, n + 2], {
      retry: retry.fixed(5, 20),
      execute: async (x, s) => {
        await guard(s);
        await pause(100);
        if (s.attempt === 1) throw new Error("first attempts fail");
        return x;
      },
    });
    await ctx.sleep("nap", 600);
    const got = await ctx.waitForSignal("go");
    return parts.reduce((a, b) => a + b, 0) + Number(got);
  });

  return [seq, map];
}

/** What a run of either workflow with input `n` returns once signalled `go`. */
export function expected(n: number, go: number): number {
  return 3 * n + 3 + go;
}
