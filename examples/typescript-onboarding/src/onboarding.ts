// The workflow: create the account, send a welcome mail, then wait (for up to
// a day) for the user to verify their address; remind them if they do not.
//
// Every side effect is a step: on a crash or a redeploy the run resumes by
// replaying its history, and finished steps are not run again. The wait
// suspends the run: no worker holds it while it waits.

import { retry, workflow, type Workflow } from "@durable/typescript";
import { z } from "zod";

/** What the workflow calls out to: your own clients, passed in. */
export interface Services {
  accounts: {
    /** Returns the new account's id. Must be idempotent on `idempotencyKey`. */
    create(email: string, idempotencyKey: string): Promise<string>;
  };
  mailer: {
    /** Pass `idempotencyKey` to the provider, so a retried send is not delivered twice. */
    send(to: string, subject: string, idempotencyKey: string): Promise<void>;
  };
}

export const Signup = z.object({ email: z.string().email() });
export type Signup = z.infer<typeof Signup>;

export type Outcome = { accountId: string; status: "verified" | "reminded" };

export interface OnboardingOptions {
  /** How long to wait for the verification signal (default one day). */
  verifyWithinMs?: number;
}

export const ONBOARDING = "user.onboarding";
const DAY_MS = 24 * 60 * 60 * 1000;

export function onboarding(services: Services, opts: OnboardingOptions = {}): Workflow<Signup, Outcome> {
  const verifyWithinMs = opts.verifyWithinMs ?? DAY_MS;

  return workflow(ONBOARDING, { timeoutMs: 7 * DAY_MS }, async (ctx, input: Signup) => {
    // Inputs cross JSON: check them once, at the top.
    const parsed = Signup.safeParse(input);
    if (!parsed.success) throw new Error(`invalid signup: ${parsed.error.issues[0]?.message ?? "bad input"}`);
    const { email } = parsed.data;

    const accountId = await ctx.step("create-account", {
      output: z.string(),
      execute: (s) => services.accounts.create(email, s.idempotencyKey),
    });

    await ctx.step("send-welcome", {
      retry: retry.exponential(5),
      timeoutMs: 30_000,
      execute: (s) => services.mailer.send(email, "Welcome!", s.idempotencyKey),
    });

    // `null` once the deadline passes; otherwise the signal's payload.
    const verified = await ctx.waitForSignal("verified", { payload: z.boolean() }).timeout(verifyWithinMs);

    if (verified !== true) {
      await ctx.step("send-reminder", {
        retry: retry.exponential(5),
        timeoutMs: 30_000,
        execute: (s) => services.mailer.send(email, "Please verify your address", s.idempotencyKey),
      });
      return { accountId, status: "reminded" };
    }
    return { accountId, status: "verified" };
  });
}
