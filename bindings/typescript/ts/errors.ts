// What workflow code sees from ctx calls, what step bodies throw, and how a
// thrown value maps onto the SDK-neutral `WorkflowFailure`.

import type { Abort, FailureKind, WorkflowFailure } from "./wire.ts";

/** How the engine retries a failed attempt. */
export type ErrorClass = { type: "retryable" } | { type: "fatal" } | { type: "retry_after"; ms: number };

/**
 * Throw from a step body to choose how the failure is retried. Anything else a
 * body throws is retryable.
 */
export class StepError extends Error {
  readonly class: ErrorClass;

  constructor(message: string, cls: ErrorClass = { type: "retryable" }) {
    super(message);
    this.name = "StepError";
    this.class = cls;
  }

  static retryable(message: string): StepError {
    return new StepError(message);
  }

  static fatal(message: string): StepError {
    return new StepError(message, { type: "fatal" });
  }

  /** Retry no sooner than `ms` from now, whatever the policy's backoff. */
  static retryAfter(ms: number, message: string): StepError {
    return new StepError(message, { type: "retry_after", ms });
  }
}

/** Base of every error a ctx call rejects with. */
export abstract class WorkflowError extends Error {
  abstract readonly kind: FailureKind;
}

/** A step failed for good: retries exhausted, or a fatal error. */
export class StepFailedError extends WorkflowError {
  readonly kind = "step";
  readonly key: string;
  readonly retryable: boolean;

  constructor(key: string, message: string, retryable: boolean) {
    super(`step failed: ${message}`);
    this.name = "StepFailedError";
    this.key = key;
    this.retryable = retryable;
  }
}

/** A hook token is held by another run. */
export class HookConflictError extends WorkflowError {
  readonly kind = "hook_conflict";
  readonly token: string;

  constructor(token: string) {
    super(`hook token \`${token}\` is held by another run`);
    this.name = "HookConflictError";
    this.token = token;
  }
}

export class CancelledError extends WorkflowError {
  readonly kind = "cancelled";

  constructor() {
    super("run cancelled");
    this.name = "CancelledError";
  }
}

/** The engine is done with this execution. Let it propagate. */
export class InterruptedError extends WorkflowError {
  readonly kind = "interrupted";

  constructor() {
    super("workflow interrupted by the engine");
    this.name = "InterruptedError";
  }
}

/** Non-determinism, an invalid name, ...: the run stays open until the code is fixed. */
export class FaultError extends WorkflowError {
  readonly kind = "fault";

  constructor(message: string) {
    super(`engine fault: ${message}`);
    this.name = "FaultError";
  }
}

/** The code no longer matches the run's history. */
export class NonDeterminismError extends WorkflowError {
  readonly kind = "fault";

  constructor(message: string) {
    super(`non-determinism: ${message}`);
    this.name = "NonDeterminismError";
  }
}

export function abortError(abort: Abort): WorkflowError {
  switch (abort.type) {
    case "interrupted":
      return new InterruptedError();
    case "cancelled":
      return new CancelledError();
    case "fault":
      return new FaultError(abort.message);
  }
}

/** How a body that threw `e` failed. */
export function failureOf(e: unknown): WorkflowFailure {
  if (e instanceof WorkflowError) return { kind: e.kind, message: e.message };
  return { kind: "failed", message: e instanceof Error ? e.message : String(e) };
}

/** What `RunHandle.result()` rejects with. */
export abstract class RunError extends Error {
  readonly runId: string;

  constructor(runId: string, message: string) {
    super(message);
    this.runId = runId;
  }
}

/** The run failed: its body threw, a step failed for good, or it timed out. */
export class RunFailedError extends RunError {
  constructor(runId: string, message: string) {
    super(runId, `run failed: ${message}`);
    this.name = "RunFailedError";
  }
}

export class RunCancelledError extends RunError {
  constructor(runId: string) {
    super(runId, "run was cancelled");
    this.name = "RunCancelledError";
  }
}

export class RunNotFoundError extends RunError {
  constructor(runId: string) {
    super(runId, `run ${runId} not found`);
    this.name = "RunNotFoundError";
  }
}

/** The workflow's start-time rate limit is exceeded; try again at `retryAt`. */
export class RateLimitedError extends Error {
  readonly retryAt: Date;

  constructor(workflow: string, retryAt: Date) {
    super(`\`${workflow}\` is rate limited until ${retryAt.toISOString()}`);
    this.name = "RateLimitedError";
    this.retryAt = retryAt;
  }
}
