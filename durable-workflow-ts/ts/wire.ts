// The JSON the native addon speaks: the replay corpus's wire shape.
// Internally tagged enums, `*_ms` durations, RFC 3339 timestamps, snake_case.

export type Json = null | boolean | number | string | Json[] | { [k: string]: Json };

export interface WireRetryPolicy {
  max_attempts: number;
  initial_delay_ms: number;
  factor: number;
  max_delay_ms: number;
}

export type WaitKind = "signal" | "event" | "hook";

export interface Event {
  run_id: string;
  seq: number;
  at: string;
  kind: { type: string; key?: string; [k: string]: unknown };
}

export type Command =
  | { type: "step"; policy: WireRetryPolicy; timeout_ms: number | null }
  | { type: "sleep"; dur_ms: number }
  | { type: "wait"; kind: WaitKind; name: string; filter: Json | null; timeout_ms: number | null }
  | { type: "hook"; token: string | null }
  | { type: "hook_dispose"; token: string };

export type Resolution =
  | { type: "step_succeeded"; value: Json }
  | { type: "step_failed"; failure: { message: string; retryable: boolean } }
  | { type: "slept" }
  | { type: "signal"; payload: Json | null }
  | { type: "hook"; token: string; conflict: boolean }
  | { type: "disposed" };

export type Abort = { type: "interrupted" } | { type: "cancelled" } | { type: "fault"; message: string };

export type Advance = "run_code" | "live";

/** An unresolved command, as the engine sees it. Opaque to the SDK. */
export type Pending = { type: string; key: string; [k: string]: unknown };

export interface Resolved {
  key: string;
  resolution: Resolution;
}

export type TraceOp =
  | { op: "user_key"; name: string; key: string }
  | { op: "check_name"; name: string }
  | { op: "key_for"; name: string; key: string }
  | { op: "issue"; key: string; command: Command }
  | { op: "release"; key: string }
  | { op: "random"; value: string }
  | { op: "uuid"; value: string }
  | { op: "set_fault"; message: string }
  | { op: "interrupt" }
  | { op: "cancel" }
  | { op: "advance"; result: Advance; resolved?: Resolved[] };

export type FailureKind = "failed" | "step" | "hook_conflict" | "cancelled" | "interrupted" | "fault";

export interface WorkflowFailure {
  kind: FailureKind;
  message: string;
}

/** How a body ended. */
export type Finish = { ok: Json } | { err: WorkflowFailure };
