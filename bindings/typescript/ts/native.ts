// Loads the Node-API addon (`durable.node`, built by `npm run build:native`).
// Set DURABLE_NATIVE to load it from elsewhere.

import { createRequire } from "node:module";
import { fileURLToPath } from "node:url";

import type {
  Abort,
  Advance,
  Command,
  Event,
  Finish,
  Json,
  Pending,
  Resolution,
  TraceOp,
  WireRetryPolicy,
} from "./wire.ts";

/** One execution's replay state machine. Every method is one synchronous call into Rust. */
export interface NativeReplayer {
  runId(): string;
  record(): void;
  takeTrace(): TraceOp[];

  userKey(name: string): string;
  checkName(name: string): void;
  keyFor(name: string): string;
  issue(key: string, command: Command): number;
  isIssued(key: string): boolean;
  keyOf(handle: number): string | null;
  release(handle: number): void;
  resolution(handle: number): Resolution | null;
  abort(): Abort | null;
  now(): string;
  randomU64(): string;
  uuid(): string;
  isReplaying(): boolean;
  setFault(message: string): void;

  advance(): Advance;
  takeResolved(): { handle: number; resolution: Resolution }[];
  pushLive(events: Event[]): void;
  frontier(): Pending[];
  fault(): string | null;
  interrupt(): void;
  cancel(): void;
}

/** What a running step attempt can ask the engine. */
export interface NativeStepHandle {
  idempotencyKey(): string;
  heartbeat(): void;
  isCancelled(): boolean;
}

export interface NativeBackend {
  readonly namespace: string;
}

/** The JavaScript side of a worker: the engine calls these through threadsafe functions. */
export interface NativeHost {
  start(id: number, workflow: string, runId: string, input: Json, replayer: NativeReplayer): void;
  activate(id: number, resolved: { handle: number; resolution: Resolution }[]): Promise<WireExecStatus>;
  runStep(id: number, key: string, attempt: number, step: NativeStepHandle): Promise<WireStepOutcome>;
  abortStep(id: number, key: string, attempt: number): void;
  dispose(id: number): void;
}

export type WireExecStatus = { status: "pending" } | { status: "finished"; result: Finish };

export type WireErrorClass = { type: "retryable" } | { type: "fatal" } | { type: "retry_after"; after_ms: number };

export type WireStepOutcome = { ok: Json } | { err: { message: string; class: WireErrorClass } };

export interface WireWorkflow {
  name: string;
  timeout_ms?: number | undefined;
}

export interface WireWorkerOptions {
  concurrency?: number | undefined;
  lease_ms?: number | undefined;
  poll_interval_ms?: number | undefined;
  reconcile_interval_ms?: number | undefined;
  inline_retry_threshold_ms?: number | undefined;
  max_deliveries?: number | undefined;
  max_payload_bytes?: number | undefined;
  drain_timeout_ms?: number | undefined;
  worker_id?: string | undefined;
}

export interface NativeWorker {
  start(): Promise<void>;
  shutdown(): Promise<void>;
  abort(): void;
  replay(
    workflow: string,
    runId: string,
    input: Json,
    history: Event[],
  ): Promise<{ trace: TraceOp[]; frontier: Pending[]; fault: string | null; result: Finish | null }>;
}

export type WireStartOutcome =
  | { outcome: "started" | "existing"; run_id: string }
  | { outcome: "rate_limited"; retry_at: string };

export type WireRunResult = { ok: Json } | { err: { kind: "failed" | "cancelled" | "not_found"; message: string } };

export interface NativeClient {
  start(workflow: string, input: Json, idempotencyKey: string | null): Promise<WireStartOutcome>;
  result(runId: string): Promise<WireRunResult>;
  getRun(runId: string): Promise<Json>;
  events(runId: string): Promise<Event[]>;
  signal(runId: string, name: string, payload: Json): Promise<void>;
  sendEvent(name: string, data: Json): Promise<number>;
  resumeHook(token: string, payload: Json): Promise<void>;
  cancel(runId: string, reason: string | null): Promise<void>;
}

interface NativeModule {
  Replayer: new (runId: string, history: Event[]) => NativeReplayer;
  defaultRetryPolicy(): WireRetryPolicy;
  Backend: { memory(): NativeBackend };
  Worker: new (
    backend: NativeBackend,
    workflows: WireWorkflow[],
    options: WireWorkerOptions | null,
    start: NativeHost["start"],
    activate: NativeHost["activate"],
    runStep: NativeHost["runStep"],
    abortStep: NativeHost["abortStep"],
    dispose: NativeHost["dispose"],
  ) => NativeWorker;
  Client: new (backend: NativeBackend) => NativeClient;
}

const path = process.env.DURABLE_NATIVE ?? fileURLToPath(new URL("../durable.node", import.meta.url));

export const native: NativeModule = createRequire(import.meta.url)(path);

export type { Json };
