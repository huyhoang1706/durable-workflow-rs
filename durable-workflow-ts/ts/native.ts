// Loads the Node-API addon. In order:
//
// 1. DURABLE_NATIVE, a path to a `.node` file;
// 2. `durable.<platform>.node` next to `ts/` and `dist/`, which `npm run
//    build:native` writes (a checkout of the repository);
// 3. the prebuilt `@durable/typescript-<platform>` package, an optional
//    dependency npm installs only on its platform.

import { existsSync } from "node:fs";
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

export interface WireConnectOptions {
  redis_url?: string | undefined;
  namespace?: string | undefined;
  schema?: string | undefined;
  migrate?: boolean | undefined;
  stream_ttl_ms?: number | undefined;
  connect_timeout_ms?: number | undefined;
}

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
  Backend: {
    memory(): NativeBackend;
    connect(url: string, options: WireConnectOptions | null): Promise<NativeBackend>;
  };
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

/** The platform part of the addon's name, as `@napi-rs/cli` spells it: `linux-x64-gnu`, `darwin-arm64`, ... */
export function platformTriple(): string {
  const { platform, arch } = process;
  switch (platform) {
    case "linux": {
      const report = process.report?.getReport() as { header?: { glibcVersionRuntime?: string } } | undefined;
      return `linux-${arch}-${report?.header?.glibcVersionRuntime ? "gnu" : "musl"}`;
    }
    case "win32":
      return `win32-${arch}-msvc`;
    default:
      return `${platform}-${arch}`;
  }
}

function load(): NativeModule {
  const require = createRequire(import.meta.url);
  const explicit = process.env.DURABLE_NATIVE;
  if (explicit) return require(explicit);

  const triple = platformTriple();
  const local = fileURLToPath(new URL(`../durable.${triple}.node`, import.meta.url));
  if (existsSync(local)) return require(local);

  const pkg = `@durable/typescript-${triple}`;
  try {
    return require(pkg);
  } catch (e) {
    const code = (e as { code?: string }).code;
    if (code !== "MODULE_NOT_FOUND" && code !== "ERR_MODULE_NOT_FOUND") throw e;
    throw new Error(
      `@durable/typescript has no native addon for ${triple}: \`${pkg}\` is not installed. ` +
        `Prebuilt addons exist for linux-x64-gnu, linux-arm64-gnu, darwin-x64, darwin-arm64 and win32-x64-msvc; ` +
        `elsewhere, build one with \`npm run build:native\` in a checkout, or point DURABLE_NATIVE at it.`,
      { cause: e },
    );
  }
}

export const native: NativeModule = load();

export type { Json };
