// The backend, the embedded worker and the client.
//
// The worker runs in Rust (store, queue, leases, timers, retries). It reaches
// workflow code through `Host`: one table of executions per worker, called by
// the engine through threadsafe functions. See `src/host.rs`.

import { RateLimitedError, RunCancelledError, RunFailedError, RunNotFoundError } from "./errors.ts";
import { Execution, type ExecStatus, type StepOutcome, type Workflow } from "./execution.ts";
import {
  native,
  type NativeBackend,
  type NativeClient,
  type NativeHost,
  type NativeReplayer,
  type NativeStepHandle,
  type NativeWorker,
  type WireStepOutcome,
} from "./native.ts";
import type { ReplayOutcome } from "./replay.ts";
import type { Event, Json, Resolution } from "./wire.ts";

/** Where runs, queues and pub/sub live. A worker and a client share one. */
export class Backend {
  /** @internal */
  readonly native: NativeBackend;

  private constructor(n: NativeBackend) {
    this.native = n;
  }

  /** Everything in this process's memory; nothing survives a restart. For tests and local development. */
  static memory(): Backend {
    return new Backend(native.Backend.memory());
  }

  get namespace(): string {
    return this.native.namespace;
  }
}

export interface WorkerOptions {
  /** Runs executing at once on this worker (default 16). */
  concurrency?: number;
  /** How long a claimed run stays leased without renewal (default 30 s). */
  leaseMs?: number;
  /** How often to poll the queue when no nudge arrives (default 1 s). */
  pollIntervalMs?: number;
  reconcileIntervalMs?: number;
  /** Timers and retry backoffs up to this long are waited out in-process instead of suspending (default 5 s). */
  inlineRetryThresholdMs?: number;
  /** Fail a run after this many crashed executions (default 48). */
  maxDeliveries?: number;
  maxPayloadBytes?: number;
  /** How long `shutdown()` waits for in-flight runs (default 30 s). */
  drainTimeoutMs?: number;
  /** Shown as the executing worker in the ops API (default: random). */
  workerId?: string;
}

function toJson(v: unknown): Json {
  return v === undefined ? null : (JSON.parse(JSON.stringify(v)) as Json);
}

function wireOutcome(o: StepOutcome): WireStepOutcome {
  if ("ok" in o) return o;
  const c = o.err.class;
  const cls = c.type === "retry_after" ? { type: c.type, after_ms: Math.max(0, Math.round(c.ms)) } : c;
  return { err: { message: o.err.message, class: cls } };
}

interface Entry {
  exec: Execution;
  /** Step attempts still running in JavaScript. */
  attempts: number;
  disposed: boolean;
}

/** The executions of one worker, by the id the engine gave them. */
class Host implements NativeHost {
  #workflows: Map<string, Workflow<any, any>>;
  #execs = new Map<number, Entry>();

  constructor(workflows: Map<string, Workflow<any, any>>) {
    this.#workflows = workflows;
  }

  start(id: number, workflow: string, _runId: string, input: Json, replayer: NativeReplayer): void {
    const wf = this.#workflows.get(workflow);
    if (!wf) throw new Error(`workflow \`${workflow}\` is not registered`);
    this.#execs.set(id, { exec: new Execution(wf, replayer, input), attempts: 0, disposed: false });
  }

  async activate(id: number, resolved: { handle: number; resolution: Resolution }[]): Promise<ExecStatus> {
    return this.#entry(id).exec.activate(resolved);
  }

  async runStep(id: number, key: string, attempt: number, step: NativeStepHandle): Promise<WireStepOutcome> {
    const e = this.#entry(id);
    e.attempts++;
    try {
      const out = await e.exec.runStep(key, attempt, {
        idempotencyKey: step.idempotencyKey(),
        heartbeat: () => step.heartbeat(),
      });
      return wireOutcome(out);
    } finally {
      e.attempts--;
      this.#collect(id, e);
    }
  }

  abortStep(id: number, key: string, attempt: number): void {
    this.#execs.get(id)?.exec.abortStep(key, attempt);
  }

  dispose(id: number): void {
    const e = this.#execs.get(id);
    if (!e) return;
    e.disposed = true;
    this.#collect(id, e);
  }

  /** Executions alive in this worker; for tests. */
  get size(): number {
    return this.#execs.size;
  }

  #entry(id: number): Entry {
    const e = this.#execs.get(id);
    if (!e) throw new Error(`no execution ${id}`);
    return e;
  }

  // A disposed execution stays until its step attempts finish, so the
  // engine can still abort them.
  #collect(id: number, e: Entry): void {
    if (e.disposed && e.attempts === 0) this.#execs.delete(id);
  }
}

/**
 * Runs workflows: claims runs from the backend's queue and executes them,
 * with workflow code and step bodies running on this thread.
 *
 * ```ts
 * const worker = new Worker(backend, [onboarding]);
 * await worker.start();
 * // ...
 * await worker.shutdown();
 * ```
 */
export class Worker {
  #native: NativeWorker;
  #host: Host;
  #keepAlive: ReturnType<typeof setInterval> | undefined;

  constructor(backend: Backend, workflows: Workflow<any, any>[], options: WorkerOptions = {}) {
    const byName = new Map<string, Workflow<any, any>>();
    for (const w of workflows) {
      if (byName.has(w.name)) throw new Error(`workflow \`${w.name}\` is registered twice`);
      byName.set(w.name, w);
    }
    const host = new Host(byName);
    this.#host = host;
    this.#native = new native.Worker(
      backend.native,
      workflows.map((w) => ({ name: w.name, timeout_ms: w.options.timeoutMs })),
      {
        concurrency: options.concurrency,
        lease_ms: options.leaseMs,
        poll_interval_ms: options.pollIntervalMs,
        reconcile_interval_ms: options.reconcileIntervalMs,
        inline_retry_threshold_ms: options.inlineRetryThresholdMs,
        max_deliveries: options.maxDeliveries,
        max_payload_bytes: options.maxPayloadBytes,
        drain_timeout_ms: options.drainTimeoutMs,
        worker_id: options.workerId,
      },
      (id, workflow, runId, input, replayer) => host.start(id, workflow, runId, input, replayer),
      (id, resolved) => host.activate(id, resolved),
      (id, key, attempt, step) => host.runStep(id, key, attempt, step),
      (id, key, attempt) => host.abortStep(id, key, attempt),
      (id) => host.dispose(id),
    );
  }

  /** Start claiming runs. Resolves once the workflow definitions are synced to the backend. */
  async start(): Promise<void> {
    await this.#native.start();
    // The engine reaches JavaScript through weak threadsafe functions, which
    // do not keep Node alive; a running worker does, like a server.
    this.#keepAlive ??= setInterval(() => {}, 2 ** 30);
  }

  /** Stop claiming, wait for in-flight runs (up to `drainTimeoutMs`), then hand back the rest. */
  async shutdown(): Promise<void> {
    try {
      await this.#native.shutdown();
    } finally {
      this.#release();
    }
  }

  /**
   * Stop at once, as if the process crashed: nothing is acked or handed back,
   * and other workers resume the runs once their leases expire. For tests.
   */
  abort(): void {
    this.#native.abort();
    this.#release();
  }

  /**
   * Replay a stored history through a registered workflow, the way the engine
   * does, with recording on. No store and no step execution. The conformance
   * tests check the binding with it.
   */
  async replay(workflow: Workflow<any, any> | string, runId: string, input: unknown, history: Event[]): Promise<ReplayOutcome> {
    const name = typeof workflow === "string" ? workflow : workflow.name;
    return this.#native.replay(name, runId, toJson(input), history);
  }

  /** @internal Executions this worker still holds. */
  get executions(): number {
    return this.#host.size;
  }

  #release(): void {
    if (this.#keepAlive !== undefined) clearInterval(this.#keepAlive);
    this.#keepAlive = undefined;
  }
}

export interface StartOptions {
  /** Starting twice with the same key (per workflow) yields the same run. */
  idempotencyKey?: string;
}

/** A run, from the outside. */
export class RunHandle<O = Json> {
  readonly id: string;
  /** True when `start` matched an existing run by idempotency key. */
  readonly existing: boolean;
  #client: NativeClient;

  /** @internal */
  constructor(client: NativeClient, id: string, existing: boolean) {
    this.#client = client;
    this.id = id;
    this.existing = existing;
  }

  /**
   * Wait for the run to finish. Resolves with its output; rejects with
   * `RunFailedError`, `RunCancelledError` or `RunNotFoundError`.
   */
  async result(): Promise<O> {
    const r = await this.#client.result(this.id);
    if ("ok" in r) return r.ok as O;
    switch (r.err.kind) {
      case "failed":
        throw new RunFailedError(this.id, r.err.message);
      case "cancelled":
        throw new RunCancelledError(this.id);
      case "not_found":
        throw new RunNotFoundError(this.id);
    }
  }
}

/** Starts runs and talks to them. Works without a worker in this process. */
export class Client {
  #native: NativeClient;

  constructor(backend: Backend) {
    this.#native = new native.Client(backend.native);
  }

  /** Start a run. Rejects with `RateLimitedError` when the workflow's rate limit is exceeded. */
  async start<I, O>(workflow: Workflow<I, O>, input: I, opts?: StartOptions): Promise<RunHandle<O>>;
  async start(workflow: string, input: unknown, opts?: StartOptions): Promise<RunHandle<Json>>;
  async start(workflow: Workflow<any, any> | string, input: unknown, opts: StartOptions = {}): Promise<RunHandle<unknown>> {
    const name = typeof workflow === "string" ? workflow : workflow.name;
    const out = await this.#native.start(name, toJson(input), opts.idempotencyKey ?? null);
    if (out.outcome === "rate_limited") throw new RateLimitedError(name, new Date(out.retry_at));
    return new RunHandle(this.#native, out.run_id, out.outcome === "existing");
  }

  /** A handle to an existing run. */
  handle<O = Json>(runId: string): RunHandle<O> {
    return new RunHandle<O>(this.#native, runId, true);
  }

  /** The run as stored (status, output, error, ...), or `null`. */
  async getRun(runId: string): Promise<Json> {
    return this.#native.getRun(runId);
  }

  /** The run's history, as stored. */
  async events(runId: string): Promise<Event[]> {
    return this.#native.events(runId);
  }

  /** Send signal `name` to a run; a `waitForSignal(name)` receives `payload`. */
  async signal(runId: string, name: string, payload: unknown = null): Promise<void> {
    await this.#native.signal(runId, name, toJson(payload));
  }

  /** Publish event `name` to every run waiting for it. Returns how many runs it reached. */
  async sendEvent(name: string, data: unknown = null): Promise<number> {
    return this.#native.sendEvent(name, toJson(data));
  }

  /** Deliver `payload` to the hook holding `token`. */
  async resumeHook(token: string, payload: unknown = null): Promise<void> {
    await this.#native.resumeHook(token, toJson(payload));
  }

  async cancel(runId: string, reason?: string): Promise<void> {
    await this.#native.cancel(runId, reason ?? null);
  }
}
