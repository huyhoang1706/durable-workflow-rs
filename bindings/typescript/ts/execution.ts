// The host loop for one execution, and the ctx calls workflow code makes.
//
// The replayer owns every decision: keys, matching history, faults. This file
// only connects it to JavaScript promises. A ctx call reserves its key at once
// and issues its command when first awaited (a lazy thenable), the way Rust
// builders issue on first poll, so both SDKs make the same replayer calls.

import {
  HookConflictError,
  NonDeterminismError,
  StepError,
  StepFailedError,
  abortError,
  failureOf,
  FaultError,
} from "./errors.ts";
import { native, type NativeReplayer } from "./native.ts";
import { validate, type StandardSchemaV1 } from "./schema.ts";
import type { Command, Finish, Json, Resolution, WaitKind, WireRetryPolicy } from "./wire.ts";

export interface RetryPolicy {
  /** Total attempts, including the first. */
  maxAttempts: number;
  initialDelayMs: number;
  factor: number;
  maxDelayMs: number;
}

export const retry = {
  /** One attempt. */
  none: (): RetryPolicy => retry.fixed(1, 0),
  fixed: (maxAttempts: number, delayMs: number): RetryPolicy => ({
    maxAttempts,
    initialDelayMs: delayMs,
    factor: 1,
    maxDelayMs: delayMs,
  }),
  exponential: (maxAttempts: number): RetryPolicy => ({
    maxAttempts,
    initialDelayMs: 1_000,
    factor: 2,
    maxDelayMs: 300_000,
  }),
};

function wirePolicy(p: Partial<RetryPolicy>): WireRetryPolicy {
  const d = native.defaultRetryPolicy();
  return {
    max_attempts: p.maxAttempts ?? d.max_attempts,
    initial_delay_ms: p.initialDelayMs ?? d.initial_delay_ms,
    factor: p.factor ?? d.factor,
    max_delay_ms: p.maxDelayMs ?? d.max_delay_ms,
  };
}

/** What a step body gets. */
export interface StepContext {
  /** The step's key in the run's history (`name`, `name#1`, `name[0]`, ...). */
  readonly key: string;
  /** 1-based. */
  readonly attempt: number;
  /** `run_id:key`, stable across retries and replays. */
  readonly idempotencyKey: string;
  /** Aborted when the engine gives up on this attempt (timeout, cancel, shutdown). */
  readonly signal: AbortSignal;
  /** Tell the engine a long attempt is alive. */
  heartbeat(): void;
}

export type StepBody<T> = (step: StepContext) => T | Promise<T>;

/** A schema for a value that crosses history: any Standard Schema (zod, valibot, arktype, ...). */
export type Schema<Output = unknown, Input = unknown> = StandardSchemaV1<Input, Output>;

/** A step, as an options object. `output` gives its result a type that is checked at runtime. */
export interface StepOptions<T> {
  execute: StepBody<T>;
  retry?: Partial<RetryPolicy>;
  /** Per-attempt limit. */
  timeoutMs?: number;
}

/** As `StepOptions`, for each item of `ctx.map`. */
export interface MapOptions<I, T> {
  execute: (item: I, step: StepContext) => T | Promise<T>;
  retry?: Partial<RetryPolicy>;
  timeoutMs?: number;
}

/** Options of a wait for a signal or an event. */
export interface WaitOptions<S extends Schema | undefined> {
  /** Checks the payload, and types it. */
  payload?: S;
}

/** Options of `ctx.createHook`. */
export interface HookOptions<S extends Schema | undefined> extends WaitOptions<S> {
  /** The token webhooks are sent to. Without one, the engine mints one. */
  token?: string;
}

/** The type a payload schema yields, or `Json` without one. */
type Payload<S> = S extends Schema ? StandardSchemaV1.InferOutput<S> : Json;

/** What the engine passes to `runStep`. */
export interface StepInfo {
  idempotencyKey: string;
  heartbeat?: () => void;
}

/** A step attempt's outcome, as the engine wants it. */
export type StepOutcome = { ok: Json } | { err: { message: string; class: StepError["class"] } };

export type ExecStatus = { status: "pending" } | { status: "finished"; result: Finish };

export type WorkflowBody<I, O> = (ctx: Ctx, input: I) => PromiseLike<O>;

export interface WorkflowOptions {
  /** Fail a run that has not finished this long after it started. */
  timeoutMs?: number;
}

export interface Workflow<I = unknown, O = unknown> {
  readonly name: string;
  readonly options: WorkflowOptions;
  readonly body: WorkflowBody<I, O>;
}

/** Define a workflow. Register it with a worker (S2) or replay it (`replay`). */
export function workflow<I, O>(name: string, body: WorkflowBody<I, O>): Workflow<I, O>;
export function workflow<I, O>(name: string, options: WorkflowOptions, body: WorkflowBody<I, O>): Workflow<I, O>;
export function workflow<I, O>(
  name: string,
  a: WorkflowOptions | WorkflowBody<I, O>,
  b?: WorkflowBody<I, O>,
): Workflow<I, O> {
  return typeof a === "function" ? { name, options: {}, body: a } : { name, options: a, body: b! };
}

interface Waiter {
  resolve(r: Resolution): void;
  reject(e: unknown): void;
}

/**
 * A ctx call. Await it (or hand it to `Promise.all` / `ctx.race`); the first
 * `then` issues its command. Configure it before that.
 */
export class DurableCall<T> implements PromiseLike<T> {
  #exec: Execution;
  #key: string;
  #command: (() => Command) | undefined;
  #map: (r: Resolution) => T;
  #promise: Promise<T> | undefined;
  #handle: number | undefined;
  #settled = false;

  /** @internal */
  constructor(exec: Execution, key: string, command: (() => Command) | undefined, map: (r: Resolution) => T) {
    this.#exec = exec;
    this.#key = key;
    this.#command = command;
    this.#map = map;
  }

  /** The key this call is recorded under in the run's history. */
  get key(): string {
    return this.#key;
  }

  /** The command to issue. Subclasses with builder options override it. */
  protected command(): Command {
    return this.#command!();
  }

  protected get issued(): boolean {
    return this.#promise !== undefined;
  }

  protected assertConfigurable(): void {
    if (this.issued) throw new Error(`\`${this.#key}\` cannot be configured after it has been awaited`);
  }

  then<R1 = T, R2 = never>(
    onFulfilled?: ((value: T) => R1 | PromiseLike<R1>) | null,
    onRejected?: ((reason: unknown) => R2 | PromiseLike<R2>) | null,
  ): Promise<R1 | R2> {
    this.#promise ??= this.#issue();
    return this.#promise.then(onFulfilled, onRejected);
  }

  catch<R = never>(onRejected?: ((reason: unknown) => R | PromiseLike<R>) | null): Promise<T | R> {
    return this.then(undefined, onRejected);
  }

  finally(onFinally?: (() => void) | null): Promise<T> {
    this.#promise ??= this.#issue();
    return this.#promise.finally(onFinally);
  }

  /** Stop awaiting this call: it leaves the frontier. `ctx.race` does this for the losers. */
  release(): void {
    if (this.#handle !== undefined && !this.#settled) {
      this.#settled = true;
      this.#exec.release(this.#handle);
    }
  }

  #issue(): Promise<T> {
    return new Promise<T>((resolve, reject) => {
      const issued = this.#exec.issue(this.#key, this.command(), {
        resolve: (r) => {
          this.#settled = true;
          try {
            resolve(this.#map(r));
          } catch (e) {
            reject(e);
          }
        },
        reject: (e) => {
          this.#settled = true;
          reject(e);
        },
      });
      if (issued !== undefined) this.#handle = issued;
    });
  }
}

export class StepCall<T> extends DurableCall<T> {
  #policy: Partial<RetryPolicy> = {};
  #timeoutMs: number | null = null;

  /** @internal */
  constructor(exec: Execution, key: string, map: (r: Resolution) => T) {
    super(exec, key, undefined, map);
  }

  protected override command(): Command {
    return { type: "step", policy: wirePolicy(this.#policy), timeout_ms: this.#timeoutMs };
  }

  /** Fields left out keep their defaults. */
  retry(policy: Partial<RetryPolicy>): this {
    this.assertConfigurable();
    this.#policy = policy;
    return this;
  }

  /** Per-attempt limit. */
  timeout(ms: number): this {
    this.assertConfigurable();
    this.#timeoutMs = ms;
    return this;
  }
}

export class WaitCall<T> extends DurableCall<T> {
  #kind: WaitKind;
  #name: string;
  #filter: Json | null = null;
  #timeoutMs: number | null = null;

  /** @internal */
  constructor(exec: Execution, key: string, kind: WaitKind, name: string, map: (r: Resolution) => T) {
    super(exec, key, undefined, map);
    this.#kind = kind;
    this.#name = name;
  }

  protected override command(): Command {
    return { type: "wait", kind: this.#kind, name: this.#name, filter: this.#filter, timeout_ms: this.#timeoutMs };
  }

  /** JSON containment filter applied to published events. */
  filter(filter: Json): this {
    this.assertConfigurable();
    this.#filter = filter;
    return this;
  }

  /** Give up after `ms`; the wait then yields `null`. */
  timeout(ms: number): WaitCall<T | null> {
    this.assertConfigurable();
    this.#timeoutMs = ms;
    return this as unknown as WaitCall<T | null>;
  }
}

/** A webhook-style endpoint. `recv` may be called repeatedly. */
export class Hook<T> {
  #exec: Execution;
  #key: string;
  readonly token: string;
  #schema: Schema<T> | undefined;

  /** @internal */
  constructor(exec: Execution, key: string, token: string, schema: Schema<T> | undefined) {
    this.#exec = exec;
    this.#key = key;
    this.token = token;
    this.#schema = schema;
  }

  recv(): WaitCall<T> {
    const key = this.#exec.replayer.keyFor(`${this.#key}[recv]`);
    return new WaitCall(this.#exec, key, "hook", this.token, (r) => this.#exec.payload(key, r, this.#schema));
  }

  /** Release the token: later webhooks to it are rejected. The run keeps going. */
  dispose(): DurableCall<void> {
    const exec = this.#exec;
    const key = exec.replayer.keyFor(`${this.#key}[dispose]`);
    const token = this.token;
    return new DurableCall(exec, key, () => ({ type: "hook_dispose", token }), (r) => {
      if (r.type !== "disposed") throw exec.unexpected(key, r);
    });
  }
}

/** What `ctx.select` resolves to: the winning call's name and value. */
export type Selected<M extends Record<string, DurableCall<unknown>>> = {
  [K in keyof M]: { key: K; value: Awaited<M[K]> };
}[keyof M];

/** What workflow code calls. Every method is deterministic. */
export class Ctx {
  #exec: Execution;

  /** @internal */
  constructor(exec: Execution) {
    this.#exec = exec;
  }

  /**
   * A step: `execute` runs until an attempt succeeds, and its result is memoized by `name`.
   *
   * ```ts
   * const n = await ctx.step("charge", () => charge(order));
   * const m = await ctx.step("count", { output: z.number(), execute: () => count(), retry: { maxAttempts: 5 } });
   * ```
   *
   * With `output`, the result is checked against the schema: a live attempt whose result does not
   * fit fails for good, and a recorded result that no longer fits (the code changed) faults the run
   * with non-determinism, as serde does in the Rust SDK.
   */
  step<T>(name: string, execute: StepBody<T>): StepCall<T>;
  step<S extends Schema>(
    name: string,
    opts: StepOptions<StandardSchemaV1.InferInput<S>> & { output: S },
  ): StepCall<StandardSchemaV1.InferOutput<S>>;
  step<T>(name: string, opts: StepOptions<T> & { output?: undefined }): StepCall<T>;
  step(name: string, opts: StepBody<unknown> | (StepOptions<unknown> & { output?: Schema | undefined })): StepCall<unknown> {
    return this.#exec.step(this.#exec.replayer.userKey(name), typeof opts === "function" ? { execute: opts } : opts);
  }

  /** A durable timer. */
  sleep(name: string, ms: number): DurableCall<void> {
    const exec = this.#exec;
    const key = exec.replayer.userKey(name);
    return new DurableCall(exec, key, () => ({ type: "sleep", dur_ms: ms }), (r) => {
      if (r.type !== "slept") throw exec.unexpected(key, r);
    });
  }

  /**
   * Wait for signal `name`, sent to this run. A `payload` schema checks and types the payload; one
   * that no longer fits faults the run, as for a step's `output`.
   */
  waitForSignal<S extends Schema | undefined = undefined>(name: string, opts: WaitOptions<S> = {}): WaitCall<Payload<S>> {
    return this.#wait("signal", name, opts.payload);
  }

  /** Wait for a published event `name`; narrow it with `.filter(json)`. `payload` as for signals. */
  waitForEvent<S extends Schema | undefined = undefined>(name: string, opts: WaitOptions<S> = {}): WaitCall<Payload<S>> {
    return this.#wait("event", name, opts.payload);
  }

  /** A webhook endpoint for this run. `payload` as for signals. */
  createHook<S extends Schema | undefined = undefined>(name: string, opts: HookOptions<S> = {}): DurableCall<Hook<Payload<S>>> {
    const exec = this.#exec;
    const key = exec.replayer.userKey(name);
    return new DurableCall(exec, key, () => ({ type: "hook", token: opts.token ?? null }), (r) => {
      if (r.type !== "hook") throw exec.unexpected(key, r);
      if (r.conflict) throw new HookConflictError(r.token);
      return new Hook<Payload<S>>(exec, key, r.token, opts.payload as Schema<Payload<S>> | undefined);
    });
  }

  /** Run `execute` for each item as parallel steps `name[0]`, `name[1]`, ... Options as for `step`. */
  map<I, T>(name: string, items: I[], execute: (item: I, step: StepContext) => T | Promise<T>): Promise<T[]>;
  map<I, S extends Schema>(
    name: string,
    items: I[],
    opts: MapOptions<I, StandardSchemaV1.InferInput<S>> & { output: S },
  ): Promise<StandardSchemaV1.InferOutput<S>[]>;
  map<I, T>(name: string, items: I[], opts: MapOptions<I, T> & { output?: undefined }): Promise<T[]>;
  map<I>(
    name: string,
    items: I[],
    opts: MapOptions<I, unknown>["execute"] | (MapOptions<I, unknown> & { output?: Schema | undefined }),
  ): Promise<unknown[]> {
    const exec = this.#exec;
    const { execute, ...rest } = typeof opts === "function" ? { execute: opts } : opts;
    exec.replayer.checkName(name);
    const calls = items.map((item, i) =>
      exec.step(exec.replayer.keyFor(`${name}[${i}]`), { ...rest, execute: (s) => execute(item, s) }),
    );
    return Promise.all(calls);
  }

  /** The first call to settle wins; the others are released. */
  race<T>(calls: DurableCall<T>[]): Promise<T> {
    const release = () => calls.forEach((c) => c.release());
    return Promise.race(calls).then(
      (v) => {
        release();
        return v;
      },
      (e: unknown) => {
        release();
        throw e;
      },
    );
  }

  /**
   * As `race`, for calls of different types: resolves to the winner's name and value.
   *
   * `const r = await ctx.select({ paid: ctx.waitForEvent("paid"), late: ctx.sleep("late", 60_000) })`
   */
  select<M extends Record<string, DurableCall<unknown>>>(calls: M): Promise<Selected<M>> {
    const entries = Object.entries(calls);
    const tagged = entries.map(([key, call]) => call.then((value) => ({ key, value }) as Selected<M>));
    const release = () => entries.forEach(([, c]) => c.release());
    return Promise.race(tagged).then(
      (v) => {
        release();
        return v;
      },
      (e: unknown) => {
        release();
        throw e;
      },
    );
  }

  /** The time of the latest event the run has seen. */
  now(): Date {
    return new Date(this.#exec.replayer.now());
  }

  /** A deterministic float in [0, 1). */
  random(): number {
    return Number(this.randomU64() >> 11n) / 2 ** 53;
  }

  randomU64(): bigint {
    return BigInt(this.#exec.replayer.randomU64());
  }

  uuid(): string {
    return this.#exec.replayer.uuid();
  }

  /** True while the code runs on events an earlier execution already saw. */
  isReplaying(): boolean {
    return this.#exec.replayer.isReplaying();
  }

  #wait<T>(kind: WaitKind, name: string, schema: Schema | undefined): WaitCall<T> {
    const exec = this.#exec;
    const key = exec.replayer.userKey(name);
    return new WaitCall<T>(exec, key, kind, name, (r) => exec.payload(key, r, schema as Schema<T> | undefined));
  }
}

function waitForMicrotasks(): Promise<void> {
  // Every microtask (settled promises, the `then` calls that issue commands)
  // runs before the next macrotask.
  return new Promise((resolve) => setImmediate(resolve));
}

function asyncSchema(key: string): string {
  return `the schema for \`${key}\` validates asynchronously; durable calls need a synchronous schema`;
}

function toJson(v: unknown, what: string): Json {
  const text = JSON.stringify(v === undefined ? null : v);
  if (text === undefined) throw new TypeError(`${what} is not JSON-serializable`);
  return JSON.parse(text) as Json;
}

/** One execution of one workflow body, driven by `activate`. */
export class Execution {
  readonly replayer: NativeReplayer;
  #workflow: Workflow<any, any>;
  #input: unknown;
  #waiters = new Map<number, Waiter>();
  #steps = new Map<string, { execute: StepBody<unknown>; output: Schema | undefined }>();
  #attempts = new Map<string, AbortController>();
  #started = false;
  #result: Finish | undefined;

  constructor(workflow: Workflow<any, any>, replayer: NativeReplayer, input: unknown) {
    this.#workflow = workflow;
    this.replayer = replayer;
    this.#input = input;
  }

  /** Settle these resolutions in order, then run the body until it can do nothing more. */
  async activate(resolved: { handle: number; resolution: Resolution }[]): Promise<ExecStatus> {
    const abort = this.replayer.abort();
    if (abort) {
      // As every pending Rust future sees the abort when the driver polls.
      const waiters = [...this.#waiters.values()];
      this.#waiters.clear();
      for (const w of waiters) w.reject(abortError(abort));
    } else {
      for (const { handle, resolution } of resolved) {
        const w = this.#waiters.get(handle);
        this.#waiters.delete(handle);
        w?.resolve(resolution);
      }
    }
    if (!this.#started) this.#start();
    await waitForMicrotasks();
    return this.#result ? { status: "finished", result: this.#result } : { status: "pending" };
  }

  /** Run attempt `attempt` of the step issued under `key`. */
  async runStep(key: string, attempt: number, info: StepInfo): Promise<StepOutcome> {
    const step = this.#steps.get(key);
    if (!step) return { err: { message: `no step body for \`${key}\``, class: { type: "fatal" } } };
    const controller = new AbortController();
    this.#attempts.set(`${key}:${attempt}`, controller);
    try {
      const value = await step.execute({
        key,
        attempt,
        idempotencyKey: info.idempotencyKey,
        signal: controller.signal,
        heartbeat: () => info.heartbeat?.(),
      });
      let ok: Json;
      try {
        ok = toJson(value, "step output");
      } catch (e) {
        return { err: { message: (e as Error).message, class: { type: "fatal" } } };
      }
      if (step.output) {
        // Check what will be recorded, not what the schema makes of it: replay
        // runs the schema again on the recorded value.
        const v = validate(step.output, ok);
        if (v.ok === "async") return { err: { message: asyncSchema(key), class: { type: "fatal" } } };
        if (!v.ok) {
          return { err: { message: `step output does not match its schema: ${v.message}`, class: { type: "fatal" } } };
        }
      }
      return { ok };
    } catch (e) {
      if (e instanceof StepError) return { err: { message: e.message, class: e.class } };
      return { err: { message: e instanceof Error ? e.message : String(e), class: { type: "retryable" } } };
    } finally {
      this.#attempts.delete(`${key}:${attempt}`);
    }
  }

  /** The engine dropped this attempt. */
  abortStep(key: string, attempt: number): void {
    this.#attempts.get(`${key}:${attempt}`)?.abort();
  }

  // ---- used by ctx calls ----

  /** @internal Returns the handle, or `undefined` if the call failed at once. */
  issue(key: string, command: Command, waiter: Waiter): number | undefined {
    const abort = this.replayer.abort();
    if (abort) {
      waiter.reject(abortError(abort));
      return undefined;
    }
    const duplicate = this.replayer.isIssued(key);
    const handle = this.replayer.issue(key, command);
    if (duplicate) {
      waiter.reject(new FaultError(this.replayer.fault() ?? ""));
      return undefined;
    }
    this.#waiters.set(handle, waiter);
    return handle;
  }

  /** @internal */
  release(handle: number): void {
    this.#waiters.delete(handle);
    this.replayer.release(handle);
  }

  /** @internal */
  step(key: string, opts: StepOptions<unknown> & { output?: Schema | undefined }): StepCall<unknown> {
    this.#steps.set(key, { execute: opts.execute, output: opts.output });
    const call = new StepCall<unknown>(this, key, (r) => {
      switch (r.type) {
        case "step_succeeded":
          return this.decode(key, r.value, opts.output);
        case "step_failed":
          throw new StepFailedError(key, r.failure.message, r.failure.retryable);
        default:
          throw this.unexpected(key, r);
      }
    });
    if (opts.retry) call.retry(opts.retry);
    if (opts.timeoutMs !== undefined) call.timeout(opts.timeoutMs);
    return call;
  }

  /** @internal A wait's payload; `null` when it timed out. */
  payload<T>(key: string, r: Resolution, schema: Schema<T> | undefined): T {
    if (r.type !== "signal") throw this.unexpected(key, r);
    return r.payload === null ? (null as T) : this.decode(key, r.payload, schema);
  }

  /** @internal A recorded value, through the code's schema if it has one. */
  decode<T>(key: string, value: Json, schema: Schema<T> | undefined): T {
    if (!schema) return value as T;
    const v = validate(schema, value);
    if (v.ok === "async") {
      this.replayer.setFault(asyncSchema(key));
      throw new FaultError(asyncSchema(key));
    }
    if (!v.ok) return this.#nonDeterminism(`\`${key}\` no longer deserializes into the requested type: ${v.message}`);
    return v.value;
  }

  /** @internal */
  unexpected(key: string, r: Resolution): NonDeterminismError {
    const msg = `\`${key}\` resolved to an unexpected kind: ${JSON.stringify(r)}`;
    this.replayer.setFault(`non-determinism: ${msg}`);
    return new NonDeterminismError(msg);
  }

  #nonDeterminism(msg: string): never {
    this.replayer.setFault(`non-determinism: ${msg}`);
    throw new NonDeterminismError(msg);
  }

  #start(): void {
    this.#started = true;
    let body: Promise<unknown>;
    try {
      body = Promise.resolve(this.#workflow.body(new Ctx(this), this.#input));
    } catch (e) {
      body = Promise.reject(e);
    }
    body.then(
      (v) => {
        try {
          this.#result = { ok: toJson(v, "workflow output") };
        } catch (e) {
          this.#result = { err: failureOf(e) };
        }
      },
      (e: unknown) => {
        this.#result = { err: failureOf(e) };
      },
    );
  }
}
