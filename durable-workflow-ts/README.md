# durable-workflow for TypeScript

The TypeScript SDK of [durable-workflow](../README.md): the `@durable/typescript` npm package (Node >= 22.18, not
yet published to npm). Workflows are `async` functions; inputs, outputs and payloads can be checked with any
[Standard Schema](https://standardschema.dev) library (zod, valibot, arktype, ...).

The concepts (replay, determinism, at-least-once steps, retries, the ops API and the CLI) are the same in every SDK
and are described in the [project README](../README.md). This page covers the TypeScript API.

```ts
import { Backend, Client, Worker, retry, workflow } from "@durable/typescript";
import { z } from "zod";

const onboarding = workflow("user.onboarding", { timeoutMs: 7 * DAY_MS }, async (ctx, user: { email: string }) => {
  await ctx.step("send-welcome", {                       // memoized step
    retry: retry.exponential(5),
    timeoutMs: 30_000,
    execute: (s) => mailer.send(user.email, "Welcome!", s.idempotencyKey),
  });
  const verified = await ctx.waitForSignal("verified", { payload: z.boolean() }).timeout(DAY_MS); // suspends, holds no worker
  return verified === true ? "verified" : "reminded";
});

const backend = await Backend.connect(process.env.DATABASE_URL ?? "memory://", { redisUrl: process.env.REDIS_URL });
const worker = new Worker(backend, [onboarding]);
await worker.start();

const client = new Client(backend);
const run = await client.start(onboarding, { email: "ann@example.com" }, { idempotencyKey: "signup:ann" });
const outcome = await run.result();
```

See [`examples/typescript-onboarding`](../examples/typescript-onboarding): an HTTP API and an embedded worker in one
process, on the memory backend or on Postgres + Redis.

## Backends

`Backend.connect(url, options)` picks the backend from the URL:

- `memory://` (or `Backend.memory()`): everything in this process's memory; nothing survives a restart.
- `postgres://...`: runs, history and timers in Postgres. Add `redisUrl` for the queue, pub/sub, cache, rate limiter
  and streams in Redis.

Options: `namespace`, `schema`, `migrate`, `streamTtlMs`, `connectTimeoutMs`. A TypeScript worker and a Rust worker can
share the same database: each runs the workflows it registered, and the ops API and the `durable` CLI see all runs.

## The workflow context

| | |
|---|---|
| step | `ctx.step(name, execute)` or `ctx.step(name, { execute, output, retry, timeoutMs })`; `.retry(..)` / `.timeout(ms)` on the call |
| fan-out | `ctx.map(name, items, execute)` |
| sleep | `ctx.sleep(name, ms)` |
| signal / event | `ctx.waitForSignal(name, { payload })`, `ctx.waitForEvent(name, { payload, filter })`; `.timeout(ms)` resolves `null` on the deadline |
| hook | `ctx.createHook(name)` returns a `Hook` with a `token`, `recv()` and `dispose()` |
| race | `ctx.race([a, b])` |
| determinism | `ctx.now()`, `ctx.random()`, `ctx.uuid()`, `ctx.isReplaying()` |

A step body gets a `StepContext`: `key`, `attempt`, `idempotencyKey`, an `AbortSignal` that fires on timeout or
cancellation, and `heartbeat()`.

## Determinism in TypeScript

The rules are in the [project README](../README.md#determinism-rules). In TypeScript:

- Use `ctx.now()`, `ctx.random()` and `ctx.uuid()` instead of `Date.now()`, `Math.random()` and `crypto.randomUUID()`.
- Await only `ctx` calls in the workflow body. Race them with `ctx.race`, not `Promise.race`.
- Inputs and outputs cross JSON. Check them with a schema (`output`, `payload`) rather than trusting their type.

## Client and worker

`Client`: `start`, `handle(runId)`, `getRun`, `events`, `signal`, `sendEvent`, `resumeHook`, `cancel`. A `RunHandle`
has `id`, `existing` (an idempotency key matched an earlier run) and `result()`.

`Worker` options: `concurrency`, `leaseMs`, `pollIntervalMs`, `reconcileIntervalMs`, `inlineRetryThresholdMs`,
`maxDeliveries`, `maxPayloadBytes`. `worker.shutdown()` waits for running steps and hands the other runs back to the
backend. `worker.replay(..)` and `replay(..)` run a recorded history through a workflow, for tests.

## Not yet in this SDK

Compared with the Rust SDK: flow control (`concurrency`, `throttle`, `rate_limit` per workflow), user streams written
from steps, `watch`, and `retry` of a failed run. They are tracked in [docs/BACKLOG.md](../docs/BACKLOG.md).

## Development

```bash
npm ci && npm run build:native && npm run typecheck && npm test && npm run build
```

```bash
npm run test:servers   # also the Postgres/Redis cases, on throwaway containers
```

```bash
npm run build:native:release && npm run bench   # tiny-step throughput
```

The tests replay the shared conformance corpus (`crates/durable-testkit/corpus`) through the native core, and a
chaos test kills worker processes mid-run.
