# durable-workflow

Durable workflows that **adapt to your infrastructure** instead of the other way round, in **Rust** and
**TypeScript** (Python is planned). Every SDK is a first-class citizen: the same engine, the same backends, the same
guarantees and the same conformance tests, with an API that feels native in each language.

- **Decentralized.** No server. Workers coordinate only through the backends, so you can embed a
  worker in an existing app or run dedicated worker processes.
- **Replay-based.** A workflow is ordinary code in your language. Step results are memoized in a per-run event log,
  and a run resumes by replaying that log.
- **Pluggable.** Postgres is the source of truth. Redis is a rebuildable queue/pub-sub/cache index.
  Every slot is a trait; new backends are plugins, and every SDK gets them.
- **One engine, many languages.** The replay state machine is a sans-IO core (`durable-sm`) that each SDK embeds.
  Workers written in different languages can share one database, and the ops API and the CLI see all their runs.

## SDKs

| Language | Directory | Package | Status |
|---|---|---|---|
| Rust | [`durable-workflow-rs`](durable-workflow-rs) | crate `durable-workflow` | usable; `async fn` and blocking `fn` workflows |
| TypeScript | [`durable-workflow-ts`](durable-workflow-ts) | npm `@durable/typescript` | usable, not yet published to npm |
| Python | `durable-workflow-py` | | planned ([backlog](docs/BACKLOG.md#python-sdk)) |

The same workflow in each:

<table>
<tr><th>Rust</th><th>TypeScript</th></tr>
<tr><td>

```rust
#[workflow(name = "user.onboarding", timeout = "7d")]
async fn onboarding(ctx: WfAsyncCtx, user: Signup)
    -> Result<String, WorkflowError>
{
    send_email(&ctx, user.email.clone(), "Welcome!".into()).await?;
    let verified: Option<bool> = ctx
        .wait_for_signal("verified")
        .timeout(Duration::from_secs(86_400))
        .await?;
    Ok(if verified == Some(true) { "verified" } else { "reminded" }.into())
}

#[step(retry = 5, timeout = "30s")]
async fn send_email(mailer: Dep<dyn Mailer>, to: String, subject: String)
    -> Result<(), StepError>
{
    mailer.send(&to, &subject, step::current().idempotency_key());
    Ok(())
}
```

</td><td>

```ts
const onboarding = workflow(
  "user.onboarding",
  { timeoutMs: 7 * DAY_MS },
  async (ctx, user: Signup) => {
    await ctx.step("send-welcome", {
      retry: retry.exponential(5),
      timeoutMs: 30_000,
      execute: (s) =>
        mailer.send(user.email, "Welcome!", s.idempotencyKey),
    });
    const verified = await ctx
      .waitForSignal("verified", { payload: z.boolean() })
      .timeout(DAY_MS);
    return verified === true ? "verified" : "reminded";
  },
);
```

</td></tr>
</table>

Each SDK has its own guide: [Rust](durable-workflow-rs/README.md), [TypeScript](durable-workflow-ts/README.md).
Examples: [`examples/typescript-onboarding`](examples/typescript-onboarding),
[`examples/embedded-axum`](examples/embedded-axum), [`examples/standalone-worker`](examples/standalone-worker),
[`examples/llm-streaming`](examples/llm-streaming).

## Layout

```
durable-workflow-rs/  Rust SDK (crate `durable-workflow`); features: memory (default), macros (default), postgres,
                      redis, standalone, observable (local JSON logs + spans), otel (opt-in OpenTelemetry)
durable-workflow-ts/  TypeScript SDK (npm `@durable/typescript`): napi-rs addon + TypeScript API
crates/               the shared engine, used by every SDK
  durable-sm/         sans-IO replay state machine, engine decisions and event model; no tokio, no IO
  durable-spi/        ids, records, errors, every plugin trait (no infrastructure); re-exports the event model
  durable-runtime/    replay engine (async + sync drivers), host abstraction, compute pool, Worker, Client
  durable-macros/     #[workflow], #[step] for the Rust SDK
  durable-api/        axum ops router (mountable or standalone)
  durable-cli/        the `durable` binary
  durable-testkit/    conformance suites for plugin authors, ManualClock, and the replay conformance corpus
                      every SDK must pass
plugins/
  durable-memory/     in-memory reference implementation (tests, dev)
  durable-postgres/   Store (sqlx), migrations
  durable-redis/      Queue (Lua), PubSub, Cache, RateLimiter, Streams
  durable-otel/       OpenTelemetry traces, metrics and logs (opt-in)
examples/             embedded-axum, standalone-worker, llm-streaming (Rust); typescript-onboarding (TypeScript)
```

Plugins depend only on `durable-spi`. `durable-runtime` depends only on `durable-spi` and `durable-sm`.
`durable-sm` depends on neither, and CI keeps it free of async runtimes so any language can embed it.

## Determinism rules

Replay re-runs your workflow function against the recorded log, so the body must make the same
`ctx` calls in the same order every time. Each SDK spells out the details for its language.

- Do I/O and side effects **inside steps** only.
- Use the context's clock, random numbers and UUIDs (`ctx.now()`, `ctx.uuid()`, ...) instead of the system clock and
  RNG. `ctx.uuid()` is a UUIDv7 stamped with `ctx.now()`, strictly increasing within a run.
- Wait only on `ctx` calls in the workflow body, and race them with the SDK's own race.
- Steps are matched by key: your name plus an occurrence counter (`charge`, `charge#1`). Names of
  steps, sleeps, waits and hooks must be non-empty and may not contain `#`, `[` or `]`, which
  generated keys use; such a name faults the run. Renaming a step, or changing its kind, for an
  in-flight run is reported as non-determinism. The run stays open (visible via `last_error`) and
  resumes once a compatible worker is deployed.

## At-least-once semantics

A step's *result* is recorded exactly once, but its *body* may run more than once: if a worker
dies after the body finished and before the result committed, another worker re-runs it. Make side
effects idempotent and pass `step::current().idempotency_key()` (`run_id:step_key`, stable across
retries, replays and crashes) to external APIs.

A poison run that keeps crashing workers is failed with `MaxDeliveriesExceeded` after 48
executions (configurable).

## Failures, retries, cancellation

- Any step error is retryable by default; each SDK has a way to mark an error fatal or set its backoff.
- Backoffs up to the worker's inline retry threshold are waited out in-process; longer ones suspend the run.
- An exhausted step fails the step call in your workflow code, which can recover or let it fail the run.
- Runs can be cancelled while waiting and mid-step. A failed run can be retried from its failure point (succeeded
  steps stay memoized, failed ones get a fresh budget).
- Whole-run deadline: a workflow timeout.

## Streaming

Two different things, on purpose:

- **Run state (durable).** A live, resumable feed of the run's events, built on the event log: every committed
  event in order, exactly once, ending after the terminal event.
- **User streams (transient).** A step writes named chunks (LLM tokens, progress) that live in the
  `StreamStore` (Redis Streams, with a TTL, 24 h by default) and are **never** written to Postgres. The step's return
  value is the durable answer; the chunks are a live preview of it. Losing a stream loses nothing durable.

Over HTTP both feeds are Server-Sent Events: `GET /v1/runs/{id}/watch`, `GET /v1/runs/{id}/streams` and
`GET /v1/runs/{id}/streams/{name}`; the SSE `id` is the event seq or stream cursor, and `Last-Event-ID` (or `?after=`)
resumes. The [Rust guide](durable-workflow-rs/README.md#streaming-in-rust) covers writing and reading streams and how
readers see retries.

## Ops API and CLI

`durable_api::router(client)` returns a plain `axum::Router`; nest it and wrap it with your own auth,
or serve it standalone behind `with_bearer_token`.

```
POST /v1/workflows/{name}/runs          GET /v1/runs            GET /v1/runs/{id}[/events]
POST /v1/runs/{id}/cancel|retry         POST /v1/runs/{id}/signals/{name}
POST /v1/hooks/{token}   (webhook: the body is the payload)      POST /v1/events
GET  /v1/runs/{id}/watch                                         (SSE, resume with Last-Event-ID)
GET  /v1/runs/{id}/streams   /v1/runs/{id}/streams/{name}        (list; SSE of chunks)
GET  /healthz  /readyz
```

```
durable --database-url $DB [--redis-url $REDIS] migrate
durable start <workflow> --input '{...}' [--idempotency-key K]
durable runs list|get|events|cancel|retry      durable runs watch <run-id> [--after SEQ]
durable streams list <run-id>      durable streams tail <run-id> [name] [--after CURSOR]
durable signal <run-id> <name> --payload '...'      durable event send <name> --data '...'
```

## Writing a plugin

Implement a trait from `durable-spi` (`Store`, `Queue`, `PubSub`, `Cache`, `RateLimiter`,
`StreamStore`) in a new
crate under `plugins/`, then prove it with the shared suites:

```rust
durable_testkit::store_conformance!(async { Arc::new(MyStore::new()) as Arc<dyn Store> });
durable_testkit::queue_conformance!(async { Arc::new(MyQueue::new()) as Arc<dyn Queue> });
durable_testkit::cache_conformance!(..);  durable_testkit::pubsub_conformance!(..);
durable_testkit::rate_limiter_conformance!(..);
// StreamStore: the factory takes the TTL to configure and returns a harness that can let time pass
durable_testkit::stream_conformance!(|ttl| async move { StreamHarness::real_time(Arc::new(MyStreams::new(ttl))) });
```

The suites include the lost-wake-up race (a signal delivered concurrently with a suspending commit)
and the queue's concurrency, throttle and lease semantics.

## Development

```
./ci.sh     # fmt, clippy -D warnings, all Rust tests, the TypeScript SDK and example (needs Docker and Node >= 22.18)
docker compose up -d    # postgres:17 + redis:7 for the examples
```

Each SDK's guide lists its own commands. The Rust workspace also builds with Bazel, for per-crate and per-test-file
caching: `bazel test //...`. Cargo stays the source of truth and Bazel is optional; see [docs/bazel.md](docs/bazel.md).

The replay conformance corpus (`crates/durable-testkit/corpus`) pins how every history replays, and every SDK runs it. A change to
replay behaviour fails `replay_corpus`; if the change is intended, regenerate the corpus with
`DURABLE_UPDATE_CORPUS=1 cargo test -p durable-runtime --test replay_corpus` and review the diff.

The chaos suite (`durable-workflow-rs/tests/chaos.rs`) runs on real Postgres + Redis with
several workers in one process: killing workers mid-step, `FLUSHALL` mid-flight, concurrency limits
across workers, throttle spacing and start-time rate limits, and streaming steps through worker
kills and a Redis flush.

Without Docker, point the tests at running servers: `DURABLE_TEST_POSTGRES_URL`,
`DURABLE_TEST_REDIS_URL` and (for the test that runs `FLUSHALL`) `DURABLE_TEST_REDIS_FLUSHABLE_URL`.

## Status

Not yet implemented: child workflows, cron, debounce/batching/priority, blob offload for large payloads,
versioning/patching APIs, retention cleanup, a dashboard UI and first-party integration clients.

Iteration 3 ([plan](docs/plans/iter3-sansio-core.md)) moved the engine onto one sans-IO core shared by every SDK:
the replay core and the engine's decisions in `durable-sm`, the replay conformance corpus, the host abstraction, and
the TypeScript SDK. The Rust SDK runs on the same core. The Python SDK is on hold and tracked in
[docs/BACKLOG.md](docs/BACKLOG.md), with the rest of the deferred work.
