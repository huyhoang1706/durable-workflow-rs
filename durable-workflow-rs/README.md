# durable-workflow for Rust

The Rust SDK of [durable-workflow](../README.md): the `durable-workflow` crate. Workflows are `async fn` or
plain blocking `fn`, declared with `#[workflow]` and `#[step]`.

The concepts (replay, determinism, at-least-once steps, retries, streams, the ops API and the CLI) are the same in
every SDK and are described in the [project README](../README.md). This page covers the Rust API.

```rust
#[workflow(name = "user.onboarding", timeout = "7d")]
async fn onboarding(ctx: WfAsyncCtx, _mailer: Dep<dyn Mailer>, user: Signup)
    -> Result<String, WorkflowError>
{
    send_email(&ctx, user.email.clone(), "Welcome!".into()).await?;      // memoized step
    let verified: Option<bool> =
        ctx.wait_for_signal("verified").timeout(Duration::from_secs(86_400)).await?; // suspends, holds no worker
    Ok(if verified.is_some() { "verified" } else { "reminded" }.into())
}

#[step(retry = 5, timeout = "30s")]
async fn send_email(mailer: Dep<dyn Mailer>, to: String, subject: String) -> Result<(), StepError> {
    mailer.send(&to, &subject, step::current().idempotency_key());
    Ok(())
}
```

Features: `memory` (default), `macros` (default), `postgres`, `redis`, `standalone`, `observable` (local JSON logs +
spans), `otel` (opt-in OpenTelemetry), `reqwest` / `sqlx` (`ClassifyError` impls).

Examples: [`examples/embedded-axum`](../examples/embedded-axum) (an existing axum app that shares its sqlx pool and
mounts the ops API), [`examples/standalone-worker`](../examples/standalone-worker) (a dedicated worker with a CPU-bound
sync workflow) and [`examples/llm-streaming`](../examples/llm-streaming) (streams an LLM answer to a browser while a
step runs).

## Quick start (in-memory)

```rust
let backend = durable_workflow::memory::backend();
let worker = Worker::builder(backend.clone())
    .dep::<dyn Mailer>(Arc::new(SmtpMailer::from_env()?))
    .register(onboarding)                 // the #[workflow] fn itself
    .build()?                              // fails fast if a declared Dep<T> is missing
    .spawn();                              // runs inside your tokio runtime

let client = Client::new(backend);
let run = client.start(onboarding, &signup, StartOptions::default().idempotency_key("signup:ann")).await?;
let welcome: String = run.handle().unwrap().result().await?;
```

Production backends:

```rust
let store = PostgresStore::from_pool(app_pool).schema("durable");   // reuse the host's pool
store.migrate().await?;                                             // own migration table, advisory-locked
let redis = durable_workflow::redis::connect(&redis_url, "dw").await?;
let backend = Backend::builder()
    .namespace("billing")
    .store(store).queue(redis.queue).pubsub(redis.pubsub)
    .cache(redis.cache).rate_limiter(redis.rate_limiter)
    .build()?;
```

`PostgresStore` + the in-memory queue already gives a durable single-process mode without Redis:
the reconciler rebuilds the queue from `wake_at` on boot.

## Async or sync?

Workflow bodies are orchestration code. Heavy work belongs in steps, and every step can be async
(I/O) or blocking (CPU) in either kind of workflow.

| | `WfAsyncCtx` (`async fn`) | `WfSyncCtx` (`fn`) |
|---|---|---|
| I/O step on tokio | `ctx.step(name, async move {..})` | `ctx.step(name, async move {..})` (driver thread blocks) |
| CPU step on the compute pool | `ctx.step_blocking(name, \|\| ..)`, `ctx.map_blocking(..)` | `ctx.step_blocking(name, \|\| ..)`, `ctx.map(..)`, `ctx.join((..))`, `ctx.join2(..)` |
| sleep / signal / event / hook | `.await` | blocking call |

A sync workflow runs on its own driver thread (separate from the compute pool, so a blocked body
can never deadlock its own steps). Blocking steps cannot be preempted: on timeout or cancel the
step is marked failed and `step::current().is_cancelled()` turns true, but the pool slot stays busy
until the closure returns, so long CPU steps should poll it.

## Determinism in Rust

The rules are in the [project README](../README.md#determinism-rules). In Rust:

- `Dep<T>` for effectful dependencies refuses to be dereferenced outside a step and turns the mistake into a visible
  engine fault. Register deterministic, read-only config with `.dep_pure()` to use it in the body.
- Use `ctx.now()`, `ctx.random_u64()`, `ctx.uuid()` instead of the system clock and RNG.
  `ctx.uuid()` is a UUIDv7 stamped with `ctx.now()`, strictly increasing within a run.
- Await only `ctx` futures. Awaiting anything else (a raw `tokio::sleep`, a channel) is detected and
  reported as an engine fault.
- Race `ctx` futures with `tokio::select! { biased; .. }`. An unbiased `select!` polls its branches in
  random order. Replay still matches by key and picks the same winner, but the commands are issued in
  a different order each time.

## Failures, retries, cancellation

- `StepError::fatal(..)` stops retries, `StepError::retry_after(d, ..)` sets the backoff, and `ClassifyError` lets a
  dependency's error type choose. Panics inside a step are retryable failures.
- An exhausted step returns `Err(WorkflowError::Step(..))` to your code, which can recover or `?`.
- `client.cancel(id, reason)`, `client.retry(id)`; whole-run deadline: `timeout = "1h"`.

## Flow control

```rust
#[workflow(name = "video.publish",
           concurrency(limit = 4, key = "/channel_id"),   // executing runs only; sleeping runs hold no slot
           throttle(limit = 100, period = "1m"),          // delays starts, never drops them (burst = 1)
           rate_limit(limit = 10, period = "1s"))]        // rejects excess starts: StartOutcome::RateLimited
```

## Streaming in Rust

```rust
#[step]
async fn generate(llm_token_stream: BoxStream<'static, String>) -> Result<String, StepError> {
    let tokens = durable_workflow::step::current().stream::<String>("tokens");
    let parts = tokens.forward(llm_token_stream).await?;   // or tokens.write(&chunk)? per chunk
    Ok(parts.concat())                                     // the durable answer
}
// in the workflow body: generate(&ctx, llm_token_stream).await?

let mut feed = client.stream::<String>(run_id, "tokens").await?;   // any process
```

The step's return value is the durable answer; the chunks are a live preview of it. A client that
renders tokens should swap in `StepSucceeded.output` (from `watch`) when it arrives. That also
recovers a client whose stream expired, was flushed, or that connected late. Losing a stream loses
nothing durable.

How readers see retries: each attempt's first chunk is preceded by an `AttemptStarted` marker, and
a failed attempt ends with `AttemptFailed`. **When you see either, drop what you hold for that step
key** (a crashed worker's partial output is replaced the same way, because its re-run starts with
a new marker). A stream store that caps its length (Redis keeps about `max_len` entries,
10 000 by default) drops the oldest chunks first, markers included. A reader that starts from
the beginning of a long stream should therefore keep only the highest `attempt` it sees for each
step key, rather than rely on an `AttemptStarted` being there. Writers are limited to steps. Writes are buffered and flushed every 25 ms
(`WorkerBuilder::stream_flush_interval`) or 64 chunks, before the step's result is committed; a
flush failure is logged and never fails the step. With no stream store configured, writes are
no-ops (one warning per worker). The stream feed ends when the stream closes (the run ending
closes all its streams) and never hangs on an expired stream of a finished run.

`ctx.is_replaying()` is true while history is replayed into the workflow, so logging in a workflow
body can skip the repeats.

Over HTTP the same feeds are Server-Sent Events: `GET /v1/runs/{id}/watch`,
`GET /v1/runs/{id}/streams` and `GET /v1/runs/{id}/streams/{name}`; the SSE `id` is the event seq or
stream cursor, and `Last-Event-ID` (or `?after=`) resumes. From the CLI: `durable runs watch <id>`
and `durable streams list|tail <id> [name]` (streams need `--redis-url`).

## Observability

The runtime emits `tracing` spans (`workflow.start`, `workflow.execute`, `step`) with
`durable.run_id`, `durable.workflow`, `durable.step.key` and `durable.step.attempt`, and reports
metrics to an `Observer` slot of the backend. Nothing in `durable-runtime` depends on OpenTelemetry.

**Local first (feature `observable`, on in `standalone`).** With zero configuration the standalone
worker prints one JSON line per closed span to stdout (with `time.busy` / `time.idle`) and writes
rotating JSON logs to `./logs`; every log line carries the enclosing span fields, so logs and spans
join on the run id. Embedded apps that own their subscriber add `observable::layers(..)` to it.

| Variable | Default | |
|---|---|---|
| `DURABLE_TRACES` | `stdout` | `stdout`, `none` |
| `DURABLE_LOGS` | `file` | `file`, `stdout` (pretty), `none` |
| `DURABLE_METRICS` | `none` | local metrics are not implemented |
| `DURABLE_LOG_DIR` | `./logs` | created if missing; an unwritable path fails startup with a clear error |
| `DURABLE_LOG_FILE_PREFIX` | `durable` | |
| `DURABLE_LOG_ROTATION` | `daily` | `minutely`, `hourly`, `daily`, `never` |
| `DURABLE_LOG_MAX_FILES` | `7` | older files are pruned |
| `DURABLE_STREAM_TTL_SECS` | `86400` | how long user streams are kept |

**OpenTelemetry (opt-in feature `otel`, never on by default).** Add
`features = ["standalone", "otel"]` and set the standard variables
(`OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_EXPORTER_OTLP_{TRACES,METRICS,LOGS}_ENDPOINT`,
`OTEL_{TRACES,METRICS,LOGS}_EXPORTER`, `OTEL_SERVICE_NAME`, `OTEL_RESOURCE_ATTRIBUTES`). Each
signal that has OTLP configured is exported over OTLP; every other signal keeps its local sink, and
an explicit `DURABLE_*` choice wins. The trace context is stored on the run, so a run is **one
trace** however many executions, workers and sleeps it spans: `workflow.start` → `workflow.execute`
(one per execution) → `step`. Spans you create inside a step nest under it, and a `tracing` event
in a step becomes a log record with that trace id. Metrics: `durable.runs.started|finished`,
`durable.run.duration`, `durable.execution.duration`, `durable.step.duration|attempts`,
`durable.schedule_to_start`, `durable.commit.duration`, `durable.engine.faults`,
`durable.stream.chunks`, and worker gauges. `docker compose up -d otel-lgtm` starts a collector with
Tempo, Mimir, Loki and Grafana (http://localhost:3000). Other sinks (DuckDB, ClickHouse) are
`OtelConfig` exporters away: `span_exporter`, `log_exporter`, `metric_exporter`.

## Standalone worker

Standalone worker: `durable_workflow::standalone::run(Config::from_env()?, |w| w.register(my_workflow))`
handles SIGTERM/SIGINT (drain, then hand back anything still running), health endpoints, the optional
ops API (`DURABLE_ENABLE_API`, `DURABLE_API_TOKEN`) and observability setup (see above). It listens on `DURABLE_HTTP_ADDR`,
`127.0.0.1:17080` by default; in a container set `0.0.0.0:17080` so probes can reach it. It refuses
to serve the ops API without a token on a non-loopback address unless `DURABLE_API_INSECURE=1` is set.

A `Client` (and so the ops API and the CLI) only sees its backend's namespace: runs, hook tokens and
workflow definitions of other namespaces sharing the same database are invisible to it.

## Notes

Added after iteration 1:

- **History cache.** A suspended run's event log is cached (`Cache`, key `dw:hist:<run>`) together
  with its `last_seq`; the next execution skips `Store::load_events` when it still matches the
  run's `last_seq`, and falls back to the store otherwise.
- **In-flight overlay.** `GET /v1/runs/{id}` on a non-terminal run adds `in_flight`
  (`worker`, `since`, `deliveries`, `steps`: step key to attempt) when the backend has a cache.
  Name workers with `WorkerBuilder::worker_id`. `Client::in_flight(id)` exposes the same data.
- **`hook.dispose()`** (`ctx.dispose_hook(hook)` in sync workflows) releases a hook token while the
  run keeps going: new `HookDisposed` event, honoured by every `Store` (conformance case
  `hook_dispose_releases_the_token`).
- **`ClassifyError` for `reqwest` and `sqlx`** behind the `reqwest` / `sqlx` cargo features of
  `durable-runtime` (forwarded by `durable-workflow`). `durable_runtime::classify::check_response`
  turns a `reqwest::Response` into a `StepError`: 429 honours `Retry-After`, 408 and 5xx retry, other
  statuses are fatal. For `sqlx`, serialization failures, deadlocks, connection and pool errors
  retry; everything else is fatal. Use `StepError::classified(err)`.
- **`ctx.join((("a", fa), ("b", fb), ...))`** runs up to 8 blocking steps with different output
  types in parallel in sync workflows (`join2` remains).
- **Compile-fail tests** (`trybuild`, `durable-workflow-rs/tests/ui`) pin the `#[workflow]` /
  `#[step]` error messages.

