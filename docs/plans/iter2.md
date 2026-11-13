# Iteration 2: streaming and observability (OpenTelemetry)

## Context

Iteration 1 left the following out of scope ([docs/plans/iter1.md](docs/plans/iter1.md)): streaming (Vercel's `getWritable`) and any real observability. Today:
- A caller can only poll a run or wait for its final result. `RunHandle::result` listens on `dw:run:{id}`, and the engine publishes there only when the run finishes.
- A step cannot emit partial output, such as LLM tokens or progress.
- Telemetry is a handful of `tracing::warn!` and `error!` calls with no spans, metrics or trace propagation.

Goals:
1. **Run-state streaming (durable).** A live, resumable feed of a run's events, built on the event log that already exists.
2. **User streams (transient).** Named streams that steps write to, for example LLM tokens. They are kept for a TTL and are **not** durable state. The durable part is the step's output, the final answer, which is memoized in the event log as today.
3. **OpenTelemetry as a first-class plugin.** Traces, metrics and logs, with trace context carried across durable boundaries: start → many executions, across workers and long sleeps.

Decisions confirmed with the user:
- **LLM and step chunks are transitive data.**
  - They go to **Redis Streams with a TTL**, never to Postgres.
  - Only the step's final result is persisted, as `StepSucceeded.output`.
  - This fits the iteration 1 rule: PG is truth, Redis is a rebuildable index or cache. Losing a stream loses nothing durable.
- **Retried steps.** Chunks are tagged with step key and attempt. A failed attempt appends an `AttemptFailed` marker, so readers discard its partial output.
- **OTel is a plugin.** `durable-runtime` gets no `opentelemetry` dependency. A new crate, `plugins/durable-otel`, does the integration.
- **Signals.** Iteration 2 exports traces, metrics and logs.
- **Local-first.**
  - The default (`standalone`) build has **no** OTel dependency: logs go to rotating JSON files, spans print to stdout, and metrics are off.
  - OTel is an opt-in `otel` feature. When it is enabled and a signal has OTLP configured, that signal goes over OTLP, and every other signal keeps its local sink.
- **Backlog.** Queryable observability stores: a DuckDB plugin (local) and ClickHouse (production). Iteration 2 only keeps exporters swappable so they can be added later.
- **Research background.**
  - DBOS and Vercel persist chunks in Postgres tables. Vercel's hosted world uses Redis.
  - Inngest Realtime is ephemeral pub/sub, which is closest to the user's choice.
  - Neither DBOS nor Vercel has attempt markers. We add one.
  - From Vercel we borrow: steps-only writers, batched flushes, flushing before the step commits, and closing streams automatically when the run ends.

## Part A: run-state streaming (durable, from the event log)

- **Channel.** Change `channels::run_finished` to `channels::run(id)`, still `dw:run:{id}`. The engine publishes a small JSON notice on it **after every commit**:
  - `{"seq": n}` from `apply`, `finish_commit` and `on_fault` in [engine.rs](crates/durable-runtime/src/engine.rs);
  - an ephemeral `{"step_started": {"key", "attempt"}}` from `launch`.

  `RunHandle::result` re-reads the run on any message, so it is unaffected.
- **`Client::watch(run_id)`** returns a `WatchBuilder` with `.after(seq)`. It yields a `Stream<Item = Result<RunUpdate>>`.
  - `RunUpdate` is `Event(Event)` or `StepStarted { key, attempt }`.
  - Order of work: subscribe, then `store.load_events(id, after)`, then on every notice (or a 200 ms poll fallback, as in `RunHandle::result`) load events after the last seq seen.
  - The stream ends after a terminal event. Only `StepStarted` is best effort.
- **Recovery for transient streams.** A UI renders tokens live from Part B. When `StepSucceeded{key, output}` arrives on `watch`, it replaces the partial text with the durable final answer. That is also how a client recovers after a stream expires or Redis is flushed. Document this pattern.
- **`ctx.is_replaying()`** on both contexts, true while `Core` feeds history. User logs in the body can then avoid repeating.

## Part B: user streams (transient, TTL-bounded)

### durable-core: new optional `StreamStore` slot (`crates/durable-core/src/stream.rs`)

```rust
pub enum ChunkBody { Data(Value), AttemptFailed, Closed }
pub struct NewChunk { pub step_key: Option<String>, pub attempt: u32, pub body: ChunkBody }
/// Opaque, ordered cursor (a Redis stream entry id; a counter in memory).
pub struct StreamCursor(pub String);
pub struct StreamChunk { pub cursor: StreamCursor, pub at: DateTime<Utc>,
                         pub step_key: Option<String>, pub attempt: u32, pub body: ChunkBody }
pub struct StreamInfo { pub stream: String, pub len: u64, pub closed: bool }

#[async_trait]
pub trait StreamStore: Send + Sync + 'static {
    /// Appends in order and refreshes the stream's TTL. `Error::Closed` after `Closed`.
    async fn append(&self, run_id: RunId, stream: &str, chunks: Vec<NewChunk>) -> Result<StreamCursor>;
    /// Chunks strictly after `after` (from the start when None). An expired or
    /// unknown stream reads as empty, and `info` returns None.
    async fn read(&self, run_id: RunId, stream: &str, after: Option<&StreamCursor>, limit: usize) -> Result<Vec<StreamChunk>>;
    async fn info(&self, run_id: RunId, stream: &str) -> Result<Option<StreamInfo>>;
    async fn list(&self, run_id: RunId) -> Result<Vec<StreamInfo>>;
    /// Appends `Closed` to every open stream of the run; idempotent.
    async fn close_all(&self, run_id: RunId) -> Result<()>;
}
```

- **Backend.** `Backend` gains `streams: Option<Arc<dyn StreamStore>>` plus `BackendBuilder::streams(..)`. Existing third-party backends keep compiling.
- **No stream store configured** (for example PG only, without Redis): writes become no-ops, with a single `warn!` per worker. `forward()` still returns the collected output, so the durable final answer is unaffected. Clients see no chunks and rely on `watch`.
- **Errors.** Add `Error::Closed`.

### Plugins
- **durable-redis: `RedisStreams`.** `connect()` returns it as `redis.streams`.
  - **Keys.**
    - `dw:{ns}:s:{run}:{name}` is a Redis Stream.
    - `dw:{ns}:s:{run}` is a SET of the run's stream names.
    - `dw:{ns}:s:{run}:closed` is a SET of the closed streams.
    - The key layout keeps cluster safety per run, which the Lua script needs.
  - **`append`** is one Lua script (EVALSHA, like the existing scripts in `src/lua/`). It does `XADD … MAXLEN ~ max_len` for each chunk, `SADD` on the index, `EXPIRE` on all keys with `ttl`, and rejects the append if the stream is closed.
    - Fields: `k` (step key), `a` (attempt), `t` (data, failed or closed), `d` (JSON).
  - **`read`** is `XRANGE key (after + COUNT limit`. Cursors are the stream entry ids.
  - **Live tail** uses no blocking `XREAD`, which would tie up the multiplexed connection. The writer publishes a notice on `PubSub` channel `dw:stream:{run}:{name}` after each flush, and readers poll as a fallback.
  - **Config.** `RedisStreams::ttl(Duration)`, default 24 h, and `.max_len(n)`, default 10 000 entries.
- **durable-memory: `MemoryStreams`** with the same TTL and max-len semantics against `Clock`, so expiry is testable with `ManualClock`. `durable_memory::backend()` wires it in.
- **durable-postgres:** unchanged for streams.
- **durable-testkit: `stream_conformance!`.**
  - Order is preserved under concurrent appenders.
  - `read` honours `after` and `limit`.
  - Appending after `Closed` fails.
  - `close_all` is idempotent.
  - `info` and `list` report correctly.
  - The TTL expires a stream. Run this case with a short TTL; the factory takes the TTL.

  Run the suite in the memory and Redis plugins (Redis on testcontainers).

### Runtime writer (`crates/durable-runtime/src/stream.rs`)
- **API.** `step::current().stream::<T: Serialize>("tokens")` returns a `StreamWriter<T>`. It works in async and blocking steps, because `StepInfo` gains an `Arc<StreamHub>` holding a tokio `Handle`.
  - `write(&T) -> Result<(), StepError>` is synchronous and does not block. It buffers, and checks `max_payload_bytes` per chunk plus a per-step buffered-bytes cap.
  - `async fn forward(stream) -> Vec<T>` pipes a `futures::Stream<Item = T>` (an LLM SDK stream) and returns the items. The step then returns its final, durable value, for example the concatenated text.
  - `close()` is optional.
- **Batching.** One flusher per (run, stream). It flushes every `stream_flush_interval`, default 25 ms (`WorkerBuilder::stream_flush_interval`), or every 64 chunks. A flush is one `append` followed by the pub/sub notice.
  - A flush error is a `warn!` and never fails the step. The data is transient by design.
- **Engine integration** ([engine.rs](crates/durable-runtime/src/engine.rs) `on_step_done`):
  1. Run `hub.drain(key, attempt)` before committing `StepSucceeded` or `StepFailed`, bounded by a short timeout. Readers then see all chunks before the step's event appears on `watch`.
  2. On `StepFailed`, append `AttemptFailed{step_key, attempt}` to the streams that attempt wrote to. A crashed worker writes no marker, so readers also treat a higher attempt for the same step key as an implicit reset.
  3. `finish_commit` and `poison` call `streams.close_all(run_id)` after the terminal commit, best effort.
- **Writers are limited to steps,** as in Vercel. The body's progress is visible durably through `watch`.

### Client reader
- **API.** `client.stream::<T>(run_id, "tokens")` returns a `StreamReadBuilder` with `.after(cursor)`. It yields a `Stream<Item = Result<StreamItem<T>>>`.
  - `StreamItem { cursor, step_key, attempt, body: Data(T) | AttemptFailed }`.
  - The stream ends on `Closed`. It also ends when the run is terminal and the stream is gone or expired, so it never hangs.
  - Order of work: `scoped_run` (namespace check), subscribe, `read`, then wait for a notice or poll.
- **`client.streams(run_id)`** lists the run's streams.

### Ops API and CLI
- **Routes** ([durable-api/src/lib.rs](crates/durable-api/src/lib.rs), SSE via `axum::response::sse`):
  - `GET /v1/runs/{id}/watch`: SSE of `RunUpdate`. The SSE `id` is the seq, and `Last-Event-ID` resumes.
  - `GET /v1/runs/{id}/streams`: JSON list.
  - `GET /v1/runs/{id}/streams/{name}`: SSE of chunks. The SSE `id` is the cursor, and `Last-Event-ID` resumes.
- **CLI** ([durable-cli/src/main.rs](crates/durable-cli/src/main.rs)): `durable runs watch <id>` and `durable streams list|tail <id> [name]`. `tail` needs `--redis-url`.

## Part C: observability as a plugin

### durable-core: the `Observer` slot (`crates/durable-core/src/observe.rs`)
- `durable-core` gains a `tracing` dependency, the facade only.
- The runtime emits **live** `tracing` spans, and the plugin bridges them to OTel. The user's own spans inside a step body (reqwest, LLM SDKs, sqlx) then nest under the step span.
- No explicit-timestamp spans and no deterministic span IDs. The research flagged those as risky on opentelemetry 0.32 and later.

```rust
pub type TraceCarrier = BTreeMap<String, String>;   // W3C traceparent/tracestate/baggage

pub trait Observer: Send + Sync + 'static {
    fn inject(&self, _span: &tracing::Span) -> TraceCarrier { TraceCarrier::new() }
    fn set_parent(&self, _span: &tracing::Span, _carrier: &TraceCarrier) {}
    fn record(&self, _m: &Metric) {}
}
pub enum Metric<'a> {
    RunStarted { namespace: &'a str, workflow: &'a str },
    RunFinished { namespace, workflow, status: RunStatus, e2e: Duration },
    Execution { namespace, workflow, outcome: &'static str /* finished|suspended|fault|abandoned */, duration, replayed_events: usize },
    ScheduleToStart { namespace, workflow, kind: WorkKind, lag: Duration },
    StepAttempt { namespace, workflow, step: &'a str /* name without #n/[i] */, outcome: &'static str, duration },
    Commit { duration, conflict: bool },
    EngineFault { namespace, workflow },
    StreamFlush { chunks: usize, bytes: usize, duration, ok: bool },
    WorkerGauges { active_runs: usize, concurrency: usize, pool_backlog: usize },
}
```
- **Backend.** `Backend` gains `observer: Arc<dyn Observer>`, a no-op by default, set with `BackendBuilder::observer(..)`. Both `Client` and `Worker` reach it.
- **`Run` and `NewRun`** gain `trace_context: Option<TraceCarrier>`. It is persisted in memory, and in Postgres through migration `0002_trace_context.sql` (`ALTER TABLE runs ADD COLUMN trace_context jsonb`). `store_conformance` checks the round trip.

### Runtime instrumentation (`durable-runtime`, via `tracing` only)
- **Start.** `Client::start_raw` opens span `workflow.start` (`otel.kind=producer`, `durable.workflow`, `durable.namespace`). It stores `observer.inject(&span)` on `NewRun` and records `RunStarted`.
- **Other client calls.** `signal`, `send_event`, `cancel` and `resume_hook` each get a span.
- **Execution.** In `engine::execute`, after `get_run`, open span `workflow.execute`:
  - fields `otel.kind=consumer`, `durable.run_id`, `durable.workflow`, `durable.delivery`, and `durable.outcome` set at the end;
  - `observer.set_parent(&span, run.trace_context)`, so all executions of a run share one trace under the start span (the "continuous" model of Temporal and Vercel).

  The rest of `execute` is `.instrument(span)`.
- **Steps.** `Exec::launch` opens span `step {name}` with `durable.step.key`, `.attempt` and `.kind`.
  - Async steps use `.instrument`. Blocking steps call `span.in_scope` on the pool thread.
  - The outcome and error are set in `on_step_done`.
  - Replayed steps never launch, so they emit no spans.
- **Other spans.** A debug-level `store.commit` span. The existing `warn!` and `error!` calls gain `run_id` and `workflow` fields, and the engine-fault log goes inside the execute span.
- **Metrics.** Recorded from `run_one`, `engine`, the stream flusher and the lease-keeper tick ([worker.rs](crates/durable-runtime/src/worker.rs)). `ScheduleToStart` is `now - lease.item.due_at`.

### `plugins/durable-otel` (new; depends on durable-core plus the OTel crates)
- **Dependencies:** opentelemetry, opentelemetry_sdk and opentelemetry-otlp (grpc-tonic and http-proto) at 0.33, tracing-opentelemetry 0.34, opentelemetry-appender-tracing 0.33, opentelemetry-semantic-conventions 0.33, tracing-subscriber.
- **`OtelObserver`.**
  - Composite W3C TraceContext and Baggage propagator over `TraceCarrier` (`Injector`/`Extractor` impls).
  - `inject` and `set_parent` go through `OpenTelemetrySpanExt`.
  - `record` maps each `Metric` to instruments created once:
    - `durable.runs.started` and `durable.runs.finished` (counters, with `status`);
    - `durable.run.duration`, `durable.execution.duration`, `durable.step.duration`, `durable.schedule_to_start` and `durable.commit.duration` (histograms in seconds);
    - `durable.step.attempts` (counter, with `outcome`);
    - `durable.engine.faults`;
    - `durable.stream.chunks`;
    - observable gauges for active runs and pool backlog.
- **`durable_otel::layers(OtelConfig::from_env()) -> Result<OtelParts>`** builds OTLP providers **only for the signals that have OTLP configured**: a per-signal or global `OTEL_EXPORTER_OTLP_*` endpoint, or `OTEL_{TRACES,METRICS,LOGS}_EXPORTER=otlp`.
  - The tracer provider is batch, the meter provider uses a periodic reader, and there is a logger provider.
  - It sets the global providers explicitly.
  - It returns which signals it took over (`traces`, `logs`, `metrics` flags), the tracing layers (`tracing_opentelemetry::layer()` and/or `OpenTelemetryTracingBridge`), `.observer()`, and a guard that runs `force_flush` and `shutdown`.
  - Standard variables: `OTEL_SERVICE_NAME` and `OTEL_RESOURCE_ATTRIBUTES`.
  - `OtelConfig` also accepts programmatic exporters (`.span_exporter(..)`, `.log_exporter(..)`, `.metric_reader(..)`). A future DuckDB or ClickHouse plugin is then just another exporter, with no runtime change.
- **Tests** use the SDK in-memory exporters (the `testing` feature).

### Local-first observability (`observable`) with no OTel dependency (`durable-workflow-rs/src/observable.rs`)
- This is the **default** experience for local development. Feature `observable` in the facade pulls only `tracing-subscriber` (fmt, json, env-filter) and `tracing-appender` 0.2.
- **`durable_workflow::observable::init(ObservableConfig::from_env()) -> Result<ObservableGuard>`** installs the global subscriber. Embedded apps that already own a subscriber can call `observable::layers(..)` instead.
- **Per-signal resolution.** Local sinks are the fallback for every signal OTel does not take over: OTel is off, or that signal has no OTLP configured.

  | Signal | Local default | Other values (`DURABLE_{TRACES,LOGS,METRICS}`) | With `otel` feature + OTLP configured |
  |---|---|---|---|
  | spans | `stdout`: `fmt` layer with `FmtSpan::CLOSE` in JSON, so each closed span prints once with `time.busy`/`time.idle` and its fields (`durable.run_id`, `durable.workflow`, `durable.step.key`, outcome) | `none` | OTLP |
  | logs | `file`: rotating JSON lines | `stdout` (pretty, for dev), `none` | OTLP |
  | metrics | `none` | none (local metrics are backlog) | OTLP |

- **File logs.**
  - `tracing-appender` `RollingFileAppender::builder()`, configured by:
    - `DURABLE_LOG_DIR`, default `./logs`;
    - `DURABLE_LOG_FILE_PREFIX`, default `durable`;
    - `DURABLE_LOG_ROTATION` (`minutely|hourly|daily|never`), default `daily`;
    - `DURABLE_LOG_MAX_FILES`, default 7, pruning old files.
  - The writer is `non_blocking`, and its `WorkerGuard` lives in `ObservableGuard`.
  - Records come from `fmt().json()` with `with_current_span(true)` and `with_span_list(true)`. Every line therefore carries `timestamp`, `level`, `target`, `message`, fields, and the enclosing span fields `durable.run_id`, `durable.workflow`, `durable.step.key` and `durable.step.attempt`. That is enough to join logs to the stdout spans without OTel.
  - When the `otel` feature and an OTel tracer are active, a small custom `FormatEvent` also adds `trace_id` and `span_id`.
- **The `Observer`** stays the core no-op unless `otel` provides one. Local mode needs no propagation, because the span fields carry the run id.
- **Tests:**
  - resolution matrix: no env vars; the `otel` feature with only `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`, giving OTLP spans plus file logs; explicit overrides;
  - file logs in a temp dir are valid JSON with `durable.run_id`;
  - `DURABLE_LOG_DIR` pointing at a fresh temp path creates it and receives the logs;
  - an unwritable `DURABLE_LOG_DIR` yields a `Config` error;
  - `minutely` rotation with `max_files=2` prunes;
  - span JSON goes to an injected writer.

### Facade and standalone
- **Facade** [durable-workflow/Cargo.toml](durable-workflow-rs/Cargo.toml):
  - feature `observable` (local-first), enabled by `standalone`;
  - feature `otel` (`dep:durable-otel`, `durable_workflow::otel`) is **opt-in and never on by default**: not in `default`, not in `standalone`. Users add `features = ["standalone", "otel"]`;
  - the `redis` feature's `connect()` now also returns `streams`.
- **[standalone.rs](durable-workflow-rs/src/standalone.rs).**
  - `init_tracing` is replaced by `observable::init`. With zero configuration this gives span JSON on stdout and rotating JSON logs in `./logs`.
  - The log directory is set with **`DURABLE_LOG_DIR`**, read by `ObservableConfig::from_env()` (`Config::from_env()` delegates to it). It is also settable in code with `ObservableConfig::log_dir(..)`.
  - The directory is created if missing. An unwritable directory fails startup with a clear `Error::Config` naming the path and the variable; it does not silently drop logs.
  - The same goes for `DURABLE_LOG_FILE_PREFIX`, `DURABLE_LOG_ROTATION` and `DURABLE_LOG_MAX_FILES`. All four are documented in the README env table and in the standalone-worker example.
  - With `cfg(feature = "otel")`, `observable::init` asks `durable_otel::layers` which signals it takes over, attaches the OTel observer to the backend, and keeps local sinks for the rest.
  - The guard is held until after `worker.shutdown()`, so spans and logs flush.
  - [connect.rs](durable-workflow-rs/src/connect.rs) wires `RedisStreams` when a Redis URL is configured, with a `DURABLE_STREAM_TTL_SECS` override.
- **[docker-compose.yml](docker-compose.yml):** add `grafana/otel-lgtm`, which bundles a collector, Tempo, Mimir, Loki and Grafana.

## Examples and docs
- `examples/llm-streaming`: an axum app.
  - The workflow has a `generate` step that does `step::current().stream("tokens").forward(llm_stream)` and returns the full answer, which is the durable output.
  - The fake generator fails its first attempt once, to show `AttemptFailed`.
  - With `ANTHROPIC_API_KEY` set, an optional real Messages API SSE call goes through reqwest; check the current model id with the claude-api skill at implementation time.
  - A plain HTML page uses `/watch` together with `/streams/tokens`, and swaps in the final `StepSucceeded` output.
- `examples/standalone-worker`: documents OTel via env vars.
- This plan lives at `docs/plans/iter2.md`.
- `docs/BACKLOG.md` tracks deferred work. Its observability-store entry covers:
  - a `plugins/durable-duckdb` exporter for spans, logs and metrics into a local `.duckdb` file, with `durable traces|logs` CLI queries;
  - the production approach on ClickHouse: OTLP into an OTel Collector, then the ClickHouse exporter with its schema, TTL and retention.

  It also lists the other out-of-scope items below.
- README: "Streaming" (transient chunks vs. durable output) and "Observability" sections, plus a `stream_conformance!` note for plugin authors.

## Build order
1. `durable-core`: `stream` and `observe` modules, `Backend` slots, `Run.trace_context`, `Error::Closed`.
2. `MemoryStreams`, testkit `stream_conformance!`, the `store_conformance` trace-context case, and the trace-context column in the memory and Postgres stores (migration 0002).
3. Runtime: run notices and `Client::watch`, `is_replaying`, `StreamHub`/`StreamWriter` with engine drain, marker and close, the client stream reader, then spans and metrics.
4. `RedisStreams` (Lua append) plus conformance on testcontainers.
5. Facade `observable` module (local first) and standalone wiring, then `plugins/durable-otel` behind the opt-in `otel` feature.
6. API SSE routes, CLI subcommands, examples, docs.

## Verification
- **Runtime tests** on memory with `ManualClock`, async and sync where relevant:
  - `watch` yields all events in order, resumes from `after(seq)` without gaps or duplicates, sees `StepStarted`, and ends at the terminal event;
  - a step streams N chunks, and the reader gets them in order and then `Closed` after the run finishes;
  - write, fail, then succeed: the reader sees attempt-1 chunks, `AttemptFailed{1}`, then attempt-2 chunks;
  - the step's durable output equals the concatenation, and the chunks are not in the store's event log;
  - all chunks are readable before `StepSucceeded` appears on `watch`;
  - parallel steps on one stream keep per-writer order;
  - writes from a blocking step work;
  - a stream expires after the TTL (by advancing `ManualClock`), and the reader ends instead of hanging once the run is terminal;
  - a backend without a stream store: writes are no-ops and the run still succeeds with its output;
  - `is_replaying` is correct.
- **Conformance:** `stream_conformance!` passes for memory and Redis, and `store_conformance` round-trips `trace_context` for memory and Postgres.
- **Redis plugin:** the TTL is set on every key after an append, `MAXLEN` trims, and `FLUSHALL` mid-stream does not fail the run.
- **OTel plugin** (in-memory exporters):
  - a run with a sleep, so two executions, gives one trace: `workflow.start` → 2× `workflow.execute` → `step` spans;
  - a user span inside a step nests under the step span;
  - replayed steps produce no spans;
  - the expected metrics are recorded;
  - a `tracing::info!` inside a step yields a log record carrying that trace id.
- **Local observable tests:** the ones listed under the local-first section. CI also runs `cargo build -p durable-workflow --features standalone` and checks that `cargo tree` shows no `opentelemetry` crate.
- **API:** SSE route tests via `tower::ServiceExt::oneshot`, including `Last-Event-ID` resume and namespace scoping.
- **Chaos test** ([chaos.rs](durable-workflow-rs/tests/chaos.rs)): add streaming steps. Runs still complete with correct durable outputs despite worker aborts and Redis `FLUSHALL`.
- **Manual:** `docker compose up -d`, run `examples/llm-streaming` and watch tokens arrive, reset on the injected failure and be replaced by the final answer. Run `examples/standalone-worker` with `OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317` and check the traces, metrics and correlated logs in Grafana on :3000.
- `./ci.sh` passes: fmt, clippy `-D warnings`, all tests.

## Implementation notes (where the code differs from the plan above)

- **`AttemptStarted` marker.** The plan relied on `AttemptFailed` plus "a higher attempt number is an implicit reset". A chaos test showed this is not enough: attempts are counted from `StepFailed` events, so a step re-run after a worker crash starts again as the *same* attempt number, and its tokens would be appended to the dead run's partial tokens with nothing to tell a reader. The first write of each (step, attempt) per execution is now preceded by `ChunkBody::AttemptStarted`, and readers drop what they hold for the step key on either marker.
- **Builders are awaited.** `client.watch(id).after(seq).await?` and `client.stream::<T>(id, name).after(cursor).await?` implement `IntoFuture` and resolve to a stream (the run lookup and subscription happen first). Both report `RunError`. `watch` also ends when started after a run's terminal event.
- **`StreamWriter::forward`** returns `Result<Vec<T>, StepError>`, because writing can fail (oversized chunk, full buffer).
- **Writer limits.** Chunks are limited by `max_payload_bytes`; unflushed bytes per stream by 4 x that. A step that overruns the buffer gets a retryable error.
- **Redis keys.** `{prefix}:s:{<run>}:n:{name}` (stream), `{prefix}:s:{<run>}:names` and `{prefix}:s:{<run>}:closed` (sets). The run id is a hash tag, and the `:n:` segment keeps a stream called `closed` or `names` from colliding with the bookkeeping keys.
- **Metrics exporter.** OpenTelemetry's `MetricReader` trait is private in `opentelemetry_sdk` 0.33 (behind an experimental feature), so `OtelConfig` takes `metric_exporter(impl PushMetricExporter)` and wraps it in a periodic reader instead of `metric_reader`.
- **OTLP protocol.** When no protocol variable is set the exporter uses gRPC (so `OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317` works), or HTTP/protobuf if the endpoint is on port 4318.
- **`is_replaying`** is true for every history event except the newest: that poll is the first time the body sees the event.
- **Explicit beats OTLP.** An explicit `DURABLE_TRACES` / `DURABLE_LOGS` / `DURABLE_METRICS` keeps that signal local even when OTLP is configured for it.
- **No Redis in `connect()`.** The in-memory fallback now also wires `MemoryStreams`, so a single-process deployment can tail its streams.
- **Extra metrics:** `durable.execution.replayed_events`, `durable.stream.bytes`, `durable.stream.flush.duration`, and the `durable.worker.concurrency` gauge.

## Out of scope (recorded in docs/BACKLOG.md)
- DuckDB (local) and ClickHouse (production) observability stores.
- A Prometheus pull endpoint.
- Durable or Postgres-backed stream storage.
- Stream writes from the workflow body.
- A server-side "latest attempt only" filter.
- Span links from signals and events.
- The GenAI semantic-convention attributes, which are still in Development status. A later LLM-step helper can add them.
