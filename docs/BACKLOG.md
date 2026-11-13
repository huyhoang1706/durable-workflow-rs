# Backlog

Deferred work, grouped by area. Each entry says where it came from: [iter1](plans/iter1.md), [iter2](plans/iter2.md) or [iter3](plans/iter3-sansio-core.md).

## Observability

- **DuckDB observability store** (`plugins/durable-duckdb`), from iter2.
  - A local, zero-infra sink for spans, logs and metrics, written to a `.duckdb` file.
  - Plugged in through `OtelConfig` programmatic exporters (`.span_exporter`, `.log_exporter`, `.metric_exporter`), so the runtime needs no changes.
  - Add `durable traces|logs` CLI queries, for example all spans of a run and the logs of a step attempt.
- **ClickHouse for production**, from iter2.
  - Path: OTLP → OpenTelemetry Collector → ClickHouse exporter.
  - Decide the table schema (traces, logs, metrics), TTL and retention, partitioning by day and namespace, and the query patterns for run/step drill-down.
  - Ship a reference Collector config and docker-compose profile.
- **Local metrics sink**, from iter2. Metrics are off in local-first mode. Options: a Prometheus pull endpoint (`/metrics` on the standalone HTTP server) or periodic JSON snapshots to file.
- **Span links from signals and events**, from iter2. Capture the sender's trace context in `deliver_signal`, `publish_event` and `resolve_hook`, and link it from the execution that consumes it.
- **GenAI semantic conventions**, from iter2. Add `gen_ai.*` attributes, and possibly time-to-first-chunk, in an optional LLM-step helper once the conventions leave Development status.

## Streaming

- **Durable stream storage**, from iter2. Postgres-backed chunks for use cases that need replayable streams, not transient LLM tokens.
- **Workflow-body stream writes**, from iter2. Exactly-once writes from the body, recorded through the event log as DBOS does.
- **Server-side "latest attempt only" filter**, from iter2. Hide chunks of failed attempts from readers, instead of relying on `AttemptFailed` markers.

## Workflow features

These come from iter1.

- Child workflows.
- Cron / scheduled starts.
- Debounce, batching and priority (flow control).
- Versioning / patching APIs for changing in-flight workflow code.
- First-party integration clients (email, Slack, Teams) under `plugins/`.

## Storage and operations

These come from iter1.

- Blob offload for large payloads (a `BlobStore` plugin, for example S3).
- Retention cleanup for finished runs and their events.
- Dashboard UI.

## Engine

- **Fan-out is quadratic in the number of steps in flight**, from [iter3](plans/iter3-sansio-core.md) (S4
  measurements). One run with n parallel steps costs O(n²) in total: 117 µs per step at n = 250, 1686 µs at
  n = 4000, in Rust and TypeScript alike. Each step completion is one turn of `run_loop` in
  `crates/durable-runtime/src/engine.rs` (`Wake::Done` handles a single `StepDone`), and every turn recomputes the
  frontier and walks it in `reap`, `decide` and `build_commit`. Drain every completion that is ready in one turn
  (one commit for all of them), and keep the frontier incremental, or index it by key.

## TypeScript binding

From [iter3](plans/iter3-sansio-core.md), S4.

- **First npm release.** Choose the npm scope (`@durable` is a placeholder until it is registered), add `NPM_TOKEN`
  to a GitHub `npm` environment, run `.github/workflows/typescript-release.yml` once without a tag, then push
  `typescript-v0.1.0`.
- **More targets:** `linux-x64-musl` / `linux-arm64-musl` (Alpine), and `win32-arm64-msvc`.
- **A README for the npm package.**

## Python binding

From [iter3](plans/iter3-sansio-core.md), where it was phase P4. On hold for now.

- **`bindings/python`** (PyO3 + maturin), with the same surface as the TypeScript binding: an embedded `Worker` and
  `Client`, `workflow()`, `ctx.step/sleep/wait_for_signal/wait_for_event/create_hook`, and Postgres/Redis/memory
  backends selected by URL. Workflow bodies run on a dedicated asyncio loop; sync `def` steps run in a thread pool
  (see "How a TS workflow maps onto it" in the plan).
- **Done when:** like P3, a Python onboarding example passes the replay conformance corpus and a crash/resume chaos
  test.
- The host contract it builds on (`HostWorkflow` / `HostExecution`, P3a) and the corpus (P2) are already in place,
  so no engine change is expected.

## Test suite

These come from [iter3](plans/iter3-sansio-core.md), seen while running `ci.sh` for P3 S3.

- **Flaky chaos test under full-suite load.** `streaming_steps_finish_with_correct_output_despite_worker_kills`
  (`crates/durable-workflow/tests/chaos.rs`) failed once in a full `./ci.sh` with "run 20: stream closed at the end":
  the run succeeded with the right output, but its `tokens` stream was not marked closed. It passed 6 out of 6 times
  when rerun alone or with the rest of the chaos file. Likely a race between a killed worker's attempt and the
  `Closed` marker when the machine is busy; find out whether the stream can stay open for good, or whether the test
  checks too early.
- **Testcontainers left running.** After a full `./ci.sh`, about 28 `postgres:17` / `redis:7` containers labelled
  `org.testcontainers.managed-by=testcontainers` keep running. The shared containers held in statics are probably
  never dropped. Stop them when the test binary exits (or enable Ryuk), so repeated local runs do not pile them up.
