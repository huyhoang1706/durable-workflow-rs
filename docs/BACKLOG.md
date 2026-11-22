# Backlog

Deferred work, grouped by area. Each entry says where it came from: [iter1](plans/iter1.md), [iter2](plans/iter2.md), [iter3](plans/iter3-sansio-core.md) or [iter4](plans/iter4-local-first.md).

## Observability

- **DuckDB observability store** (`plugins/durable-duckdb`), from iter2. Planned in [iter4](plans/iter4-local-first.md), except the CLI queries.
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

## Engine

- **The reconciler re-enqueues runs that are already executing**, seen while fixing flaky tests on
  `feat/bazel-lint-format`. A new run is stored as `pending` with `wake_at = created_at`, and a resumed run keeps
  its past `wake_at`, until the execution's first commit. A reconciler tick in that window finds the run due
  (`due_runs`) and enqueues it; the queue sees the lease and records a rerun, which `ack` makes due at once. The
  run is replayed one extra time before its timer fires. Harmless, since replay is deterministic, but wasted work
  for any execution that is still running at a tick (every 5 s by default) without having committed.
  - Fix options: `due_runs` / the reconciler skip runs the queue holds a live lease for, or the queue drops an
    enqueue that brings nothing new for a leased run. The second must not drop a signal delivered mid-execution,
    whose item can carry the same due time (`ManualClock` tests), so it needs more than a `due_at` comparison.
  - Touches every queue backend (memory, Redis, Postgres); `durable-testkit`'s queue conformance should pin the
    new behaviour.
  - Tests that count executions or store loads exactly have to work around it today:
    `history_cache.rs` turns the reconciler off, `engine_async.rs`'s
    `deterministic_helpers_are_stable_across_replays` asserts at least two executions.

## Storage and operations

These come from iter1.

- Blob offload for large payloads (a `BlobStore` plugin, for example S3).
- Retention cleanup for finished runs and their events.
- Dashboard UI.

## TypeScript SDK

From [iter3](plans/iter3-sansio-core.md), S4.

- **First npm release.** Choose the npm scope (`@durable` is a placeholder until it is registered), add `NPM_TOKEN`
  to a GitHub `npm` environment, run `.github/workflows/typescript-release.yml` once without a tag, then push
  `typescript-v0.1.0`.
- **More targets:** `linux-x64-musl` / `linux-arm64-musl` (Alpine), and `win32-arm64-msvc`.
- **Parity with the Rust SDK:** per-workflow flow control (`concurrency`, `throttle`, `rate_limit`), user streams
  written from steps, `Client.watch`, and `Client.retry` of a failed run.

## Python SDK

From [iter3](plans/iter3-sansio-core.md), where it was phase P4. On hold for now.

- **`durable-workflow-py`** (PyO3 + maturin), with the same surface as the TypeScript SDK: an embedded `Worker` and
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
  (`durable-workflow-rs/tests/chaos.rs`) failed once in a full `./ci.sh` with "run 20: stream closed at the end":
  the run succeeded with the right output, but its `tokens` stream was not marked closed. It passed 6 out of 6 times
  when rerun alone or with the rest of the chaos file. Likely a race between a killed worker's attempt and the
  `Closed` marker when the machine is busy; find out whether the stream can stay open for good, or whether the test
  checks too early.
- **Two load-sensitive tests in `engine_async`**, seen while fixing flaky tests on `feat/bazel-lint-format`.
  `parallel_steps_run_concurrently` ("steps ran serially") and `failing_step_is_retried_and_attempt_is_visible`
  (the 10 s `Harness::until` timeout) each failed 2 times in 100 with 100 copies of the test file running at once
  (`bazel test //crates/durable-runtime:engine_async_test --runs_per_test=100`). They passed 60 out of 60 with
  `--local_test_jobs=4`, and never failed in a normal run. Both rely on real time (a concurrency window, a
  wall-clock timeout); find out whether they can wait on events instead, now that CI no longer retries failing
  tests.
- **Testcontainers left running.** After a full `./ci.sh`, about 28 `postgres:17` / `redis:7` containers labelled
  `org.testcontainers.managed-by=testcontainers` keep running. The shared containers held in statics are probably
  never dropped. Stop them when the test binary exits (or enable Ryuk), so repeated local runs do not pile them up.
