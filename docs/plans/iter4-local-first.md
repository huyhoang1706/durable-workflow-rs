# Iteration 4: local-first development (SQLite store, DuckDB observability)

> Status: in progress. Parts A (`plugins/durable-sqlite`) and B (the memory backend on in-memory SQLite) are built;
> Parts C–E are planned.

## Context

Two things still need infrastructure on a developer's machine:

- **Durable state needs Postgres.** The only durable `Store` is `plugins/durable-postgres`, so a run that should
  survive a restart needs Docker. The alternative, `durable-memory`, loses everything when the process exits.
- **Queryable telemetry needs an OTLP stack.** The local sinks of [iteration 2](iter2.md) are JSON spans on stdout
  and rotating JSON log files; metrics are off. Answering "show me every span of this run" means running a
  collector, Tempo and Loki.

Goal: develop against the framework with no services at all. One `.db` file holds the runs, one `.duckdb` file holds
the spans, logs and metrics, and both can be opened with stock tools (`sqlite3`, `duckdb`).

Decisions confirmed with the user:

- **SQLite covers the `Store` slot only.** Queue, pub/sub, cache and streams stay in memory, exactly as in the
  Postgres-without-Redis mode: the store is the source of truth and the reconciler rebuilds the queue from `wake_at`.
  - Runs survive restarts, and the CLI and a worker can share one file.
  - User streams are only visible inside the process that wrote them, and a run woken from another process waits up
    to one reconcile interval (5 s by default).
- **DuckDB is a set of OpenTelemetry exporters**, as sketched in iteration 2 and the [backlog](../BACKLOG.md):
  span, log and metric exporters plugged into `OtelConfig`. The runtime does not change. The `duckdb` feature pulls
  in the `otel` feature, so it stays out of `default` and `standalone`.
- **`MemoryStore` is removed.** The memory backend keeps its in-process queue, pub/sub, cache, rate limiter and
  streams, and uses SQLite in `:memory:` mode for the store. There is then one embedded store implementation to
  maintain instead of two.
- **In scope:** `sqlite://` URLs in both SDKs and the CLI, `DURABLE_{TRACES,LOGS,METRICS}=duckdb`, an example and
  docs.
- **Out of scope:** CLI queries over the DuckDB file (`durable traces|logs`). They stay in the backlog.

## Part A: `plugins/durable-sqlite` (new)

Laid out like `plugins/durable-postgres`: `lib.rs`, `store.rs`, `migrate.rs`, `sql.rs`, `migrations/0001_init.sql`
and a `BUILD.bazel` (the template `./ci.sh` prints, plus `compile_data = glob(["migrations/*.sql"])`; no
`DOCKER_TEST_TAGS`, since nothing here needs a container).

**Dependency.** `sqlx` with the `sqlite` feature (bundled `libsqlite3-sys`, already in `Cargo.lock`). The `postgres`
feature moves out of `[workspace.dependencies] sqlx` and into the crates that use it (`durable-postgres`,
`examples/embedded-axum`, the dev-dependencies of `durable-workflow-rs`), so a SQLite-only user does not compile the
Postgres driver.

**API.**

```rust
impl SqliteStore {
    /// A file database: created if missing, WAL, `synchronous = NORMAL`, a busy timeout, foreign keys on.
    pub async fn connect(url: &str) -> Result<Self>;
    pub async fn open(path: impl AsRef<Path>) -> Result<Self>;
    pub fn from_pool(pool: SqlitePool) -> Self;
    /// `None` for `in_memory()`, which holds one connection instead of a pool.
    pub fn pool(&self) -> Option<&SqlitePool>;
    /// A connection for raw SQL (the in-memory one, held exclusively until dropped).
    pub async fn acquire(&self) -> Result<impl DerefMut<Target = SqliteConnection> + '_>;
    /// Create or upgrade the tables. Safe to call from every process at boot.
    pub async fn migrate(&self) -> Result<()>;
    /// A private in-memory database; connects and migrates on first use.
    pub fn in_memory() -> Self;
}
```

- `in_memory()` is synchronous and lazy on purpose. `durable_memory::backend()` and the TypeScript SDK's
  `Backend.memory()` are synchronous and may be called outside a Tokio runtime, so the connection is opened and
  migrated on the first store call.
- Every connection to `:memory:` is its own database, so the in-memory store has exactly one connection. As built in
  Part B it holds it in a `tokio::sync::Mutex` instead of a one-connection pool: a pooled release spawns a task.
- Migrations use their own `schema_migrations` table, as in Postgres, and are serialized by `BEGIN IMMEDIATE`.

**Translating the Postgres SQL.** `plugins/durable-postgres/src/store.rs` is the reference; `commit` keeps its order
of operations.

| Postgres | SQLite |
|---|---|
| `SELECT … FOR UPDATE` on the run | every write path opens its transaction with `BEGIN IMMEDIATE` (SQLite has one writer) |
| `uuid` | `TEXT`, hyphenated, so rows are readable in `sqlite3` (UUIDv7 text sorts like the bytes) |
| `timestamptz` | `INTEGER` Unix microseconds, so `wake_at <= ?` compares exactly |
| `jsonb`, `text[]` | `TEXT` holding JSON |
| `UNNEST(...)` batches | `QueryBuilder::push_values` inside the same transaction |
| `data @> filter` | SQL narrows the waits by `name`; containment runs in Rust, with the `contains` function moved from `plugins/durable-memory/src/store.rs` |
| `= ANY($n)` | an `IN (…)` list built with `QueryBuilder` |

These carry over unchanged: the partial unique index with `ON CONFLICT … WHERE … DO NOTHING RETURNING`, the
row-value comparison `(created_at, id) < (…)` for paging, and `same_waits` (unchanged waits are not rewritten).

With one connection, a query inside a transaction must go through the transaction, never through a second call to
the store: the connection it would wait for is the one already held.

**Plain commits** (added in Part B). A commit that registers no wait or hook and does not end the run (most step
results and timers) cannot consume a signal or generate an event, so it skips reading the run, its waits and its
inbox: the version check moves into `UPDATE … WHERE last_seq = ? RETURNING`, and `runs.has_waits` (stale-true,
never stale-false) says whether waits it no longer lists must be deleted. Four round trips instead of seven.

**Tests** (`plugins/durable-sqlite/tests/`, no Docker):

- `conformance.rs`: `durable_testkit::store_conformance!` twice, for a file database in a temp directory and for
  `in_memory()`.
- `migrations.rs`: idempotent, versions recorded, concurrent migrations from several pools on one file.
- `persistence.rs`: create a run and commit, close the pool, reopen the file and read the run and its events back;
  two pools on one file (the CLI next to a worker) interleave writes without losing a signal.

## Part B: remove `MemoryStore`

- Delete `plugins/durable-memory/src/store.rs`. `durable-memory` depends on `durable-sqlite`, and `builder()` in
  `plugins/durable-memory/src/lib.rs` uses `SqliteStore::in_memory()`.
- `backend()` and `backend_with_clock()` keep their signatures, so their callers do not change: the tests of
  `durable-runtime`, `durable-api`, `durable-otel` and the Rust SDK, the `tiny_steps` and `llm-streaming` examples,
  and `durable-workflow-ts/src/worker.rs`.
- `plugins/durable-memory/tests/conformance.rs` drops `store_conformance!`; it now runs in `durable-sqlite`.
- `crates/durable-runtime/tests/streaming.rs` builds a backend by hand with `MemoryStore::new()`; it switches to
  `SqliteStore::in_memory()` (a new dev-dependency on `durable-sqlite`).

Consequences:

- The Rust SDK's default `memory` feature now compiles the bundled SQLite.
- The "memory" store is much slower than a `HashMap`. Every query is a round trip to the thread sqlx runs the
  connection on (about 27 µs), and the first cut cost 582 µs/step sequential and 468 µs/step for 64 runs at once in
  `tiny_steps` (from 40 and 7). Asked whether to keep `MemoryStore` (the fallback under Risks), the user chose to
  optimize instead: holding the connection and plain commits bring it to about 280 and 260 µs/step, about 3k steps/s
  (TypeScript `npm run bench`: 484 and 293 µs/step). Fan-outs are unchanged. The numbers are recorded in
  [iteration 3](iter3-sansio-core.md), "Memory store on SQLite".
- Tried and dropped: sending a plain commit's statements as one multi-statement query. sqlx steps through them one by
  one, so it saved nothing.

## Part C: `sqlite://` in the SDKs and the CLI

- **Rust SDK** (`durable-workflow-rs`).
  - `Cargo.toml`: a `sqlite = ["dep:durable-sqlite"]` feature, and `sqlite` joins `standalone`, so the standalone
    worker runs without Docker.
  - `lib.rs`: re-export `durable_sqlite as sqlite`; the `connect` module is built with `any(postgres, sqlite)`.
  - `connect.rs`: `BackendConfig::build` picks the store from the scheme of `database_url`. `sqlite:` gives
    `SqliteStore::connect` (and `migrate`); `postgres:` / `postgresql:` behave as today. A scheme whose feature is
    off is an `Error::Config` that names the feature. `schema` and `connect_timeout` apply to Postgres only. The
    Redis and `with_memory_slots` halves are shared by both stores.
- **CLI** (`crates/durable-cli`). Enable `sqlite`; update the help of `--database-url`, `--schema` and `migrate`.
  Add a SQLite smoke test (migrate, start, `runs get`) that needs no Docker.
- **TypeScript SDK** (`durable-workflow-ts`).
  - `src/worker.rs`: a `BackendUrl::Sqlite` variant that goes through `BackendConfig`. `schema` and
    `connectTimeoutMs` are rejected for SQLite, the way `memory()` rejects Postgres-only options today.
  - `ts/worker.ts`: document the scheme on `Backend.connect`.
  - A Vitest case that runs a workflow on `sqlite://` in a temp file, then resumes it from a new worker.

## Part D: `plugins/durable-duckdb` (new)

Depends on `duckdb` (feature `bundled`), `opentelemetry`, `opentelemetry_sdk` 0.33 and `serde_json`. It does not
depend on the runtime.

```rust
let sink = DuckDbSink::open("./durable.duckdb")?;
let config = OtelConfig::from_env()
    .span_exporter(sink.span_exporter())
    .log_exporter(sink.log_exporter())
    .metric_exporter(sink.metric_exporter());
```

The three exporters implement `SpanExporter`, `LogExporter` and `PushMetricExporter`.

**Tables**, created with `CREATE TABLE IF NOT EXISTS` on the first flush:

- `spans`: `trace_id`, `span_id`, `parent_span_id`, `name`, start, end, `duration_us`, status; dedicated columns for
  `durable.run_id`, `durable.workflow`, `durable.step.key` and `durable.step.attempt`; `attributes` and `events` as
  JSON.
- `logs`: time, severity, target, body, `trace_id`, `span_id`, `attributes` as JSON.
- `metrics`: time, name, kind, unit, `attributes` as JSON, value / count / sum / min / max, bucket bounds and counts.
- A `run_logs` view that joins `logs` to `spans` on `span_id`, to filter logs by run and step.

**One writer per file.** DuckDB lets a single process open a file for writing, and a reader cannot open it
meanwhile. So an exporter does not hold the file: for each batch it opens the database, appends, and closes. It
retries briefly when the file is locked and keeps a bounded buffer if it stays locked. That lets `duckdb
durable.duckdb` read the file while a worker runs, and lets several workers share one file.

**Wiring into the Rust SDK.**

- `plugins/durable-otel/src/config.rs`: a way to limit OTLP to chosen signals. Today `wants_otlp` is `env_otlp ||
  !programmatic`, so with OTLP variables present `DURABLE_TRACES=duckdb` would export to both.
- `durable-workflow-rs/Cargo.toml`: `duckdb = ["otel", "dep:durable-duckdb"]`. It is not part of `default` or
  `standalone`, so the "standalone has no opentelemetry" check in `ci.sh` still holds.
- `durable-workflow-rs/src/observable.rs`:
  - `SpanSink::DuckDb`, `LogSink::DuckDb`, `MetricSink::DuckDb`;
  - `duckdb` as a value of `DURABLE_TRACES`, `DURABLE_LOGS` and `DURABLE_METRICS`;
  - `DURABLE_DUCKDB_PATH` (default `./durable.duckdb`) and a `.duckdb_path()` builder;
  - `layers()` builds an `OtelConfig` with the DuckDB exporters for the signals that chose `duckdb`. The observer it
    returns is the `OtelObserver`, so a run keeps one trace across executions and metrics are recorded;
  - choosing `duckdb` with the feature off is an `Error::Config` that names the feature.

**Tests.**

- `plugins/durable-duckdb/tests/`: export hand-built batches and query each table back; two sinks on one file.
- `durable-workflow-rs/tests/observable_duckdb.rs` (`required-features = ["duckdb"]`): run a workflow on the memory
  backend with all three signals on `duckdb`, drop the guard, then check that the run's spans share one `trace_id`,
  `run_logs` holds a step's log line and `metrics` holds a step attempt.

## Part E: example and docs

- `examples/standalone-worker`: enable `sqlite` and `duckdb`. Its README shows a run with no Docker, plus a few
  sample queries:

  ```bash
  DURABLE_DATABASE_URL=sqlite://./durable.db \
  DURABLE_TRACES=duckdb DURABLE_LOGS=duckdb DURABLE_METRICS=duckdb \
  cargo run -p standalone-worker
  ```
- Root `README.md`: the plugin table and a local-first section. Each SDK's `README.md`: features and URL schemes.
- `AGENTS.md`: add `durable-sqlite` and `durable-duckdb` to the crates that test without Docker.
- `docs/bazel.md`: a note on the two new `-sys` crates if they need one.
- `docs/BACKLOG.md`: drop "DuckDB observability store", keep the `durable traces|logs` CLI queries, and record that
  the TypeScript SDK has no observability sinks yet (DuckDB is Rust-only for now).

## Build order

One commit per step, each leaving `./ci.sh` green:

1. `feat(sqlite): add the SQLite store plugin` (Part A).
2. `refactor(memory): back the memory backend with in-memory SQLite` (Part B).
3. `feat(sdk): select the store by URL scheme (sqlite://)` (Part C: Rust SDK, CLI, TypeScript SDK).
4. `feat(duckdb): add the DuckDB observability exporters` (Part D, the plugin).
5. `feat(observable): route signals to DuckDB from DURABLE_* variables` (Part D, the wiring).
6. `docs: local-first setup with SQLite and DuckDB` (Part E).

## Verification

- Per crate, no Docker: `cargo test -p durable-sqlite`, `-p durable-duckdb`, `-p durable-memory`,
  `-p durable-runtime`, `-p durable-api`, `-p durable-otel`.
- `cargo test -p durable-workflow --all-features` and `cargo test -p durable-cli`.
- `cargo fmt --all --check`, `cargo clippy --workspace --all-targets --all-features -- -D warnings`, and the two
  `cargo tree` checks in `ci.sh`.
- TypeScript: `cd durable-workflow-ts && npm ci && npm run build:native && npm run typecheck && npm test`.
- End to end: run `examples/standalone-worker` on SQLite and DuckDB, start a run with
  `durable --database-url sqlite://./durable.db start …`, kill and restart the worker to see the run resume, then
  query the `.duckdb` file.
- The full `./ci.sh` with Docker running, and `bazel build //plugins/durable-sqlite/... //plugins/durable-duckdb/...`.

## Risks

- **Build time.** Bundled DuckDB compiles a large C++ code base (minutes when cold), and `--all-features` in `ci.sh`
  always builds it. If that is too heavy, the fallback is to keep `duckdb` out of the example's default features.
- **Bazel.** `libsqlite3-sys` and `libduckdb-sys` run C/C++ build scripts under the hermetic toolchain. `aws-lc-sys`
  already builds this way, but these two have not been tried.
- **Joining logs to spans.** `opentelemetry-appender-tracing` 0.33 must put the trace and span ids on log records.
  If it does not, the log exporter copies `durable.run_id` from the enclosing span into the record's attributes.
- **Throughput of the memory backend.** It drops with Part B. If the drop hurts the test suite or the benchmark
  baseline, the fallback is to keep `MemoryStore` as a `#[doc(hidden)]` test double. *Outcome:* the test suites
  take about the same time; the benchmark drop was taken, with the optimizations above (Part B).

## Out of scope (recorded in docs/BACKLOG.md)

- `durable traces|logs|metrics` CLI queries over the DuckDB file.
- SQLite-backed `Queue` and `StreamStore`, for several worker processes on one file without the reconcile delay.
- Observability sinks for the TypeScript SDK.
- Retention for the DuckDB file.
