# Durable workflow framework for Rust — iteration 1 plan

## Context

The repo is an empty `cargo new` scaffold: `Cargo.toml` (package `durable-workflow`, edition 2024, no deps), `src/main.rs` (hello world) and `.gitignore`. Nothing is committed yet. Toolchain: rustc/cargo 1.97.1. Docker is available locally for integration tests.

Goal: a durable workflow framework that adapts to the user's infrastructure through plugins, not the other way round. It runs as a standalone worker process or embedded in an existing app. Iteration 1 backends are **PostgreSQL** (durable truth) and **Redis** (in-flight state, queue, pub/sub, cache). References:
- **Vercel Workflow**: the `World` = Storage + Queue + Streamer interface, event-log replay, hooks/webhooks, and the `world-testing` conformance suite.
- **Inngest**: memoized-step replay, a Redis queue with partitions and leases, Constraint-API flow control, and waitForEvent pauses.

Decisions confirmed with the user:
- **Decentralized library.** There is no central server; workers coordinate only through the backends. Standalone mode is a worker binary built with our runner. Embedded mode spawns the worker inside the host's tokio runtime.
- **Replay-based authoring.** Results of `ctx.step(...)` come back from the event log on resume. Determinism goes through `ctx` helpers.
- **Both async and sync paradigms**, so CPU-bound work (video/audio/image, batch data) is efficient:
  - async *and* sync (blocking) workflow functions;
  - async steps that run on tokio, and blocking steps that run on a dedicated, bounded compute pool, never on tokio worker threads.
- **Dependency injection.** App clients (mailer, Slack/Teams SDKs, DB pools) are registered on the worker. Workflows and steps reach them through `Dep<T>` macro arguments, `ctx.dep::<T>()`, or struct fields. Effectful deps can only be used inside steps.
- **PG is the source of truth; Redis is a rebuildable index.** Every state change is committed to PG first. Redis can be flushed without losing runs.
- **Layout.** Framework crates live in `crates/`. Every infrastructure plugin lives in `plugins/`.
- **Iteration 1 scope.**
  - Core: steps, retries/backoff, durable sleep, parallel steps, timeouts, cancellation, idempotent start.
  - Plus: signals/hooks/wait-for-event, flow control (concurrency, throttle, rate limit), and an ops HTTP API + CLI.
  - Out of scope: first-party integration clients (email/Slack/Teams). Users bring their own SDK clients as dependencies (see "Dependencies"); first-party ones can come later under `plugins/`. Also out: child workflows, cron, debounce/batching/priority, streaming (Vercel `getWritable`), blob offload for large payloads (a `BlobStore` plugin, e.g. S3), versioning/patching APIs, retention cleanup, a dashboard UI.

## Workspace layout

The root `Cargo.toml` becomes a virtual workspace: `members = ["crates/*", "plugins/*", "examples/*"]`, `resolver = "3"`, shared `[workspace.dependencies]`, edition 2024. `src/main.rs` is removed.

```
crates/                    framework: no infrastructure dependencies
  durable-core/            ids, records, event model, errors, every plugin trait
  durable-runtime/         replay engine (async + sync drivers), contexts, compute pool, Worker, Client, reconciler, standalone runner
  durable-macros/          #[workflow] (async fn or fn), #[step]
  durable-api/             axum ops router (mountable or standalone)
  durable-cli/             `durable` binary: migrate, runs, signal, event, start
  durable-testkit/         conformance suites for plugin authors, ManualClock, test harness
  durable-workflow/        facade; features: memory (default), macros (default), postgres, redis, api
plugins/                   infrastructure adapters; each implements durable-core traits only
  durable-memory/          Store, Queue, PubSub, Cache, RateLimiter in-memory (reference impl; tests/dev)
  durable-postgres/        Store (sqlx), migration runner and SQL
  durable-redis/           Queue (Lua), PubSub, Cache, RateLimiter
examples/
  embedded-axum/           existing axum app with its own sqlx pool and Redis; mounts the API, spawns the worker
  standalone-worker/       dedicated worker binary, including a CPU-bound sync workflow
docker-compose.yml         postgres:17 + redis:7 for local dev
```

Dependency rule:
- Plugins depend only on `durable-core` (plus `durable-testkit` as a dev-dependency, for the conformance tests).
- `durable-runtime` depends only on `durable-core`; it uses `durable-memory` only as a dev-dependency.
- New backends (MySQL, NATS, SQS, …) go in `plugins/` without touching the framework.

Key dependencies (latest at plan time):
- Async and runtime: tokio 1.53, futures.
- Postgres: sqlx 0.9 (runtime-tokio, postgres, uuid, chrono, json).
- Redis: redis 1.7 (tokio-comp, connection-manager, script).
- API and CLI: axum 0.8, clap 4.
- Serialization and types: serde/serde_json, uuid 1 (v7), chrono.
- Plumbing: async-trait (dyn-compatible plugin traits), thiserror 2, tracing.
- Determinism: rand + rand_chacha (seeded RNG).
- Tests: testcontainers-modules (postgres, redis).
- syn is at 3.x; confirm macro crate compatibility when writing durable-macros.

## Plugin architecture (durable-core traits, plugins/ impls)

A `Backend` (our counterpart of Vercel's "World") bundles trait objects. Each slot can be swapped on its own:

```rust
Backend::builder()
    .store(PostgresStore::from_pool(app_pool).schema("durable"))   // reuse the host's pool
    .queue(RedisQueue::new(redis_conn.clone()))
    .pubsub(RedisPubSub::new(redis_client.clone()))
    .cache(RedisCache::new(redis_conn.clone()))                    // optional
    .rate_limiter(RedisRateLimiter::new(redis_conn))               // optional
    .namespace("billing")                                          // PG column + Redis key prefix
    .build()?;
durable_memory::backend()                                          // tests and dev
```

Traits (all `#[async_trait]`, `Send + Sync + 'static`):

- **`Store`**: durable truth.
  - `create_run` is idempotent on `(namespace, workflow, idempotency_key)` and returns `Created | Existing`.
  - Reads: `get_run`, `list_runs` (filter + cursor), `load_events(run, after_seq)`.
  - **`commit(Commit)`** is the single atomic write path for executions. It takes the run row lock, checks the optimistic `expected_seq`, then: appends events; registers waits, hooks and event filters; consumes matching inbox signals (appended as `SignalReceived`); and updates status, `wake_at`, `awaiting` and output. It returns the new seq, any appended signal events, and the cancel flag. A seq mismatch returns `Conflict`: another worker owns the run, so we abort without side effects.
  - External inputs: `deliver_signal`, `resolve_hook(token)`, `publish_event` (matches waits, then inbox and wake), `request_cancel` and `retry_run`. Each one locks the run row, writes, and sets `wake_at = now()` if the run awaits it.
  - Reconciler support: `due_runs(before, limit)`.
  - Definitions: `upsert_definitions` / `get_definition`. Flow-control config is synced by workers so a start by name (API/CLI) can compute keys.
- **`Queue`**: dispatch index.
  - `enqueue(items)` is deduped per run and keeps the earliest due time. If the run is leased, it sets a `rerun` flag instead.
  - `claim(workflow, limits, max, lease)` enforces workflow- and key-level concurrency plus throttle atomically. It returns the leases and the next due time.
  - `extend` returns the leases that were lost. `ack(lease, next_wake)` releases and reschedules atomically, honoring `rerun`. `nack(lease, retry_at)`.
  - `requeue_expired(workflow)`.
- **`PubSub`**: `publish(channel, bytes)` / `subscribe(channel) -> Stream`. Used for queue nudges, run updates (`handle.result()`) and cancel notices.
- **`Cache`**: KV with TTL: `get/set/delete/incr` plus `try_lock(key, ttl)` for leader election. It holds the history cache, the in-flight state (step attempt and delivery counters, the worker currently running a step) and the reconciler lock.
- **`RateLimiter`**: GCRA `check(key, limit, period) -> Allowed | Denied { retry_at }` for start-time rate limiting.
- **`ComputePool`**: runs blocking jobs: `spawn(job) -> JobHandle`, plus a backlog gauge. The default `ThreadPool` in `durable-runtime` is a fixed size (`available_parallelism`, configurable) with a bounded queue. Adapters, for example rayon, can be added as plugins.
- **`PayloadCodec`**: `Value -> Value` encode/decode around every stored payload, so encryption or compression can wrap the JSONB columns. Iteration 1 ships the identity codec and a `max_payload_bytes` guard; large media should be passed by reference.
- **`Clock`**: `SystemClock`, plus `ManualClock` in the testkit.

`durable-testkit` exposes `store_conformance(factory)`, `queue_conformance(factory)` and others, mirroring Vercel's `world-testing`. Every plugin runs them in its own tests, so third-party plugins can prove compatibility. A useful consequence of "PG truth, queue is an index": `PostgresStore + MemoryQueue` already gives a durable single-process mode without Redis, because the reconciler rebuilds the queue from `wake_at` on boot.

## Event model

Per-run append-only log `(run_id, seq)`:
- `RunStarted`, `RunSucceeded{output}`, `RunFailed{error}`, `RunCancelled{reason}`, `RunRetried`
- `StepSucceeded{key, output}` and `StepFailed{key, attempt, error, retryable, retry_at}`
- `TimerScheduled{key, fire_at}` and `TimerFired{key}`
- `WaitStarted{key, kind: Signal|Hook|Event, name, timeout_at}`, `SignalReceived{key, signal_id, payload}` and `WaitTimedOut{key}`
- `HookCreated{key, token}`

Run statuses are `pending | running | waiting | succeeded | failed | cancelled`. The last three are terminal, and "finished" means any terminal status. Outcome names never say "completed", so success and failure can't be confused.

Step starts are not written to PG. Attempt and delivery counters, plus "currently running", live in Redis as in-flight state (`Cache.incr`). That protects against crash loops without paying an extra PG write per step. Losing Redis only resets the counters.

## Programming model: async and sync

Workflow bodies are orchestration code. Heavy work belongs in steps, and every step can be async (I/O) or blocking (CPU), in either kind of workflow.

```rust
// Async workflow: I/O-heavy orchestration, CPU work offloaded to the compute pool
#[workflow(name = "video.publish", concurrency(limit = 4, key = "/channel_id"), throttle(limit = 100, period = "1m"))]
async fn publish(ctx: WfAsyncCtx, job: Job) -> Result<Published, WorkflowError> {
    let meta = ctx.step("probe", move || probe(job.url.clone())).retry(RetryPolicy::exponential(5)).await?;   // tokio
    let renditions = ctx.map_blocking("transcode", meta.renditions, |r| transcode(r)).await?;  // compute pool, parallel, memoized per item
    let (a, b) = futures::try_join!(ctx.step("upload", ..), ctx.step("notify", ..))?;            // parallel async steps
    ctx.sleep("embargo", Duration::from_secs(3600)).await?;
    let approval: Option<Approval> = ctx.wait_for_signal("approval").timeout(Duration::from_secs(86_400)).await?;
    let hook = ctx.create_hook::<Payload>("callback").await?;   // hook.token() -> POST /v1/hooks/{token}
    let paid = ctx.wait_for_event::<Paid>("order.paid").filter(json!({"order_id": id})).await?;
    Ok(..)
}

// Sync workflow: plain blocking code on a driver thread, no async anywhere
#[workflow(name = "images.resize")]
fn resize(ctx: &WfSyncCtx, batch: Batch) -> Result<Report, WorkflowError> {
    let chunks = ctx.step("split", || split(&batch))?;               // blocking step -> compute pool
    let out = ctx.map("resize", chunks, |c| resize_chunk(c))?;       // parallel fan-out on compute pool
    let url = ctx.step_async("upload", || async { upload(&out).await })?;   // I/O on tokio; thread blocks
    ctx.sleep("cool-off", Duration::from_secs(60))?;
    Ok(report(url))
}
```

- **Contexts.**
  - `WfAsyncCtx` has `step` (async closure, runs on tokio), `step_blocking` (sync closure, runs on the compute pool) and `map_blocking`.
  - `WfSyncCtx` has `step` (sync, compute pool), `step_async` (on tokio, with the driver thread blocking), `map`, and `join(vec![..])` for heterogeneous parallel steps.
  - Both have the same sleep, wait, hook, event, `now`, `random` and `uuid` API.
- **Ambient step metadata.** `durable::step::current()` returns attempt, idempotency key, `is_cancelled()` and `heartbeat()`. It is a tokio task-local for async steps and a thread-local for blocking ones, so step closures stay `|| …`.
  - Blocking steps cannot be preempted. A step timeout or cancel marks the step as failed, sets the cancel flag and stops waiting. The pool slot stays busy until the closure returns, so long CPU steps should poll `is_cancelled()`.
- **Definition.** There are two traits, `AsyncWorkflow` and `SyncWorkflow`. Both carry `NAME`, `Input`, `Output`, `options()` and `required_deps()`, and both take `run(self: Arc<Self>, ctx, input)`, so a workflow can be a struct holding its own dependencies. `#[workflow]` accepts `async fn(WfAsyncCtx, …)` or `fn(&WfSyncCtx, …)` and generates the matching impl. Closures can be registered with `worker.register_fn` / `worker.register_sync_fn`.
- **Step keys and replay checks.** A step key is the user-given name plus an occurrence counter (`charge`, `charge#1`), assigned when the call is constructed. `map` items get `name[i]`. Vercel derives call-order ULIDs instead; name keys keep the history readable and tolerate inserting new steps. Matching is by key. A kind mismatch (history says sleep, code says step) or an output that no longer deserializes into `T` fails as `NonDeterminism`, our counterpart of Vercel's `ReplayDivergenceError`.
- **Step errors.** Any error is retryable by default. `Fatal(e)` stops retries, and `RetryAfter(d, e)` sets a custom backoff. When attempts are exhausted the step returns `Err(StepError)` to the workflow code, which can handle it or propagate with `?`. A panic inside a step is a retryable failure.
- **Hooks** behave as Vercel's do. `create_hook` accepts an optional deterministic token. A token already held by another live run resolves to `Err(HookConflict)`. `hook.recv()` can be called repeatedly for multiple payloads, and `hook.dispose()` releases the token.
- **Determinism.** `ctx.now()` returns the timestamp of the latest event consumed (Vercel's approach, with zero writes). `ctx.random*()` and `ctx.uuid()` come from a ChaCha RNG seeded by the run id plus a call counter.

## Dependencies: accessing app clients inside workflows

Workflows and steps need live handles: a mailer, a Slack/Teams SDK client, the app's DB pool, an HTTP client, config. These are **not** serialized or replayed. They are registered on the worker and shared by every execution (`durable-runtime/src/deps.rs`). In embedded mode, these are the same `Arc`s the host app already holds, for example in its axum `State`.

```rust
let mailer: Arc<dyn Mailer> = Arc::new(SmtpMailer::from_env()?);     // user's own client (lettre, SES SDK, …)
let worker = Worker::builder(backend)
    .dep::<dyn Mailer>(mailer.clone())        // trait object
    .dep(Arc::new(slack_client))               // concrete type (slack-morphism, reqwest wrapper, …)
    .dep(Arc::new(app_pg_pool.clone()))
    .dep_pure(Arc::new(AppConfig::load()?))    // deterministic and read-only: usable in the workflow body
    .register::<Onboarding>()                  // generated by #[workflow]
    .register(InvoiceWorkflow { billing })     // constructor injection
    .build()?;                                 // fails fast if any workflow's required dep is missing
```

There are three ways to reach a dependency. All three resolve against the same type map, `HashMap<TypeId, Arc<dyn Any>>`, and `T: ?Sized` allows `dyn Trait`.

1. **Macro injection.** Arguments typed `Dep<T>` are injected, and the one remaining argument is the input. The macro records each `Dep` type in `required_deps()`, which `build()` validates.
   ```rust
   #[workflow(name = "user.onboarding")]
   async fn onboarding(ctx: WfAsyncCtx, mailer: Dep<dyn Mailer>, cfg: Dep<AppConfig>, user: Signup) -> Result<(), WorkflowError> {
       let msg = render_welcome(&cfg, &user);                       // pure dep in the body: fine
       ctx.step("send-welcome", move || async move {
           mailer.send(msg).idempotency_key(step::current().idempotency_key()).await   // effectful dep inside a step
       }).await?;
       notify_sales(&ctx, user.id).await?;                         // #[step] fn: deps resolved from ctx
       Ok(())
   }

   #[step(retry = 5)]
   async fn notify_sales(slack: Dep<SlackClient>, user_id: UserId) -> Result<(), StepError> { … }
   ```
   `#[step]` turns a function into a wrapper `fn(&ctx, non-dep args)` that runs the body as `ctx.step("notify_sales", …)`. The same works in sync form with `&WfSyncCtx`.
2. **Context lookup.** `ctx.dep::<T>()` and `step::current().dep::<T>()` work for hand-written workflows and closures. A missing dep is an engine fault (configuration error), not a step failure, so the run resumes once a fixed worker deploys.
3. **Constructor injection.** Struct fields: `impl AsyncWorkflow for InvoiceWorkflow { async fn run(self: Arc<Self>, ctx, id) { let b = self.billing.clone(); ctx.step("charge", move || async move { b.charge(id).await }).await } }`. This is fully static, with no lookup.

Rules that keep this durable:
- **Effectful deps are guarded.** `Dep<T>` checks the step scope (the same task-local/thread-local as `step::current()`) when dereferenced. Calling a mailer in the workflow body, where it would re-run on every replay, fails immediately with "dependency `dyn Mailer` used outside a step". That turns a silent duplicate-send bug into an engine fault the user can see. Deps registered with `.dep_pure()` skip the guard.
- **Multiple instances** of one type use newtypes (`struct MarketingMailer(Arc<dyn Mailer>)`). Per-tenant clients use a factory dep: `ctx.dep::<SlackClients>()?.for_tenant(&t)`.
- **Error classification.** An error returned from a step is retryable by default. A `ClassifyError` trait lets dependency errors pick retryable, fatal or retry-after. Built-in impls behind features: `reqwest` treats 429 (honoring Retry-After) and 5xx as retryable and other 4xx as fatal; `sqlx` treats serialization failures and deadlocks as retryable.
- **Lifecycle.** The worker drains in-flight steps before dropping its deps. In embedded mode the host keeps ownership of the shared `Arc`s.
- **Testing.** The test harness swaps deps for fakes, e.g. `.dep::<dyn Mailer>(Arc::new(FakeMailer::default()))`. Tests then assert side-effect counts across forced replays and crashes.

## Replay engine (the core mechanism)

The engine core (history, resolved results, the frontier, step tasks and commits) is **driver-agnostic**. Two drivers produce commands and consume resolutions:

- **`AsyncDriver`.** It polls the workflow future. Each ctx future stores its `Waker`. A custom root waker sets a flag, and the engine re-polls until quiescent. The future is **disposable, a pure function of history**: the engine owns the truth, so it can drop and rebuild the future at any time.
- **`SyncDriver`.** It runs the sync body on a driver thread. Driver threads are separate from the compute pool, so a blocked body can never deadlock its own steps, and they are bounded by worker concurrency.
  - Each ctx call sends a command over a channel and blocks for the resolution.
  - `map` and `join` submit all their commands at once.
  - To suspend or rebuild, the engine answers blocked calls with `Err(WorkflowError::Interrupted)`, which user code propagates with `?`. Every later ctx call fails the same way, and the engine ignores whatever an interrupted body returns.

Execution loop, per claimed run:
1. **Load.** The engine loads the run and its history, from the cache when `cache.last_seq == run.last_seq`, otherwise from the store, then starts the driver.
2. **Replay.** It feeds history events one at a time in seq order. Feeding in order keeps `select!`/race results deterministic.
3. **Frontier.** At quiescence, the live, unresolved commands are the frontier, which covers parallel discovery as Inngest's planning does. The engine:
   - spawns async steps on tokio and blocking steps on the `ComputePool` (each with its timeout);
   - fires due timers and wait timeouts by appending `TimerFired` or `WaitTimedOut`;
   - commits new waits, hooks and timers.
4. **Continue in-process.** As each step finishes, the engine commits `StepSucceeded` or `StepFailed` (one `Store::commit` with `expected_seq`), feeds the result and continues. A run does **not** replay between steps while it stays on one worker; it replays only on resume. Inngest's newer checkpointing and Vercel's retained VM made the same choice.
5. **Retries.**
   - Short backoff (≤ an inline threshold): the engine waits, then rebuilds the driver from in-memory history (cheap). That re-creates the `FnOnce` step closure. Steps still in flight survive the rebuild because the engine owns the tasks.
   - Long backoff: suspend with `wake_at = retry_at`.
6. **Drops.** Commands whose futures were dropped (select losers) and not re-registered after a poll have their tasks aborted, and late results are discarded.
7. **Suspend** when only waits and timers remain, or on a time/step budget, or when the lease is nearly gone. The engine commits `wake_at = min(timers, timeouts, deadline)` and `awaiting = [signal names]`, writes the history cache, then `queue.ack(lease, wake_at)`.
8. **Finish.** The engine commits `RunSucceeded` or `RunFailed`, publishes on `run:{id}`, then acks.
9. **Engine faults.**
   - A panic in the workflow body, a `NonDeterminism` error, or an async root blocked on a non-ctx future ("awaited a non-durable future" diagnostic): the engine records `last_error` on the run, then nacks with capped exponential backoff. The run stays visible as stuck, and a code fix resumes it, as in Temporal.
   - A delivery counter in Redis fails a run with `MaxDeliveriesExceeded` after N crashed executions (Vercel caps deliveries at 48), so a poison run cannot crash-loop workers.
10. **Cancel.** The commit outcome or a pub/sub notice reports the cancel. The engine aborts async tasks, flags blocking steps as cancelled and commits `RunCancelled`.

**Back-pressure.** The claim loop stops claiming new runs while the compute backlog is above a threshold. The lease keeper extends leases for runs waiting on long CPU steps.

Step side effects are at-least-once. The step idempotency key (`run_id:step_key`) is exposed for external APIs.

## Postgres plugin (plugins/durable-postgres)

- **Migration runner.** Our own small runner: embedded SQL via `include_str!`, a `{schema}`-templated `durable.schema_migrations` table, and `pg_advisory_xact_lock`. It does not use `sqlx::migrate!`, because that would collide with the host app's `_sqlx_migrations`.
- **Queries.** Runtime `sqlx::query` (no compile-time `DATABASE_URL` for library users), with SQL strings prebuilt once per configured schema.
- **Tables**:
  - `runs`: `last_seq` (optimistic version), `wake_at` (the runnable index; it doubles as the state-derived outbox), `awaiting text[]`, `concurrency_key`, `throttle_key`, `deadline_at`, `cancel_requested_at`, `last_error`, input/output/error JSONB, timestamps. Indexes: a unique partial index on the idempotency key, a partial index on `wake_at` where it is not null, and listing indexes.
  - `run_events (run_id, seq)` primary key.
  - `signals`: the inbox, with `consumed_seq`.
  - `hooks`: `token` primary key.
  - `event_waits`: `(namespace, event_name)` index. Matching uses `event_data @> filter` (JSONB containment) instead of Inngest's CEL plus aggregate evaluator. As in Inngest, a wait only sees events that arrive after it is committed.
  - `workflow_definitions`.
- **Lost wake-ups.** `commit` and every external input take `SELECT … FOR UPDATE` on the run row, so a signal can never land between "check inbox" and "set waiting".

## Redis plugin (plugins/durable-redis)

- **Keys.** Hash-tagged per workflow (`dw:{ns:wf}:…`) so every claim script is single-slot and Redis Cluster stays viable:
  - `ready` ZSET: partition → earliest due;
  - `part:{key}:resume` and `part:{key}:start` ZSETs: run → due;
  - `inflight` and `inflight:{key}` ZSETs: lease token → lease expiry;
  - `item:{run}` HASH: partition, kind, lease token (generation), rerun;
  - `throttle:{key}`: GCRA state.
- **Lua scripts**: `enqueue`, `claim`, `extend`, `ack`, `nack` and `requeue_expired`, loaded via `redis::Script` (EVALSHA with fallback). These follow Inngest's `redis_state/lua/queue` design, simplified.
  - `claim` scans due partitions oldest-first.
  - It drains `resume` before `start`, so in-flight runs win over new ones; Inngest weights non-start backlogs 10:1.
  - Capacity is `limit − ZCOUNT(inflight, now, +inf)`, checked at both workflow and key level, as in Inngest's Constraint API. A crashed worker's slots free themselves when its leases expire.
  - A partition at capacity is pushed back in `ready` to avoid head-of-line blocking.
  - A stale lease token cannot ack or extend.
- **Flow-control semantics match Inngest.**
  - Concurrency counts *executing* runs only; sleeping or waiting runs hold no slot.
  - Throttle is GCRA on the `start` set only, and it delays runs rather than dropping them.
  - Rate limit is a lossy GCRA in `Client::start`, which returns `StartOutcome::RateLimited`.
- **Pub/sub** uses a dedicated connection. Workers wake on `nudge:{wf}`, with a jittered poll interval as fallback.
- **Cache** includes `SET NX PX` locks.

## Worker, client, runner

- **`Worker::builder(backend).register::<W>().concurrency(64).compute_threads(n).lease(30s).build()`** runs these tasks:
  - claim loop, per registered workflow. Workers only claim what they registered, which is how fleets route. For example, CPU-heavy workflows can be registered only on standalone workers running on compute nodes, while light ones stay embedded in the app.
  - lease keeper;
  - reconciler, leader-elected via `Cache::try_lock`: it re-enqueues overdue `wake_at` runs (dedup makes this idempotent) and requeues expired leases;
  - definition sync on boot.
- **Graceful shutdown**: stop claiming, drain until a timeout, then nack anything still in flight so other workers resume it promptly.
- **Embedded**: `worker.spawn() -> WorkerHandle { shutdown() }`.
- **Standalone**: `durable_workflow::standalone::run(worker, Config::from_env())`. It handles SIGTERM/SIGINT, health/readiness endpoints, the optional ops API and tracing setup.
- **`Client`** works without a worker (for example in a web tier):
  - `start::<W>(input).id().idempotency_key()` and `start_raw(name, json)`;
  - `RunHandle::result()`, using pub/sub with a polling fallback;
  - `get_run`, `list_runs`, `events`, `cancel`, `signal`, `resume_hook`, `send_event`, `retry` (re-runs a failed run from its failure point: succeeded steps stay memoized and failed steps get a fresh budget).

## Ops API and CLI

- **`durable_api::router(client) -> axum::Router`.** Host apps `nest("/durable", …)` and wrap it with their own auth layers. The standalone runner can serve it with an optional bearer token. Routes:
  - `POST /v1/workflows/{name}/runs`
  - `GET /v1/runs`, `GET /v1/runs/{id}` (merged with in-flight state from Redis), `GET /v1/runs/{id}/events`
  - `POST /v1/runs/{id}/cancel|retry|signals/{name}`
  - `POST /v1/hooks/{token}` (webhooks)
  - `POST /v1/events`
  - `/healthz`, `/readyz`
- **`durable` CLI** (clap) talks to the backends directly, since there is no server: `migrate`, `runs list|get|events|cancel|retry`, `signal`, `event send`, `start`.

## Build order

1. Workspace scaffolding (`crates/`, `plugins/`, `examples/`), `docker-compose.yml`, CI script (fmt, clippy `-D warnings`, test).
2. `durable-core` (types, events, errors, traits), then `plugins/durable-memory`.
3. `durable-runtime`:
   - async driver, engine core, compute pool, deps (type map, `Dep<T>` step-scope guard, `ClassifyError`), Worker and Client, all on the memory backend;
   - then the `SyncDriver` and `WfSyncCtx`.

   Most unit tests go here.
4. `durable-testkit` conformance suites, then `plugins/durable-postgres`, then `plugins/durable-redis`. Each must pass the suites.
5. Signals, hooks and events plus flow control end-to-end on PG + Redis.
6. `durable-macros` (async and sync forms), `durable-api`, `durable-cli`, the standalone runner.
7. Examples and README (architecture, async vs sync guidance, determinism rules, at-least-once semantics, plugin authoring).

## Verification

- **Engine unit tests** (memory backend + `ManualClock`), each in **both async and sync** form:
  - memoization (a step body runs once across resumes);
  - parallelism (`try_join!` / `join` / `map`);
  - deterministic `select!` (async);
  - inline vs suspended retries, `Fatal` and exhausted retries;
  - sleep suspend and resume;
  - signal, hook and event waits with timeouts;
  - cancellation mid-step;
  - run deadline;
  - non-determinism diagnostics;
  - idempotent start;
  - an interrupted sync body that unwinds cleanly.
- **Dependency tests**:
  - all three access styles (macro `Dep<T>`, `ctx.dep`, constructor injection), in async and sync;
  - `build()` rejects a missing required dep;
  - an effectful dep used in the workflow body raises an engine fault with a clear message, while `dep_pure` works in the body;
  - a fake mailer records exactly one send across forced replays, and at least one across a crash between send and commit;
  - `ClassifyError` mapping for reqwest 429/5xx/4xx (wiremock) and sqlx errors.
- **Compute isolation tests**:
  - CPU-burning blocking steps saturating the pool must not delay tokio work. Lease heartbeats and a concurrent async step stay on time.
  - Pool size bounds parallel `map` work.
  - A blocking-step timeout marks the step failed, and the cancel flag is observed.
- **Conformance suites** pass for memory, Postgres and Redis (testcontainers, which needs Docker), including the lost-wake-up race: concurrent `deliver_signal` against `commit`.
- **Chaos e2e on PG + Redis**:
  - 3 workers in one test process, hundreds of mixed async and sync runs with steps, sleeps and signals;
  - abort a worker mid-step: assert every run completes and no committed step re-executes;
  - an atomic counter shows concurrency limits are never exceeded;
  - throttle spacing holds;
  - `FLUSHALL` Redis mid-test: the reconciler recovers every run.
- **API and CLI**: router tests via `tower::ServiceExt::oneshot`; CLI smoke test against docker-compose.
- **Examples**: run `examples/embedded-axum` against docker-compose, start a run with curl, signal it, and watch it complete via `GET /durable/v1/runs/{id}`. Run `examples/standalone-worker` with the sync image-resize workflow.
- `cargo fmt --check`, `cargo clippy --workspace --all-targets -D warnings` and `cargo test --workspace` all pass.
