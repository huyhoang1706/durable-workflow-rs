# Iteration 3: a sans-IO core for TS/JS and Python SDKs

> Status: P0–P2 and P3a are done. P3 (the TypeScript binding, `bindings/typescript`) is designed below and its first
> slice (S1) is built. The design sections below are the original sketch. The
> "as built" sections record where the code differs. `durable-core` was renamed `durable-spi` after P1 (#9); the
> as-built notes keep the name used at the time.

## Context

Workflows can only be written in Rust today. Most users want TypeScript or Python. Temporal (`sdk-core`)
and Restate (`sdk-shared-core`) solve this the same way: replay logic lives once, in Rust, as a
passive state machine with no IO; each language SDK is a thin shell that runs user code and talks to
that state machine.

We are closer to this than it looks. `durable-runtime/src/core.rs` already works on keyed commands,
`feed(event)`, and a `frontier()` of unresolved commands. What ties it to Rust is:

| Coupling | Where | Why it blocks other languages |
|---|---|---|
| `CmdFuture` + `Waker` | `core.rs` | Resolution is delivered by waking a Rust future; JS/Python have their own promise machinery. |
| `StepStart` closures inside commands | `core.rs` `StepSpec.start` | The step body is stored in the core; a JS function cannot live there. |
| `Deps`, `Arc<Mutex<…>>` | `core.rs` | Host concerns inside replay state. |
| Decisions mixed with IO | `engine.rs` `run_loop`, `on_step_done`, `registrations`, `next_wake` | The "what to do next" logic (retry math, timers due, suspend vs wait inline) is only callable from inside the tokio engine. |
| Driver polls after every event | `engine.rs` `rebuild` | Fine for a Rust future (a poll is a function call), too costly for JS where reaching quiescence means draining the microtask queue. |

Decisions confirmed with the user:
- **Handles in the in-process API, keys everywhere else.**
  - `Replayer` ctx calls return an opaque `Handle(u32)`, and resolutions come back as `(Handle, Resolution)`.
    Only the core knows the key scheme (`name`, `name#1`, `name[0]`, `key[recv]`, `key[dispose]`), so it
    can change without breaking any SDK, and no SDK can compute a key differently from another.
  - Keys stay what history stores. They are exposed in `frontier()`, faults, logs/OTel, and in the future HTTP
    protocol, where each request is a fresh process and a handle would mean nothing.
- **Rust workflows switch to the new core in P0.** There is no feature flag and no period with both
  cores. The existing test suite is the safety net.

## Goals

1. A crate, **`durable-sm`**, holding all replay and decision logic, with no tokio, no futures, no IO
   and no `Arc`. Its dependencies: `serde`, `serde_json`, `chrono`, `rand_chacha`, `uuid` (it owns the event model
   itself, see "P0 as built").
2. Every input and output type is `Serialize + Deserialize`, so bindings can pass structs (napi/PyO3)
   or JSON (HTTP, WASM) without hand-written glue.
3. The Rust SDK becomes the first client of `durable-sm`, with no user-visible change. The existing test
   suite is the regression gate.
4. The worker (store, queue, lease, Redis, streams, OTel) stays in Rust for every language. Only the
   replay half crosses the FFI boundary.

Non-goals for this iteration: a deterministic JS sandbox (Temporal-style V8 isolates), WASM
builds, and the HTTP/serverless protocol. The design must not rule them out.

## Shape: two state machines and a shell

```
           user code (Rust | TS | Python)
                 │ ctx.step / sleep / wait / hook          ▲ resolve(handle, value)
                 ▼                                          │
        ┌───────────────────── durable-sm ─────────────────────────┐
        │  Replayer   – one per execution; matches commands to     │
        │               history, owns now/random/uuid, detects     │
        │               non-determinism, exposes the frontier      │
        │  Decider    – pure policy: frontier + now + running →    │
        │               what the engine must do next               │
        └──────────────────────────────────────────────────────────┘
                 ▲ events, step results                    │ Effects
                 │                                          ▼
        engine shell (durable-runtime, tokio): Store, Queue, lease,
        step tasks, timers, cancel subscription, streams, observer
```

- **Replayer** replaces `Core`. It is what the language SDK's `ctx` calls into.
- **Decider** is `run_loop` steps 1–4 plus the event-building part of `on_step_done`, lifted out as pure functions.
- **Shell** is what is left of `engine.rs`: it performs effects and feeds results back in.

For in-process bindings (napi-rs, PyO3) only the Replayer API and step execution cross the boundary. The
Decider and shell run in Rust, inside the embedded worker.

## Replayer API

```rust
/// Opaque per-execution id for an issued command. Hosts map it to a Promise / Future / asyncio.Future.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Handle(pub u32);

pub struct Replayer { /* the Inner of today's Core, minus wakers, closures and deps */ }

impl Replayer {
    pub fn new(run_id: RunId, history: Vec<Event>) -> Self;     // history is already `normalize`d

    // ---- called by user code through ctx (synchronous, never blocks) ----
    pub fn step(&mut self, name: &str, opts: StepOpts) -> Handle;
    pub fn sleep(&mut self, name: &str, dur: Duration) -> Handle;
    pub fn wait(&mut self, kind: WaitKind, name: &str, filter: Option<Value>,
                timeout: Option<Duration>) -> Handle;
    pub fn create_hook(&mut self, name: &str, token: Option<String>) -> Handle;
    pub fn dispose_hook(&mut self, hook: Handle) -> Handle;
    /// Generated child keys (`name[0]` for map, `key[recv]` for hook recv).
    pub fn step_at(&mut self, parent: &str, index: u32, opts: StepOpts) -> Handle;
    /// The awaiting promise was abandoned (select/race loser, GC). Same as `CmdFuture::drop` today.
    pub fn release(&mut self, h: Handle);

    pub fn now(&self) -> DateTime<Utc>;
    pub fn random_u64(&mut self) -> u64;
    pub fn uuid(&mut self) -> Uuid;
    pub fn is_replaying(&self) -> bool;

    /// The body returned or threw. A thrown error is the workflow's error.
    pub fn finish(&mut self, result: Result<Value, String>);

    // ---- called by the host / shell ----
    /// Deliver as much history as possible without running user code (see "Feeding").
    pub fn advance(&mut self) -> Advance;
    /// Resolutions produced since the last call, in history order. The host settles the matching promises.
    pub fn take_resolved(&mut self) -> Vec<(Handle, Resolution)>;
    /// Append live events (just committed by the shell) to the tail of history.
    pub fn push_live(&mut self, events: &[Event]);
    pub fn frontier(&self) -> Vec<Pending>;                     // as today, plus `handle`
    pub fn status(&self) -> ReplayStatus;                       // Running | Finished(result) | Fault(msg)
    pub fn interrupt(&mut self);
    pub fn cancel(&mut self);
}

#[derive(Serialize, Deserialize)]
pub struct StepOpts {
    pub retry: Option<RetryPolicy>,
    pub timeout: Option<Duration>,
    pub kind: StepKind,   // Async | Blocking — a hint for the Rust host; other hosts ignore it
}

#[derive(Serialize, Deserialize)]
pub enum Resolution {                     // unchanged, minus the Rust-only StepError type
    Step(Result<Value, StepFailure>),     // StepFailure { message, class }
    Slept,
    Signal(Option<Value>),                // None = timed out
    Hook { token: String, conflict: bool },
    Disposed,
    Aborted(AbortReason),                 // Interrupted | Cancelled | Fault(msg)
}

pub enum Advance {
    /// The next history event is for a key the code has not issued yet. Settle `take_resolved()`,
    /// let user code run until it is quiescent, then call `advance()` again.
    NeedsQuiescence,
    /// All history delivered. From here on the engine drives the run (frontier → Decider).
    Live,
}
```

Errors on the ctx calls (invalid name, duplicate key, interrupted, cancelled, fault) do not return
`Result`. They set `status()` and the handle resolves with `Resolution::Aborted(reason)`, the way
`CmdFuture::poll` returns `Interrupted` / `Cancelled` / `Fault` today. This keeps every binding to a single
code path: one await, one resolution.

### What leaves the core

- **Step bodies.** The core only knows a step's key and options. The host keeps `key → body` itself:
  a `Map<string, () => Promise<unknown>>` in JS, a dict in Python, and a `HashMap<String, StepStart>` in the
  Rust host. A JS/Python function can be called again, so retrying a step no longer needs the
  `needs_rebuild` path. The Rust host keeps it, because its closures are `FnOnce`. It reports
  `StepBodyUnavailable` and the shell rebuilds, as today.
- **Deps, streams and step context** (`step::current()`). These are host-side; the shell passes the
  idempotency key and attempt when it launches a step.

### Feeding: resolve in history order, but only stop when blocked

Today `rebuild` polls the driver after **every** event. With JS, each poll means a round trip through
the event loop. `advance()` keeps the same semantics but batches:

```
loop over history from the cursor:
    ev.key() is None                    → apply (now = ev.at), continue
    ev.key() is issued                  → apply, queue the resolution, continue
    ev.key() not issued yet             → return NeedsQuiescence   (code has to reach that call first)
    ev.key() is issued, but the code moved past it without the event (it released it)
                                        → still apply: matches today, where dropped cmds still resolve
history exhausted                       → Live
```

`now()` is the `at` of the latest applied event, as today.

Resolutions are handed out in history order, so a `Promise.race(sleep, signal)` replays to the same
winner. The number of quiescence rounds equals the number of sequential await stages in the code, not the
number of events. A workflow that runs 200 steps through `Promise.all` replays in 2 rounds instead of 200.

Non-determinism detection is unchanged: an event whose key the code never issues, once the code is
quiescent and blocked, is a fault ("history has an event for `x` but the workflow did not issue it").
So is a kind mismatch.

### Host loop (any language)

```
r = Replayer::new(run_id, history)
start user body (it calls ctx → r.step(...) and awaits the returned handle's promise)
loop {
    await quiescence                       // Rust: poll until Pending; JS: drain microtasks; Py: run loop until idle
    match r.advance() {
        NeedsQuiescence => settle(r.take_resolved()); continue,
        Live            => settle(r.take_resolved()); await quiescence; break,
    }
}
// Live phase: shell asks Decider, performs effects, push_live(new events),
// settle(take_resolved()), await quiescence, repeat — until status() != Running.
```

"Await quiescence" is the only per-language primitive that is hard to get right:

- **Rust**: today's `AsyncDriver::poll` (loop while the wake flag is set) and `SyncDriver::quiesce`.
- **Node**: settle promises, then `await new Promise(r => setImmediate(r))`. All microtasks have run by
  then. User code may only await ctx promises, the same rule as today's "awaited a non-durable future"
  fault. An empty frontier after quiescence still raises that fault.
- **Python**: run on a dedicated asyncio loop. After settling futures, step the loop until
  `loop._ready` is empty, or use a small custom loop class. Sync workflows use a thread, like `SyncDriver`.

## Decider API

Pure functions. Each takes a snapshot and returns what to do; the shell does it.

```rust
pub struct DecideInput<'a> {
    pub frontier: &'a [Pending],
    pub now: DateTime<Utc>,
    pub running: &'a HashSet<String>,       // step keys with a live attempt
    pub inbox_checked: bool,
    pub deadline_at: Option<DateTime<Utc>>,
    pub inline_threshold: Duration,
}

pub enum Decision {
    /// run_loop step 1: register timers/waits/hooks (and sync the inbox for open waits)
    Register { events: Vec<EventKind>, hooks: Vec<HookReg> },
    /// step 2: due timers and wait timeouts
    Fire { events: Vec<EventKind> },
    /// step 3: launch these attempts (key, attempt, timeout)
    Launch(Vec<StepLaunch>),
    /// step 4, nothing running and the next wake is far off: release the lease
    Suspend { wake_at: Option<DateTime<Utc>> },
    /// step 4, something is running or the next wake is close: wait in-process until this time
    Idle { until: Option<DateTime<Utc>> },
    DeadlineExceeded,
    /// empty frontier while Running
    Fault(String),
}

pub fn decide(input: &DecideInput) -> Decision;

/// The event-building half of `on_step_done`: retry math, RetryAfter, payload limit.
pub fn step_outcome(policy: &RetryPolicy, attempt: u32, result: Result<Value, StepFailure>,
                    now: DateTime<Utc>, max_payload_bytes: usize) -> EventKind;
```

`run_loop` becomes a `match decide(..)` whose arms are only IO. This is also what an HTTP/serverless
executor needs later: the same decisions, with "launch" meaning "call the SDK endpoint".

## How a TS workflow maps onto it

```ts
import { workflow, step } from "@durable/typescript";

export const onboarding = workflow("user.onboarding", { timeout: "7d" }, async (ctx, user: Signup) => {
  await ctx.step("send-email", () => mailer.send(user.email, "Welcome!"), { retry: 5, timeout: "30s" });
  const verified = await ctx.waitForSignal<boolean>("verified", { timeout: "1d" });
  return verified ? "verified" : "reminded";
});
```

- `ctx.step(name, fn, opts)` → `h = replayer.step(name, opts)`, `bodies.set(key(h), fn)`, return `promiseFor(h)`.
- On `Decision::Launch`, the Rust shell calls back into JS through a napi `ThreadsafeFunction`
  `(key, attempt, idempotencyKey) → Promise<json>`. The result goes through `step_outcome` → commit →
  `push_live` → the promise settles.
- `Date.now()` and `Math.random()` are not patched in v1. The docs point to `ctx.now()` and `ctx.random()`, and an
  opt-in dev check warns if they are called during replay.
- Payloads are JSON (`serde_json::Value` ↔ JS values via napi's serde support), the same as the
  Rust SDK's `Value` boundary.

Python is the same shape with PyO3: `ctx.step(...)` returns an `asyncio.Future`, and steps run on the loop
or, for sync `def` steps, in a thread pool.

## Plan

| Phase | Work | Done when |
|---|---|---|
| P0 ✅ | Create `crates/durable-sm`. Move `Core` → `Replayer` (handles, `advance`, no wakers/closures/deps). `CmdFuture` becomes a ~30-line adapter: handle + waker map in `durable-runtime`. | Whole existing suite passes unchanged. `durable-sm` has no tokio/futures dependency (CI check). |
| P1 ✅ | Lift `decide` and `step_outcome` out of `engine.rs`. `run_loop` matches on `Decision`. | Suite passes. Decider unit tests cover each arm without a backend. |
| P2 ✅ | **Replay conformance corpus**: record histories plus the expected command sequences as JSON in `durable-testkit`, generated from the Rust workflows in the test suite. | Every binding must pass this corpus. It is the cross-language contract. |
| P3a ✅ | **Host abstraction** (see "P3a: hosting workflows written in other languages"): the engine runs any workflow through `HostWorkflow` / `HostExecution`, and Rust becomes the first host. | Suite and corpus pass unchanged. A scripted host with no Rust futures runs corpus workflows end to end on the real engine. |
| P3 | `bindings/typescript` (napi-rs), see "P3: the TypeScript binding": embedded `Worker` + `Client`, `workflow()`, `ctx.step/sleep/waitForSignal/waitForEvent/createHook`, Postgres/Redis/memory backends selected by URL. | An `examples/typescript-onboarding` passes the conformance corpus and a crash/resume chaos test. |
| P4 | `bindings/python` (PyO3 + maturin), same surface. | Same as P3. |
| Later | HTTP executor (Inngest-style), using `durable-sm` compiled to WASM in the SDK so serverless functions can replay. | — |

## P0 as built

P0 landed on `feat/sm-core-p0`. The full suite passes, and so do the Postgres/Redis plugin and chaos suites against
local servers. `chaos::streaming_steps_finish_with_correct_output_despite_worker_kills` fails about 1 run in 4
with "stream closed at the end", and it fails the same way on `main`, so it is a pre-existing flake, not a
regression. Where the code differs from the sketch above:

- **`durable-sm` also owns the event model and `RetryPolicy`.** `durable-core` depends on tokio and futures, so
  the replayer cannot depend on it. `Event`, `EventKind` and `WaitKind` moved to `durable-sm`; `durable-core`
  (now `durable-spi`) and `durable_runtime::retry` re-export them, so every public path is unchanged. `ci.sh` fails if `durable-sm`
  ever pulls in tokio, futures or async-trait.
- **Keys are reserved apart from issuing.** The API is `user_key(name)` / `key_for(name)`, then
  `issue(key, Command) -> Handle`. Rust builders take their key when `ctx.step(..)` is called but issue the
  command on first poll, after `.retry()` / `.timeout()`. Merging the two would renumber `#n` keys for code that
  creates builders in one order and awaits them in another, which breaks stored histories. The one-call
  `step(name, opts)` the bindings want is a thin wrapper, added with the Node binding.
- **Aborts are a state, not a resolution.** `abort() -> Option<Abort>` (`Interrupted` beats `Cancelled` beats
  `Fault`) is checked before a handle's resolution, as `CmdFuture::poll` always did.
- **`advance()` stops after every resolving event**, and returns `RunCode` / `Live`. Batching several
  resolutions per round is not safe for Rust: `select!` and `join!` choose among ready futures in their own
  order, so delivering a timer and a signal together could pick a different winner than the live execution
  did. Non-resolving events (`TimerScheduled`, `WaitStarted`, retried `StepFailed`) are still applied without
  running the code. A batched mode for hosts whose promises settle in a defined order (Node) is a P3 question.
- **Not added yet:** `finish(result)` (the driver still returns the body's result) and handles in
  `frontier()`. Neither is needed until a binding exists.
- **Rust host** (`durable-runtime/src/core.rs`): it is now `Mutex<Replayer>` plus `Handle → Waker` and
  `key → StepStart` maps. The engine replays and delivers live commits through the same
  `while core.advance() == RunCode { driver.poll() }` loop.

## P1 as built

P1 landed on `feat/sm-decider-p1`. `durable-sm/src/decide.rs` now holds `decide`, `step_outcome` and
`open_waits`, and `run_loop` in `engine.rs` is a `match` on `Decision` whose arms do only IO. The full suite
passes against local Postgres/Redis, apart from the known streaming chaos flake. Where the code differs from the
sketch above:

- **`Decision` has `Run` and `Suspend` instead of `Launch` and `Idle`.** Launching and waiting always come
  together: `Run { launch, wake_at }` means "start these attempts in order, then wait in-process until a step
  finishes or `wake_at`". `Suspend { wake_at }` is only returned when nothing is running and nothing is launched.
  `wake_at` already ignores the retry time of steps that are running or being launched, as the old code did by
  computing it after launching.
- **The host still decides about spent step bodies.** The decider cannot know that a Rust `FnOnce` body was taken
  by an earlier attempt. The engine launches the listed attempts up to the first spent body, then rebuilds,
  exactly as before. JS and Python hosts can call a body again and never need this.
- **Hook tokens are minted by the shell.** `Register` carries `HookRequest { key, token: Option<String> }`. A
  missing token becomes a fresh UUIDv7 in the engine, because it is random and the decider is pure.
- **`open_waits` was lifted too.** It is pure and every commit needs it, including from the HTTP executor later.
- **More pure types moved to `durable-sm`:** `ErrorClass`, `WaitReg`, `far_future` and `saturating_add`.
  `durable-core` and `durable-runtime` re-export them. `StepError` and `ClassifyError` stay in `durable-runtime`:
  the built-in `ClassifyError` impls for `reqwest` and `sqlx` errors would break the orphan rule if the trait moved.
  `step_outcome` takes `Result<Value, (String, ErrorClass)>` instead.
- **Fault, cancel and "body finished" checks stay in the engine,** ahead of `decide`. They depend on driver and
  store state, not on the frontier.

## P2 as built

The corpus lives in `crates/durable-testkit/corpus/*.json`, 22 cases. Each one holds a normalized history written by
a real run on the memory backend, and what replaying that history did. Where the code differs from the sketch above:

- **The contract is a trace of replayer calls, recorded by `durable-sm` itself.** `Replayer::record()` turns on a
  log of `TraceOp`s: every `user_key` / `key_for` / `check_name` with the key it returned, every `issue` and
  `release`, every `random_u64` / `uuid` with its value, host `set_fault`, `interrupt` and `cancel`, and every
  `advance` with its result and the resolutions it produced (by key). A case ends with the `frontier`, `fault` and
  `result` the replay left behind. Recording lives in the replayer rather than in each host, so every binding gets
  the same trace for free and can compare its own replay against the corpus.
- **Two checks, both in CI.** `durable_testkit::corpus::play` drives a bare `Replayer` with the host calls from a
  trace and checks each answer: a binding runs the same check through its FFI layer. The `replay_corpus` test in
  `durable-runtime` replays each committed history through its Rust workflow and must reproduce the trace exactly.
  `DURABLE_UPDATE_CORPUS=1` reruns the workflows and rewrites the corpus.
- **`Worker::replay(workflow, run_id, input, history)`** (`#[doc(hidden)]`) replays a stored log offline with
  recording on. `rebuild` and it share one loop, `driver::replay`, so the corpus records exactly what the engine does.
- **Errors are compared by kind, as Temporal (failure type) and Restate (failure code) do.** `result` is
  `{"ok": value}` or `{"err": {"kind", "message"}}`. `WorkflowFailure` and `FailureKind` (`failed`, `step`,
  `hook_conflict`, `cancelled`, `interrupted`, `fault`) live in `durable-sm`, and `WorkflowError::failure()` maps the
  Rust errors onto them. Bindings must match `ok` values and `err` kinds. They do not have to match messages the host
  writes (`err`, `set_fault`, host faults). Faults raised by the replayer come from the shared core, so their
  messages must match.
- **The wire shape was settled before any binding exists**, following Restate's WASM bindings:
  - Internally tagged enums everywhere: a step resolves to `step_succeeded { value }` or
    `step_failed { failure: { message, retryable } }`, named after its history events, instead of serde's
    `{"Ok": …}`.
  - Durations are whole milliseconds in `*_ms` fields (`dur_ms`, `timeout_ms`, `initial_delay_ms`, `max_delay_ms`,
    `after_ms`) instead of `{secs, nanos}`.
  - Timestamps stay RFC 3339 strings.

  None of these types is stored, so no data migration is needed.
- **Random values are strings in the trace**, because JSON numbers above 2^53 lose precision in JavaScript.
- **Unbiased `tokio::select!` makes the trace nondeterministic.** It polls its branches in random order, so the
  race's commands were issued in a different order on each replay. Replay is still correct, because it matches by
  key, but the frontier order and the order of registered events changed. The corpus race uses `biased;`.
- **Not covered yet:** live-phase interaction (`push_live` after `Live`), hook token conflicts (they need two runs)
  and `RunRetried` (the corpus stores normalized histories, and `normalize` runs in the Rust shell for every SDK).

## P3a: hosting workflows written in other languages (design)

### The problem

The engine only knows Rust workflows. Five places tie it to Rust:

| Coupling | Where | Why it blocks Node |
|---|---|---|
| `Exec` holds a Rust `Driver` (`Async` / `Sync`) and `driver.poll()` | `engine.rs`, `driver.rs` | "Run the code until quiescent" is a JS event-loop operation for Node. |
| `Core::advance` wakes Rust wakers itself | `core.rs` | Node settles promises, and has no wakers. |
| Step bodies are `StepStart` (a Rust future or `FnOnce`) taken from `Core` | `engine.rs` `launch` | A JS step body is a JS function, called through napi. |
| `ErasedWorkflow::start(core, input) -> Driver` is `pub(crate)` | `workflow.rs` | `bindings/node` is another crate and has to register its own workflows. |
| `on_finished` matches on `WorkflowError` | `engine.rs` | Node reports a JS error, not a Rust enum. |

### Shape: the shell owns replay, the host owns user code

```
           engine shell (durable-runtime)                         host (Rust today, Node in P3)
 ┌──────────────────────────────────────────────┐        ┌────────────────────────────────────────┐
 │ advance / take_resolved / frontier / faults  │        │ runs the body; ctx calls go straight    │
 │ decide → commit, timers, step tasks, streams │        │ into the shared Replayer (sync calls)   │
 │                                              │activate│                                        │
 │   loop { advance() == RunCode ──────────────────────► settle resolutions, run to quiescence    │
 │          ◄──────────────────────────────────────────── ExecStatus: Pending | Finished(result)  │
 │   Run { launch } ───────────────────────────────────► start_step(key, attempt) → attempt future │
 └──────────────────────┬───────────────────────┘        └──────────────────┬─────────────────────┘
                        └──────── ReplayHandle (Arc<Mutex<Replayer>>) ─────┘
```

The `Replayer` sits in a `ReplayHandle` that the shell and the host share. The shell drives history through
it: `advance`, `push_live`, `frontier`, `fault`, `cancel`. The host's ctx calls go into it directly and
synchronously: `user_key`, `issue`, `release`, `random_u64`, `uuid`. This is Restate's model (sync calls into the
core), not Temporal's (the SDK buffers commands and sends them back per activation). `ctx.random()` and
`ctx.uuid()` have to return a value synchronously, and a napi call costs well under a microsecond. The handle only
exposes `with(|r: &mut Replayer| ..)`, so nobody can hold the lock across an `.await`. The engine (a tokio
thread) and JS (the main thread) each take it only for one short call.

### API (in a new `durable_runtime::host` module, `#[doc(hidden)]` until the Node binding ships)

```rust
/// The replayer of one execution, shared by the engine and the host.
#[derive(Clone)]
pub struct ReplayHandle(Arc<Mutex<Replayer>>);
impl ReplayHandle {
    pub fn with<R>(&self, f: impl FnOnce(&mut Replayer) -> R) -> R;
}

/// A registered workflow, written in any language. Replaces `ErasedWorkflow`.
pub trait HostWorkflow: Send + Sync + 'static {
    fn name(&self) -> &str;
    fn definition(&self) -> WorkflowDefinition;
    fn required_deps(&self) -> Vec<DepKey> { vec![] }
    /// Start the body for one execution. It must not run user code yet: that waits for the first `activate`.
    fn start(&self, env: ExecEnv) -> Box<dyn HostExecution>;
}

pub struct ExecEnv {
    pub run_id: RunId,
    pub input: Value,
    pub replay: ReplayHandle,
    pub deps: Deps,                      // Rust-only today; other hosts ignore it
    pub pool: Arc<dyn ComputePool>,      // for blocking Rust steps
}

/// One execution of one workflow body.
#[async_trait]
pub trait HostExecution: Send {
    /// Settle these resolutions (in order), then run user code until it can do nothing more without new events.
    async fn activate(&mut self, resolved: Vec<(Handle, Resolution)>) -> ExecStatus;
    /// Start attempt `attempt` of the step issued under `key`. `None` means the body can no longer start it
    /// (a Rust `FnOnce` an earlier attempt took); the engine then rebuilds the execution.
    fn start_step(&mut self, key: &str, attempt: u32, info: StepInfo, span: &Span) -> Option<StepAttempt>;
    /// The engine is done with this execution: unwind or abandon the body.
    fn interrupt(&mut self);
}

pub enum ExecStatus {
    Pending,
    Finished(Result<Value, WorkflowFailure>),   // the SDK-neutral failure from P2
}

pub struct StepAttempt {
    pub kind: &'static str,   // "async" | "blocking" | "node" ... for the span
    pub fut: BoxFuture<'static, Result<Value, StepError>>,
}

impl WorkerBuilder {
    pub fn register_host(self, w: Arc<dyn HostWorkflow>) -> Self;
}
```

### What the engine keeps

- **The replay loop.** It is generic now: `while replay.with(Replayer::advance) == RunCode { status =
  exec.activate(replay.with(Replayer::take_resolved)).await; if faulted, stop }`. `rebuild`, `apply` and
  `Worker::replay` all use it, so the corpus records what the engine does for every host.
- **Running step attempts.** The engine creates the `StepInfo` (idempotency key, cancel flag, heartbeat, streams,
  deps) and the span. It spawns the attempt future with its timeout, turns a panic into a retryable failure,
  sends `StepDone`, and reaps attempts whose command left the frontier. The host only turns a key into a future.
- **Outcomes.** `on_finished` matches on `FailureKind`: `cancelled` cancels the run, `interrupted` and `fault`
  record a fault, and `failed` / `step` / `hook_conflict` fail the run with the message.

### What the hosts do

- **Rust (`RustExecution`, this PR):** it holds today's `Core`, minus the replayer (wakers, `StepStart`s, deps), and
  the `Driver`.
  - `activate` wakes the wakers of the resolved handles, then polls the driver.
  - `start_step` takes the `StepStart`. An async step becomes `scope_async(info, fut)`. A blocking step runs on
    the pool under `scope_blocking`, with its own panic catch, and a oneshot turns it into the future.
  - The `#[workflow]` structs and `register_fn*` closures implement `HostWorkflow`.
- **Node (P3, sketch):**
  - `start` asks JS (through a `ThreadsafeFunction`) to create an execution object bound to the napi
    `ReplayHandle`.
  - `activate` calls JS with the resolutions. JS settles the promises, waits for one `setImmediate`, and returns
    the status.
  - `start_step` returns a future that calls JS `runStep(key, attempt, info)` and awaits its promise.
  - `interrupt` drops the execution. The JS step map can call a body again, so `start_step` never returns `None`.
- **Conformance:** `Worker::replay` takes a `HostWorkflow`, so the Node binding can record its trace through the
  same Rust entry point the corpus test uses.

### Alternatives considered

- **The host owns the replayer, and the shell asks it for frontier and faults.** Every binding would have to
  forward a dozen methods, and could get them wrong. Rejected.
- **Temporal-style batching: the host buffers commands and returns them with each activation.** It saves the
  mutex, but `random_u64`, `uuid` and key reservation have to answer synchronously. Rejected; the sync-call
  cost is negligible next to one activation round trip.
- **Keep `Driver` as an enum and add a `Driver::Foreign` variant.** That keeps Rust step bodies in the engine and
  does not let another crate register workflows. Rejected.

### Risks

- **Activation round trips.** `advance()` still stops after every resolving event (P0), so a Node replay costs one
  `ThreadsafeFunction` round trip per resolution. Measure this in P3, then consider batched advance for hosts
  whose promises settle in a defined order. The corpus guards that change.
- **Lock contention** is limited to single short calls. The `with` API keeps any lock from being held across an
  await.

### Done when

The existing suite and the corpus pass unchanged. A test-only `ScriptedHost` replays corpus traces through the
real engine on the memory backend, issuing commands from the trace with no Rust futures or `Driver`, and runs
`sequential_steps`, `step_retry`, `signal` and `race_timer_wins` to the recorded result.

## P3a as built

P3a landed on `refactor/host-abstraction`. The full suite passes apart from the tests that need Docker, and so does
the corpus. The engine no longer knows about `Driver`, `Core` or `WorkflowError`: it runs every workflow through
`durable_runtime::host` (`#[doc(hidden)]`). Where the code differs from the design above:

- **`host::replay` takes the previous status.** `apply` delivers live commits through the same loop as `rebuild` and
  `Worker::replay`, and must keep the body's last status when a commit resolves nothing. The loop now stops on a
  fault in `apply` too; before, it kept polling a faulted body, which then returned an error the engine ignored in
  favour of the fault.
- **The Rust host keeps its state behind its own lock.** `Core` holds the shared `ReplayHandle` plus a
  `Mutex<HostState>` (wakers, `StepStart`s). The lock order is host state, then replayer, so a ctx call can issue a
  command and park its waker atomically while the engine (which only ever takes the replayer) resolves handles.
  `RustExecution` lives in `driver.rs`.
- **The sync driver's thread starts on the first activation**, not in `start`. It used to start right away and could
  race the first `advance`, so `start` now runs no user code for any host, as the trait requires.
- **Spent bodies are found while launching.** `Run { launch }` launches attempts in order; the first `start_step`
  that returns `None` stops the loop and the engine rebuilds, the same outcome as the old `has_start` pre-check.
  The span's `durable.step.kind` is recorded from `StepAttempt::kind` once the host has answered.
- **`Worker::replay` still looks the workflow up by name.** Every registered workflow is a `HostWorkflow`, so a
  binding registers with `WorkerBuilder::register_host` and replays through the same entry point.
- **`ScriptedHost`** lives in `durable-runtime/tests/replay_corpus.rs`. It splits a case's trace into one segment per
  `advance`, makes the segment's recorded calls when the matching resolutions arrive (live activations that resolve
  nothing are ignored), and answers `start_step` with the attempt's outcome from the case history. It runs
  `sequential_steps`, `step_retry`, `signal` and `race_timer_wins` on the memory backend to the recorded output, and
  checks that the engine writes a history of the same shape. A mismatch becomes a replayer fault, so the run
  records it instead of hanging silently.

## P3: the TypeScript binding (design)

The binding lives in `bindings/typescript`. It is "TypeScript", not "Node", because the authoring surface is the
product. Node is the first runtime that loads the native addon; Bun and Deno load Node-API addons too. The design
follows the P3a sketch for Node; this section settles what the sketch left open.

### Layout

```
bindings/typescript/
  Cargo.toml, build.rs, src/lib.rs   crate `durable-typescript`: a napi-rs cdylib, not published to crates.io
  package.json, tsconfig.json        npm package, ESM only, Node >= 22.18, TypeScript 7
  tsdown.config.ts                   bundles ts/ into dist/index.mjs + dist/index.d.mts
  vitest.config.ts                   unit tests
  ts/                                the SDK: workflow(), ctx, the host loop, the native loader
  test/                              Vitest suites
  scripts/build-native.mjs           cargo build + copy the addon to durable.node
```

The workspace builds the crate like any other (`cargo clippy --workspace` covers it). Its lib target sets
`test = false`: a test executable cannot link, because the `napi_*` symbols only exist inside a Node process.
Prebuilt per-platform packages (`@napi-rs/cli`) come with S4; until then `npm run build:native` compiles locally.

### Two layers

```
  TypeScript (main thread)                                Rust addon (tokio threads)
 ┌──────────────────────────────────────────┐          ┌───────────────────────────────────────────┐
 │ workflow(name, opts, body)               │ register │ JsWorkflow: HostWorkflow                   │
 │ Execution: handle → {resolve, reject},   │◄─────────┤ JsExecution: HostExecution                 │
 │   key → step body, body promise          │  TSFN    │   activate   → execution.activate(resolved)│
 │ ctx.step / sleep / waitFor* / race ...   │          │   start_step → execution.runStep(..)       │
 │        │ sync calls                      │          │   drop guard → execution.abortStep(..)     │
 │        ▼                                 │          │   interrupt  → execution.dispose()         │
 │ native Replayer ─────────────────────────┼── same ReplayHandle (Arc<Mutex<Replayer>>) ──┐        │
 └──────────────────────────────────────────┘          │ engine, store, queue, lease, timers ◄┘     │
                                                       └───────────────────────────────────────────┘
```

- **Native layer.** One `Replayer` class wraps a `durable_runtime::host::ReplayHandle`. Each method takes the lock
  for one call. Values cross as JSON in the P2 wire shape (internally tagged, `*_ms` durations, RFC 3339 times), so
  the corpus files, the trace and the JS objects are the same thing. `u64` randoms cross as decimal strings. Later
  slices add `Worker`, `Client` and the `JsWorkflow` host. The host calls JS through napi `ThreadsafeFunction`s
  and awaits the promises they return.
- **TypeScript layer.** It holds everything a language owns: the authoring API, the promise per handle, the step
  bodies, and "run to quiescence". It never computes a key, matches history or decides anything; it asks the
  replayer.

### Ctx calls are lazy thenables

A ctx call reserves its key right away and issues its command when it is first awaited, the way Rust builders
issue on first poll (see "P0 as built"). `ctx.step(..)` returns a `DurableCall<T>`, a `PromiseLike<T>` whose
`then` issues the command once and then behaves like a promise.

This matters for conformance. The corpus traces record when keys are reserved and when commands are issued. With
eager promises, `Promise.race([ctx.waitForSignal("approval"), ctx.sleep("deadline", 60_000)])` would trace
`user_key approval, issue approval, user_key deadline, issue deadline`. With lazy thenables it traces
`user_key approval, user_key deadline, issue approval, issue deadline`, exactly what the Rust race recorded.
`Promise.all` and `Promise.race` call `then` on their inputs in order, in microtasks, so the issue order matches
`try_join!` and `select! { biased; .. }`.

The lazy shape also gives builders for free, as in Rust: `ctx.step("send", fn).retry({ maxAttempts: 5 }).timeout(30_000)`
configures the command before the first `await` issues it. Configuring after that throws.

### Racing releases the losers

Rust releases a `select!` loser by dropping its future. JS has no drop, so `ctx.race(calls)` and
`ctx.select({ a: call, b: call })` release every call that has not settled once the winner settles. They also make the
intent explicit. A plain `Promise.race` still replays correctly, because the loser stays on the frontier and later
history for it is accepted, but its wait or timer stays registered, so the trace differs from Rust's. The docs
steer users to `ctx.race`.

### Activation and quiescence

`activate(resolved)` settles the matching promises in order, then waits with `await new Promise(r =>
setImmediate(r))`. All microtasks have run by then, including the `then` calls that issue newly awaited commands.
It returns `{ status: "pending" }` or `{ status: "finished", result }`. Before settling a handle it checks
`abort()`, as `CmdFuture::poll` does: interrupted, cancelled and fault reject with `InterruptedError`,
`CancelledError` and `FaultError`.

A body that awaits something else (a `fetch`, a `setTimeout`) leaves the frontier empty while it is not finished,
and the decider faults the run (`Decision::Fault`), the same rule as "awaited a non-durable future" in Rust.

`interrupt` drops the execution: its pending promises never settle and are garbage collected. Nothing in the body
runs again, so no unwind is needed.

### Steps

- `ctx.step(name, execute)` or `ctx.step(name, { execute, output?, retry?, timeoutMs? })` stores `execute` under
  the reserved key when the call is created. The engine's
  `start_step(key, attempt, info)` becomes `execution.runStep(key, attempt, info)`, which calls the body with a
  `StepContext { key, attempt, idempotencyKey, signal: AbortSignal, heartbeat() }`. A JS function can be called
  again, so `start_step` never returns `None` and retries never rebuild.
- The Rust attempt future awaits the JS promise. A drop guard inside it calls `execution.abortStep(key, attempt)`
  when the engine drops the attempt (reaped, timed out, execution over), which aborts the `AbortSignal`. The engine
  keeps owning timeouts, retries and panics (here: a thrown non-`Error` value becomes a retryable failure).
- A step's value goes through JSON on the way to history. The body sees the value from history both live and on
  replay, so a step that returns a `Date` gets a string back, on every run. Values that do not serialize (BigInt,
  functions, cycles) fail the step fatally. `StepError.fatal(msg)` and `StepError.retryAfter(ms, msg)` choose the
  class; anything else thrown is retryable.

### Errors and outcomes

| Body ends with | `WorkflowFailure.kind` |
|---|---|
| a returned value | (succeeds) |
| `StepFailedError` (a step failed for good, not caught) | `step` |
| `HookConflictError` | `hook_conflict` |
| `CancelledError` | `cancelled` |
| `InterruptedError` | `interrupted` |
| `FaultError`, `NonDeterminismError` | `fault` |
| any other thrown value | `failed` |

A resolution of the wrong kind (history says `x` was a timer, the code awaits a step) sets a fault through
`setFault`, as Rust's `unexpected` does. Faults the replayer raises itself have the shared core's messages.

TypeScript types vanish at runtime, so a payload cannot fail to deserialize the way a Rust `T` can. Calls that
return a payload take an optional schema instead: `output` on a step, `payload` on `waitForSignal`, `waitForEvent`
and `createHook`. Any [Standard Schema](https://standardschema.dev) works (zod, valibot, arktype); the SDK copies the
spec's interface and has no runtime dependency. The call's type is inferred from the schema, and a step's `execute`
must return what the schema accepts:

```ts
const n = await ctx.step("count", { output: z.number(), execute: () => count() }); // n: number
const ok = await ctx.waitForSignal("approval", { payload: z.object({ by: z.string() }) });
```

- A recorded value that does not fit faults the run with "non-determinism: `key` no longer deserializes ...", as
  Rust does when serde fails. The code gets the schema's output (transforms apply).
- A live step result that does not fit fails the attempt as fatal, before it is recorded, as Rust does for an output
  that does not serialize. What is recorded is what `execute` returned, so replay runs the schema on the same value.
- Replay cannot wait, so a schema that validates asynchronously is a fault.
- Without a schema the value is passed through as `Json`.

### Determinism

No sandbox in v1. `ctx.now()`, `ctx.random()` (a float in `[0, 1)` from `random_u64`), `ctx.randomU64()` (a
`bigint`) and `ctx.uuid()` are deterministic. `Date.now()`, `Math.random()` and `crypto.randomUUID()` are not; a later
opt-in dev mode can patch them to warn while `isReplaying()`.

### Conformance

Three checks, in the order the slices build them:

1. **Replayer through the FFI** (S1): a TypeScript port of `durable_testkit::corpus::play` drives the native
   `Replayer` with every case's host calls and checks each answer and the final frontier and fault.
2. **Replay of TypeScript ports** (S1): TypeScript versions of the corpus
   workflows replay each history through the real host loop (`Execution`) and must reproduce the trace, frontier,
   fault and result. S1 runs this offline over the native `Replayer`; S2 moves it onto `Worker.replay` so it goes
   through the same Rust entry point as `replay_corpus`.
3. **End to end** (S2, S3): runs on the memory backend, then Postgres/Redis, plus the crash/resume chaos test.

### Slices

| Slice | Work | Done when |
|---|---|---|
| S1 | Crate, native `Replayer`, the TypeScript ctx and `Execution` host loop, an offline replay driver, checks 1 and 2 | All 22 cases pass check 1; the ported cases pass check 2. |
| S2 | `JsWorkflow` host over `ThreadsafeFunction`s, `Worker` + `Client` on the memory backend, `Worker.replay` | A TypeScript workflow runs steps, timers and signals on the real engine from Vitest. |
| S3 | Backends by URL (`memory://`, `postgres://` + optional Redis) through `durable_workflow::connect`, hooks, events, `examples/typescript-onboarding`, chaos test | The P3 "done when" row. |
| S4 | Prebuilt binaries, CI job, measure activation round trips and tiny-step throughput (open question 2) | Published, and the numbers are written down. |

## S1 as built

S1 is on `refactor/host-abstraction`, after P3a. `npm test` in `bindings/typescript` runs 51 tests: check 1 on all 22
cases, check 2 on TypeScript ports of all 22 (not only a subset), and unit tests of the step side of `Execution`.
`ci.sh` runs them when `node` is on the path. Where the code differs from the design above:

- **No `@napi-rs/cli` yet.** `scripts/build-native.mjs` runs `cargo build -p durable-typescript` and copies the library
  to `durable.node`. The TypeScript is written to be erasable (`erasableSyntaxOnly`): no enums, namespaces or
  parameter properties.
- **Toolchain:** TypeScript 7 (`npm run typecheck`), Vitest (`npm test`, forks pool) and tsdown (`npm run build`).
  tsdown bundles `ts/index.ts` into `dist/index.mjs` and emits `dist/index.d.mts` with TypeScript 7's Go compiler
  (it warns that its TypeScript 7.0 backend is deprecated in favour of 7.1, still in beta). The package's `exports`
  point at `dist/`, and `files` ships `dist/` and `durable.node`. `dist/` sits next to `ts/`, so the loader's
  `../durable.node` resolves the same from source and from the bundle. Tests import the source, not `dist/`.
- **The native `Replayer` is the whole addon so far,** plus `defaultRetryPolicy()`, so TypeScript never copies Rust's
  defaults. It already wraps a `ReplayHandle`, so S2 can hand JavaScript the replayer of an engine execution.
- **`select` is how a race of mixed types is written.** `ctx.select({ signal, timeout })` resolves to
  `{ key, value }` and releases the losers; `ctx.race([...])` is for calls of one type. `select` calls `then` on its
  calls as it starts, rather than in a microtask, but they are still issued after every key the body reserved
  before it, so the trace matches Rust's `select! { biased; .. }`.
- **On an abort, `activate` rejects every pending call.** A Rust driver polls the root future on every activation, so
  every pending `CmdFuture` sees the abort. The non-determinism cases need this: the body must finish with the fault
  in the same activation, as it does in Rust.
- **The ports use only public API,** with two places where TypeScript has to say what Rust's types say: zod schemas for
  integers in `signal_timeout`, `hook` and `nondeterminism_output_type`, and a helper that formats `ctx.now()` the way
  chrono serializes it in `deterministic_helpers`. `sync_workflow` becomes an async body, since TypeScript has no
  blocking workflows.
- **`compare` ignores what the host writes,** as P2 specified: `set_fault` messages, the fault when the host raised it,
  and `err` messages.
- **Not done yet:** `runStep` and `abortStep` exist and are tested, but nothing calls them until the S2 host bridge.
  `WorkflowOptions` holds only `timeoutMs`. A `Promise.all` that rejects early leaves its other calls pending, where
  Rust's `try_join!` would drop (release) them; no corpus case covers it yet.

## Open questions

1. **Mixed-language workers**: can one worker host Rust and TS workflows at once? Nothing prevents it,
   because registration is by workflow name. Defer until someone asks.
2. **Node threading**: tokio runs on its own threads and JS on the main thread. Step results cross
   via `ThreadsafeFunction`. Check that the throughput of many tiny steps is acceptable before P3 is called done.
