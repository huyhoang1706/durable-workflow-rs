# Iteration 3 (draft): a sans-IO core for TS/JS and Python SDKs

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
   and no `Arc`. Its dependencies: `durable-core` types, `serde`, `serde_json`, `chrono`, `rand_chacha`, `uuid`.
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
import { workflow, step } from "@durable/node";

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
| P0 | Create `crates/durable-sm`. Move `Core` → `Replayer` (handles, `advance`, no wakers/closures/deps). `CmdFuture` becomes a ~30-line adapter: handle + waker map in `durable-runtime`. | Whole existing suite passes unchanged. `durable-sm` has no tokio/futures dependency (CI check). |
| P1 | Lift `decide` and `step_outcome` out of `engine.rs`. `run_loop` matches on `Decision`. | Suite passes. Decider unit tests cover each arm without a backend. |
| P2 | **Replay conformance corpus**: record histories plus the expected command sequences as JSON in `durable-testkit`, generated from the Rust workflows in the test suite. | Every binding must pass this corpus. It is the cross-language contract. |
| P3 | `bindings/node` (napi-rs): embedded `Worker` + `Client`, `workflow()`, `ctx.step/sleep/waitForSignal/waitForEvent/createHook`, Postgres/Redis/memory backends selected by URL. | An `examples/node-onboarding` passes the conformance corpus and a crash/resume chaos test. |
| P4 | `bindings/python` (PyO3 + maturin), same surface. | Same as P3. |
| Later | HTTP executor (Inngest-style), using `durable-sm` compiled to WASM in the SDK so serverless functions can replay. | — |

## Open questions

1. **Mixed-language workers**: can one worker host Rust and TS workflows at once? Nothing prevents it,
   because registration is by workflow name. Defer until someone asks.
2. **Node threading**: tokio runs on its own threads and JS on the main thread. Step results cross
   via `ThreadsafeFunction`. Check that the throughput of many tiny steps is acceptable before P3 is called done.
