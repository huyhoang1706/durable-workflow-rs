# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
./ci.sh                                   # fmt --check, clippy -D warnings (all targets/features), full test suite
cargo test --workspace --all-features     # tests only
cargo test -p durable-runtime --test engine_async            # one integration test file
cargo test -p durable-runtime --test engine_async <name>     # one test
cargo test -p durable-workflow --test compile_fail           # trybuild UI tests for the macros (tests/ui/*.stderr)
docker compose up -d                      # postgres:17 + redis:7 for the examples
```

Docker must be running for the full suite: the Postgres/Redis plugin tests, the CLI smoke test and the chaos suite start their own containers (testcontainers). Without Docker, run per-crate tests for `durable-core`, `durable-runtime`, `durable-memory` and `durable-macros`.

Rust edition 2024, resolver 3. Clippy warnings are errors.

## Architecture

Decentralized, replay-based durable workflows. There is no server: workers coordinate only through the backends. See [README.md](README.md) for the user-facing model (determinism rules, at-least-once semantics, flow control) and `docs/plans/iter*.md` / `docs/BACKLOG.md` for design decisions and deferred work.

**Crate dependency rule:** `durable-core` holds ids, records, the event model, errors and every plugin trait, with no infrastructure. `durable-runtime` and every crate under `plugins/` depend only on `durable-core`, never on each other. `durable-workflow` is the facade that wires them through cargo features (`memory`, `macros`, `postgres`, `redis`, `standalone`).

**Plugin slots** (traits in `durable-core`): `Store`, `Queue`, `PubSub`, `Cache`, `RateLimiter`, bundled into a `Backend` (with a namespace). Postgres is the source of truth (`Store`); Redis is a rebuildable queue / pub-sub / cache index. Losing Redis must lose nothing durable (the reconciler rebuilds the queue from `wake_at`). New backends must pass the shared suites in `durable-testkit` (`store_conformance!`, `queue_conformance!`, etc.).

**Replay engine** (`crates/durable-runtime/src`):
- `core.rs` is driver-agnostic replay state for one execution: workflow code issues *commands* (steps, timers, waits, hooks), history events resolve them, and whatever stays unresolved is the *frontier*.
- `driver.rs` runs user code against a `Core`: an async future polled by the engine, or a blocking body on its own thread (separate from the compute pool so a blocked body can't deadlock its own steps). Drivers are disposable, a pure function of history.
- `engine.rs` is the loop for one claimed run: load history, replay, act on the frontier, commit every state change to the store first, and suspend (holding no worker) when it can only wait.
- `ctx.rs` has the user-facing `WfAsyncCtx` / `WfSyncCtx`; `worker.rs` / `client.rs` are the entry points; `inflight.rs` is a best-effort cache view of executing runs.

**Macros** (`durable-macros`): `#[workflow]` generates the `AsyncWorkflow`/`SyncWorkflow` impl (used via `register_default::<Name>()`) and `#[step]` wraps step functions. Error messages are pinned by trybuild snapshots in `crates/durable-workflow/tests/ui/`; regenerate with `TRYBUILD=overwrite` when you change them intentionally.

**Other crates:** `durable-api` (axum ops router, mountable or standalone), `durable-cli` (the `durable` binary), `durable-workflow::standalone` (a worker with signal handling, health endpoints, optional API). Examples in `examples/` (`embedded-axum`, `standalone-worker`).

A `Client` only sees its backend's namespace, so runs, hook tokens and definitions of other namespaces sharing a database are invisible to it.

## Conventions

- Naming favours unambiguous outcomes: `Succeeded`, not `Completed`; contexts are `WfAsyncCtx` / `WfSyncCtx`; the sync executor is `SyncDriver`.
- Commit messages must be meaningful and follow this format:

  ```
  type(scope): short description

  [detailed description if needed]
  ```

  Common types:
  - `feat`: a new feature, e.g. `feat(api): add /chat/stream endpoint`
  - `fix`: a bug fix, e.g. `fix(agent): handle empty input`
  - `docs`: documentation, e.g. `docs: update README with install instructions`
  - `test`: add or change tests, e.g. `test(agent): add test for search tool`
  - `refactor`: restructure code without changing behavior, e.g. `refactor(config): move config to pydantic-settings`
  - `chore`: maintenance such as dependency updates, e.g. `chore: bump ruff to v0.4.0`

  Scope is optional but recommended: `agent`, `api`, `config`, `models`, `tests`, or another module name.
- Workflow bodies must be deterministic: side effects only inside steps, and use `ctx.now()` / `ctx.random_u64()` / `ctx.uuid()` instead of system time/RNG. Engine code relies on this when matching steps by key (name plus occurrence counter; names may not contain `#`, `[` or `]`).
