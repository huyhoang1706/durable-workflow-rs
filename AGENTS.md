# AGENTS.md

## Commands

```bash
./ci.sh                                   # fmt --check, clippy -D warnings (all targets/features), full test suite
cargo test --workspace --all-features     # tests only
cargo test -p durable-runtime --test engine_async            # one integration test file
cargo test -p durable-runtime --test engine_async <name>     # one test
cargo test -p durable-workflow --test compile_fail           # trybuild UI tests for the macros (tests/ui/*.stderr)
DURABLE_UPDATE_CORPUS=1 cargo test -p durable-runtime --test replay_corpus   # rewrite the replay conformance corpus (durable-testkit/corpus)
docker compose up -d                      # postgres:17 + redis:7 for the examples
bazel test //... --test_tag_filters=-docker   # optional: the Rust workspace through Bazel (docs/bazel.md); drop the filter with Docker running
bazel test //crates/durable-runtime:engine_async_test   # one test file under Bazel (`tests/<name>.rs` -> `:<name>_test`)
bazel build --config=clippy //...         # clippy -D warnings under Bazel
cd durable-workflow-ts && npm ci && npm run build:native && npm run typecheck && npm test && npm run build   # TypeScript SDK (Node >= 22.18): TS 7, Vitest, tsdown
cd durable-workflow-ts && npm run build:native:release && npm run bench   # tiny-step throughput; Rust baseline: cargo run --release -p durable-workflow --example tiny_steps
cd durable-workflow-ts && npm run test:servers   # also the SDK's Postgres/Redis cases, on throwaway containers (npm test skips them)
DURABLE_TEST_DATABASE_URL=postgres://... DURABLE_TEST_REDIS_URL=redis://... npm test   # the same cases on your own servers
cd examples/typescript-onboarding && npm ci && npm run typecheck && npm run test:servers   # the TypeScript example; build the SDK (incl. npm run build) first
```

`examples/typescript-onboarding` is an npm project, not a crate: it is excluded from the Cargo workspace.

The Rust crates build with Cargo or Bazel. Cargo is the source of truth; Bazel reads `Cargo.toml` / `Cargo.lock` through `rules_rs`, so dependency and feature changes need no Bazel edits. A new crate needs a `BUILD.bazel` (`./ci.sh` prints it), a container test needs `DOCKER_TEST_TAGS`; see `docs/bazel.md`. Do not hand-write dependency lists in BUILD files.

Docker must be running for the full suite: the Postgres/Redis plugin tests, the CLI smoke test and the chaos suite start their own containers (testcontainers). Without Docker, run per-crate tests for `durable-sm`, `durable-spi`, `durable-runtime`, `durable-testkit`, `durable-memory` and `durable-macros`.

## Conventions

- The SDKs are peers, none is primary: `durable-workflow-rs/` (Rust, crate `durable-workflow`) and `durable-workflow-ts/` (TypeScript, npm `@durable/typescript`) at the root, the next ones as `durable-workflow-<lang>/`. Call them SDKs, not bindings. The shared engine lives in `crates/` and `plugins/`; language-neutral docs go in the root `README.md`, language-specific docs in each SDK's `README.md`. A user-facing feature that one SDK lacks is listed in `docs/BACKLOG.md`.
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
