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
cd bindings/typescript && npm ci && npm run build:native && npm run typecheck && npm test && npm run build   # TypeScript binding (Node >= 22.18): TS 7, Vitest, tsdown
cd bindings/typescript && npm run test:servers   # also the binding's Postgres/Redis cases, on throwaway containers (npm test skips them)
DURABLE_TEST_DATABASE_URL=postgres://... DURABLE_TEST_REDIS_URL=redis://... npm test   # the same cases on your own servers
```

Docker must be running for the full suite: the Postgres/Redis plugin tests, the CLI smoke test and the chaos suite start their own containers (testcontainers). Without Docker, run per-crate tests for `durable-sm`, `durable-spi`, `durable-runtime`, `durable-testkit`, `durable-memory` and `durable-macros`.

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
