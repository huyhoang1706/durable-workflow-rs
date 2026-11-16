# Building with Bazel

The Rust workspace and the TypeScript SDK build with **Cargo/pnpm or Bazel**, your choice. Cargo and pnpm are the
default and stay the source of truth: `Cargo.toml` and `Cargo.lock` decide crates, versions, features and
dependencies, `pnpm-lock.yaml` decides npm packages, and Bazel reads them. You never need Bazel to work on this
repository; `./ci.sh`, every `cargo` command and every `pnpm` command work as before.

Bazel adds per-target caching: an unchanged crate's compile, clippy run and test results are cache hits, and
each `tests/*.rs` file is its own target, so a change to one plugin reruns only that plugin's tests.

Rust and TypeScript are one graph: the SDK's tests load the addon Bazel compiles from the crates, so editing a `.ts`
file compiles no Rust, and editing a plugin reruns the SDK tests but not its type check.

## Commands

Install [bazelisk](https://github.com/bazelbuild/bazelisk) (`brew install bazelisk`, or
`npm install -g @bazel/bazelisk`); it fetches the Bazel version in `.bazelversion`. The Rust toolchain and the
C/C++ toolchain are downloaded by Bazel (hermetic), so the machine's compilers are not used.

```bash
bazel build //...                                   # every crate, binary and the Node addon
bazel test //...                                    # every test; Postgres/Redis suites need Docker
bazel test //... --test_tag_filters=-docker         # without Docker
bazel test //plugins/durable-redis/...              # one crate
bazel test //durable-workflow-ts/... //examples/typescript-onboarding/...   # the TypeScript SDK and example
bazel test //crates/durable-runtime:engine_async_test   # one test file (tests/engine_async.rs)
bazel test //crates/durable-runtime:engine_async_test --test_arg=<name filter>   # like `cargo test <name>`
bazel build --config=clippy //...                   # clippy, warnings are errors
```

| Cargo | Bazel |
|---|---|
| `cargo build --workspace` | `bazel build //...` |
| `cargo test --workspace --all-features` | `bazel test //...` |
| `cargo test -p durable-runtime --test engine_async` | `bazel test //crates/durable-runtime:engine_async_test` |
| `cargo test -p durable-runtime --lib` | `bazel test //crates/durable-runtime:unit_test` |
| `cargo test -p durable-runtime --doc` | `bazel test //crates/durable-runtime:doc_test` |
| `cargo clippy --workspace --all-targets --all-features -- -D warnings` | `bazel build --config=clippy //...` |
| `cargo run -p durable-cli -- <args>` | `bazel run //crates/durable-cli:durable -- <args>` |
| `pnpm run typecheck` (in `durable-workflow-ts`) | `bazel test //durable-workflow-ts:typecheck` |
| `pnpm test` | `bazel test //durable-workflow-ts:test` |
| `pnpm run test:servers` | `bazel test //durable-workflow-ts:servers_test` (Docker) |
| `pnpm run build` | `bazel build //durable-workflow-ts:dist` |
| the example's `pnpm run typecheck` / `pnpm test` / `pnpm run test:servers` | `//examples/typescript-onboarding:typecheck` / `:test` / `:servers_test` |
| `cargo fmt --all` | stays Cargo |
| `cargo test -p durable-workflow --test compile_fail` (trybuild) | stays Cargo: trybuild runs `cargo` |
| `DURABLE_UPDATE_CORPUS=1 cargo test ... replay_corpus` | stays Cargo: it writes into the source tree |
| `pnpm run bench`, `pnpm run test:watch` | stay pnpm: developer loops |
| publishing the npm packages | stays in `.github/workflows/typescript-release.yml` (Cargo, napi CLI) |

`--config=ci` (used by the CI job) also forces coloured output (`--color=yes`, `FORCE_COLOR=1` for the test
runners) and turns off the redrawn progress bar, since a CI log is not a terminal.

Machine-local flags (a disk cache, the BuildBuddy key) go in `user.bazelrc`, which is git-ignored, for example
`common --disk_cache=~/.cache/bazel-disk`.

## Remote cache and results UI: BuildBuddy

[BuildBuddy](https://www.buildbuddy.io) (free for open source) hosts a shared remote cache and a results page
for every Bazel invocation. It is optional: without it, Bazel caches locally and CI uses the GitHub cache.

`--config=buildbuddy` (in `.bazelrc`) turns on the results UI and the remote cache. The API key is never
checked in, because the repository is public.

**CI.** The `bazel` job in `.github/workflows/ci.yml` reads two repository secrets:

| Secret | BuildBuddy key | Used by |
|---|---|---|
| `BUILDBUDDY_API_KEY` | read-write | pushes to `main`: they fill the cache |
| `BUILDBUDDY_READONLY_API_KEY` | read-only | pull requests and the weekly run: they read `main`'s cache and write nothing |

A pull request therefore cannot put a bad entry into the cache that `main` and other pull requests read. Forks
get no secrets, so their runs use the GitHub disk cache instead, as they do when the secrets are not set.
The job log prints the results link (`Streaming build results to: https://app.buildbuddy.io/invocation/...`).
Each Monday the job also reruns every test with `--nocache_test_results`, so a cached pass cannot hide a
changed `postgres`/`redis` image or a flaky test.

Setting it up (once, by a maintainer):

1. Sign in at <https://app.buildbuddy.io> and create the organization.
2. In the organization settings, under API keys, create two keys: one for CI pushes, and one for pull requests
   with **Read-only key (disable remote cache uploads)** checked.
3. In the GitHub repository, under Settings, Secrets and variables, Actions, add them as `BUILDBUDDY_API_KEY`
   and `BUILDBUDDY_READONLY_API_KEY`.
4. Optional: link your GitHub account in BuildBuddy (user menu, *Link GitHub Account*) for commit statuses.
   Results pages are visible to organization members only; add `common:ci --build_metadata=VISIBILITY=PUBLIC`
   to `.bazelrc` to show them to everyone, for example to contributors from forks.

**Locally.** Put a key in `user.bazelrc`. A read-only key lets you reuse what CI built without writing your
machine's results into the shared cache:

```
common --config=buildbuddy-readonly
common --remote_header=x-buildbuddy-api-key=<key>
common --bes_header=x-buildbuddy-api-key=<key>
```

Use `--config=buildbuddy` instead of `buildbuddy-readonly` only with a key meant for writing. Without any key,
`--config=buildbuddy` still uploads results, unauthenticated: anyone with the link can see them.

Two things to keep in mind:

- The results UI receives target names, command lines and test logs. The container tests' logs hold only the
  throwaway containers' passwords.
- The Docker-tagged tests run outside the sandbox, so their cached results are only as good as their declared
  inputs. That is why the weekly uncached run exists.

## How it fits together

```
.bazelversion, .bazelrc, .bazelignore
MODULE.bazel, MODULE.bazel.lock   rules_rs reads Cargo.toml/Cargo.lock into the @crates repository,
                                  rules_js reads pnpm-lock.yaml into @npm; Node is pinned there
rust-toolchain.toml               the Rust version for rustup; MODULE.bazel pins the same one
package.json, pnpm-workspace.yaml the pnpm workspace: durable-workflow-ts and examples/typescript-onboarding
bazel/rust.bzl                    durable_rust_crate: Cargo's layout as Bazel targets
bazel/typescript.bzl              the addon's platform names (durable.<platform>.node)
bazel/checks/                     dependency-hygiene tests
<crate>/BUILD.bazel               one durable_rust_crate call per crate
durable-workflow-ts/BUILD.bazel   the crate, plus the TypeScript targets (below)
```

The ruleset is [`rules_rs`](https://github.com/hermeticbuild/rules_rs) (built on `rules_rust`), the same one
openai/codex uses for its Cargo workspace. `crate.from_cargo` reads the lockfile directly, without a second,
Bazel-specific lockfile, and generates `@crates//<package>:data.bzl` for every workspace member with its crate
name, edition, enabled features, binaries and dependency lists.

Every `BUILD.bazel` is the same few lines:

```starlark
load("@crates//durable-sm:data.bzl", "aliases", "all_crate_deps", "crate_data")
load("//bazel:rust.bzl", "durable_rust_crate")

durable_rust_crate(
    aliases = aliases,
    all_crate_deps = all_crate_deps,
    crate_data = crate_data,
)
```

`durable_rust_crate` follows Cargo's conventions:

| Cargo | Bazel target |
|---|---|
| `src/lib.rs` | `//crates/durable-sm` (the target is named after the directory) |
| `#[cfg(test)]` modules | `:unit_test` |
| doctests | `:doc_test` |
| `tests/engine_async.rs` | `:engine_async_test`; `tests/common/**` is shared by every test file |
| `[[bin]]`, `src/main.rs` | `:<bin name>`, e.g. `//crates/durable-cli:durable` |
| `crate-type = ["cdylib"]` | `:<lib name>_shared`, e.g. `//durable-workflow-ts:durable_typescript_shared` |
| `build.rs` | `:build_script` |
| non-`.rs` files under `src/` (`include_str!`) | compile inputs |

## The TypeScript targets

The SDK's `BUILD.bazel` calls `durable_rust_crate` for the addon and adds:

| Target | What it is |
|---|---|
| `:addon` | `:durable_typescript_shared` copied to `durable.<platform>.node`, the name `pnpm run build:native` gives it, so `ts/native.ts` finds it next to `ts/` with no environment variable |
| `:typecheck` | `tsc --project tsconfig.json` from the npm `typescript` 7 package (a native binary), so the existing `tsconfig.json` is used as it is |
| `:test` | `vitest run` (`rules_js` `vitest_test`), with the addon and the replay corpus as data |
| `:servers_test` | `:test` through `scripts/with-servers.sh`, which starts Postgres and Redis with the `docker` CLI; tagged like the Rust container suites |
| `:dist` | `tsdown`: the same `dist/` as `pnpm run build`, byte for byte |
| `:pkg` | `npm_package` of `package.json`, `dist/` and the addon. `workspace:*` in the example links it, so the example tests the built package as a consumer installs it |

`examples/typescript-onboarding/BUILD.bazel` has `:typecheck`, `:test` and `:servers_test` the same way.

What is worth knowing:

- **pnpm is required.** `rules_js` reads `pnpm-lock.yaml`, so the repository is one pnpm workspace and has no
  `package-lock.json`. After changing a `package.json`, run `pnpm install` and commit the lockfile; Bazel picks it up.
  New dependencies with install scripts must be listed under `allowBuilds` in `pnpm-workspace.yaml`.
- **Every source file in one `js_library`.** `rules_js` copies sources into `bazel-bin`; two targets that copy the
  same file (say `tsconfig.json`) are conflicting actions. Add new configuration files to `:srcs`.
- **Files from another package** must be in `bazel-bin` too: the corpus is `//crates/durable-testkit:corpus_bin`
  (`copy_to_bin`), not the `:corpus` filegroup the Rust tests read.
- **`:dist` runs unsandboxed** (still cached). tsgo, which writes the `.d.mts`, resolves the sandbox's symlinks and
  then cannot match its output to its input.
- **The addon links the C++ runtime statically** (`cc_runtime_linkage = "static"` in `durable_rust_crate`), so the
  copied `.node` does not depend on a `libc++.so` next to the original.
- **Compilation mode.** The addon is built in the invocation's mode (`fastbuild` by default): right for tests. The
  tiny-step benchmark wants `pnpm run build:native:release`.

## When you need to touch Bazel files

Most changes need none: dependencies, features and versions come from `Cargo.toml` and `pnpm-lock.yaml`, and new
source or test files are picked up by globs.

- **New crate.** Add a `BUILD.bazel` like the one above. `./ci.sh` fails with the exact file to create if you
  forget.
- **A test that starts containers.** Tag it in the crate's `BUILD.bazel`:
  `test_tags = {"conformance": DOCKER_TEST_TAGS}`. Such tests run outside the sandbox with network access.
- **`include_str!` of a file outside `src/`.** List it: `compile_data = glob(["migrations/*.sql"])`.
- **A test with `required-features` that Cargo's workspace feature unification does not turn on.** Add the
  feature and the dependency it enables: `crate_features = ["reqwest"]`, `deps = ["@crates//reqwest"]` (see
  `crates/durable-runtime/BUILD.bazel`).
- **`MODULE.bazel.lock`.** Bazel updates it after `Cargo.lock` changes; commit the update if you ran Bazel. If
  you only use Cargo, leave it: CI does not require it to be current.

## Differences from Cargo worth knowing

- **One configuration per crate.** Cargo builds a crate once per feature set requested; Bazel builds each target
  once. Features are the ones Cargo's workspace feature unification resolves, plus the extras listed in BUILD
  files, which together match `--all-features`. The `standalone` feature set has its own target,
  `//durable-workflow-rs:standalone`, because `//bazel/checks` asserts it stays free of OpenTelemetry.
- **Dependency hygiene** (the `cargo tree | grep` checks in `ci.sh`) runs as tests:
  `//bazel/checks:durable_sm_is_sans_io` and `//bazel/checks:standalone_has_no_opentelemetry`.
- **No absolute paths in compiled code.** rules_rs refuses an output that embeds the sandbox path, which is what
  `env!("CARGO_MANIFEST_DIR")` would bake in. `durable-testkit` sets `CARGO_MANIFEST_DIR` to its package path,
  which is where the corpus sits in a test's runfiles; `durable-cli`'s smoke test gets `CARGO_BIN_EXE_durable`
  the same way.
- **The Node addon** built by Bazel carries the hermetic toolchain's C++ runtime and has no checked glibc floor.
  Fine for tests under Bazel; published addons keep coming from the `TypeScript release` workflow (Cargo and the
  napi CLI, on the oldest supported runners).

## Workarounds in `MODULE.bazel`

- `sqlx-postgres` gets the `any` and `offline` features. rules_rs 0.2.4 unions the `sqlx-core` features that
  `sqlx-macros-core` requests even though that crate is only in `Cargo.lock` and never built, so `sqlx-core`
  is compiled with them and `sqlx-postgres` has to match. Remove the annotation once rules_rs resolves features
  like Cargo.
- `aws-lc-sys` (reqwest → rustls → aws-lc-rs) does not run its build script. AWS-LC comes from the `aws-lc`
  Bazel module, compiled by the hermetic C toolchain, and the Rust bindings are generated by rules_rs's bindgen
  toolchain. The annotations are a copy of `@rules_rs//3rd_party/aws-lc-sys:include.MODULE.bazel` with the
  `deps` label pointed at `@crates`: the file cannot be `include`d as it names another hub. The AWS-LC version
  is the `aws-lc` module's, not the one vendored in the `aws-lc-sys` crate that Cargo builds; when bumping
  `aws-lc-rs`, bump `aws-lc` to what that rules_rs file names.

## Not covered yet

- Publishing the npm packages, and cross-building the addons from one runner (Phase 5 of the plan).
- Windows: not in `platform_triples`.
