# Bazel integration: one build graph for Rust and TypeScript

> Status: proposal, nothing built. Researched 2026-11-12. Items marked **(verify)** come from reading docs and
> other repositories, not from running Bazel on this workspace; Phase 1 settles them.

## Context

The repository is a polyglot monorepo: 13 Rust crates (`crates/*`, `plugins/*`, three Rust examples), a Node-API
addon with a TypeScript SDK (`bindings/typescript`), and an npm example (`examples/typescript-onboarding`). A Python
binding is planned (iteration 3).

CI today is one job that runs `./ci.sh` top to bottom:

| Measured (run of 2026-11-12, warm `Swatinem/rust-cache`) | |
|---|---|
| `./ci.sh` | 382 s |
| whole `CI` workflow | about 7 min |
| `TypeScript release` workflow on a PR (5 platform builds, in parallel) | about 4.5 min |

Only one successful `CI` run was available to measure, so treat 382 s as an order of magnitude, not a baseline.
Phase 0 collects a real one.

What makes it slow, and what will get worse as the repo grows:

1. **Nothing is skipped.** A change to `bindings/typescript/ts/*.ts` still runs clippy and every Rust test,
   including the Postgres/Redis container suites. A change to `plugins/durable-otel` still runs the TypeScript suite.
2. **It is serial.** fmt, clippy, tests, two `cargo tree` checks, a second `cargo build`, then two npm projects.
3. **The cache is coarse.** `rust-cache` restores `target/` as one blob keyed on `Cargo.lock`. It saves
   dependency compilation, never test execution.
4. **Two workflows compile the same Rust.** `CI` and `TypeScript release` both build the addon for
   `x86_64-unknown-linux-gnu` and share nothing.

Bazel addresses 1 to 3 directly: every compile and every test is an action keyed on its inputs, so an unchanged
crate's tests are a cache hit, and independent actions run in parallel.

### What Bazel will not do here

- It does not make a cold build faster. `rules_rust` compiles one crate per action without Cargo's incremental
  compilation, so a cache-less build is as slow as Cargo or slower.
- A change to `durable-sm` or `durable-spi` invalidates almost everything, because almost everything depends on
  them. The gain is on changes near the leaves (plugins, bindings, examples, docs), and on re-runs.
- Cargo stays. The crates are consumed through Cargo, so `Cargo.toml` remains the source of truth and a slim Cargo
  job stays in CI. This is a second build system to keep working, not a replacement.

At 7 minutes the absolute saving is small today. The case for doing it now is that the graph is still small enough
to convert in a few days, before the Python binding adds a third toolchain. Phase 4 has an explicit go/no-go gate
on measured numbers.

## What the two references do

### perplexityai/perplexity-node (TypeScript only)

A single npm package at the repository root, Bazel 9.2, bzlmod only.

| Piece | How they do it | Use here? |
|---|---|---|
| Modules | `aspect_rules_js` 3.x, `aspect_rules_ts` 3.x, `rules_nodejs` (Node pinned in `MODULE.bazel`), `aspect_bazel_lib`, `jq.bzl` | Yes |
| npm deps | pnpm; `npm.npm_translate_lock(pnpm_lock = "//:pnpm-lock.yaml")`, then `npm_link_all_packages()` | Yes, which means moving from npm to pnpm (decision 1) |
| TypeScript 7 | `typescript` 7.0.2 from npm, wrapped with `typescript_bin.tsc_binary(name = "tsc")` and passed as `tsc =` to `ts_project` | Yes. We are on the same TypeScript version, so this is the proven path for TS 7 under `rules_ts` |
| Typecheck | One `ts_project(no_emit = True)` over all sources; it yields a `*_typecheck_test` target | Yes, one per npm package |
| Tests | `ts_project` compiles tests to `test-dist/`, then `js_test` runs `node --test` through a small runner | No. We keep Vitest (see Phase 3) |
| Packaging | `npm_package(name = "pkg")` plus `npm_link_package`, so `e2e/` and `examples/` import the *built* package | Yes, for `examples/typescript-onboarding` |
| Package checks | `publint` and `attw` as Bazel tests against `:pkg` | Later, cheap to add |
| BUILD generation | `gazelle` + `gazelle_ts`, with `map_kind` to their own `ts_library` / `ts_test` macros that only collect sources | No. Two TS packages do not need a generator |
| C toolchain | `llvm` module (hermetic-llvm), host C++ autodetection turned off | Yes. `rules_rs` builds on the same module (decision 2) |
| Lint, format, commit checks | `oxlint`, `oxfmt`, `commitlint` through `lefthook`, **outside** Bazel after a plain `pnpm install` | Same idea: `cargo fmt` stays outside Bazel |
| CI | `bazel-contrib/setup-bazel` with `bazelisk-cache`, `disk-cache` keyed per job, `repository-cache`; then `bazel test //...` | Yes |

Two things worth copying beyond the table: live tests that need credentials are tagged `manual` so `//...` skips
them, and the published `package.json` is derived by a rule (`jq` strips `devDependencies`) rather than by a script.

### openai/codex (Rust Cargo workspace under Bazel)

perplexity-node has no Rust, so this is the reference for the Rust half. Its `defs.bzl` defines one macro,
`codex_rust_crate`, that every crate's `BUILD.bazel` calls. The macro mirrors Cargo conventions: a library from
`src/`, a unit-test target, one test target per `tests/*.rs`, build-script wiring, and `CARGO_BIN_EXE_*` exported to
integration tests. Dependencies come from `all_crate_deps` over the Cargo lockfile. We copy the shape and almost
none of the size: most of that file handles Windows, Wine and snapshot testing.

It uses `rules_rs` (hermeticbuild), a newer ruleset layered on `rules_rust`, and so do we (decision 2). What
`rules_rs` adds over plain `rules_rust`:

- `crate.from_cargo` reads `Cargo.lock` directly through Bazel's downloader: no Cargo workspace splicing and no
  second, Bazel-specific lockfile. Resolution is incremental.
- Per-member metadata. Each workspace member loads `aliases()`, `all_crate_deps()` and `crate_data()` from
  `@crates//<cargo-package-name>:data.bzl`. In codex, `all_crate_deps()` alone supplies a crate's dependencies,
  workspace members included **(verify)**; `crate_data()` carries the package name, version and binaries, so the
  macro does not restate `Cargo.toml`.
- Hermetic Rust toolchains and a hermetic C toolchain (`@llvm`), with cross-compilation from any supported host.
- `generate_lint_config = True` exposes each member's Cargo `[lints]` table to the Rust rules.
- It redistributes a patched `rules_rust` and can expose it as `@rules_rust`, so upstream documentation (clippy and
  rustfmt aspects, rust-analyzer setup) still applies.

What codex's setup also shows about the cost: it pins `rules_rs` to a commit with five local patches, and carries
five more against the bundled `rules_rust`. Most of them concern Windows, bindgen and release profiles, none of
which we need, and codex was pinned at 0.0.96 where the registry is now at 0.2.4. Still, expect to read the
ruleset's source at some point.

## Decisions

Recommendations first; the open ones are listed again at the end.

1. **pnpm workspace instead of two npm projects (confirmed 2026-11-12).** `rules_js` models `node_modules` from a pnpm lockfile. It can
   import a `package-lock.json` on the fly (`npm_package_lock`), but then Bazel resolves with pnpm while developers
   resolve with npm, and the two trees can differ. One `pnpm-workspace.yaml` at the root with both packages, one
   `pnpm-lock.yaml`, and the example's `"@durable/typescript": "file:../../bindings/typescript"` becomes
   `workspace:*`. This changes the documented commands from `npm` to `pnpm`.
2. **`rules_rs` 0.2.x (confirmed 2026-11-12), not plain `rules_rust`.** It is what openai/codex runs a much larger
   Cargo workspace on. We follow its paved path. The trade-off accepted: it is a 0.x ruleset with deprecations
   already scheduled (root `@crates//:defs.bzl` and root aliases go away after 2027-03-01, so we use the per-member
   `data.bzl` loads and `@crates//serde`-style labels from the start), and less documentation than `rules_rust`.
   The fallback is `rules_rust` with `crate_universe`; the two differ in `MODULE.bazel` and in the macro's load
   statements, not in the BUILD files.
3. **Cargo is the source of truth.** No hand-written dependency lists: `crate.from_cargo` reads `Cargo.toml` and
   `Cargo.lock`, and BUILD files call `all_crate_deps()`.
4. **Bazel 9.3.x, bzlmod only**, pinned in `.bazelversion`, run through bazelisk. Commit `MODULE.bazel.lock`.
5. **Hermetic C toolchain from the start.** The `rules_rs` paved path registers `@llvm//toolchain:all`, so the
   `aws-lc-sys` and `ring` build scripts compile with it rather than with the runner's gcc. Cache keys then do not
   depend on the runner image. Host C++ autodetection is turned off, as in both references.
6. **The release workflow stays on Cargo and `napi` CLI for now.** It builds on five native runners with a glibc
   2.35 floor, npm provenance and `napi pre-publish`. `rules_rs` makes cross-building the addons from one Linux
   runner plausible (the `@llvm` constraints include a glibc version), but that is a separate project with its own
   verification (Phase 5), and it saves no time on the PR path.
7. **`cargo fmt --check` stays outside Bazel.** It takes about a second and needs no build.
8. **`trybuild` stays on Cargo.** `tests/compile_fail.rs` shells out to `cargo` at test time, which a Bazel test
   cannot do. It runs in the slim Cargo job.

## Target layout

```
.bazelversion                 9.3.2
.bazelrc                      shared flags, :ci and :clippy configs
.bazelignore                  target, node_modules, bindings/typescript/node_modules, ...
MODULE.bazel, MODULE.bazel.lock
BUILD.bazel                   npm_link_all_packages, exports Cargo.toml / Cargo.lock
pnpm-workspace.yaml, pnpm-lock.yaml, package.json (root, private)
bazel/
  rust.bzl                    durable_rust_crate macro
  docker.bzl                  tags for tests that need the Docker socket
  checks/BUILD.bazel          dependency-hygiene tests (replaces the two `cargo tree | grep` checks)
crates/*/BUILD.bazel          one macro call each
plugins/*/BUILD.bazel
examples/*/BUILD.bazel
bindings/typescript/BUILD.bazel   addon (cdylib -> .node), typecheck, vitest, tsdown, npm_package
```

### `MODULE.bazel` (sketch)

```starlark
module(name = "durable_workflow")

bazel_dep(name = "rules_rs", version = "0.2.4")
bazel_dep(name = "llvm", version = "0.8.26")
bazel_dep(name = "aspect_rules_js", version = "3.6.0")
bazel_dep(name = "aspect_rules_ts", version = "3.10.1")
bazel_dep(name = "rules_nodejs", version = "6.7.7")
bazel_dep(name = "aspect_bazel_lib", version = "2.22.5")
bazel_dep(name = "bazel_skylib", version = "1.9.2")
bazel_dep(name = "platforms", version = "1.1.0")
bazel_dep(name = "rules_shell", version = "0.8.0")

toolchains = use_extension("@rules_rs//rs/toolchains:module_extension.bzl", "toolchains")
toolchains.toolchain(edition = "2024", version = "1.97.1")
use_repo(toolchains, "default_rust_toolchains")

# The patched rules_rust that rules_rs ships, under its usual name (clippy aspect, cargo_build_script, ...).
rules_rust = use_extension("@rules_rs//rs:rules_rust.bzl", "rules_rust")
use_repo(rules_rust, "rules_rust")

register_toolchains(
    "@default_rust_toolchains//...",
    "@llvm//toolchain:all",
)

crate = use_extension("@rules_rs//rs:extensions.bzl", "crate")
crate.from_cargo(
    name = "crates",
    cargo_lock = "//:Cargo.lock",
    cargo_toml = "//:Cargo.toml",
    generate_lint_config = True,
    incompatible_no_root_alias_targets = True,
    incompatible_no_root_crate_data = True,
    # Every exec and target triple that takes part in a build: CI, and the machines we develop on.
    platform_triples = [
        "x86_64-unknown-linux-gnu",
        "aarch64-unknown-linux-gnu",
        "aarch64-apple-darwin",
    ],
)
use_repo(crate, "crates")

node = use_extension("@rules_nodejs//nodejs:extensions.bzl", "node", dev_dependency = True)
node.toolchain(node_version = "22.18.0")   # the floor in package.json `engines`

npm = use_extension("@aspect_rules_js//npm:extensions.bzl", "npm", dev_dependency = True)
npm.npm_translate_lock(name = "npm", pnpm_lock = "//:pnpm-lock.yaml")
use_repo(npm, "npm")
```

CI uses floating `stable` Rust today. Bazel needs a pinned version, so add a `rust-toolchain.toml` with the same
version and let `dtolnay/rust-toolchain` read it: Cargo and Bazel then cannot drift.

`examples/typescript-onboarding` is excluded from the Cargo workspace and has no `Cargo.toml`, so `from_cargo`
does not see it. Windows is left out of `platform_triples`: nothing on the PR path builds there, and codex's
Windows support is where most of its patches and platform definitions go.

### `.bazelrc` (sketch)

```
common --enable_platform_specific_config
# Only the hermetic @llvm toolchain: never pick up the machine's compiler.
common --repo_env=BAZEL_DO_NOT_DETECT_CPP_TOOLCHAIN=1
common --repo_env=BAZEL_NO_APPLE_CPP_TOOLCHAIN=1
common --incompatible_strict_action_env
common --test_output=errors

common:ci --keep_going
common:ci --verbose_failures
common:ci --lockfile_mode=error
```

Both references add two more lines whose need here is unknown **(verify)**: an explicit Linux `--host_platform`
carrying a glibc constraint (codex: "so that rust can grab a toolchain", because a musl-built rustc cannot load
proc macros), and `--@llvm//config:experimental_stub_libgcc_s`. The `rules_rs` README says Linux hosts work with
the default host platform, so start without them.

### The crate macro (sketch)

```starlark
# crates/durable-cli/BUILD.bazel
load("//bazel:rust.bzl", "durable_rust_crate")
load("//bazel:docker.bzl", "DOCKER_TEST_TAGS")

durable_rust_crate(
    name = "durable-cli",
    test_tags = {"smoke": DOCKER_TEST_TAGS},
)
```

Name, version and binaries come from `crate_data()`; dependencies, workspace members included, from
`all_crate_deps()` **(verify)**. Starlark cannot compute a `load` label, so either each BUILD file loads its own
`@crates//<package>:data.bzl` and passes the three helpers to the macro, or the macro uses the root
`@crates//:data.bzl` table keyed by package path, as codex does. The root table is the one scheduled for removal,
so take the first form.

`durable_rust_crate` expands to:

- `rust_library` (or `rust_proc_macro`, or only `rust_binary`) from `@rules_rs//rs:*.bzl` over
  `glob(["src/**/*.rs"])`, with `deps = all_crate_deps(normal = True)`, `aliases = aliases()`,
  `lint_config = crate_data().get("lint_config")`, and `compile_data` for files pulled in by `include_str!`;
- `rust_test(name = "unit_test", crate = ":lib")` for `#[cfg(test)]` modules;
- one `rust_test` per `tests/*.rs`, with `tests/common/**` as shared sources and the dev dependencies;
- `rust_doc_test`, since `cargo test --workspace` runs doctests today;
- for each binary, `CARGO_BIN_EXE_<name>` set through `rustc_env` with the binary in `data`. `smoke.rs` reads it
  with `env!`, at compile time, so a path relative to the runfiles root is enough. codex resolves it at run time
  through a launcher rule because it disables runfiles trees; we do not need that.

One test target per file is what gives per-file caching and parallelism: `plugins/durable-redis` has three test
files, and a change to a Lua script reruns only the tests that embed it.

## What has to change in the code

Found by grepping the workspace for things that assume Cargo:

| Where | Assumption | Change |
|---|---|---|
| `crates/durable-testkit/src/corpus.rs:64` | `env!("CARGO_MANIFEST_DIR")` baked in at compile time locates the corpus | Read `DURABLE_CORPUS_DIR` at run time first, fall back to the compile-time path. Bazel sets the variable and lists `corpus/**` as `data`. Rewriting the corpus (`DURABLE_UPDATE_CORPUS=1`) stays a Cargo command |
| `crates/durable-cli/tests/smoke.rs:20` | `env!("CARGO_BIN_EXE_durable")` | None in the source. The macro sets it via `rustc_env` |
| `plugins/durable-postgres/src/migrate.rs`, `plugins/durable-redis/src/{queue,services,streams}.rs`, `examples/llm-streaming/src/main.rs` | `include_str!` of `.sql`, `.lua`, `.html` | None in the source. List them as `compile_data` (a glob per crate) |
| `bindings/typescript/build.rs` | `napi_build::setup()` emits linker flags | `cargo_build_script` target; its flags reach the `rust_shared_library` **(verify on macOS)** |
| `bindings/typescript/ts/native.ts` | Looks for `durable.<platform>.node` next to `ts/` | None. It already honours `DURABLE_NATIVE`, so Bazel tests point that at the built addon |
| `bindings/typescript/test/corpus.ts:24` | Relative path `../../../crates/durable-testkit/corpus/` | Works if the corpus is in the test's `data` and the layout is preserved in runfiles **(verify)**; otherwise read the same `DURABLE_CORPUS_DIR` |
| `examples/typescript-onboarding/package.json` | `file:../../bindings/typescript` | `workspace:*` |
| `crates/durable-workflow/tests/compile_fail.rs` | `trybuild` runs `cargo` | Tagged `manual` in Bazel, runs in the Cargo job |

### Features

Cargo builds a crate once per feature set requested; Bazel builds each target in exactly one configuration.
`./ci.sh` already tests with `--all-features`, so the main targets set `crate_features` to the full list and
behave like today. Three places need care:

- **`durable-workflow` standalone check.** Today: `cargo tree --features standalone | grep opentelemetry` must be
  empty, plus a separate `cargo build --features standalone`. In Bazel: a second library target
  `//crates/durable-workflow:standalone` with only `postgres`, `observable` and `standalone` enabled and an
  explicit dependency list, and a test in `//bazel/checks` asserting that
  `deps(//crates/durable-workflow:standalone)` contains no `opentelemetry*` crate. Building the target replaces the
  extra `cargo build`.
- **`durable-sm` purity check.** Same mechanism: `deps(//crates/durable-sm)` must not contain `tokio`, `futures*`
  or `async-trait`. This becomes a property of the graph, checked without compiling anything.
- **`bindings/typescript`** depends on `durable-workflow` with `default-features = false` and three features. Under
  Bazel it links the all-features target, so the addon built by Bazel carries more code than the released one.
  That is fine for tests. It is one more reason the release workflow stays on Cargo.

Whether `all_crate_deps` includes optional dependencies whose feature is off needs checking **(verify)**; the
`standalone` target avoids the question by listing its dependencies.

### Tests that need Docker

`plugins/durable-postgres`, `plugins/durable-redis`, `crates/durable-cli` (`smoke`) and `crates/durable-workflow`
(`chaos`, `standalone`, and whatever else uses `tests/common`) start containers through testcontainers. The
TypeScript `backend`, `chaos` and example tests want `DURABLE_TEST_DATABASE_URL` / `_REDIS_URL`.

```starlark
# bazel/docker.bzl
DOCKER_TEST_TAGS = ["requires-network", "no-sandbox", "docker"]
```

- They run unsandboxed with network access, and `size = "large"`.
- They **stay cacheable**. This is where most of the CI time goes, so a cached pass for an unchanged plugin is the
  main win. The cost is that a cached result does not notice a new `postgres:17` image. A weekly scheduled run with
  `--nocache_test_results` covers that.
- `bazel test //... --test_tag_filters=-docker` is the no-Docker command AGENTS.md describes today.
- The chaos test noted as flaky in `docs/BACKLOG.md` gets `flaky = True` only if it still flakes under Bazel; a
  retry is not a fix.
- TypeScript server tests: wrap the Vitest target with `scripts/with-servers.sh` as it is today (one container pair
  per test target), or split into a memory-only target and a `docker`-tagged one. Start with the split, so the
  memory-only target is sandboxed and hermetic.

## Phases

Each phase ends in a mergeable PR. `./ci.sh` keeps working until Phase 4.

### Phase 0: baseline (half a day)

- Time each stage of `./ci.sh` in CI over about ten runs: fmt, clippy, Rust tests, the two builds, each npm
  project. Record cold (no cache) and warm numbers here.

Done when: the table at the top of this file has real numbers, and a target is written down (suggested: a change
confined to `bindings/typescript/ts` or to one plugin finishes in under 2 minutes; a change to `durable-sm` is no
slower than today).

### Phase 1: Rust builds (1 to 2 days)

- `.bazelversion`, `.bazelrc`, `.bazelignore`, `MODULE.bazel`, `rust-toolchain.toml`.
- `bazel/rust.bzl` with the library, proc-macro and binary parts of the macro.
- `BUILD.bazel` for every crate, in dependency order: `durable-sm`, `durable-spi`, `durable-macros`,
  `durable-testkit`, `durable-memory`, `durable-runtime`, `durable-postgres`, `durable-redis`, `durable-otel`,
  `durable-api`, `durable-workflow`, `durable-cli`, the three examples, then the addon as `rust_shared_library`.
- `.gitignore`: `bazel-*`.

Likely trouble, in the order it will appear:

1. Toolchain resolution on the first build: the hermetic Rust and `@llvm` toolchains must both match the host
   platform. If Bazel reports no matching toolchain on Linux, add the explicit host platform from the `.bazelrc`
   notes above.
2. `aws-lc-sys` and `ring` build scripts (pulled in only through `testcontainers` -> `bollard` -> `rustls`). They
   compile C, and `aws-lc-sys` may want cmake, inside the build-script action with the hermetic toolchain.
   `rules_rs` ships annotations for some `-sys` crates (`3rd_party/` in its repository) and codex takes AWS-LC from
   the registry (`bazel_dep(name = "aws-lc")`); check whether `aws-lc-sys` is covered out of the box **(verify)**.
   Escape hatch: `crate.annotation(crate = "aws-lc-sys", build_script_env = {...})`.
3. `sqlx` and `redis` with `default-features = false`: feature resolution must match Cargo's. Compare
   `bazel query 'deps(//...)' | grep @crates` against `cargo tree` once.
4. macOS: `rules_rs` uses a hermetic macOS SDK by default, which it downloads. If that is a problem locally,
   `--@rules_rust//rust/settings:use_hermetic_macos_sdkroot=false` uses Xcode's.
5. The proc-macro crate uses `syn` 3; nothing special expected.

Done when: `bazel build //...` passes locally on Linux and on macOS arm64, and a no-op rebuild is fully cached.

### Phase 2: Rust tests and lint (2 days)

- The test parts of the macro; the `durable-testkit` corpus change; `DOCKER_TEST_TAGS`.
- `//bazel/checks`: the two dependency-hygiene tests.
- Clippy as an aspect, behind a config so local builds are not slowed down:

  ```
  build:clippy --aspects=@rules_rust//rust:defs.bzl%rust_clippy_aspect
  build:clippy --output_groups=+clippy_checks
  build:clippy --@rules_rust//rust/settings:clippy_flag=-Dwarnings     # (verify the flag name)
  ```

  Clippy then runs per crate and is cached per crate. `@rules_rust` here is the copy `rules_rs` exposes. The
  workspace has no `[lints]` tables today, so `generate_lint_config` changes nothing yet; it is there so lints added
  to `Cargo.toml` later apply to both build systems.

Done when: `bazel test //...` runs the same tests as `cargo test --workspace --all-features`. Check by count:
compare the number of `test result:` cases per crate from both. A missing test file is the failure mode to guard
against, because a glob that matches nothing fails silently; set `allow_empty = False` on the test glob.

### Phase 3: TypeScript (2 to 3 days)

- Root `package.json` (private), `pnpm-workspace.yaml`, `pnpm-lock.yaml` generated with `pnpm import` from the two
  `package-lock.json` files so versions do not move; delete the npm lockfiles; update README and AGENTS.md
  commands.
- `bindings/typescript/BUILD.bazel`:
  - `copy_file` from the `rust_shared_library` output to `durable.<platform>.node`. The platform suffix is a
    `select()` over OS and CPU, matching `napi build --platform`.
  - `ts_project(name = "typecheck", no_emit = True, tsc = ":tsc")` with the TS 7 binary, as perplexity-node does.
    The tsconfig uses `allowImportingTsExtensions` and `erasableSyntaxOnly`; pass the existing `tsconfig.json`
    rather than restating the options.
  - Vitest through the generated `vitest_bin.vitest_test`, with `env = {"DURABLE_NATIVE": "$(rootpath :addon)"}`
    and the addon, `ts/**`, `test/**` and the corpus as `data`.
  - tsdown through `tsdown_bin.tsdown` producing `dist/`, then `npm_package(name = "pkg")` with `dist/` and the
    addon, so `examples/typescript-onboarding` resolves `@durable/typescript` to the built package through the
    pnpm workspace link.
- `examples/typescript-onboarding/BUILD.bazel`: typecheck and Vitest.

Vitest under `rules_js` is the least certain part of the plan **(verify)**: `pool: "forks"` spawns child processes,
Vitest writes a cache directory, and `rules_js` runs out of a symlinked `node_modules` in runfiles. Spike this
first, before the rest of the phase. If it does not work cleanly within half a day, the fallback is a `sh_test`
that runs the pnpm-installed Vitest unsandboxed with the Bazel-built addon: it loses hermeticity but keeps the
caching, which is what we are after.

Done when: `bazel test //bindings/... //examples/typescript-onboarding/...` passes, and editing a `.ts` file
recompiles no Rust.

### Phase 4: CI (1 day, then a soak)

```yaml
jobs:
  bazel:
    runs-on: ubuntu-24.04
    steps:
      - uses: actions/checkout@v4
      - uses: bazel-contrib/setup-bazel@<pinned sha>
        with:
          bazelisk-cache: true
          disk-cache: ${{ github.workflow }}
          repository-cache: true
      - run: bazel test --config=ci --config=clippy //...
  cargo:
    # Keeps the Cargo build honest: it is what users of the crates run.
    steps:
      - cargo fmt --all --check
      - cargo check --workspace --all-targets --all-features
      - cargo test -p durable-workflow --test compile_fail
```

- Run the `bazel` job next to the existing `ci` job for about two weeks. Both must agree on every PR.
- **Go/no-go gate**: compare against the Phase 0 target. If leaf changes are not clearly faster, stop here, keep
  `./ci.sh`, and write down why.
- On go: `./ci.sh` becomes a wrapper around the two jobs' commands so local and CI stay the same, the `ci` job is
  removed, and AGENTS.md is updated.
- Add the weekly `--nocache_test_results` run.

GitHub's cache holds 10 GB per repository and a PR can read the default branch's cache, so a PR starts from
`main`'s state. The disk cache is saved whole, so set `--experimental_disk_cache_gc_max_size` to keep it bounded.

### Phase 5: optional follow-ups, each on its own merits

| Item | Worth it when |
|---|---|
| Remote cache on BuildBuddy (see below) | The disk cache is evicted often, or developers want CI's cache locally |
| Reuse in `TypeScript release`: the linux-x64 build and test leg becomes a Bazel invocation sharing the cache | After a remote cache exists |
| Cross-build the release addons from one Linux runner (the toolchains are already in place with `rules_rs`) | The five-runner matrix becomes a cost or a reliability problem. Needs its own check that the glibc floor and the macOS and Windows artifacts match what `napi build` produces today |
| `publint` / `attw` tests on the npm package | Any time; small |
| Python binding | Start it inside Bazel (`rules_python`, `rules_uv`, PyO3 cdylib built the same way as the addon) |
| `gazelle` for BUILD files | The number of TypeScript packages grows past a handful |

#### Remote cache: BuildBuddy (confirmed 2026-11-12)

The hosted service, cache and build-event UI only; no remote execution. The flags are the ones codex uses for its
generic BuildBuddy config:

```
common:buildbuddy --remote_cache=grpcs://remote.buildbuddy.io
common:buildbuddy --bes_backend=grpcs://remote.buildbuddy.io
common:buildbuddy --bes_results_url=https://app.buildbuddy.io/invocation/
common:buildbuddy --remote_cache_compression
common:buildbuddy --remote_timeout=3600

common:ci --remote_download_minimal
```

- The API key is a GitHub secret, passed on the command line as
  `--remote_header=x-buildbuddy-api-key=${{ secrets.BUILDBUDDY_API_KEY }}`, never written to `.bazelrc`.
- **Two keys.** A read-write key for pushes to `main`; a read-only key for pull-request runs, so a PR cannot
  poison the cache that `main` and other PRs read.
- **Forks get no secrets**, so a fork's PR runs without `--config=buildbuddy`. Keep `setup-bazel`'s disk cache
  enabled for that case; for runs that do have the remote cache, turn the disk cache off (codex does the same)
  so the same blobs are not also stored in GitHub's 10 GB.
- The build-event stream uploads target names, command lines and test logs. The repository is public, so that is
  acceptable, but the Docker tests' logs go there too: check they print no credentials beyond the throwaway
  container passwords.
- The Docker-tagged tests are unsandboxed, so their cached results are only as trustworthy as their declared
  inputs. With a shared cache this matters more than with a per-branch disk cache: the weekly
  `--nocache_test_results` run stays.
- Check the current free-tier limits (cache size, retention, whether open-source projects need to apply) before
  relying on it **(verify)**; they were not looked up for this plan.

Done when: a PR that touches only documentation finishes `bazel test //...` with every action a remote cache hit,
and the invocation link appears in the job log.

## Developer workflow after the change

- Cargo keeps working for everything it does today, including rust-analyzer. Bazel is not required for the inner
  loop.
- BUILD files change only when a new crate is created or a test needs Docker. Dependencies and new test files are
  picked up from `Cargo.toml` and globs.
- After changing `Cargo.toml` or `Cargo.lock`, the next Bazel run re-resolves `@crates` and may update
  `MODULE.bazel.lock` (`rules_rs` records crate checksums there); commit it. CI runs with `--lockfile_mode=error` so a stale lockfile fails fast.
- New commands for AGENTS.md: `bazel test //...`, `bazel test //... --test_tag_filters=-docker`,
  `bazel test //plugins/durable-redis/...`, `bazel build --config=clippy //...`.

## Risks

| Risk | Likelihood | Mitigation |
|---|---|---|
| Cargo and Bazel builds drift (a feature or dependency works in one only) | Medium | The Cargo job in CI; `all_crate_deps` instead of hand-written lists; the test-count comparison in Phase 2 |
| Vitest does not run cleanly under `rules_js` | Medium | Spike first; `sh_test` fallback |
| A native build script (`aws-lc-sys`, `ring`) fails under the hermetic toolchain | Medium | `rules_rs` annotations; `crate.annotation`; both are dev-only dependencies |
| `rules_rs` is a 0.x ruleset: a breaking release, a bug we have to patch, or thin documentation | Medium | Pin the version; use the non-deprecated per-member API from day one; fall back to `rules_rust` + `crate_universe`, which changes `MODULE.bazel` and the macro only |
| Cached results hide a broken container image or a flaky test | Low | Weekly uncached run; no `flaky = True` without a reason |
| The GitHub cache is too small or evicted, so CI is slower than today | Medium | Bound the disk cache; move to BuildBuddy (Phase 5) |
| A pull request writes a bad entry into the shared remote cache | Low | Read-only key for PR runs; only `main` writes |
| Maintenance cost exceeds the time saved | Real at this size | The Phase 4 gate. Phases 1 to 3 are additive and can be deleted without touching the Cargo build |

## Open decisions

Settled on 2026-11-12: npm to pnpm (decision 1), `rules_rs` over plain `rules_rust` (decision 2), and BuildBuddy
as the remote cache, set up in Phase 5 so the Phase 4 comparison against `./ci.sh` measures Bazel alone.

1. **Is the Phase 0 target right?** "Leaf change under 2 minutes, core change no slower than today" is a
   suggestion.

## Sources

- perplexityai/perplexity-node at `main` (cloned 2026-11-12): `MODULE.bazel`, `.bazelrc`, `BUILD.bazel`, `ts.bzl`,
  `tsconfig.bzl`, `.github/workflows/ci.yml`.
- openai/codex at `main`: `defs.bzl`, `MODULE.bazel`, `.bazelrc`, root `BUILD.bazel` (host platforms).
- rules_rust documentation: crate_universe with bzlmod, `all_crate_deps`, clippy and rustfmt aspects,
  `rust_test_suite`, rust-analyzer setup. <https://bazelbuild.github.io/rules_rust/>
- hermeticbuild/rules_rs README. <https://github.com/hermeticbuild/rules_rs>
- Aspect `rules_js` documentation on `npm_translate_lock` and pnpm. <https://docs.aspect.build/bazel/javascript>
- Module versions from the Bazel Central Registry on 2026-11-12: rules_rust 0.75.0, rules_rs 0.2.4,
  aspect_rules_js 3.6.0, aspect_rules_ts 3.10.1, rules_nodejs 6.7.7; Bazel 9.3.2.
