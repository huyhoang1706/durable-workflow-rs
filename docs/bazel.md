# Building with Bazel

The Rust workspace builds with **Cargo or Bazel**, your choice. Cargo is the default and stays the source of
truth: `Cargo.toml` and `Cargo.lock` decide crates, versions, features and dependencies, and Bazel reads them.
You never need Bazel to work on this repository; `./ci.sh` and every `cargo` command work as before.

Bazel adds per-target caching: an unchanged crate's compile, clippy run and test results are cache hits, and
each `tests/*.rs` file is its own target, so a change to one plugin reruns only that plugin's tests.

The TypeScript binding and its example still build with npm only (see the plan in
[`plans/bazel-integration.md`](plans/bazel-integration.md), Phase 3).

## Commands

Install [bazelisk](https://github.com/bazelbuild/bazelisk) (`brew install bazelisk`, or
`npm install -g @bazel/bazelisk`); it fetches the Bazel version in `.bazelversion`. The Rust toolchain and the
C/C++ toolchain are downloaded by Bazel (hermetic), so the machine's compilers are not used.

```bash
bazel build //...                                   # every crate, binary and the Node addon
bazel test //...                                    # every test; Postgres/Redis suites need Docker
bazel test //... --test_tag_filters=-docker         # without Docker
bazel test //plugins/durable-redis/...              # one crate
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
| `cargo fmt --all` | stays Cargo |
| `cargo test -p durable-workflow --test compile_fail` (trybuild) | stays Cargo: trybuild runs `cargo` |
| `DURABLE_UPDATE_CORPUS=1 cargo test ... replay_corpus` | stays Cargo: it writes into the source tree |

Machine-local flags (a disk cache, a remote cache key) go in `user.bazelrc`, which is git-ignored, for example
`common --disk_cache=~/.cache/bazel-disk`.

## How it fits together

```
.bazelversion, .bazelrc, .bazelignore
MODULE.bazel, MODULE.bazel.lock   rules_rs reads Cargo.toml/Cargo.lock into the @crates repository
rust-toolchain.toml               the Rust version for rustup; MODULE.bazel pins the same one
bazel/rust.bzl                    durable_rust_crate: Cargo's layout as Bazel targets
bazel/checks/                     dependency-hygiene tests
<crate>/BUILD.bazel               one durable_rust_crate call per crate
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
| `crate-type = ["cdylib"]` | `:<lib name>_shared`, e.g. `//bindings/typescript:durable_typescript_shared` |
| `build.rs` | `:build_script` |
| non-`.rs` files under `src/` (`include_str!`) | compile inputs |

## When you need to touch Bazel files

Most changes need none: dependencies, features and versions come from `Cargo.toml`, and new source or test
files are picked up by globs.

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
  `//crates/durable-workflow:standalone`, because `//bazel/checks` asserts it stays free of OpenTelemetry.
- **Dependency hygiene** (the `cargo tree | grep` checks in `ci.sh`) runs as tests:
  `//bazel/checks:durable_sm_is_sans_io` and `//bazel/checks:standalone_has_no_opentelemetry`.
- **No absolute paths in compiled code.** rules_rs refuses an output that embeds the sandbox path, which is what
  `env!("CARGO_MANIFEST_DIR")` would bake in. `durable-testkit` sets `CARGO_MANIFEST_DIR` to its package path,
  which is where the corpus sits in a test's runfiles; `durable-cli`'s smoke test gets `CARGO_BIN_EXE_durable`
  the same way.
- **The Node addon** built by Bazel links the hermetic toolchain's `libc++` dynamically, found through an
  `$ORIGIN`-relative path inside `bazel-bin`. Fine for tests under Bazel; published addons keep coming from the
  `TypeScript release` workflow (Cargo and the napi CLI).

## Workarounds in `MODULE.bazel`

- `sqlx-postgres` gets the `any` and `offline` features. rules_rs 0.2.4 unions the `sqlx-core` features that
  `sqlx-macros-core` requests even though that crate is only in `Cargo.lock` and never built, so `sqlx-core`
  is compiled with them and `sqlx-postgres` has to match. Remove the annotation once rules_rs resolves features
  like Cargo.
- `aws-lc-sys` (reqwest → rustls → aws-lc-rs) runs its own build script, compiled with the hermetic C toolchain.
  rules_rs also offers a fully hermetic replacement built from the `aws-lc` Bazel module
  (`@rules_rs//3rd_party/aws-lc-sys:include.MODULE.bazel`); it has not been tried here yet.

## Not covered yet

- The TypeScript binding and example (Phase 3 of the plan).
- A remote cache (BuildBuddy, Phase 5). Until then CI keeps a disk cache through `setup-bazel`.
- Windows: not in `platform_triples`.
