"""`durable_rust_crate`: Bazel targets for one Cargo package, following Cargo's layout.

Cargo stays the source of truth. Everything a target needs that Cargo already knows
(crate name, edition, dependencies, enabled features, binaries, cdylibs) comes from
the per-member metadata `rules_rs` generates from `Cargo.toml` and `Cargo.lock`:

    load("@crates//durable-sm:data.bzl", "aliases", "all_crate_deps", "crate_data")
    load("//bazel:rust.bzl", "durable_rust_crate")

    durable_rust_crate(
        aliases = aliases,
        all_crate_deps = all_crate_deps,
        crate_data = crate_data,
    )

What it expands to, by Cargo convention:

| Cargo                          | Bazel target                                  |
|--------------------------------|-----------------------------------------------|
| `src/lib.rs`                   | `:<dir name>` (`rust_library` / `rust_proc_macro`); |
|                                | the name is what `//crates/<dir>` resolves to |
| `#[cfg(test)]` in the library  | `:unit_test`                                  |
| doctests                       | `:doc_test`                                   |
| `tests/<name>.rs`              | `:<name>_test` (one target per file, so each  |
|                                | file is cached and run on its own)            |
| `[[bin]]` / `src/main.rs`      | `:<bin name>`                                 |
| `crate-type = ["cdylib"]`      | `:<lib name>_shared` (`rust_shared_library`)  |
| `build.rs`                     | `:build_script` (`cargo_build_script`)        |

Shared test modules (`tests/common/mod.rs`, ...) are sources of every test target.
"""

load("@rules_rs//rs:cargo_build_script.bzl", "cargo_build_script")
load("@rules_rs//rs:rust_binary.bzl", "rust_binary")
load("@rules_rs//rs:rust_library.bzl", "rust_library")
load("@rules_rs//rs:rust_proc_macro.bzl", "rust_proc_macro")
load("@rules_rs//rs:rust_shared_library.bzl", "rust_shared_library")
load("@rules_rs//rs:rust_test.bzl", "rust_test")
load("@rules_rust//rust:defs.bzl", "rust_doc_test")

# Tests that start Postgres/Redis containers through testcontainers. They need the
# Docker socket and the network, so they run outside the sandbox. They stay cacheable:
# an unchanged plugin's container suite is the most expensive thing not to rerun.
# `bazel test //... --test_tag_filters=-docker` runs everything else.
DOCKER_TEST_TAGS = ["docker", "no-sandbox", "requires-network"]

# The replay corpus, for tests that read it through `durable_testkit::corpus`.
CORPUS_DATA = ["//crates/durable-testkit:corpus"]

def _dedupe(items):
    return {item: None for item in items}.keys()

def durable_rust_crate(
        crate_data,
        all_crate_deps,
        aliases,
        proc_macro = False,
        crate_features = [],
        deps = [],
        compile_data = None,
        rustc_env = {},
        test_tags = {},
        test_data = {},
        test_rustc_env = {},
        visibility = ["//visibility:public"]):
    """Declares the Bazel targets of the Cargo package in this directory.

    Args:
      crate_data: `crate_data` from `@crates//<package>:data.bzl`.
      all_crate_deps: `all_crate_deps` from the same file.
      aliases: `aliases` from the same file.
      proc_macro: `[lib] proc-macro = true` in `Cargo.toml` (the generated metadata
        does not carry the crate type).
      crate_features: Features to enable on top of the ones Cargo's workspace feature
        unification resolves (what `--all-features` would add). The dependencies
        those features turn on go in `deps`.
      deps: Extra normal dependencies, for `crate_features`.
      compile_data: Non-Rust files the crate reads at compile time (`include_str!`).
        Defaults to every non-`.rs` file under `src/`.
      rustc_env: Compile-time environment of the library (what `env!` reads).
      test_tags: `{"<test file stem>": [tags]}`; for example `DOCKER_TEST_TAGS`, or
        `["manual"]` for a test that cannot run under Bazel.
      test_data: `{"<test file stem>": [labels]}`, runtime files of a test.
      test_rustc_env: `{"<test file stem>": {"VAR": "value"}}`, set when the test
        compiles (what `env!` reads).
      visibility: Visibility of the library, binaries and shared libraries.
    """
    data = crate_data()

    # The directory name: `//crates/<dir>` is the label other members' generated
    # dependency lists point at.
    lib_name = native.package_name().split("/")[-1]
    crate_name = data["crate_name"]
    edition = data["edition"]
    features = list(_dedupe(data["crate_features"] + crate_features))
    lint_config = data.get("lint_config")
    crate_aliases = aliases()

    normal_deps = all_crate_deps(normal = True) + deps
    test_deps = all_crate_deps(normal = True, normal_dev = True) + deps

    if compile_data == None:
        compile_data = native.glob(["src/**"], exclude = ["src/**/*.rs"], allow_empty = True)

    # build.rs: its `cargo:` output (link flags, cfgs, env) reaches the crate's
    # library, binaries and shared libraries through `deps`.
    build_script = []
    if native.glob(["build.rs"], allow_empty = True):
        cargo_build_script(
            name = "build_script",
            srcs = ["build.rs"],
            crate_root = "build.rs",
            edition = edition,
            deps = all_crate_deps(build = True),
            crate_features = features,
        )
        build_script = [":build_script"]

    common = dict(
        rustc_env = rustc_env,
        edition = edition,
        crate_features = features,
        compile_data = compile_data,
        lint_config = lint_config,
    )

    srcs = native.glob(["src/**/*.rs"])
    has_lib = bool(native.glob(["src/lib.rs"], allow_empty = True)) and not data["shared_libraries"]
    lib_label = ":" + lib_name

    if has_lib:
        rule = rust_proc_macro if proc_macro else rust_library
        rule(
            name = lib_name,
            crate_name = crate_name,
            crate_root = "src/lib.rs",
            srcs = srcs,
            aliases = crate_aliases,
            deps = normal_deps + build_script,
            visibility = visibility,
            **common
        )
        rust_test(
            name = "unit_test",
            crate = lib_label,
            aliases = crate_aliases,
            deps = test_deps,
            **common
        )
        if not proc_macro:
            rust_doc_test(
                name = "doc_test",
                crate = lib_label,
                deps = test_deps,
            )

    # cdylib crates (a Node-API addon): the library is only ever loaded by a host
    # process, so there is no rlib, unit test or doctest, matching `test = false`.
    for so_name, root in data["shared_libraries"].items():
        rust_shared_library(
            name = so_name + "_shared",
            crate_name = so_name,
            crate_root = root,
            srcs = srcs,
            aliases = crate_aliases,
            deps = normal_deps + build_script,
            visibility = visibility,
            **common
        )

    for bin_name, root in data["binaries"].items():
        rust_binary(
            name = bin_name,
            crate_name = bin_name.replace("-", "_"),
            crate_root = root,
            srcs = srcs,
            aliases = crate_aliases,
            deps = normal_deps + build_script + ([lib_label] if has_lib else []),
            visibility = visibility,
            **common
        )

    # One target per `tests/*.rs`, like Cargo. Shared modules (`tests/common/mod.rs`)
    # are sources of each; `tests/ui/` holds trybuild fixtures, not modules.
    shared_test_srcs = native.glob(["tests/*/**/*.rs"], exclude = ["tests/ui/**"], allow_empty = True)
    for test_file in native.glob(["tests/*.rs"], allow_empty = True):
        stem = test_file[len("tests/"):-len(".rs")]
        rust_test(
            name = stem + "_test",
            crate_name = stem.replace("-", "_"),
            crate_root = test_file,
            srcs = [test_file] + shared_test_srcs,
            aliases = crate_aliases,
            deps = test_deps + ([lib_label] if has_lib else []),
            data = test_data.get(stem, []),
            rustc_env = test_rustc_env.get(stem, {}),
            tags = test_tags.get(stem, []),
            size = "large" if "docker" in test_tags.get(stem, []) else None,
            edition = edition,
            crate_features = features,
            lint_config = lint_config,
        )
