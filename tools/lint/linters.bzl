"""Linter aspects, run by `bazel build --config=lint //...` (.bazelrc, docs/bazel.md).

Each one visits the existing targets: nothing is added to the BUILD files.

- clippy: every rust_library / rust_binary / rust_shared_library / rust_test, with the
  same lints as `cargo clippy` (clippy.toml at the root, `[lints]` from Cargo.toml).
- shellcheck: sh_binary / sh_library / sh_test sources.
- buildifier: filegroups tagged "starlark" (//:starlark_files, //bazel:starlark_files,
  ...) and bzl_library targets.

A target opts out with the `no-lint` tag (clippy also honours `noclippy`).
"""

load("@aspect_rules_lint//lint:buildifier.bzl", "lint_buildifier_aspect")
load("@aspect_rules_lint//lint:shellcheck.bzl", "lint_shellcheck_aspect")
load("@aspect_rules_lint_rust//:clippy.bzl", "lint_clippy_aspect")

clippy = lint_clippy_aspect(
    config = Label("//:clippy.toml"),
)

shellcheck = lint_shellcheck_aspect(
    binary = Label("@aspect_rules_lint//lint:shellcheck_bin"),
    config = Label("//:.shellcheckrc"),
)

buildifier = lint_buildifier_aspect(
    binary = Label("@multitool//tools/buildifier"),
)
