#!/usr/bin/env bash
# fmt, clippy (warnings are errors) and the full test suite.
# Tests for the Postgres and Redis plugins, the CLI and the chaos suite start
# their own containers, so Docker must be running.
set -euo pipefail
cargo fmt --all --check
# Shell and Starlark formatting (shfmt, buildifier), which CI checks too. rules_lint
# runs them, so this needs Bazel (bazelisk); without it, run it before you push.
if command -v bazel >/dev/null; then
    bazel run //:format.check
else
    echo "warning: bazel not found; skipping shfmt/buildifier (bazel run //:format.check)" >&2
fi

# Bazel builds the same crates (docs/bazel.md). It needs no Bazel to check, and it is
# the one thing a Cargo-only change can forget: a new crate needs a BUILD.bazel.
missing=0
for dir in $(cargo metadata --no-deps --format-version 1 |
    grep -o '"manifest_path":"[^"]*"' | sed -E 's|.*"(.*)/Cargo.toml"|\1|'); do
    if [[ ! -f "$dir/BUILD.bazel" ]]; then
        pkg=$(sed -nE 's/^name = "(.*)"/\1/p' "$dir/Cargo.toml" | head -1)
        cat >&2 <<EOF
error: ${dir#"$PWD"/} has no BUILD.bazel. Create it with:

load("@crates//$pkg:data.bzl", "aliases", "all_crate_deps", "crate_data")
load("//bazel:rust.bzl", "durable_rust_crate")

durable_rust_crate(
    aliases = aliases,
    all_crate_deps = all_crate_deps,
    crate_data = crate_data,
)

EOF
        missing=1
    fi
done
[[ $missing -eq 0 ]]
cargo clippy --workspace --all-targets --all-features -- -D warnings
# libtest decides colours on its own; pass CARGO_TERM_COLOR on (CI sets `always`).
cargo test --workspace --all-features -- --color "${CARGO_TERM_COLOR:-auto}"

# The default (`standalone`) build must stay free of OpenTelemetry: it is the
# opt-in `otel` feature that pulls it in.
if cargo tree -p durable-workflow --features standalone -e normal | grep -q opentelemetry; then
    echo "error: the standalone build depends on opentelemetry" >&2
    exit 1
fi
cargo build -p durable-workflow --features standalone

# Every SDK embeds durable-sm: it must stay free of async runtimes and IO.
if cargo tree -p durable-sm -e normal | grep -qE ' (tokio|futures|async-trait)[a-z_-]* v'; then
    echo "error: durable-sm depends on an async runtime" >&2
    exit 1
fi

# The TypeScript SDK: the native addon, type checks, the corpus checks
# through the FFI and the end-to-end tests (Vitest), and the tsdown bundle.
# Needs Node >= 22.18 and pnpm (the root is a pnpm workspace of both packages).
# `test:servers` runs the Postgres/Redis cases too, on containers it starts (or
# on DURABLE_TEST_DATABASE_URL / _REDIS_URL if set).
if command -v node >/dev/null && command -v pnpm >/dev/null; then
    pnpm install --frozen-lockfile
    (cd durable-workflow-ts && pnpm run build:native && pnpm run typecheck && pnpm run test:servers && pnpm run build)
    # The example imports the built SDK (`dist/`, `durable.<platform>.node`) from above.
    (cd examples/typescript-onboarding && pnpm run typecheck && pnpm run test:servers)
else
    echo "warning: node or pnpm not found; skipping the TypeScript SDK" >&2
fi
