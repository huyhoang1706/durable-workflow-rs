#!/usr/bin/env bash
# fmt, clippy (warnings are errors) and the full test suite.
# Tests for the Postgres and Redis plugins, the CLI and the chaos suite start
# their own containers, so Docker must be running.
set -euo pipefail
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features

# The default (`standalone`) build must stay free of OpenTelemetry: it is the
# opt-in `otel` feature that pulls it in.
if cargo tree -p durable-workflow --features standalone -e normal | grep -q opentelemetry; then
    echo "error: the standalone build depends on opentelemetry" >&2
    exit 1
fi
cargo build -p durable-workflow --features standalone

# Language bindings embed durable-sm: it must stay free of async runtimes and IO.
if cargo tree -p durable-sm -e normal | grep -qE ' (tokio|futures|async-trait)[a-z_-]* v'; then
    echo "error: durable-sm depends on an async runtime" >&2
    exit 1
fi

# The TypeScript binding: the native addon, type checks and the corpus checks
# through the FFI (Vitest), and the tsdown bundle. Needs Node >= 22.18.
if command -v node >/dev/null; then
    (cd bindings/typescript && npm ci && npm run build:native && npm run typecheck && npm test && npm run build)
else
    echo "warning: node not found; skipping the TypeScript binding" >&2
fi
