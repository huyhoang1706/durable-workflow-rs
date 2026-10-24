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
