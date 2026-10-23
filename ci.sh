#!/usr/bin/env bash
# fmt, clippy (warnings are errors) and the full test suite.
# Tests for the Postgres and Redis plugins, the CLI and the chaos suite start
# their own containers, so Docker must be running.
set -euo pipefail
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
