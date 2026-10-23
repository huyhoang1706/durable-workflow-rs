#!/bin/bash
set -euo pipefail

echo '{"async": true, "asyncTimeout": 600000}'

# Only needed in Claude Code cloud sessions.
if [ "${CLAUDE_CODE_REMOTE:-}" != "true" ]; then
  exit 0
fi

cd "$CLAUDE_PROJECT_DIR"

# Ensure the components ci.sh relies on are present (no-ops when installed).
rustup component add rustfmt clippy >/dev/null 2>&1 || true

# Download all crates and pre-compile workspace dependencies so the container
# snapshot is cached with a warm registry and target dir.
cargo fetch
cargo test --workspace --all-features --no-run
