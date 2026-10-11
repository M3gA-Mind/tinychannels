#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
python3 scripts/test-e2e-version.py
python3 scripts/test-feature-lanes.py
cargo test --locked --workspace
cargo test --locked --workspace --all-features
cargo test --locked -p tinychannels --no-default-features
cargo test --locked -p tinychannels --no-default-features --features email-send
case "${RUNNER_OS:-$(uname -s)}" in
  Windows|MINGW*|MSYS*|CYGWIN*) ;;
  *) python3 scripts/test-package-layout.py ;;
esac
