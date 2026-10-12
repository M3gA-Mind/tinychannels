#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
python3 scripts/test-e2e-version.py
# Native Windows Python resolves bare bash to the WSL launcher on some runners.
# Reuse this Git Bash executable, converting its MSYS path for native Python.
feature_test_bash="$BASH"
case "${RUNNER_OS:-$(uname -s)}" in
  Windows|MINGW*|MSYS*|CYGWIN*) feature_test_bash="$(cygpath -m "$BASH")" ;;
esac
FEATURE_TEST_BASH="$feature_test_bash" python3 scripts/test-feature-lanes.py
python3 scripts/test-feature-shell.py
cargo test --locked --workspace
cargo test --locked --workspace --all-features
cargo test --locked -p tinychannels --no-default-features
cargo test --locked -p tinychannels --no-default-features --features email-send
case "${RUNNER_OS:-$(uname -s)}" in
  Windows|MINGW*|MSYS*|CYGWIN*) ;;
  *) python3 scripts/test-package-layout.py ;;
esac
