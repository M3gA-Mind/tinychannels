#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
cargo test --locked -p tinychannels-module configured_adapter_ -- --nocapture
cargo test --locked -p tinychannels-module configured_running_telegram_adapter_passes_real_mock_wire_conformance -- --nocapture
cargo test --locked -p tinychannels-conformance -- --nocapture
