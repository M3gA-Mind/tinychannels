#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
cargo test --locked -p tinychannels-module configured_adapter_ -- --nocapture
cargo test --locked -p tinychannels-module configured_adapter_disables_retry_for_continuation_only_and_unknown_outcomes -- --nocapture
cargo test --locked -p tinychannels-module raw_voice_synthesis_requires_the_configured_native_synthesis_capability -- --nocapture
cargo test --locked -p tinychannels-module configured_running_telegram_adapter_passes_real_mock_wire_conformance -- --nocapture
cargo test --locked -p tinychannels-conformance -- --nocapture
cargo test --locked -p tinychannels-bus unsupported_send_flag_is_additive_to_legacy_error_wire_shape -- --nocapture
