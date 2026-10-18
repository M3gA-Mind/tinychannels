#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
cargo test --locked -p tinychannels-module configured_adapter_ -- --nocapture
cargo test --locked -p tinychannels-module configured_adapter_disables_retry_for_continuation_only_and_unknown_outcomes -- --nocapture
cargo test --locked -p tinychannels-module raw_voice_synthesis_requires_the_configured_native_synthesis_capability -- --nocapture
cargo test --locked -p tinychannels-module configured_running_telegram_adapter_passes_real_mock_wire_conformance -- --nocapture
cargo test --locked -p tinychannels-module configured_slack_native_adapter_routes_every_port_through_the_ordered_queue -- --nocapture
cargo test --locked -p tinychannels --lib slack_adapter_passes_the_full_conformance_matrix_against_the_mock_platform -- --nocapture
cargo test --locked -p tinychannels-module configured_signal_native_adapter_routes_every_port_through_the_ordered_queue -- --nocapture
cargo test --locked -p tinychannels --lib signal_adapter_passes_the_full_conformance_matrix_against_the_mock_daemon -- --nocapture
cargo test --locked -p tinychannels-module a_built_imessage_launch_starts_as_the_native_imessage_adapter -- --nocapture
cargo test --locked -p tinychannels --lib imessage_adapter_passes_the_full_conformance_matrix_against_a_fake_messages_folder -- --nocapture
cargo test --locked -p tinychannels-conformance -- --nocapture
cargo test --locked -p tinychannels-bus unsupported_send_flag_is_additive_to_legacy_error_wire_shape -- --nocapture
cargo test --locked -p tinychannels --lib the_cloud_adapter_passes_the_whole_conformance_matrix_against_its_wire -- --nocapture
cargo test --locked -p tinychannels --features whatsapp-web --lib the_web_adapter_passes_the_whole_conformance_matrix_against_its_session -- --nocapture
cargo test --locked -p tinychannels-module whatsapp_native -- --nocapture
