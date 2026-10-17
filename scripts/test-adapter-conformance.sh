#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
cargo test --locked -p tinychannels-module configured_adapter_ -- --nocapture
