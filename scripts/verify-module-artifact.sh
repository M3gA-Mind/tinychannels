#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

cargo build -p tinychannels-module --lib

case "$(uname -s)" in
  Darwin*) artifact="$repo_root/target/debug/libtinychannels_module.dylib" ;;
  MINGW*|MSYS*|CYGWIN*|Windows_NT) artifact="$repo_root/target/debug/tinychannels_module.dll" ;;
  *) artifact="$repo_root/target/debug/libtinychannels_module.so" ;;
esac

if [[ ! -f "$artifact" ]]; then
  echo "compiled TinyChannels module artifact not found: $artifact" >&2
  exit 1
fi

TINYCHANNELS_MODULE_ARTIFACT="$artifact" \
  cargo test -p tinychannels-module --test module_artifact -- --ignored --nocapture
