#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

cargo build -p tinychannels-module --lib

target_directory="$(cargo metadata --no-deps --format-version=1 --manifest-path "$repo_root/Cargo.toml" \
  | python3 -c 'import json, sys; print(json.load(sys.stdin)["target_directory"])')"

case "$(uname -s)" in
  Darwin*) artifact="$target_directory/debug/libtinychannels_module.dylib" ;;
  MINGW*|MSYS*|CYGWIN*|Windows_NT) artifact="$target_directory/debug/tinychannels_module.dll" ;;
  *) artifact="$target_directory/debug/libtinychannels_module.so" ;;
esac

if [[ ! -f "$artifact" ]]; then
  echo "compiled TinyChannels module artifact not found: $artifact" >&2
  exit 1
fi

if command -v sha256sum >/dev/null 2>&1; then
  artifact_sha256="$(sha256sum "$artifact" | awk '{print $1}')"
else
  artifact_sha256="$(shasum -a 256 "$artifact" | awk '{print $1}')"
fi
printf '"%s" = "%s"\n' "$(basename "$artifact")" "$artifact_sha256" \
  > "$(dirname "$artifact")/modules.toml"

TINYCHANNELS_MODULE_ARTIFACT="$artifact" \
  cargo test -p tinychannels-module --test module_artifact --test runtime_artifact --test routing_artifact -- --ignored --nocapture
