#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
if [[ "${1:-}" == --archive ]]; then shift; fi
archive="${1:?usage: verify-module.sh --archive <module-archive>}"
archive="$(cd "$(dirname "$archive")" && pwd -P)/$(basename "$archive")"
cd "$root"
target_root="${CARGO_TARGET_DIR:-$root/target}"
[[ "$target_root" == /* ]] || target_root="$root/$target_root"
mkdir -p "$target_root"
target_root="$(cd "$target_root" && pwd -P)"
[[ "$target_root" == "$root"/* ]] || { echo "CARGO_TARGET_DIR must stay inside the repository" >&2; exit 2; }
# Distro checks may run as root over a workspace bind mount owned by the host
# user; stage the untrusted module under this verifier's own cache directory.
reject_symlink_components() {
  local path="$1" current="/" component
  local -a components
  [[ "$path" == /* ]] || { echo "verification path must be absolute" >&2; return 2; }
  IFS='/' read -r -a components <<< "$path"
  for component in "${components[@]}"; do
    case "$component" in
      ""|.) continue ;;
      ..) echo "verification path must not contain .." >&2; return 2 ;;
    esac
    current="${current%/}/$component"
    [[ ! -L "$current" ]] || {
      echo "refusing symlinked verification path component: $current" >&2
      return 2
    }
  done
}
reject_symlink_components "$HOME"
stage_parent="$HOME/.cache"
mkdir -p "$stage_parent"
reject_symlink_components "$stage_parent"
work="$(mktemp -d "$stage_parent/tinychannels-module-verify-XXXXXX")"
trap 'rm -rf "$work"' EXIT
case "$archive" in
  *.tar.gz)
    members="$(tar -tzf "$archive")"
    while IFS= read -r member; do
      case "$member" in
        /*|../*|*/../*|*/..) echo "archive member escapes the extraction directory: $member" >&2; exit 2 ;;
      esac
    done <<<"$members"
    if tar -tvzf "$archive" | awk 'substr($0, 1, 1) !~ /^[-d]$/ { exit 1 }'; then
      tar -xzf "$archive" -C "$work"
    else
      echo "archive contains a link or special file" >&2
      exit 2
    fi
    ;;
  *) echo "unsupported module archive: $archive" >&2; exit 2 ;;
esac
case "$(uname -s)" in
  Darwin) library="$work/libtinychannels_module.dylib" ;;
  Linux) library="$work/libtinychannels_module.so" ;;
  *) echo "verify-module.sh requires a Unix runner" >&2; exit 1 ;;
esac
test -f "$work/modules.toml"
TINYCHANNELS_MODULE_ARTIFACT="$library" cargo test --locked --release --manifest-path Cargo.toml --package tinychannels-module --test module_artifact --test runtime_artifact -- --ignored
