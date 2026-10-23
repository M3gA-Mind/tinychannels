#!/usr/bin/env python3
"""Reject vulnerable Mozilla root bundles in the resolved Cargo graph.

webpki-roots 0.26.11 is a compatibility shim that re-exports the v1 crate;
it contains no certificate bundle itself. RustSec's version-only advisory
check still flags the shim, so allow only that exact crates.io package when
Cargo metadata proves its `parent` edge resolves to webpki-roots >= 1.0.10.
"""

from __future__ import annotations

import json
import subprocess
import sys
from typing import Any

REGISTRY = "registry+https://github.com/rust-lang/crates.io-index"
SHIM_VERSION = "0.26.11"
SAFE_VERSION = (1, 0, 10)


def version_tuple(version: str) -> tuple[int, ...] | None:
    try:
        return tuple(int(part) for part in version.split("."))
    except ValueError:
        return None


def check_metadata(data: Any) -> list[str]:
    if not isinstance(data, dict) or not isinstance(data.get("resolve"), dict):
        return ["failed closed: Cargo metadata has no resolved dependency graph"]
    packages = data.get("packages")
    nodes = data["resolve"].get("nodes")
    if not isinstance(packages, list) or not isinstance(nodes, list):
        return ["failed closed: Cargo metadata package or node list is missing"]

    by_id = {package.get("id"): package for package in packages if isinstance(package, dict)}
    node_by_id = {node.get("id"): node for node in nodes if isinstance(node, dict)}
    roots = [package for package in packages if package.get("name") == "webpki-roots"]
    if not roots:
        return ["failed closed: resolved graph has no webpki-roots package to validate"]

    errors: list[str] = []
    shim_count = 0
    for package in roots:
        package_id = package.get("id")
        version = package.get("version")
        source = package.get("source")
        parsed = version_tuple(version) if isinstance(version, str) else None
        if parsed is not None and parsed >= SAFE_VERSION:
            if source == REGISTRY:
                continue
            errors.append(
                f"safe-version webpki-roots package has unexpected source {source!r}"
            )
            continue
        if version != SHIM_VERSION or source != REGISTRY:
            errors.append(
                f"vulnerable or unexpected webpki-roots package: {version!r} from {source!r}"
            )
            continue

        shim_count += 1
        node = node_by_id.get(package_id)
        edges = node.get("deps") if isinstance(node, dict) else None
        parent_ids = [
            edge.get("pkg")
            for edge in edges or []
            if isinstance(edge, dict) and edge.get("name") == "parent"
        ]
        parents = [by_id.get(parent_id) for parent_id in parent_ids]
        if len(parents) != 1:
            errors.append("webpki-roots 0.26.11 shim does not resolve exactly one parent edge")
            continue
        parent = parents[0]
        parent_version = parent.get("version") if isinstance(parent, dict) else None
        parent_source = parent.get("source") if isinstance(parent, dict) else None
        parsed_parent = version_tuple(parent_version) if isinstance(parent_version, str) else None
        if (
            not isinstance(parent, dict)
            or parent.get("name") != "webpki-roots"
            or parent_source != REGISTRY
            or parsed_parent is None
            or parsed_parent < SAFE_VERSION
        ):
            errors.append(
                "webpki-roots 0.26.11 shim parent is not the safe crates.io bundle "
                f"(resolved {parent_version!r} from {parent_source!r})"
            )

    if shim_count == 0:
        errors.append(
            "expected the audited 0.26.11 compatibility shim; remove this exception "
            "when the dependency graph no longer contains it"
        )
    return errors


def package(name: str, version: str, source: str = REGISTRY, deps: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    return {"id": f"{source}#{name}@{version}", "name": name, "version": version, "source": source, "dependencies": deps or []}


def metadata_for(shim_parent: dict[str, Any] | None) -> dict[str, Any]:
    shim = package(
        "webpki-roots",
        SHIM_VERSION,
        deps=[{"name": "webpki-roots", "rename": "parent", "req": "^1"}],
    )
    good = package("webpki-roots", "1.0.10")
    parent = shim_parent or good
    return {
        "packages": [shim, parent],
        "resolve": {
            "nodes": [
                {"id": shim["id"], "deps": [{"name": "parent", "pkg": parent["id"]}]},
                {"id": good["id"], "deps": []},
            ]
        },
    }


def self_test() -> None:
    good = metadata_for(None)
    assert not check_metadata(good)
    assert check_metadata({"packages": [], "resolve": None})
    assert check_metadata({"packages": [package("webpki-roots", "0.26.10")], "resolve": {"nodes": []}})
    assert check_metadata(metadata_for(package("webpki-roots", "1.0.9")))
    assert check_metadata(metadata_for(package("webpki-roots", "1.0.10", "git+https://example.invalid/roots")))
    unexpected_bundle = metadata_for(None)
    git_bundle = package("webpki-roots", "1.0.10", "git+https://example.invalid/roots")
    unexpected_bundle["packages"].append(git_bundle)
    unexpected_bundle["resolve"]["nodes"].append({"id": git_bundle["id"], "deps": []})
    assert check_metadata(unexpected_bundle)
    bad_shim = package("webpki-roots", SHIM_VERSION, "git+https://example.invalid/roots")
    assert check_metadata({"packages": [bad_shim, package("webpki-roots", "1.0.10")], "resolve": {"nodes": []}})
    missing_edge = metadata_for(None)
    missing_edge["resolve"]["nodes"][0]["deps"] = []
    assert check_metadata(missing_edge)
    print("webpki-roots graph guard self-test passed")


def main() -> int:
    if sys.argv[1:] == ["--self-test"]:
        self_test()
        return 0
    if sys.argv[1:]:
        print("usage: check-webpki-roots.py [--self-test]", file=sys.stderr)
        return 2
    result = subprocess.run(
        ["cargo", "metadata", "--locked", "--all-features", "--format-version", "1"],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode:
        print(result.stderr, file=sys.stderr, end="")
        return result.returncode
    try:
        metadata = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        print(f"failed closed: invalid cargo metadata JSON: {exc}", file=sys.stderr)
        return 1
    errors = check_metadata(metadata)
    if errors:
        for error in errors:
            print(error, file=sys.stderr)
        return 1
    print("webpki-roots resolved graph is patched or uses the audited safe compatibility shim")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
