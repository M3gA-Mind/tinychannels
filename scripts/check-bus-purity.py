#!/usr/bin/env python3
"""Audit contract source and independent default/all dependency closures."""
import argparse
import json
import pathlib
import subprocess
import tomllib

ROOT = pathlib.Path(__file__).resolve().parents[1]
BUS_MANIFEST = ROOT / 'crates/tinychannels-bus/Cargo.toml'
ALLOWED = {
    'tinychannels-bus', 'serde', 'serde_core', 'serde_derive', 'serde_json',
    'schemars', 'schemars_derive', 'thiserror', 'thiserror-impl',
    'proc-macro2', 'quote', 'syn', 'unicode-ident', 'itoa', 'memchr', 'zmij',
    'serde_derive_internals', 'dyn-clone', 'ref-cast', 'ref-cast-impl', 'base64',
}
FORBIDDEN_DEPS = {'anyhow', 'async-trait', 'tokio'}
FORBIDDEN = ('async fn ', 'async_trait', 'tokio::', 'std::sync', 'std::thread',
             'std::fs', 'std::env', 'std::time', 'parking_lot::', 'sha2::', 'hmac::',
             'rand::', 'uuid::', 'fn build_session_key', 'fn classify_send_error',
             'fn authenticated_inbound_event', 'fn segment_for_delivery')


def declared_forbidden_dependencies(manifest):
    """Return forbidden direct dependencies, including dev and target-specific edges."""
    tables = [manifest]
    tables.extend(manifest.get('target', {}).values())
    found = set()
    for table in tables:
        for section in ('dependencies', 'dev-dependencies', 'build-dependencies'):
            for alias, spec in table.get(section, {}).items():
                package = spec.get('package', alias) if isinstance(spec, dict) else alias
                if package in FORBIDDEN_DEPS:
                    found.add(package)
    return found


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--offline', action='store_true', help='Use only cached Cargo registry data')
    args = parser.parse_args()

    for source in (ROOT / 'crates/tinychannels-bus/src').rglob('*.rs'):
        if source.name.endswith('_tests.rs'):
            continue
        content = source.read_text(encoding='utf-8')
        for forbidden in FORBIDDEN:
            if forbidden in content:
                raise SystemExit(f'{source.relative_to(ROOT)}: implementation token {forbidden!r}')

    manifest = tomllib.loads(BUS_MANIFEST.read_text(encoding='utf-8'))
    forbidden_direct = declared_forbidden_dependencies(manifest)
    if forbidden_direct:
        raise SystemExit(f'contract manifest declares runtime dependencies: {sorted(forbidden_direct)}')

    features = list(manifest.get('features', {}))
    for mode in ('default', 'all', 'storage'):
        probe = ROOT / 'target/bus-purity' / mode
        probe.mkdir(parents=True, exist_ok=True)
        (probe / 'src').mkdir(exist_ok=True)
        (probe / 'src/lib.rs').write_text('pub use tinychannels_bus::*;\n', encoding='utf-8')
        (probe / 'Cargo.toml').write_text(
            '[package]\nname="contract-purity-probe"\nversion="0.0.0"\nedition="2024"\n'
            '[workspace]\n[dependencies]\ntinychannels-bus = { path = ' + json.dumps(str(BUS_MANIFEST.parent))
            + (', features = ' + json.dumps(features) if mode == 'all' else ', features = ["storage"]' if mode == 'storage' else '') + ' }\n',
            encoding='utf-8',
        )
        graph = json.loads(subprocess.check_output([
            'cargo', 'metadata', *(['--offline'] if args.offline else []), '--format-version=1', '--manifest-path', str(probe / 'Cargo.toml')
        ], cwd=ROOT))
        packages = {p['id']: p['name'] for p in graph['packages']}
        nodes = {n['id']: n for n in graph['resolve']['nodes']}
        pending = [next(p['id'] for p in graph['packages'] if p['name'] == 'tinychannels-bus')]
        seen = set()
        while pending:
            package = pending.pop()
            if package in seen:
                continue
            seen.add(package)
            for edge in nodes[package]['deps']:
                if any(kind['kind'] != 'dev' for kind in edge['dep_kinds']):
                    pending.append(edge['pkg'])
        exempt = set()
        if mode in {'storage', 'all'} and 'storage' in features:
            # Only explicitly optional storage-port dependencies and their
            # closure are exempt from the default contract dependency allowlist.
            optional_storage = {
                spec.get('package', name) if isinstance(spec, dict) else name
                for name, spec in manifest.get('dependencies', {}).items()
                if isinstance(spec, dict) and spec.get('optional')
            }
            pending = [p['id'] for p in graph['packages'] if p['name'] in optional_storage]
            while pending:
                package = pending.pop()
                if package in exempt:
                    continue
                exempt.add(package)
                pending.extend(
                    edge['pkg'] for edge in nodes[package]['deps']
                    if any(kind['kind'] != 'dev' for kind in edge['dep_kinds'])
                )
        unsafe = sorted({packages[p] for p in seen - exempt} - ALLOWED)
        unsafe_runtime = sorted({packages[p] for p in seen - exempt} & FORBIDDEN_DEPS)
        if unsafe_runtime:
            raise SystemExit(f'{mode}: runtime dependencies in contract closure: {unsafe_runtime}')
        if unsafe:
            raise SystemExit(f'{mode}: unsafe contract dependencies: {unsafe}')
        print(f'{mode}: pure contract closure ({len(seen)} packages)')


if __name__ == '__main__':
    main()
