import importlib.util
import json
import pathlib
import tempfile
import unittest
from contextlib import redirect_stdout
from io import StringIO
from unittest.mock import patch

SCRIPT = pathlib.Path(__file__).parents[1] / 'check-bus-purity.py'
SPEC = importlib.util.spec_from_file_location('check_bus_purity', SCRIPT)
CHECKER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CHECKER)


class ForbiddenDependencyTests(unittest.TestCase):
    def test_rejects_runtime_dependencies_in_any_manifest_section(self):
        manifest = {
            'dependencies': {'serde': '1'},
            'dev-dependencies': {'tokio': '1'},
            'build-dependencies': {'runtime-alias': {'package': 'anyhow'}},
            'target': {
                'cfg(windows)': {
                    'dev-dependencies': {'async-trait': '0.1'},
                },
            },
        }
        self.assertEqual(
            CHECKER.declared_forbidden_dependencies(manifest),
            {'anyhow', 'async-trait', 'tokio'},
        )

    def test_allows_contract_dependencies_in_manifest(self):
        manifest = {
            'dependencies': {'serde': '1'},
            'dev-dependencies': {'toml': '1'},
        }
        self.assertEqual(CHECKER.declared_forbidden_dependencies(manifest), set())

    def test_storage_exemption_does_not_hide_unrelated_optional_dependencies(self):
        manifest = {
            'dependencies': {
                'tinystoragedrivers-core': {'optional': True},
                'optional-http-client': {'optional': True},
            },
        }
        packages = {
            'bus': 'tinychannels-bus',
            'storage': 'tinystoragedrivers-core',
            'storage-port': 'tinystoragedrivers-bus',
            'http': 'optional-http-client',
        }
        nodes = {
            'bus': {'deps': [
                {'pkg': 'storage', 'dep_kinds': [{'kind': None}]},
                {'pkg': 'http', 'dep_kinds': [{'kind': None}]},
            ]},
            'storage': {'deps': [
                {'pkg': 'storage-port', 'dep_kinds': [{'kind': None}]},
            ]},
            'storage-port': {'deps': []},
            'http': {'deps': []},
        }

        exempt = CHECKER.dependency_closure(
            nodes,
            CHECKER.optional_storage_port_roots(manifest, packages),
        )
        audited = CHECKER.dependency_closure(nodes, ['bus']) - exempt
        unsafe = {packages[package] for package in audited} - CHECKER.ALLOWED

        self.assertEqual(exempt, {'storage', 'storage-port'})
        self.assertEqual(unsafe, {'optional-http-client'})


class WindowsEncodingTests(unittest.TestCase):
    def test_checker_cli_reads_utf8_rust_source_with_cp1252_default(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            source_dir = root / 'crates/tinychannels-bus/src'
            source_dir.mkdir(parents=True)
            source = source_dir / 'lib.rs'
            source.write_bytes('/// right quote: ”\n'.encode('utf-8'))
            manifest = root / 'crates/tinychannels-bus/Cargo.toml'
            manifest.write_text('[package]\nname = "tinychannels-bus"\n[features]\n', encoding='utf-8')
            bus_id = 'tinychannels-bus 0.1.0 (path+file:///tinychannels-bus)'
            graph = {
                'packages': [{'id': bus_id, 'name': 'tinychannels-bus'}],
                'resolve': {'nodes': [{'id': bus_id, 'deps': []}]},
            }
            original_read_text = pathlib.Path.read_text

            def cp1252_default(path, encoding=None, errors=None):
                return original_read_text(path, encoding=encoding or 'cp1252', errors=errors)

            with (
                patch.object(CHECKER, 'ROOT', root),
                patch.object(CHECKER, 'BUS_MANIFEST', manifest),
                patch('pathlib.Path.read_text', cp1252_default),
                patch.object(CHECKER.subprocess, 'check_output', return_value=json.dumps(graph).encode()),
                patch('sys.argv', ['check-bus-purity.py', '--offline']),
                redirect_stdout(StringIO()) as output,
            ):
                CHECKER.main()

            self.assertIn('default: pure contract closure (1 packages)', output.getvalue())
            self.assertIn('all: pure contract closure (1 packages)', output.getvalue())


if __name__ == '__main__':
    unittest.main()
