import importlib.util
import pathlib
import unittest

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


if __name__ == '__main__':
    unittest.main()
