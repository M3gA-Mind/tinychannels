"""Check feature lanes without compiling the workspace."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parent.parent


class FeatureLaneTests(unittest.TestCase):
    def test_send_only_lane_cannot_unify_full_email_from_workspace(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            calls = root / "calls.jsonl"
            cargo = root / "cargo"
            cargo.write_text(
                f"#!{sys.executable}\nimport json, os, sys\n"
                "with open(os.environ['FEATURE_LANE_CALLS'], 'a') as calls:\n"
                " calls.write(json.dumps(sys.argv[1:]) + '\\n')\n"
            )
            cargo.chmod(0o755)
            # Other script checks have their own regression suites.
            python = root / "python3"
            python.write_text("#!/bin/sh\nexit 0\n")
            python.chmod(0o755)
            result = subprocess.run(
                ["bash", str(ROOT / "scripts/test-features.sh")],
                env=dict(os.environ, PATH=str(root) + os.pathsep + os.environ["PATH"],
                         FEATURE_LANE_CALLS=str(calls), RUNNER_OS="Linux"),
                capture_output=True, text=True,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            commands = [json.loads(line) for line in calls.read_text().splitlines()]
            self.assertIn(
                ["test", "--locked", "-p", "tinychannels", "--no-default-features", "--features", "email-send"],
                commands,
            )
            self.assertIn(["test", "--locked", "-p", "tinychannels", "--no-default-features"], commands)


if __name__ == "__main__":
    unittest.main()
