"""Check feature lanes without compiling the workspace."""
import os
from pathlib import Path
import subprocess
import shlex
import tempfile
import unittest

ROOT = Path(__file__).resolve().parent.parent


class FeatureLaneTests(unittest.TestCase):
    def test_send_only_lane_cannot_unify_full_email_from_workspace(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            calls = root / "calls.log"
            cargo = root / "cargo"
            cargo.write_text(
                '#!/usr/bin/env bash\nprintf "%s\\n" "$*" >> "$FEATURE_LANE_CALLS"\n'
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
            commands = [shlex.split(line) for line in calls.read_text().splitlines()]
            self.assertIn(
                ["test", "--locked", "-p", "tinychannels", "--no-default-features", "--features", "email-send"],
                commands,
            )
            self.assertIn(["test", "--locked", "-p", "tinychannels", "--no-default-features"], commands)


if __name__ == "__main__":
    unittest.main()
