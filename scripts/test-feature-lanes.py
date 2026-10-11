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
        with tempfile.TemporaryDirectory(prefix="feature lanes ") as directory:
            root = Path(directory)
            calls = root / "calls.log"
            # Functions avoid Windows PATH/PATHEXT lookup and executable-script
            # handling; forward slashes work in both native and Git Bash paths.
            result = subprocess.run(
                ["bash", "-c", '
'.join([
                    'cargo() { printf "%s\\n" "$*" >> "$FEATURE_LANE_CALLS"; }',
                    'python3() { return 0; }',
                    'export -f cargo python3',
                    'source "$1"',
                ]), "feature-lanes", (ROOT / "scripts/test-features.sh").as_posix()],
                env=dict(os.environ, FEATURE_LANE_CALLS=calls.as_posix(), RUNNER_OS="Linux"),
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
