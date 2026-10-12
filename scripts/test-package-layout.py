"""Verify the shipped module archive layout without compiling native code."""
import hashlib
import io
import json
import os
import shlex
from pathlib import Path
import shutil
import subprocess
import tarfile
import tempfile
import tomllib
import unittest

ROOT = Path(__file__).resolve().parent.parent

class PackageTests(unittest.TestCase):
    def test_verify_extracts_artifact_under_the_current_user_home(self):
        target = ROOT / "target"; target.mkdir(exist_ok=True)
        with tempfile.TemporaryDirectory(dir=target) as directory:
            fixture = Path(directory); home = fixture/"home"; home.mkdir()
            bindir = fixture/"bin"; bindir.mkdir()
            uname = bindir/"uname"
            uname.write_text("#!/usr/bin/env bash\nprintf '%s\\n' Darwin\n")
            uname.chmod(0o755)
            archive = fixture/"module.tar.gz"
            with tarfile.open(archive, "w:gz") as package:
                for name, data in [("modules.toml", b""), ("libtinychannels_module.so", b"module")]:
                    member = tarfile.TarInfo(name); member.size = len(data)
                    package.addfile(member, io.BytesIO(data))
            cargo = bindir/"cargo"
            cargo.write_text("#!/usr/bin/env python3\nimport os,sys\nfrom pathlib import Path\n"
                "module = os.environ.get('TINYJUICE_TEST_MODULE') or os.environ.get('TINYCHANNELS_MODULE_ARTIFACT') or sys.argv[-1]\n"
                "Path(os.environ['RECORDED_MODULE_PATH']).write_text(module)\n")
            cargo.chmod(0o755)
            env = dict(os.environ, HOME=str(home), PATH=str(bindir)+os.pathsep+os.environ["PATH"],
                RECORDED_MODULE_PATH=str(fixture/"loaded-module-path"))
            env.pop("CARGO_TARGET_DIR", None)
            result = subprocess.run(["bash", str(ROOT/"scripts/verify-module.sh"),
                "--archive", str(archive)], cwd=ROOT, env=env, text=True, capture_output=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            module_path = Path((fixture/"loaded-module-path").read_text())
            self.assertTrue(module_path.is_relative_to(home/".cache"), module_path)

    def test_arch_package_install_disables_only_landlock_sandbox(self):
        script = (ROOT / "scripts/test-distro.sh").read_text()
        pacman = next(line.strip() for line in script.splitlines() if line.strip().startswith("pacman "))
        self.assertEqual(shlex.split(pacman)[:3], ["pacman", "--disable-sandbox-filesystem", "-Syu"])
        self.assertNotIn("--disable-sandbox", shlex.split(pacman))

    def test_archive_attests_host_library_bytes_from_a_symlinked_checkout(self):
        """Package the actual build script and verify the shipped bytes and attestation."""
        target = ROOT / "target"; target.mkdir(exist_ok=True)
        with tempfile.TemporaryDirectory(dir=target) as directory:
            root = Path(directory); (root/"scripts").mkdir(); (root/"bin").mkdir()
            shutil.copy2(ROOT/"scripts/build-module.sh", root/"scripts/build-module.sh")
            for file in ["LICENSE","README.md","MODULE.md"]: (root/file).write_text("fixture")
            cargo = root/"bin/cargo"
            cargo.write_text("#!/usr/bin/env python3\nimport json,sys\nfrom pathlib import Path\n"
                "root=Path.cwd()\n"
                "if 'metadata' in sys.argv: print(json.dumps({'target_directory':str(root/'target')}))\n"
                "else:\n"
                " target=sys.argv[sys.argv.index('--target')+1]\n"
                " output=root/'target'/target/'release'; output.mkdir(parents=True,exist_ok=True)\n"
                " (output/'libtinychannels_module.so').write_bytes(b'compiled-fixture')\n")
            cargo.chmod(0o755)
            env = dict(os.environ, PATH=str(root/"bin")+os.pathsep+os.environ["PATH"])
            env.pop("CARGO_TARGET_DIR", None)
            alias = root/"checkout-link"; alias.symlink_to(root, target_is_directory=True)
            result = subprocess.run(["bash",str(alias/"scripts/build-module.sh"),
                "--crate","tinychannels-module","--version","1.2.3-beta.1",
                "--target","x86_64-unknown-linux-gnu","--host","ubuntu-22.04-x86_64"],
                cwd=root,env=env,text=True,capture_output=True)
            self.assertEqual(result.returncode,0,result.stderr)
            archive=root/"release-assets/tinychannels-module-1.2.3-beta.1-ubuntu-22.04-x86_64.tar.gz"
            with tarfile.open(archive) as package:
                data=package.extractfile("./libtinychannels_module.so").read()
                manifest=tomllib.loads(package.extractfile("./modules.toml").read().decode())
                self.assertEqual(data,b"compiled-fixture")
                self.assertEqual(manifest,{"libtinychannels_module.so":hashlib.sha256(data).hexdigest()})
                for name in ["./LICENSE","./README.md","./MODULE.md"]:
                    self.assertIsNotNone(package.extractfile(name))

if __name__ == "__main__": unittest.main()
