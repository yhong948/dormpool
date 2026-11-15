"""Startup must not depend on a responsive Windows WMI provider."""

from pathlib import Path
import subprocess
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[2]


@unittest.skipUnless(sys.platform == "win32", "Windows platform fallback")
class WindowsStartupTests(unittest.TestCase):
    def test_entrypoints_bypass_wmi_for_platform_queries(self):
        for entrypoint in ("src/worker/main.py", "src/executor/run.py"):
            with self.subTest(entrypoint=entrypoint):
                code = f"""
import platform
import runpy
import socket
def broken_wmi(*args):
    raise AssertionError('startup reached the WMI provider')
platform._wmi_query = broken_wmi
platform._uname_cache = None
runpy.run_path({entrypoint!r}, run_name='startup_probe')
assert platform.node() == socket.gethostname()
assert platform.machine()
assert platform.system() == 'Windows'
"""
                result = subprocess.run(
                    [sys.executable, "-c", code], cwd=REPO_ROOT,
                    capture_output=True, text=True, timeout=8,
                )
                self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == "__main__":
    unittest.main()
