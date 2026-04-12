from __future__ import annotations

import subprocess
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parent
SCRIPT = ROOT / "microcap_top100_mom16_biweekly_live.py"


class Top100V10RuntimeBootstrapTests(unittest.TestCase):
    def run_script(self, *args: str) -> subprocess.CompletedProcess[bytes]:
        return subprocess.run(
            [sys.executable, "-S", str(SCRIPT), *args],
            cwd=ROOT,
            capture_output=True,
        )

    @staticmethod
    def decode_output(raw: bytes) -> str:
        return raw.decode("utf-8", errors="replace")

    def test_help_succeeds_without_site_packages(self) -> None:
        result = self.run_script("--help")

        stdout = self.decode_output(result.stdout)
        stderr = self.decode_output(result.stderr)

        self.assertEqual(result.returncode, 0, msg=stderr or stdout)
        self.assertIn("usage:", stdout.lower())

    def test_realtime_signal_exits_2_with_dependency_guidance(self) -> None:
        result = self.run_script("实时信号")
        stdout = self.decode_output(result.stdout)
        stderr = self.decode_output(result.stderr)
        combined = f"{stdout}\n{stderr}"

        self.assertEqual(result.returncode, 2, msg=combined)
        self.assertIn("numpy", combined)
        self.assertIn("wheelhouse", combined.lower())

    def test_bootstrap_without_wheelhouse_exits_2(self) -> None:
        result = self.run_script("--bootstrap-deps", "实时信号")
        stdout = self.decode_output(result.stdout)
        stderr = self.decode_output(result.stderr)
        combined = f"{stdout}\n{stderr}"

        self.assertEqual(result.returncode, 2, msg=combined)
        self.assertIn("wheelhouse", combined.lower())


if __name__ == "__main__":
    unittest.main()
