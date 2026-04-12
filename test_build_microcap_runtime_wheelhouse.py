from __future__ import annotations

import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import build_microcap_runtime_wheelhouse as builder


class BuildMicrocapRuntimeWheelhouseTests(unittest.TestCase):
    def test_resolve_runtime_requirement_specs_pins_installed_versions(self) -> None:
        versions = {
            "numpy": "9.0.0",
            "pandas": "9.1.0",
            "requests": "9.2.0",
            "akshare": "9.3.0",
            "matplotlib": "9.4.0",
        }

        with patch.object(builder.metadata, "version", side_effect=versions.__getitem__):
            specs = builder.resolve_runtime_requirement_specs()

        self.assertEqual(
            specs,
            [
                "numpy==2.2.6",
                "pandas==2.3.2",
                "requests==2.33.1",
                "akshare==1.18.46",
                "matplotlib==3.10.8",
            ],
        )

    def test_build_download_command_uses_local_find_links(self) -> None:
        args = SimpleNamespace(
            wheelhouse=Path("wheelhouse/linux_x86_64_cp311"),
            python_version="3.11",
            platform="manylinux2014_x86_64",
            implementation="cp",
            abi="cp311",
        )

        with patch.object(builder, "resolve_all_requirement_specs", return_value=["numpy==2.0.0"]):
            cmd = builder.build_download_command(args)

        self.assertIn("--find-links", cmd)
        self.assertIn("--no-deps", cmd)
        self.assertIn(str(args.wheelhouse), cmd)
        self.assertIn("numpy==2.0.0", cmd)

    def test_build_universal_wheel_command_targets_jsonpath(self) -> None:
        cmd = builder.build_universal_wheel_command(Path("wheelhouse/linux_x86_64_cp311"), "jsonpath")

        self.assertEqual(
            cmd,
            [
                builder.sys.executable,
                "-m",
                "pip",
                "wheel",
                "--no-deps",
                "--wheel-dir",
                str(Path("wheelhouse/linux_x86_64_cp311")),
                "jsonpath",
            ],
        )


if __name__ == "__main__":
    unittest.main()
