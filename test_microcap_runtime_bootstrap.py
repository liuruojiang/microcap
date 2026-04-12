from __future__ import annotations

import shutil
import unittest
from pathlib import Path

import microcap_runtime_bootstrap as runtime_bootstrap


class RuntimeBootstrapTests(unittest.TestCase):
    def setUp(self) -> None:
        self.root = Path(__file__).resolve().parent / "_tmp_runtime_bootstrap"
        shutil.rmtree(self.root, ignore_errors=True)
        self.root.mkdir(parents=True)

    def tearDown(self) -> None:
        shutil.rmtree(self.root, ignore_errors=True)

    def test_resolve_wheelhouse_prefers_repo_wheelhouse(self) -> None:
        wheelhouse = self.root / "wheelhouse"
        wheelhouse.mkdir()
        (wheelhouse / "numpy-0.0.0-py3-none-any.whl").write_text("stub", encoding="utf-8")

        resolved = runtime_bootstrap.resolve_wheelhouse(self.root)

        self.assertEqual(resolved, wheelhouse.resolve())

    def test_resolve_wheelhouse_falls_back_to_vendor_libs(self) -> None:
        vendor_libs = self.root / ".vendor_libs"
        vendor_libs.mkdir()
        (vendor_libs / "pandas-0.0.0-py3-none-any.whl").write_text("stub", encoding="utf-8")

        resolved = runtime_bootstrap.resolve_wheelhouse(self.root)

        self.assertEqual(resolved, vendor_libs.resolve())

    def test_resolve_wheelhouse_detects_runtime_specific_subdir(self) -> None:
        runtime_dir = self.root / "wheelhouse" / "linux_x86_64_cp311"
        runtime_dir.mkdir(parents=True)
        (runtime_dir / "numpy-0.0.0-cp311-cp311-manylinux2014_x86_64.whl").write_text("stub", encoding="utf-8")

        runtime_tag = runtime_bootstrap.build_runtime_tag(
            sys_platform="linux",
            machine="x86_64",
            implementation_name="cpython",
            version_info=(3, 11),
        )
        resolved = runtime_bootstrap.resolve_wheelhouse(
            self.root,
            sys_platform="linux",
            machine="x86_64",
            implementation_name="cpython",
            version_info=(3, 11),
        )

        self.assertEqual(runtime_tag, "linux_x86_64_cp311")
        self.assertEqual(resolved, runtime_dir.resolve())


if __name__ == "__main__":
    unittest.main()
