from __future__ import annotations

import shutil
import tempfile
import unittest
from pathlib import Path

import pandas as pd

import validate_top100_versions_consistency as validate_mod


class ValidateTop100VersionsConsistencyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp_dir = Path(__file__).resolve().parent / "_tmp_validate_versions"
        shutil.rmtree(self.tmp_dir, ignore_errors=True)
        self.tmp_dir.mkdir(parents=True)

    def tearDown(self) -> None:
        shutil.rmtree(self.tmp_dir, ignore_errors=True)

    def _write_csv(self, name: str, rows: list[dict[str, object]]) -> Path:
        path = self.tmp_dir / name
        pd.DataFrame(rows).to_csv(path, index=False, encoding="utf-8-sig")
        return path

    def test_validate_version_pair_raises_on_date_mismatch(self) -> None:
        gross = self._write_csv(
            "gross.csv",
            [
                {"date": "2026-04-09", "return": 0.01},
                {"date": "2026-04-10", "return": 0.02},
            ],
        )
        costed = self._write_csv(
            "costed.csv",
            [
                {"date": "2026-04-08", "return_net": 0.00},
                {"date": "2026-04-09", "return_net": 0.01},
                {"date": "2026-04-10", "return_net": 0.02},
            ],
        )

        with self.assertRaisesRegex(ValueError, "date mismatch"):
            validate_mod.validate_version_pair(
                version="v1.1",
                gross_path=gross,
                gross_return_col="return",
                costed_path=costed,
                costed_return_col="return_net",
            )

    def test_build_recent_window_summary_uses_common_index(self) -> None:
        v0 = pd.Series(
            [0.001] * 25,
            index=pd.bdate_range("2026-03-05", periods=25),
            name="v1.0",
        )
        v1 = pd.Series(
            [0.002] * 24,
            index=pd.bdate_range("2026-03-06", periods=24),
            name="v1.1",
        )

        summary = validate_mod.build_recent_window_summary(
            {"v1.0": v0, "v1.1": v1},
            years_list=[2],
            as_of_date=pd.Timestamp("2026-04-10"),
        )

        self.assertEqual(summary["recent2y"]["common_start_date"], "2026-03-06")
        self.assertEqual(summary["recent2y"]["common_end_date"], "2026-04-08")
        self.assertEqual(summary["recent2y"]["versions"]["v1.0"]["days"], 24)
        self.assertEqual(summary["recent2y"]["versions"]["v1.1"]["days"], 24)


if __name__ == "__main__":
    unittest.main()
