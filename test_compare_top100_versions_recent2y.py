from __future__ import annotations

import shutil
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

import compare_top100_mom16_versions_1_0_1_1_1_2 as compare_mod


class CompareTop100VersionsRecent2YTests(unittest.TestCase):
    def test_build_artifact_paths_uses_recent_year_suffix(self) -> None:
        artifacts = compare_mod.build_artifact_paths(2)

        self.assertTrue(str(artifacts["plot_png"]).endswith("_recent2y_compare.png"))
        self.assertTrue(str(artifacts["summary_json"]).endswith("_recent2y_summary.json"))

    def test_ensure_all_versions_refreshed_runs_in_dependency_order(self) -> None:
        calls: list[str] = []

        with patch.object(compare_mod, "refresh_v1_1_outputs", side_effect=lambda: calls.append("v1.1")):
            with patch.object(compare_mod, "refresh_v1_0_outputs", side_effect=lambda: calls.append("v1.0")):
                with patch.object(compare_mod, "refresh_v1_2_outputs", side_effect=lambda: calls.append("v1.2")):
                    compare_mod.ensure_all_versions_refreshed()

        self.assertEqual(calls, ["v1.1", "v1.0", "v1.2"])

    def test_load_version_series_uses_costed_columns_only(self) -> None:
        tmp_dir = Path(__file__).resolve().parent / "_tmp_compare_versions"
        shutil.rmtree(tmp_dir, ignore_errors=True)
        tmp_dir.mkdir(parents=True)
        try:
            costed_path = tmp_dir / "v1_1_costed.csv"
            pd.DataFrame(
                {
                    "date": ["2026-04-09", "2026-04-10"],
                    "return_net": [0.01, -0.02],
                    "nav_net": [1.01, 0.9898],
                    "return": [0.03, 0.04],
                }
            ).to_csv(costed_path, index=False, encoding="utf-8-sig")

            with patch.dict(
                compare_mod.VERSION_CONFIGS,
                {
                    "v1.1": {
                        "refresh_cmd": ["python", "noop.py"],
                        "path": costed_path,
                        "return_col": "return_net",
                        "nav_col": "nav_net",
                        "source": "costed",
                    }
                },
                clear=False,
            ):
                series, meta = compare_mod.load_version_series("v1.1")

            self.assertEqual(str(series.index.max().date()), "2026-04-10")
            self.assertEqual(meta["return_col"], "return_net")
            self.assertEqual(meta["nav_col"], "nav_net")
            self.assertEqual(meta["source"], "costed")
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)

    def test_build_common_recent_index_intersects_all_versions(self) -> None:
        idx0 = pd.bdate_range("2026-03-01", periods=25)
        idx1 = pd.bdate_range("2026-03-02", periods=24)
        series_map = {
            "v1.0": pd.Series([0.01] * len(idx0), index=idx0),
            "v1.1": pd.Series([0.01] * len(idx1), index=idx1),
            "v1.2": pd.Series([0.01] * len(idx1), index=idx1),
        }

        common_index, latest = compare_mod.build_common_recent_index(series_map, years=2)

        self.assertEqual(str(latest.date()), str(idx0.max().date()))
        self.assertEqual(str(common_index.min().date()), str(idx1.min().date()))
        self.assertEqual(len(common_index), len(idx1))


if __name__ == "__main__":
    unittest.main()
