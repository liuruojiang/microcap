from __future__ import annotations

import shutil
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

import compare_top100_mom16_versions_1_0_1_1_1_2 as compare_mod


class CompareVersionsFastRefreshTests(unittest.TestCase):
    def setUp(self) -> None:
        self.work_dir = Path(__file__).resolve().parent / "_tmp_compare_fast_refresh"
        shutil.rmtree(self.work_dir, ignore_errors=True)
        self.work_dir.mkdir(parents=True)

    def tearDown(self) -> None:
        shutil.rmtree(self.work_dir, ignore_errors=True)

    def test_build_refresh_query_uses_performance_chart_fast_path(self) -> None:
        self.assertEqual(compare_mod.build_refresh_query(1), "净值图 最近一年")
        self.assertEqual(compare_mod.build_refresh_query(3), "净值图 最近3年")

    def test_refresh_version_outputs_invokes_script_with_query_tokens(self) -> None:
        with patch.object(compare_mod.subprocess, "run") as run_mock:
            compare_mod.refresh_version_outputs("v1.1", 1)

        args = run_mock.call_args.args[0]
        self.assertEqual(args[0], compare_mod.sys.executable)
        self.assertEqual(args[1], "microcap_top100_mom16_biweekly_live_v1_1.py")
        self.assertEqual(args[2:], ["净值图", "最近一年"])

    def test_ensure_all_versions_refreshed_skips_versions_already_at_target_date(self) -> None:
        target_end_date = pd.Timestamp("2026-04-10")
        fresh_paths = {}
        for version in compare_mod.VERSION_CONFIGS:
            path = self.work_dir / f"{version}.csv"
            pd.DataFrame(
                {
                    "date": ["2026-04-09", "2026-04-10"],
                    "return_net": [0.0, 0.0],
                    "nav_net": [1.0, 1.0],
                }
            ).to_csv(path, index=False, encoding="utf-8-sig")
            fresh_paths[version] = path

        with patch.object(compare_mod, "detect_target_end_date", return_value=target_end_date):
            with patch.dict(
                compare_mod.VERSION_CONFIGS,
                {version: {**cfg, "path": fresh_paths[version]} for version, cfg in compare_mod.VERSION_CONFIGS.items()},
                clear=False,
            ):
                with patch.object(compare_mod, "refresh_version_outputs") as refresh_mock:
                    compare_mod.ensure_all_versions_refreshed(1)

        refresh_mock.assert_not_called()

    def test_build_relative_spread_bps_uses_v1_0_as_baseline(self) -> None:
        rebased = pd.DataFrame(
            {
                "v1.0": [1.0, 1.01, 1.02],
                "v1.1": [1.0, 1.011, 1.019],
                "v1.2": [1.0, 1.009, 1.021],
            },
            index=pd.to_datetime(["2026-04-08", "2026-04-09", "2026-04-10"]),
        )

        spread = compare_mod.build_relative_spread_bps(rebased)

        self.assertEqual(spread["v1.0"].tolist(), [0.0, 0.0, 0.0])
        self.assertAlmostEqual(float(spread["v1.1"].iloc[1]), 9.90099009900991, places=10)
        self.assertAlmostEqual(float(spread["v1.2"].iloc[1]), -9.90099009900991, places=10)


if __name__ == "__main__":
    unittest.main()
