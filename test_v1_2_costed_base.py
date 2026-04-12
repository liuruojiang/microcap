from __future__ import annotations

import json
import shutil
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

import microcap_top100_mom16_biweekly_live_v1_2 as v1_2_mod


class V12CostedBaseTests(unittest.TestCase):
    def setUp(self) -> None:
        self.work_dir = Path(__file__).resolve().parent / "_tmp_v1_2_costed_base"
        shutil.rmtree(self.work_dir, ignore_errors=True)
        self.work_dir.mkdir(parents=True)

    def tearDown(self) -> None:
        shutil.rmtree(self.work_dir, ignore_errors=True)

    def test_generate_v1_2_outputs_requires_costed_base_series(self) -> None:
        base_costed = self.work_dir / "base_costed.csv"
        base_summary = self.work_dir / "base_summary.json"
        base_signal = self.work_dir / "base_signal.csv"
        output_costed = self.work_dir / "v1_2_costed.csv"
        output_nav = self.work_dir / "v1_2_nav.csv"
        output_summary = self.work_dir / "v1_2_summary.json"
        output_signal = self.work_dir / "v1_2_signal.csv"
        perf_summary = self.work_dir / "perf_summary.csv"
        perf_yearly = self.work_dir / "perf_yearly.csv"
        perf_nav = self.work_dir / "perf_nav.csv"
        perf_json = self.work_dir / "perf.json"
        perf_png = self.work_dir / "perf.png"

        pd.DataFrame(
            {
                "date": ["2026-04-09", "2026-04-10"],
                "return": [0.50, 0.50],
                "return_net": [0.01, -0.02],
                "nav_net": [1.01, 0.9898],
                "entry_exit_cost": [0.0, 0.003],
                "rebalance_cost": [0.0, 0.0],
                "total_cost": [0.0, 0.003],
            }
        ).to_csv(base_costed, index=False, encoding="utf-8-sig")
        base_summary.write_text(json.dumps({"core_params": {}, "latest_signal": {}}, ensure_ascii=False), encoding="utf-8")
        pd.DataFrame(
            {
                "date": ["2026-04-10"],
                "current_holding": ["long_microcap_short_zz1000"],
                "next_holding": ["long_microcap_short_zz1000"],
                "microcap_close": [1.0],
                "hedge_close": [1.0],
                "microcap_mom": [0.0],
                "hedge_mom": [0.0],
                "momentum_gap": [0.0],
                "member_enter_count": [0],
                "member_exit_count": [0],
            }
        ).to_csv(base_signal, index=False, encoding="utf-8-sig")

        fake_run = pd.DataFrame(
            {
                "return": [0.01, -0.02],
                "scale": [1.0, 1.0],
                "state": ["normal", "normal"],
                "prev_drawdown": [0.0, 0.0],
                "turnover": [0.0, 0.0],
                "cost": [0.0, 0.0],
            },
            index=pd.to_datetime(["2026-04-09", "2026-04-10"]),
        )

        with patch.object(v1_2_mod, "_ensure_base_outputs"):
            with patch.object(v1_2_mod, "BASE_COSTED_NAV_CSV", base_costed):
                with patch.object(v1_2_mod, "BASE_SUMMARY_JSON", base_summary):
                    with patch.object(v1_2_mod, "BASE_SIGNAL_CSV", base_signal):
                        with patch.object(v1_2_mod, "COSTED_NAV_CSV", output_costed):
                            with patch.object(v1_2_mod, "NAV_CSV", output_nav):
                                with patch.object(v1_2_mod, "SUMMARY_JSON", output_summary):
                                    with patch.object(v1_2_mod, "LATEST_SIGNAL_CSV", output_signal):
                                        with patch.object(v1_2_mod, "PERF_SUMMARY_CSV", perf_summary):
                                            with patch.object(v1_2_mod, "PERF_YEARLY_CSV", perf_yearly):
                                                with patch.object(v1_2_mod, "PERF_NAV_CSV", perf_nav):
                                                    with patch.object(v1_2_mod, "PERF_JSON", perf_json):
                                                        with patch.object(v1_2_mod, "PERF_PNG", perf_png):
                                                            with patch.object(v1_2_mod, "apply_practical_throttle", return_value=fake_run):
                                                                _, _, out = v1_2_mod.generate_v1_2_outputs()

        self.assertAlmostEqual(float(out["return_net_v1_2"].iloc[0]), 0.01, places=10)
        self.assertAlmostEqual(float(out["return_net_v1_2"].iloc[1]), -0.02, places=10)
        self.assertAlmostEqual(float(out["return"].iloc[0]), 0.50, places=10)
        saved = pd.read_csv(output_costed)
        self.assertIn("entry_exit_cost", saved.columns)
        self.assertEqual(saved["return_net_v1_2"].round(10).tolist(), [0.01, -0.02])


if __name__ == "__main__":
    unittest.main()
