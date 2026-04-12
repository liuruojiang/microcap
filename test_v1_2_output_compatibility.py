from __future__ import annotations

import json
import shutil
import unittest
from pathlib import Path
from unittest.mock import patch

import microcap_top100_mom16_biweekly_live_v1_2 as v1_2_mod


class V12OutputCompatibilityTests(unittest.TestCase):
    def setUp(self) -> None:
        self.work_dir = Path(__file__).resolve().parent / "_tmp_v1_2_output_compatibility"
        shutil.rmtree(self.work_dir, ignore_errors=True)
        self.work_dir.mkdir(parents=True)

    def tearDown(self) -> None:
        shutil.rmtree(self.work_dir, ignore_errors=True)

    def test_summary_matches_current_v1_2_base_requires_matching_fingerprint(self) -> None:
        fingerprint = {"base_costed_nav_sha1": "abc", "base_version": "1.1"}
        summary = {
            "version": "1.2",
            "version_role": "defensive_alternative",
            "version_note": "Defensive backup alternative. Same as v1.1 (0.8x hedge).",
            "base_fingerprint": fingerprint,
        }
        with patch.object(v1_2_mod, "current_base_fingerprint", return_value=fingerprint):
            self.assertTrue(v1_2_mod.summary_matches_current_v1_2_base(summary))
        with patch.object(v1_2_mod, "current_base_fingerprint", return_value={"base_costed_nav_sha1": "xyz", "base_version": "1.1"}):
            self.assertFalse(v1_2_mod.summary_matches_current_v1_2_base(summary))

    def test_invalidate_incompatible_v1_2_outputs_removes_stale_outputs(self) -> None:
        stale_summary = self.work_dir / "summary.json"
        stale_signal = self.work_dir / "signal.csv"
        stale_nav = self.work_dir / "nav.csv"
        stale_costed = self.work_dir / "costed.csv"
        stale_perf_summary = self.work_dir / "perf_summary.csv"
        stale_perf_yearly = self.work_dir / "perf_yearly.csv"
        stale_perf_nav = self.work_dir / "perf_nav.csv"
        stale_perf_json = self.work_dir / "perf.json"
        stale_perf_png = self.work_dir / "perf.png"

        for path in [
            stale_summary,
            stale_signal,
            stale_nav,
            stale_costed,
            stale_perf_summary,
            stale_perf_yearly,
            stale_perf_nav,
            stale_perf_json,
            stale_perf_png,
        ]:
            path.write_text("stale", encoding="utf-8")

        with patch.object(v1_2_mod, "SUMMARY_JSON", stale_summary):
            with patch.object(v1_2_mod, "LATEST_SIGNAL_CSV", stale_signal):
                with patch.object(v1_2_mod, "NAV_CSV", stale_nav):
                    with patch.object(v1_2_mod, "COSTED_NAV_CSV", stale_costed):
                        with patch.object(v1_2_mod, "PERF_SUMMARY_CSV", stale_perf_summary):
                            with patch.object(v1_2_mod, "PERF_YEARLY_CSV", stale_perf_yearly):
                                with patch.object(v1_2_mod, "PERF_NAV_CSV", stale_perf_nav):
                                    with patch.object(v1_2_mod, "PERF_JSON", stale_perf_json):
                                        with patch.object(v1_2_mod, "PERF_PNG", stale_perf_png):
                                            with patch.object(v1_2_mod, "summary_matches_current_v1_2_base", return_value=False):
                                                removed = v1_2_mod.invalidate_incompatible_v1_2_outputs()

        self.assertEqual(len(removed), 9)
        self.assertFalse(stale_summary.exists())
        self.assertFalse(stale_costed.exists())
        self.assertFalse(stale_perf_png.exists())

    def test_ensure_base_outputs_rebuilds_base_costed_when_turnover_missing(self) -> None:
        base_paths = {"proxy_turnover": self.work_dir / "proxy_turnover.csv"}
        costed_path = self.work_dir / "base_costed.csv"

        with patch.object(v1_2_mod.v1_1_mod.base_mod, "build_output_paths", return_value=base_paths):
            with patch.object(v1_2_mod.v1_1_mod, "prepare_current_v1_1_outputs") as prepare_mock:
                with patch.object(v1_2_mod, "BASE_COSTED_NAV_CSV", costed_path):
                    with patch.object(v1_2_mod, "_build_v1_1_args", return_value=object()):
                        with patch.object(v1_2_mod.v1_1_mod.base_mod, "build_refreshed_panel_shadow", return_value=(Path("panel.csv"), "2026-04-10")):
                            with patch.object(v1_2_mod.v1_1_mod.base_mod, "ensure_strategy_files") as ensure_mock:
                                v1_2_mod._ensure_base_outputs()

        prepare_mock.assert_called_once()
        ensure_mock.assert_called_once()

    def test_build_base_summary_falls_back_to_v1_0_reference_summary(self) -> None:
        base_net = __import__("pandas").DataFrame(
            [
                {
                    "holding": "cash",
                    "next_holding": "long_microcap_short_zz1000",
                    "microcap_close": 100.0,
                    "hedge_close": 200.0,
                    "microcap_mom": 0.1,
                    "hedge_mom": 0.02,
                    "momentum_gap": 0.08,
                }
            ],
            index=__import__("pandas").to_datetime(["2026-04-10"]),
        )
        ref_summary = {
            "strategy": "microcap_top100_mom16_biweekly_live",
            "version": "1.0",
            "version_role": "mainline",
            "version_note": "Baseline",
            "core_params": {"fixed_hedge_ratio": 1.0},
            "latest_signal": {"member_enter_count": 10, "member_exit_count": 10},
        }
        with patch.object(v1_2_mod, "BASE_SUMMARY_JSON", self.work_dir / "missing_summary.json"):
            with patch.object(v1_2_mod, "BASE_SIGNAL_CSV", self.work_dir / "missing_signal.csv"):
                with patch.object(v1_2_mod, "V1_0_SUMMARY_JSON", self.work_dir / "v1_0_summary.json"):
                    (self.work_dir / "v1_0_summary.json").write_text(json.dumps(ref_summary, ensure_ascii=False), encoding="utf-8")
                    summary, signal_df = v1_2_mod._build_base_summary(base_net)

        self.assertEqual(summary["version"], "1.1")
        self.assertEqual(summary["version_role"], "backup_alternative")
        self.assertEqual(float(signal_df.iloc[0]["microcap_close"]), 100.0)


if __name__ == "__main__":
    unittest.main()
