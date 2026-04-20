from __future__ import annotations

import json
import shutil
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

import microcap_top100_mom16_biweekly_live_v1_4 as v1_4_mod


class V14OutputCompatibilityTests(unittest.TestCase):
    def setUp(self) -> None:
        self.work_dir = Path(__file__).resolve().parent / "_tmp_v1_4_output_compatibility"
        shutil.rmtree(self.work_dir, ignore_errors=True)
        self.work_dir.mkdir(parents=True)

    def tearDown(self) -> None:
        shutil.rmtree(self.work_dir, ignore_errors=True)

    def test_summary_matches_current_v1_4_base_requires_matching_fingerprint(self) -> None:
        fingerprint = {
            "base_costed_nav_sha1": "abc",
            "base_version": "1.1",
            "derisk_scale": 0.0,
            "recovery_ratio_threshold": 0.35,
        }
        summary = {
            "version": "1.4",
            "version_role": "signal_quality_derisk_alternative",
            "version_note": "Signal-quality derisk alternative. Same as v1.1 (0.8x hedge), plus momentum-gap peak-decay derisk with new-peak rearm guard.",
            "base_fingerprint": fingerprint,
        }
        with patch.object(v1_4_mod, "current_base_fingerprint", return_value=fingerprint):
            self.assertTrue(v1_4_mod.summary_matches_current_v1_4_base(summary))
        with patch.object(
            v1_4_mod,
            "current_base_fingerprint",
            return_value={
                "base_costed_nav_sha1": "xyz",
                "base_version": "1.1",
                "derisk_scale": 0.0,
                "recovery_ratio_threshold": 0.35,
            },
        ):
            self.assertFalse(v1_4_mod.summary_matches_current_v1_4_base(summary))

    def test_invalidate_incompatible_v1_4_outputs_removes_stale_outputs(self) -> None:
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

        with patch.object(v1_4_mod, "SUMMARY_JSON", stale_summary):
            with patch.object(v1_4_mod, "LATEST_SIGNAL_CSV", stale_signal):
                with patch.object(v1_4_mod, "NAV_CSV", stale_nav):
                    with patch.object(v1_4_mod, "COSTED_NAV_CSV", stale_costed):
                        with patch.object(v1_4_mod, "PERF_SUMMARY_CSV", stale_perf_summary):
                            with patch.object(v1_4_mod, "PERF_YEARLY_CSV", stale_perf_yearly):
                                with patch.object(v1_4_mod, "PERF_NAV_CSV", stale_perf_nav):
                                    with patch.object(v1_4_mod, "PERF_JSON", stale_perf_json):
                                        with patch.object(v1_4_mod, "PERF_PNG", stale_perf_png):
                                            with patch.object(v1_4_mod, "summary_matches_current_v1_4_base", return_value=False):
                                                removed = v1_4_mod.invalidate_incompatible_v1_4_outputs()

        self.assertEqual(len(removed), 9)
        self.assertFalse(stale_summary.exists())
        self.assertFalse(stale_costed.exists())
        self.assertFalse(stale_perf_png.exists())

    def test_generate_v1_4_outputs_applies_signal_quality_derisk_overlay(self) -> None:
        base_summary = {
            "strategy": "microcap_top100_mom16_biweekly_live_v1_1",
            "version": "1.1",
            "version_role": "backup_alternative",
            "version_note": "Backup alternative to v1.0.",
            "core_params": {"fixed_hedge_ratio": 0.8},
            "latest_signal": {"current_holding": "cash", "next_holding": "long_microcap_short_zz1000"},
        }
        gross = pd.DataFrame(
            {
                "return": [0.0, 0.01],
                "holding": ["cash", "long_microcap_short_zz1000"],
                "next_holding": ["long_microcap_short_zz1000", "long_microcap_short_zz1000"],
                "momentum_gap": [0.02, 0.05],
            },
            index=pd.to_datetime(["2026-04-09", "2026-04-10"]),
        )
        base_signal = pd.DataFrame(
            [
                {
                    "date": pd.Timestamp("2026-04-10"),
                    "current_holding": "cash",
                    "next_holding": "long_microcap_short_zz1000",
                    "microcap_close": 100.0,
                    "hedge_close": 200.0,
                    "microcap_mom": 0.1,
                    "hedge_mom": 0.02,
                    "momentum_gap": 0.08,
                }
            ]
        )
        turnover = pd.DataFrame({"rebalance_date": pd.to_datetime(["2026-04-09", "2026-04-10"])})
        overlaid = gross.copy()
        overlaid["return_net"] = [0.0, 0.009]
        overlaid["nav_net"] = [1.0, 1.009]
        overlaid["execution_scale"] = [0.0, 1.0]
        overlaid["signal_quality_derisk_triggered"] = [False, False]
        overlaid["gap_peak"] = [0.02, 0.05]
        overlaid["gap_decay_ratio"] = [None, 1.0]

        with patch.object(v1_4_mod, "_ensure_base_outputs"):
            with patch.object(v1_4_mod, "invalidate_incompatible_v1_4_outputs"):
                with patch.object(v1_4_mod, "_load_base_v1_1_context", return_value=(base_summary, base_signal, gross, turnover)):
                    with patch.object(v1_4_mod.v1_1_mod.base_mod, "apply_momentum_gap_peak_decay_derisk", return_value=overlaid) as overlay_mock:
                        with patch.object(v1_4_mod, "build_performance_payload", return_value={"summary": {"final_nav": 1.009}}):
                            with patch.object(v1_4_mod, "SUMMARY_JSON", self.work_dir / "summary.json"):
                                with patch.object(v1_4_mod, "LATEST_SIGNAL_CSV", self.work_dir / "signal.csv"):
                                    with patch.object(v1_4_mod, "NAV_CSV", self.work_dir / "nav.csv"):
                                        with patch.object(v1_4_mod, "COSTED_NAV_CSV", self.work_dir / "costed.csv"):
                                            summary, signal_df, out = v1_4_mod.generate_v1_4_outputs()

        overlay_mock.assert_called_once()
        self.assertEqual(overlay_mock.call_args.kwargs["decay_ratio_threshold"], v1_4_mod.DECAY_RATIO_THRESHOLD)
        self.assertEqual(overlay_mock.call_args.kwargs["derisk_scale"], v1_4_mod.DERISK_SCALE)
        self.assertEqual(overlay_mock.call_args.kwargs["recovery_ratio_threshold"], v1_4_mod.RECOVERY_RATIO_THRESHOLD)
        self.assertEqual(summary["version"], "1.4")
        self.assertEqual(summary["version_role"], "signal_quality_derisk_alternative")
        self.assertEqual(float(signal_df.iloc[0]["derisk_scale"]), 0.0)
        self.assertEqual(float(signal_df.iloc[0]["recovery_ratio_threshold"]), 0.35)
        self.assertTrue((self.work_dir / "costed.csv").exists())


if __name__ == "__main__":
    unittest.main()
