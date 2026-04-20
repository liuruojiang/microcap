from __future__ import annotations

import json
import shutil
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

import microcap_top100_mom16_biweekly_live_v1_5 as v1_5_mod


class V15OutputCompatibilityTests(unittest.TestCase):
    def setUp(self) -> None:
        self.work_dir = Path(__file__).resolve().parent / "_tmp_v1_5_output_compatibility"
        shutil.rmtree(self.work_dir, ignore_errors=True)
        self.work_dir.mkdir(parents=True)

    def tearDown(self) -> None:
        shutil.rmtree(self.work_dir, ignore_errors=True)

    def test_summary_matches_current_v1_5_base_requires_matching_fingerprint(self) -> None:
        fingerprint = {"base_costed_nav_sha1": "abc", "base_version": "1.2", "derisk_scale": 0.0}
        summary = {
            "version": "1.5",
            "version_role": "signal_quality_overlay_on_v1_2",
            "version_note": "Signal-quality overlay on top of v1.2.",
            "base_fingerprint": fingerprint,
        }
        with patch.object(v1_5_mod, "current_base_fingerprint", return_value=fingerprint):
            self.assertTrue(v1_5_mod.summary_matches_current_v1_5_base(summary))
        with patch.object(
            v1_5_mod,
            "current_base_fingerprint",
            return_value={"base_costed_nav_sha1": "xyz", "base_version": "1.2", "derisk_scale": 0.0},
        ):
            self.assertFalse(v1_5_mod.summary_matches_current_v1_5_base(summary))

    def test_invalidate_incompatible_v1_5_outputs_removes_stale_outputs(self) -> None:
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

        with patch.object(v1_5_mod, "SUMMARY_JSON", stale_summary):
            with patch.object(v1_5_mod, "LATEST_SIGNAL_CSV", stale_signal):
                with patch.object(v1_5_mod, "NAV_CSV", stale_nav):
                    with patch.object(v1_5_mod, "COSTED_NAV_CSV", stale_costed):
                        with patch.object(v1_5_mod, "PERF_SUMMARY_CSV", stale_perf_summary):
                            with patch.object(v1_5_mod, "PERF_YEARLY_CSV", stale_perf_yearly):
                                with patch.object(v1_5_mod, "PERF_NAV_CSV", stale_perf_nav):
                                    with patch.object(v1_5_mod, "PERF_JSON", stale_perf_json):
                                        with patch.object(v1_5_mod, "PERF_PNG", stale_perf_png):
                                            with patch.object(v1_5_mod, "summary_matches_current_v1_5_base", return_value=False):
                                                removed = v1_5_mod.invalidate_incompatible_v1_5_outputs()

        self.assertEqual(len(removed), 9)
        self.assertFalse(stale_summary.exists())
        self.assertFalse(stale_costed.exists())
        self.assertFalse(stale_perf_png.exists())

    def test_generate_v1_5_outputs_applies_overlay(self) -> None:
        base_summary = {
            "strategy": "microcap_top100_mom16_biweekly_live_v1_2",
            "version": "1.2",
            "version_role": "defensive_alternative",
            "version_note": "Defensive backup alternative.",
            "core_params": {"fixed_hedge_ratio": 0.8},
            "latest_signal": {"current_holding": "cash", "next_holding": "long_microcap_short_zz1000"},
        }
        base_signal = pd.DataFrame(
            [
                {
                    "date": pd.Timestamp("2026-04-10"),
                    "current_holding": "cash",
                    "next_holding": "long_microcap_short_zz1000",
                    "momentum_gap": 0.08,
                    "nav_control_scale_last_applied": 0.85,
                    "nav_control_scale_next_session": 0.85,
                }
            ]
        )
        base_net = pd.DataFrame(
            {
                "holding": ["cash", "long_microcap_short_zz1000"],
                "next_holding": ["long_microcap_short_zz1000", "long_microcap_short_zz1000"],
                "momentum_gap": [0.02, 0.05],
                "return_net_v1_2": [0.0, 0.01],
            },
            index=pd.to_datetime(["2026-04-09", "2026-04-10"]),
        )
        overlaid = base_net.copy()
        overlaid["return_net_overlay"] = [0.0, 0.009]
        overlaid["nav_net_overlay"] = [1.0, 1.009]
        overlaid["execution_scale_overlay"] = [1.0, 0.0]
        overlaid["signal_quality_derisk_triggered_overlay"] = [False, True]
        overlaid["gap_peak_overlay"] = [0.02, 0.05]
        overlaid["gap_decay_ratio_overlay"] = [None, 0.2]
        overlaid["holding_overlay"] = ["cash", "long_microcap_short_zz1000"]
        overlaid["next_holding_overlay"] = ["long_microcap_short_zz1000", "long_microcap_short_zz1000"]

        with patch.object(v1_5_mod, "invalidate_incompatible_v1_5_outputs"):
            with patch.object(v1_5_mod, "_load_base_v1_2_context", return_value=(base_summary, base_signal, base_net)):
                with patch.object(v1_5_mod, "apply_v1_2_signal_quality_overlay", return_value=overlaid) as overlay_mock:
                    with patch.object(v1_5_mod, "build_performance_payload", return_value={"summary": {"final_nav": 1.009}}):
                        with patch.object(v1_5_mod, "SUMMARY_JSON", self.work_dir / "summary.json"):
                            with patch.object(v1_5_mod, "LATEST_SIGNAL_CSV", self.work_dir / "signal.csv"):
                                with patch.object(v1_5_mod, "NAV_CSV", self.work_dir / "nav.csv"):
                                    with patch.object(v1_5_mod, "COSTED_NAV_CSV", self.work_dir / "costed.csv"):
                                        summary, signal_df, out = v1_5_mod.generate_v1_5_outputs()

        overlay_mock.assert_called_once()
        self.assertEqual(overlay_mock.call_args.kwargs["decay_ratio_threshold"], v1_5_mod.DECAY_RATIO_THRESHOLD)
        self.assertEqual(overlay_mock.call_args.kwargs["derisk_scale"], v1_5_mod.DERISK_SCALE)
        self.assertEqual(overlay_mock.call_args.kwargs["recovery_ratio_threshold"], v1_5_mod.RECOVERY_RATIO_THRESHOLD)
        self.assertEqual(summary["version"], "1.5")
        self.assertEqual(summary["version_role"], "signal_quality_overlay_on_v1_2")
        self.assertEqual(float(signal_df.iloc[0]["derisk_scale"]), v1_5_mod.DERISK_SCALE)
        self.assertTrue((self.work_dir / "costed.csv").exists())


if __name__ == "__main__":
    unittest.main()
