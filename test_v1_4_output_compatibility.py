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

    def test_validate_base_hedge_ratio_rejects_mismatch(self) -> None:
        with patch.object(v1_4_mod.v1_1_mod.base_mod, "FIXED_HEDGE_RATIO", 1.0):
            with self.assertRaisesRegex(ValueError, "hedge ratio mismatch"):
                v1_4_mod.validate_base_hedge_ratio()

    def test_current_base_fingerprint_records_scale_cost_model(self) -> None:
        with patch.object(v1_4_mod, "_file_sha1", return_value="sha"):
            fp = v1_4_mod.current_base_fingerprint()

        self.assertEqual(fp["base_hedge_ratio"], v1_4_mod.BASE_HEDGE_RATIO)
        self.assertEqual(fp["signal_quality_scale_cost_model"], "active_scale_delta_entry_exit_cost_v1")
        self.assertEqual(
            fp["signal_quality_rebalance_cost_model"],
            "rebalance_base_scaled_by_max_prev_current_execution_scale_v1",
        )
        self.assertTrue(bool(fp["signal_quality_scale_cost_field"]))
        self.assertTrue(bool(fp["signal_quality_scale_turnover_field"]))
        self.assertEqual(fp["v1_4_overlay_engine_version"], "2026-05-03-sq-scale-rebalance-cost-v2")

    def test_file_sha1_returns_missing_for_absent_file(self) -> None:
        self.assertEqual(v1_4_mod._file_sha1(self.work_dir / "missing.csv"), "MISSING")

    def test_generate_v1_4_outputs_ensures_base_before_invalidation(self) -> None:
        calls: list[str] = []
        base_summary = {
            "strategy": "microcap_top100_mom16_biweekly_live_v1_1",
            "version": "1.1",
            "core_params": {"fixed_hedge_ratio": 0.8},
            "latest_signal": {"current_holding": "cash", "next_holding": "long_microcap_short_zz1000"},
        }
        gross = pd.DataFrame(
            {
                "return": [0.0],
                "holding": ["cash"],
                "next_holding": ["cash"],
                "microcap_ret": [0.0],
                "hedge_ret": [0.0],
                "microcap_mom": [0.0],
                "hedge_mom": [0.0],
                "momentum_gap": [0.0],
            },
            index=pd.to_datetime(["2026-04-10"]),
        )
        overlaid = gross.copy()
        overlaid["return_net"] = [0.0]
        overlaid["nav_net"] = [1.0]
        overlaid["execution_scale"] = [0.0]
        overlaid["signal_quality_derisk_triggered"] = [False]
        overlaid["gap_peak"] = [0.0]
        overlaid["gap_decay_ratio"] = [1.0]
        turnover = pd.DataFrame({"rebalance_date": pd.to_datetime(["2026-04-10"])})

        def ensure_base() -> None:
            calls.append("ensure")

        def invalidate() -> list[Path]:
            calls.append("invalidate")
            return []

        with patch.object(v1_4_mod, "_ensure_base_outputs", side_effect=ensure_base):
            with patch.object(v1_4_mod, "invalidate_incompatible_v1_4_outputs", side_effect=invalidate):
                with patch.object(v1_4_mod, "_load_base_v1_1_context", return_value=(base_summary, pd.DataFrame(), gross, turnover)):
                    with patch.object(v1_4_mod.v1_1_mod.base_mod, "apply_momentum_gap_peak_decay_derisk", return_value=overlaid):
                        with patch.object(v1_4_mod, "build_performance_payload", return_value={"summary": {"final_nav": 1.0}}):
                            with patch.object(v1_4_mod, "SUMMARY_JSON", self.work_dir / "summary.json"):
                                with patch.object(v1_4_mod, "LATEST_SIGNAL_CSV", self.work_dir / "signal.csv"):
                                    with patch.object(v1_4_mod, "NAV_CSV", self.work_dir / "nav.csv"):
                                        with patch.object(v1_4_mod, "COSTED_NAV_CSV", self.work_dir / "costed.csv"):
                                            with patch.object(v1_4_mod, "current_base_fingerprint", return_value={"ok": True}):
                                                v1_4_mod.generate_v1_4_outputs()

        self.assertEqual(calls[:2], ["ensure", "invalidate"])

    def test_invalidate_incompatible_v1_4_outputs_removes_stale_outputs(self) -> None:
        stale_summary = self.work_dir / "summary.json"
        stale_signal = self.work_dir / "signal.csv"
        stale_realtime_signal = self.work_dir / "realtime_signal.csv"
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
            stale_realtime_signal,
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
                with patch.object(v1_4_mod, "REALTIME_SIGNAL_CSV", stale_realtime_signal):
                    with patch.object(v1_4_mod, "NAV_CSV", stale_nav):
                        with patch.object(v1_4_mod, "COSTED_NAV_CSV", stale_costed):
                            with patch.object(v1_4_mod, "PERF_SUMMARY_CSV", stale_perf_summary):
                                with patch.object(v1_4_mod, "PERF_YEARLY_CSV", stale_perf_yearly):
                                    with patch.object(v1_4_mod, "PERF_NAV_CSV", stale_perf_nav):
                                        with patch.object(v1_4_mod, "PERF_JSON", stale_perf_json):
                                            with patch.object(v1_4_mod, "PERF_PNG", stale_perf_png):
                                                with patch.object(v1_4_mod, "summary_matches_current_v1_4_base", return_value=False):
                                                    removed = v1_4_mod.invalidate_incompatible_v1_4_outputs()

        self.assertEqual(len(removed), 10)
        self.assertFalse(stale_summary.exists())
        self.assertFalse(stale_realtime_signal.exists())
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
                "microcap_ret": [0.0, 0.012],
                "hedge_ret": [0.0, 0.002],
                "microcap_mom": [0.03, 0.06],
                "hedge_mom": [0.01, 0.01],
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
        overlaid["signal_quality_scale_turnover"] = [0.0, 0.25]
        overlaid["signal_quality_scale_cost"] = [0.0, 0.00075]
        overlaid["entry_exit_cost"] = [0.003, 0.0]
        overlaid["rebalance_cost"] = [0.0, 0.0]
        overlaid["total_cost"] = [0.003, 0.00075]
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
        self.assertEqual(signal_df.iloc[0]["signal_timing"], "close_confirmed")
        self.assertTrue(bool(signal_df.iloc[0]["official_close_confirmed_signal"]))
        self.assertNotIn("target_vol_signal_timing", signal_df.columns)
        self.assertAlmostEqual(float(signal_df.iloc[0]["signal_quality_scale_turnover"]), 0.25)
        self.assertAlmostEqual(float(signal_df.iloc[0]["signal_quality_scale_cost"]), 0.00075)
        self.assertEqual(
            summary["core_params"]["signal_quality_derisk"]["scale_cost_model"],
            "abs(scale_delta) * ENTRY_COST/EXIT_COST; ENTRY/EXIT cost assumed to represent whole strategy exposure",
        )
        self.assertEqual(
            summary["core_params"]["signal_quality_derisk"]["rebalance_cost_model"],
            "rebalance_base_cost * max(previous_execution_scale, current_execution_scale); zero-scale derisk periods do not pay member rebalance cost",
        )
        self.assertTrue((self.work_dir / "costed.csv").exists())
        readback = pd.read_csv(self.work_dir / "costed.csv", parse_dates=["date"])
        self.assertEqual(str(readback.loc[0, "date"].date()), "2026-04-09")
        self.assertIn("overlay_pre_cost_return", readback.columns)

    def test_summarize_returns_rejects_empty_series(self) -> None:
        with self.assertRaisesRegex(ValueError, "empty return series"):
            v1_4_mod.summarize_returns(pd.Series(dtype=float))

    def test_short_yearly_sample_does_not_extreme_annualize(self) -> None:
        ret = pd.Series(
            [0.01, -0.02, 0.03],
            index=pd.to_datetime(["2026-01-02", "2026-01-05", "2026-01-06"]),
        )

        yearly = v1_4_mod.summarize_yearly(ret)

        self.assertTrue(pd.isna(yearly.loc[0, "annual_pct"]))

    def test_realtime_signal_query_is_supported(self) -> None:
        calls: list[str] = []
        original = getattr(v1_4_mod, "_print_realtime_signal_query", None)
        v1_4_mod._print_realtime_signal_query = lambda: calls.append("called")
        try:
            v1_4_mod._handle_query("实时信号")
        finally:
            if original is None:
                delattr(v1_4_mod, "_print_realtime_signal_query")
            else:
                v1_4_mod._print_realtime_signal_query = original
        self.assertEqual(calls, ["called"])

    def test_realtime_signal_has_intraday_timing_flag(self) -> None:
        reference_summary = {
            "latest_signal": {"current_holding": "cash", "next_holding": "long_microcap_short_zz1000"}
        }
        close_df = pd.DataFrame(
            {"microcap": [100.0], "hedge": [200.0]},
            index=pd.to_datetime(["2026-04-30"]),
        )
        context = {"close_df": close_df, "changes_df": pd.DataFrame()}
        turnover = pd.DataFrame({"rebalance_date": pd.to_datetime(["2026-04-30"])})
        meta = {
            "snapshot_time": "2026-04-30 10:30:00",
            "latest_anchor_trade_date": "2026-04-30",
            "microcap_rt_close": 101.0,
            "hedge_rt_close": 199.0,
            "member_price_count": 100,
            "member_count": 100,
        }
        overlaid = pd.DataFrame(
            {
                "holding": ["long_microcap_short_zz1000"],
                "next_holding": ["long_microcap_short_zz1000"],
                "return_net": [0.0],
                "nav_net": [1.0],
                "execution_scale": [1.0],
                "signal_quality_derisk_triggered": [False],
                "gap_peak": [0.05],
                "gap_decay_ratio": [1.0],
                "microcap_mom": [0.06],
                "hedge_mom": [0.01],
                "momentum_gap": [0.05],
            },
            index=pd.to_datetime(["2026-04-30 10:30:00"]),
        )

        with patch.object(v1_4_mod, "_load_realtime_v1_1_context", return_value=(context, turnover, reference_summary)):
            with patch.object(v1_4_mod.v1_1_mod.base_mod, "build_realtime_signal_fast", return_value=(pd.DataFrame(), meta)):
                with patch.object(v1_4_mod.v1_1_mod.base_mod, "apply_realtime_close_to_signal_frame", return_value=close_df):
                    with patch.object(v1_4_mod.v1_1_mod.base_mod, "run_signal", return_value=overlaid):
                        with patch.object(v1_4_mod.v1_1_mod.base_mod, "apply_momentum_gap_exit_buffer", return_value=overlaid):
                            with patch.object(v1_4_mod.v1_1_mod.base_mod, "apply_momentum_gap_peak_decay_derisk", return_value=overlaid):
                                with patch.object(v1_4_mod.v1_1_mod.base_mod, "assert_realtime_meta_is_actionable"):
                                    with patch.object(v1_4_mod.v1_1_mod.base_mod, "assert_signal_matches_result"):
                                        with patch.object(v1_4_mod, "REALTIME_SIGNAL_CSV", self.work_dir / "realtime.csv"):
                                            signal_df, _, _ = v1_4_mod.build_realtime_v1_4_outputs()

        self.assertEqual(signal_df.iloc[0]["signal_timing"], "intraday_hypothetical_if_now_close")
        self.assertFalse(bool(signal_df.iloc[0]["official_close_confirmed_signal"]))

    def test_print_signal_query_includes_timing_and_scale_costs(self) -> None:
        signal_df = pd.DataFrame(
            [
                {
                    "date": pd.Timestamp("2026-04-30"),
                    "current_holding": "long_microcap_short_zz1000",
                    "next_holding": "long_microcap_short_zz1000",
                    "trade_state": "hold",
                    "signal_timing": "close_confirmed",
                    "official_close_confirmed_signal": True,
                    "momentum_gap": 0.05,
                    "gap_peak": 0.08,
                    "gap_decay_ratio": 0.625,
                    "execution_scale": 0.0,
                    "signal_quality_derisk_triggered": True,
                    "signal_quality_scale_turnover": 1.0,
                    "signal_quality_scale_cost": 0.003,
                }
            ]
        )
        with patch.object(v1_4_mod, "generate_v1_4_outputs", return_value=({}, signal_df, pd.DataFrame())):
            import io
            import contextlib

            buf = io.StringIO()
            with contextlib.redirect_stdout(buf):
                v1_4_mod._print_signal_query()

        text = buf.getvalue()
        self.assertIn("signal_timing: close_confirmed", text)
        self.assertIn("official_close_confirmed_signal: True", text)
        self.assertIn("signal_quality_scale_turnover: 1.0000", text)
        self.assertIn("signal_quality_scale_cost: 0.3000%", text)


if __name__ == "__main__":
    unittest.main()
