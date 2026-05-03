from __future__ import annotations

import importlib
import unittest
from pathlib import Path
from unittest.mock import patch

import numpy as np
import pandas as pd


class V16OutputCompatibilityTests(unittest.TestCase):
    def test_official_v1_6_module_exposes_generator(self) -> None:
        module = importlib.import_module("microcap_top100_mom16_biweekly_live_v1_6")

        self.assertTrue(hasattr(module, "generate_v1_6_outputs"))

    def test_live_context_overlay_replaces_flat_microcap_tail(self) -> None:
        module = importlib.import_module("microcap_top100_mom16_biweekly_live_v1_6")
        close_df = pd.DataFrame(
            {
                "microcap": [1000.0, 1000.0, 1000.0],
                "hedge": [200.0, 202.0, 204.0],
            },
            index=pd.to_datetime(["2026-04-28", "2026-04-29", "2026-04-30"]),
        )
        live_context = {
            "close_df": [
                {"date": "2026-04-28", "microcap": 100.0, "hedge": 210.0},
                {"date": "2026-04-29", "microcap": 105.0, "hedge": 212.0},
                {"date": "2026-04-30", "microcap": 110.0, "hedge": 214.0},
            ]
        }

        patched, meta = module.overlay_live_microcap_tail(close_df, live_context)

        self.assertTrue(meta["applied"])
        self.assertEqual(float(patched.loc[pd.Timestamp("2026-04-30"), "microcap"]), 1100.0)
        self.assertEqual(float(patched.loc[pd.Timestamp("2026-04-30"), "hedge"]), 214.0)

    def test_close_confirmed_signal_does_not_use_live_context_tail_overlay(self) -> None:
        module = importlib.import_module("microcap_top100_mom16_biweekly_live_v1_6")
        tmp_dir = Path(__file__).resolve().parent / "_tmp_v1_6_close_signal"
        tmp_dir.mkdir(exist_ok=True)
        for child in tmp_dir.iterdir():
            child.unlink()

        base_summary = {
            "latest_signal": {"current_holding": "cash", "next_holding": "cash"},
            "core_params": {},
        }
        base_gross = pd.DataFrame(
            {
                "microcap_close": [100.0, 100.0, 100.0],
                "hedge_close": [200.0, 201.0, 202.0],
            },
            index=pd.to_datetime(["2026-04-28", "2026-04-29", "2026-04-30"]),
        )
        overlaid = pd.DataFrame(
            {
                "return_net": [0.0, 0.0, 0.0],
                "nav_net": [1.0, 1.0, 1.0],
                "holding": ["cash", "cash", "cash"],
                "next_holding": ["cash", "cash", "cash"],
                "microcap_mom": [0.0, 0.0, -0.01],
                "hedge_mom": [0.0, 0.0, 0.02],
                "momentum_gap": [0.0, 0.0, -0.03],
                "execution_scale": [0.0, 0.0, 0.0],
                "signal_quality_derisk_triggered": [False, False, False],
            },
            index=base_gross.index,
        )
        turnover = pd.DataFrame({"rebalance_date": base_gross.index})

        with patch.object(module, "invalidate_incompatible_v1_6_outputs"):
            with patch.object(module.v1_4_mod, "_load_base_v1_1_context", return_value=(base_summary, pd.DataFrame(), base_gross, turnover)):
                with patch.object(module.v1_4_mod.v1_1_mod.base_mod, "run_signal", return_value=base_gross):
                    with patch.object(module.v1_4_mod.v1_1_mod.base_mod, "apply_momentum_gap_exit_buffer", return_value=base_gross):
                        with patch.object(module.v1_4_mod.v1_1_mod.base_mod, "apply_momentum_gap_peak_decay_derisk", return_value=overlaid):
                            with patch.object(module, "_recent_microcap_tail_is_flat", return_value=True):
                                with patch.object(module, "overlay_live_microcap_tail", side_effect=AssertionError("live overlay used")):
                                    with patch.object(module, "build_performance_payload", return_value={"summary": {"final_nav": 1.0}}):
                                        with patch.object(module, "SUMMARY_JSON", tmp_dir / "summary.json"):
                                            with patch.object(module, "LATEST_SIGNAL_CSV", tmp_dir / "signal.csv"):
                                                with patch.object(module, "NAV_CSV", tmp_dir / "nav.csv"):
                                                    with patch.object(module, "COSTED_NAV_CSV", tmp_dir / "costed.csv"):
                                                        summary, signal_df, _ = module.generate_v1_6_outputs()

        self.assertEqual(summary["latest_signal"]["next_holding"], "cash")
        self.assertEqual(signal_df.iloc[0]["next_holding"], "cash")
        pd.read_csv(tmp_dir / "costed.csv", parse_dates=["date"])
        for child in tmp_dir.iterdir():
            child.unlink()
        tmp_dir.rmdir()

    def test_generate_v1_6_outputs_creates_missing_output_dir(self) -> None:
        module = importlib.import_module("microcap_top100_mom16_biweekly_live_v1_6")
        tmp_dir = Path(__file__).resolve().parent / "_tmp_v1_6_missing_outputs" / "outputs"
        if tmp_dir.exists():
            for child in tmp_dir.iterdir():
                child.unlink()
            tmp_dir.rmdir()
        if tmp_dir.parent.exists():
            tmp_dir.parent.rmdir()

        base_summary = {
            "latest_signal": {"current_holding": "cash", "next_holding": "cash"},
            "core_params": {},
        }
        base_gross = pd.DataFrame(
            {
                "microcap_close": [100.0, 101.0, 102.0],
                "hedge_close": [200.0, 201.0, 202.0],
            },
            index=pd.to_datetime(["2026-04-28", "2026-04-29", "2026-04-30"]),
        )
        overlaid = pd.DataFrame(
            {
                "return_net": [0.0, 0.0, 0.0],
                "nav_net": [1.0, 1.0, 1.0],
                "holding": ["cash", "cash", "cash"],
                "next_holding": ["cash", "cash", "cash"],
            },
            index=base_gross.index,
        )

        with patch.object(module, "OUTPUT_DIR", tmp_dir):
            with patch.object(module, "SUMMARY_JSON", tmp_dir / "summary.json"):
                with patch.object(module, "LATEST_SIGNAL_CSV", tmp_dir / "signal.csv"):
                    with patch.object(module, "NAV_CSV", tmp_dir / "nav.csv"):
                        with patch.object(module, "COSTED_NAV_CSV", tmp_dir / "costed.csv"):
                            with patch.object(module, "PERF_YEARLY_CSV", tmp_dir / "yearly.csv"):
                                with patch.object(module, "PERF_NAV_CSV", tmp_dir / "perf_nav.csv"):
                                    with patch.object(module, "PERF_SUMMARY_CSV", tmp_dir / "perf_summary.csv"):
                                        with patch.object(module, "PERF_JSON", tmp_dir / "perf.json"):
                                            with patch.object(module, "PERF_PNG", tmp_dir / "perf.png"):
                                                with patch.object(module, "invalidate_incompatible_v1_6_outputs"):
                                                    with patch.object(module.v1_4_mod, "_load_base_v1_1_context", return_value=(base_summary, pd.DataFrame(), base_gross, pd.DataFrame())):
                                                        with patch.object(module.v1_4_mod.v1_1_mod.base_mod, "run_signal", return_value=base_gross):
                                                            with patch.object(module.v1_4_mod.v1_1_mod.base_mod, "apply_momentum_gap_exit_buffer", return_value=base_gross):
                                                                with patch.object(module.v1_4_mod.v1_1_mod.base_mod, "apply_momentum_gap_peak_decay_derisk", return_value=overlaid):
                                                                    module.generate_v1_6_outputs()

        self.assertTrue((tmp_dir / "costed.csv").exists())
        for child in tmp_dir.iterdir():
            child.unlink()
        tmp_dir.rmdir()
        tmp_dir.parent.rmdir()

    def test_target_vol_scaling_preserves_cash_to_long_entry_cost(self) -> None:
        module = importlib.import_module("microcap_top100_mom16_biweekly_live_v1_6")
        base_result = pd.DataFrame(
            {
                "return_net": [0.0, -0.003, 0.01],
                "nav_net": [1.0, 0.997, 1.00697],
                "holding": ["cash", "cash", "long_microcap_short_zz1000"],
                "next_holding": ["cash", "long_microcap_short_zz1000", "long_microcap_short_zz1000"],
                "entry_exit_cost": [0.0, 0.003, 0.0],
                "rebalance_cost": [0.0, 0.0, 0.0],
                "total_cost": [0.0, 0.003, 0.0],
            },
            index=pd.to_datetime(["2026-04-27", "2026-04-28", "2026-04-29"]),
        )

        out = module.apply_target_vol_scaling(base_result)

        self.assertEqual(float(out["target_vol"].iloc[-1]), 0.25)
        self.assertAlmostEqual(float(out.loc[pd.Timestamp("2026-04-28"), "return_net"]), -0.003)

    def test_base_trade_cost_scale_uses_transition_timing(self) -> None:
        module = importlib.import_module("microcap_top100_mom16_biweekly_live_v1_6")
        holding = pd.Series(
            ["cash", "long_microcap_short_zz1000", "long_microcap_short_zz1000", "cash"],
            index=pd.date_range("2026-01-01", periods=4, freq="B"),
        )
        next_holding = pd.Series(
            ["long_microcap_short_zz1000", "long_microcap_short_zz1000", "cash", "cash"],
            index=holding.index,
        )
        current_scale = pd.Series([0.0, 1.2, 1.4, 0.0], index=holding.index)
        actionable_scale = pd.Series([1.1, 1.3, 0.0, 0.0], index=holding.index)

        scale = module.calc_base_trade_cost_scale(holding, next_holding, current_scale, actionable_scale)

        self.assertEqual([round(float(x), 2) for x in scale.tolist()], [1.1, 1.2, 1.4, 0.0])

    def test_target_vol_next_session_scale_uses_latest_unshifted_vol(self) -> None:
        module = importlib.import_module("microcap_top100_mom16_biweekly_live_v1_6")
        dates = pd.date_range("2026-01-01", periods=5, freq="B")
        returns = pd.Series([0.01, -0.01, 0.01, -0.02, 0.05], index=dates)
        base_result = pd.DataFrame(
            {
                "return_net": returns,
                "nav_net": (1.0 + returns).cumprod(),
                "holding": ["long_microcap_short_zz1000"] * len(dates),
                "next_holding": ["long_microcap_short_zz1000"] * len(dates),
                "total_cost": [0.0] * len(dates),
            },
            index=dates,
        )

        with patch.object(module, "TARGET_VOL_WINDOW", 3):
            with patch.object(module, "TARGET_VOL_MAX_LEVERAGE", 10.0):
                out = module.apply_target_vol_scaling(base_result)

        realized = returns.rolling(3).std(ddof=1) * np.sqrt(module.TARGET_VOL_TRADING_DAYS)
        expected_current = module.TARGET_VOL / realized.shift(1).iloc[-1]
        expected_next = module.TARGET_VOL / realized.iloc[-1]
        self.assertAlmostEqual(float(out["current_execution_scale"].iloc[-1]), float(expected_current))
        self.assertAlmostEqual(float(out["next_session_target_scale"].iloc[-1]), float(expected_next))
        self.assertNotAlmostEqual(
            float(out["current_execution_scale"].iloc[-1]),
            float(out["next_session_target_scale"].iloc[-1]),
        )

    def test_execution_scale_uses_rebalance_threshold_state_machine(self) -> None:
        module = importlib.import_module("microcap_top100_mom16_biweekly_live_v1_6")
        desired = pd.Series(
            [1.00, 1.04, 1.08, 1.11, 0.95, 1.02, 0.00, 1.03],
            index=pd.date_range("2026-01-01", periods=8, freq="B"),
        )
        active = pd.Series([True, True, True, True, True, True, False, True], index=desired.index)

        actual = module.apply_scale_rebalance_threshold(desired, active, threshold=0.10)

        self.assertEqual(
            [round(float(x), 2) for x in actual.tolist()],
            [1.00, 1.00, 1.00, 1.11, 0.95, 0.95, 0.00, 1.03],
        )

    def test_next_session_actionable_scale_respects_rebalance_threshold(self) -> None:
        module = importlib.import_module("microcap_top100_mom16_biweekly_live_v1_6")
        current = pd.Series([1.00, 1.00, 1.00, 0.00], index=pd.date_range("2026-01-01", periods=4, freq="B"))
        raw_next = pd.Series([1.08, 1.11, 1.40, 0.05], index=current.index)
        next_holding = pd.Series(
            [
                "long_microcap_short_zz1000",
                "long_microcap_short_zz1000",
                "cash",
                "long_microcap_short_zz1000",
            ],
            index=current.index,
        )

        actionable = module.calc_next_session_actionable_scale(
            current,
            raw_next,
            next_holding,
            threshold=0.10,
        )

        self.assertEqual([round(float(x), 2) for x in actionable.tolist()], [1.00, 1.11, 0.00, 0.05])

    def test_target_vol_realized_vol_prefers_base_gross_return_source(self) -> None:
        module = importlib.import_module("microcap_top100_mom16_biweekly_live_v1_6")
        dates = pd.date_range("2026-01-01", periods=5, freq="B")
        gross_returns = pd.Series([0.01, -0.01, 0.01, -0.02, 0.05], index=dates)
        base_result = pd.DataFrame(
            {
                "return_raw": gross_returns,
                "return_net": [0.0] * len(dates),
                "nav_net": [1.0] * len(dates),
                "holding": ["long_microcap_short_zz1000"] * len(dates),
                "next_holding": ["long_microcap_short_zz1000"] * len(dates),
                "total_cost": [0.0] * len(dates),
            },
            index=dates,
        )

        with patch.object(module, "TARGET_VOL_WINDOW", 3):
            out = module.apply_target_vol_scaling(base_result)

        expected = gross_returns.rolling(3).std(ddof=1).iloc[-1] * np.sqrt(module.TARGET_VOL_TRADING_DAYS)
        self.assertEqual(out["target_vol_return_source"].iloc[-1], "return_raw")
        self.assertAlmostEqual(float(out["target_vol_realized_vol"].iloc[-1]), float(expected))

    def test_scaled_return_uses_v1_4_overlay_pre_cost_return_when_return_raw_exists(self) -> None:
        module = importlib.import_module("microcap_top100_mom16_biweekly_live_v1_6")
        dates = pd.date_range("2026-01-01", periods=3, freq="B")
        base_result = pd.DataFrame(
            {
                "return_raw": [0.0, 0.02, 0.02],
                "return_net": [0.0, 0.01, 0.01],
                "nav_net": [1.0, 1.01, 1.0201],
                "holding": ["cash", "long_microcap_short_zz1000", "long_microcap_short_zz1000"],
                "next_holding": ["long_microcap_short_zz1000", "long_microcap_short_zz1000", "long_microcap_short_zz1000"],
                "total_cost": [0.0, 0.0, 0.0],
            },
            index=dates,
        )

        out = module.apply_target_vol_scaling(base_result)

        self.assertEqual(out["base_pre_cost_return_source"].iloc[-1], "return_net_cost_reversal")
        self.assertAlmostEqual(float(out["base_pre_cost_return"].iloc[-1]), 0.01)
        self.assertAlmostEqual(float(out["return_net"].iloc[-1]), 0.01)

    def test_scaled_return_prefers_explicit_overlay_pre_cost_return(self) -> None:
        module = importlib.import_module("microcap_top100_mom16_biweekly_live_v1_6")
        dates = pd.date_range("2026-01-01", periods=3, freq="B")
        base_result = pd.DataFrame(
            {
                "return_raw": [0.0, 0.02, 0.02],
                "overlay_pre_cost_return": [0.0, 0.012, 0.012],
                "return_net": [0.0, 0.01, 0.01],
                "nav_net": [1.0, 1.01, 1.0201],
                "holding": ["cash", "long_microcap_short_zz1000", "long_microcap_short_zz1000"],
                "next_holding": ["long_microcap_short_zz1000", "long_microcap_short_zz1000", "long_microcap_short_zz1000"],
                "total_cost": [0.0, 0.0, 0.0],
            },
            index=dates,
        )

        out = module.apply_target_vol_scaling(base_result)

        self.assertEqual(out["base_pre_cost_return_source"].iloc[-1], "overlay_pre_cost_return")
        self.assertAlmostEqual(float(out["base_pre_cost_return"].iloc[-1]), 0.012)
        self.assertAlmostEqual(float(out["return_net"].iloc[-1]), 0.012)

    def test_target_vol_leg_turnover_includes_microcap_and_hedge_legs(self) -> None:
        module = importlib.import_module("microcap_top100_mom16_biweekly_live_v1_6")

        self.assertAlmostEqual(
            module.calc_target_vol_turnover(
                "long_microcap_short_zz1000",
                1.0,
                "long_microcap_short_zz1000",
                1.5,
            ),
            0.9,
        )
        self.assertAlmostEqual(
            module.calc_target_vol_turnover(
                "long_microcap_short_zz1000",
                1.5,
                "cash",
                0.0,
            ),
            2.7,
        )

    def test_scale_change_cost_applies_only_to_same_holding_scale_changes(self) -> None:
        module = importlib.import_module("microcap_top100_mom16_biweekly_live_v1_6")
        holding = pd.Series(
            ["cash", "long_microcap_short_zz1000", "long_microcap_short_zz1000", "cash"],
            index=pd.date_range("2026-01-01", periods=4, freq="B"),
        )
        turnover = pd.Series([0.0, 1.8, 0.9, 1.8], index=holding.index)

        cost = module.calc_scale_change_cost(holding, turnover)

        self.assertEqual([round(float(x), 4) for x in cost.tolist()], [0.0, 0.0, 0.0009, 0.0])

    def test_costed_turnover_is_zero_on_holding_transitions(self) -> None:
        module = importlib.import_module("microcap_top100_mom16_biweekly_live_v1_6")
        holding = pd.Series(
            ["cash", "long_microcap_short_zz1000", "long_microcap_short_zz1000", "cash"],
            index=pd.date_range("2026-01-01", periods=4, freq="B"),
        )
        turnover = pd.Series([0.0, 1.8, 0.9, 1.8], index=holding.index)

        costed_turnover = module.calc_target_vol_costed_turnover(holding, turnover)

        self.assertEqual([round(float(x), 2) for x in costed_turnover.tolist()], [0.0, 0.0, 0.9, 0.0])

    def test_target_vol_scaling_scales_base_trade_cost_by_exposure(self) -> None:
        module = importlib.import_module("microcap_top100_mom16_biweekly_live_v1_6")
        dates = pd.date_range("2026-01-01", periods=64, freq="B")
        returns = pd.Series([0.001 if i % 2 == 0 else -0.001 for i in range(len(dates))], index=dates)
        base_result = pd.DataFrame(
            {
                "return_net": returns,
                "nav_net": (1.0 + returns).cumprod(),
                "holding": ["long_microcap_short_zz1000"] * len(dates),
                "next_holding": ["long_microcap_short_zz1000"] * len(dates),
                "entry_exit_cost": [0.0] * len(dates),
                "rebalance_cost": [0.0] * len(dates),
                "total_cost": [0.0] * len(dates),
            },
            index=dates,
        )
        base_result.loc[dates[-2], "total_cost"] = 0.002

        with patch.object(module, "TARGET_VOL_MAX_LEVERAGE", 1.5):
            out = module.apply_target_vol_scaling(base_result)

        scale = float(out.loc[dates[-2], "base_trade_cost_scale"])
        self.assertGreater(scale, 1.0)
        self.assertAlmostEqual(
            float(out.loc[dates[-2], "base_trade_cost_scaled"]),
            0.002 * scale,
        )

    def test_signal_row_reports_scale_rebalance_when_holding_unchanged(self) -> None:
        module = importlib.import_module("microcap_top100_mom16_biweekly_live_v1_6")
        net_df = pd.DataFrame(
            {
                "holding": ["long_microcap_short_zz1000"],
                "next_holding": ["long_microcap_short_zz1000"],
                "execution_scale": [1.0],
                "current_execution_scale": [1.0],
                "next_session_target_scale": [1.4],
                "target_vol_scale_next_session": [1.4],
                "target_vol_realized_vol": [0.18],
                "return_net": [0.0],
                "nav_net": [1.0],
            },
            index=pd.to_datetime(["2026-04-30"]),
        )

        signal = module._build_signal_row(net_df, {"latest_signal": {}}).iloc[0]

        self.assertEqual(signal["holding_trade_state"], "hold")
        self.assertEqual(signal["scale_trade_state"], "rebalance_scale")
        self.assertTrue(bool(signal["scale_trade_required"]))
        self.assertAlmostEqual(float(signal["scale_delta"]), 0.4)
        self.assertEqual(signal["effective_trade_state"], "rebalance_scale")
        self.assertAlmostEqual(float(signal["raw_next_target_scale"]), 1.4)
        self.assertAlmostEqual(float(signal["next_session_actionable_scale"]), 1.4)
        self.assertAlmostEqual(float(signal["next_session_turnover"]), 0.72)
        self.assertAlmostEqual(float(signal["next_session_leg_turnover"]), 0.72)
        self.assertAlmostEqual(float(signal["next_session_leg_cost_est_raw"]), 0.00072)
        self.assertAlmostEqual(float(signal["next_session_overlay_cost_est"]), 0.00072)
        self.assertAlmostEqual(float(signal["next_session_trade_cost_est"]), 0.00072)
        self.assertEqual(signal["next_session_trade_cost_est_type"], "overlay_only")
        self.assertEqual(signal["next_session_total_trade_cost_est_note"], "entry/exit base cost handled by v1.4 total_cost; not directly estimable here")
        self.assertAlmostEqual(float(signal["raw_scale_delta"]), 0.4)
        self.assertAlmostEqual(float(signal["actionable_scale_delta"]), 0.4)

    def test_signal_row_skips_overlay_cost_on_holding_transition(self) -> None:
        module = importlib.import_module("microcap_top100_mom16_biweekly_live_v1_6")
        net_df = pd.DataFrame(
            {
                "holding": ["cash"],
                "next_holding": ["long_microcap_short_zz1000"],
                "execution_scale": [0.0],
                "current_execution_scale": [0.0],
                "next_session_target_scale": [0.05],
                "next_session_actionable_scale": [0.05],
                "target_vol_scale_next_session": [0.05],
                "return_net": [0.0],
                "nav_net": [1.0],
            },
            index=pd.to_datetime(["2026-04-30"]),
        )

        signal = module._build_signal_row(net_df, {"latest_signal": {}}).iloc[0]

        self.assertEqual(signal["holding_trade_state"], "open")
        self.assertAlmostEqual(float(signal["next_session_leg_turnover"]), 0.09)
        self.assertAlmostEqual(float(signal["next_session_leg_cost_est_raw"]), 0.00009)
        self.assertAlmostEqual(float(signal["next_session_overlay_cost_est"]), 0.0)
        self.assertAlmostEqual(float(signal["next_session_trade_cost_est"]), 0.0)

    def test_signal_row_keeps_actionable_scale_when_delta_below_threshold(self) -> None:
        module = importlib.import_module("microcap_top100_mom16_biweekly_live_v1_6")
        net_df = pd.DataFrame(
            {
                "holding": ["long_microcap_short_zz1000"],
                "next_holding": ["long_microcap_short_zz1000"],
                "execution_scale": [1.0],
                "current_execution_scale": [1.0],
                "next_session_target_scale": [1.08],
                "target_vol_scale_next_session": [1.08],
                "target_vol_realized_vol": [0.18],
                "target_vol_turnover": [0.9],
                "return_net": [0.0],
                "nav_net": [1.0],
            },
            index=pd.to_datetime(["2026-04-30"]),
        )

        signal = module._build_signal_row(net_df, {"latest_signal": {}}).iloc[0]

        self.assertFalse(bool(signal["scale_trade_required"]))
        self.assertAlmostEqual(float(signal["raw_next_target_scale"]), 1.08)
        self.assertAlmostEqual(float(signal["next_session_actionable_scale"]), 1.0)
        self.assertAlmostEqual(float(signal["target_vol_scale_next_session"]), 1.0)
        self.assertAlmostEqual(float(signal["raw_scale_delta"]), 0.08)
        self.assertAlmostEqual(float(signal["actionable_scale_delta"]), 0.0)
        self.assertAlmostEqual(float(signal["scale_delta"]), 0.0)
        self.assertAlmostEqual(float(signal["next_session_turnover"]), 0.0)
        self.assertAlmostEqual(float(signal["target_vol_turnover"]), 0.9)

    def test_close_confirmed_signal_has_explicit_timing_flag(self) -> None:
        module = importlib.import_module("microcap_top100_mom16_biweekly_live_v1_6")
        net_df = pd.DataFrame(
            {
                "holding": ["cash"],
                "next_holding": ["cash"],
                "execution_scale": [0.0],
                "current_execution_scale": [0.0],
                "next_session_target_scale": [0.0],
                "next_session_actionable_scale": [0.0],
                "return_net": [0.0],
                "nav_net": [1.0],
            },
            index=pd.to_datetime(["2026-04-30"]),
        )

        signal = module._build_signal_row(net_df, {"latest_signal": {}}).iloc[0]

        self.assertEqual(signal["target_vol_signal_timing"], "close_confirmed")
        self.assertEqual(signal["signal_timing"], "close_confirmed")
        self.assertTrue(bool(signal["official_close_confirmed_signal"]))

    def test_invalidate_incompatible_outputs_removes_realtime_signal(self) -> None:
        module = importlib.import_module("microcap_top100_mom16_biweekly_live_v1_6")
        tmp_dir = Path(__file__).resolve().parent / "_tmp_v1_6_invalidate"
        tmp_dir.mkdir(exist_ok=True)
        for child in tmp_dir.iterdir():
            child.unlink()
        summary_path = tmp_dir / "summary.json"
        realtime_path = tmp_dir / "realtime.csv"
        summary_path.write_text('{"version": "stale"}', encoding="utf-8")
        realtime_path.write_text("stale", encoding="utf-8")

        with patch.object(module, "SUMMARY_JSON", summary_path):
            with patch.object(module, "REALTIME_SIGNAL_CSV", realtime_path):
                with patch.object(module, "LATEST_SIGNAL_CSV", tmp_dir / "latest.csv"):
                    with patch.object(module, "NAV_CSV", tmp_dir / "nav.csv"):
                        with patch.object(module, "COSTED_NAV_CSV", tmp_dir / "costed.csv"):
                            with patch.object(module, "LEGACY_COSTED_NAV_CSV", tmp_dir / "legacy.csv"):
                                with patch.object(module, "PERF_SUMMARY_CSV", tmp_dir / "perf_summary.csv"):
                                    with patch.object(module, "PERF_YEARLY_CSV", tmp_dir / "yearly.csv"):
                                        with patch.object(module, "PERF_NAV_CSV", tmp_dir / "perf_nav.csv"):
                                            with patch.object(module, "PERF_JSON", tmp_dir / "perf.json"):
                                                with patch.object(module, "PERF_PNG", tmp_dir / "perf.png"):
                                                    module.invalidate_incompatible_v1_6_outputs()

        self.assertFalse(realtime_path.exists())
        for child in tmp_dir.iterdir():
            child.unlink()
        tmp_dir.rmdir()

    def test_fingerprint_includes_all_cost_and_overlay_parameters(self) -> None:
        module = importlib.import_module("microcap_top100_mom16_biweekly_live_v1_6")

        fp = module.current_base_fingerprint()

        for key in [
            "base_hedge_ratio",
            "min_leverage",
            "trading_days",
            "decay_ratio_threshold",
            "derisk_scale",
            "recovery_ratio_threshold",
        ]:
            self.assertIn(key, fp)

    def test_validate_base_hedge_ratio_rejects_mismatch(self) -> None:
        module = importlib.import_module("microcap_top100_mom16_biweekly_live_v1_6")

        with patch.object(module.v1_4_mod, "BASE_HEDGE_RATIO", 1.0):
            with self.assertRaisesRegex(ValueError, "hedge ratio mismatch"):
                module.validate_base_hedge_ratio()

    def test_summary_rejects_empty_returns_and_uses_cn_trading_days(self) -> None:
        module = importlib.import_module("microcap_top100_mom16_biweekly_live_v1_6")
        with self.assertRaisesRegex(ValueError, "empty return series"):
            module.summarize_returns(pd.Series(dtype=float))

        ret = pd.Series(
            [0.01, -0.01, 0.02],
            index=pd.to_datetime(["2026-04-28", "2026-04-29", "2026-04-30"]),
        )
        summary = module.summarize_returns(ret)
        expected_vol = ret.std(ddof=1) * np.sqrt(module.TARGET_VOL_TRADING_DAYS) * 100.0
        self.assertAlmostEqual(float(summary["vol_pct"]), float(expected_vol))

    def test_yearly_summary_short_year_annual_is_nan(self) -> None:
        module = importlib.import_module("microcap_top100_mom16_biweekly_live_v1_6")
        ret = pd.Series(
            [0.01, -0.01, 0.02],
            index=pd.to_datetime(["2026-04-28", "2026-04-29", "2026-04-30"]),
        )

        yearly = module.summarize_yearly(ret)

        self.assertTrue(np.isnan(float(yearly.iloc[0]["annual_pct"])))

    def test_realtime_signal_query_is_supported(self) -> None:
        module = importlib.import_module("microcap_top100_mom16_biweekly_live_v1_6")
        calls: list[str] = []
        original = getattr(module, "_print_realtime_signal_query", None)
        module._print_realtime_signal_query = lambda: calls.append("called")
        try:
            module._handle_query("实时信号")
        finally:
            if original is None:
                delattr(module, "_print_realtime_signal_query")
            else:
                module._print_realtime_signal_query = original
        self.assertEqual(calls, ["called"])


if __name__ == "__main__":
    unittest.main()
