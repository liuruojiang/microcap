from __future__ import annotations

import unittest

import pandas as pd

import microcap_top100_mom16_biweekly_live as live_mod


POSITION = "long_microcap_short_zz1000"


class Top100ForcedStopLossTests(unittest.TestCase):
    def make_gross_result(self) -> pd.DataFrame:
        index = pd.to_datetime(
            [
                "2026-01-05",
                "2026-01-06",
                "2026-01-07",
                "2026-01-08",
                "2026-01-09",
                "2026-01-12",
                "2026-01-13",
            ]
        )
        return pd.DataFrame(
            {
                "return": [0.0, -0.01, -0.02, 0.03, 0.01, 0.0, 0.02],
                "holding": ["cash", POSITION, POSITION, POSITION, POSITION, "cash", POSITION],
                "next_holding": [POSITION, POSITION, POSITION, POSITION, "cash", POSITION, POSITION],
                "signal_on": [True, True, True, True, False, True, True],
                "momentum_gap": [0.01, 0.02, 0.01, 0.03, -0.01, 0.01, 0.02],
            },
            index=index,
        )

    def make_blocked_reentry_result(self) -> pd.DataFrame:
        index = pd.to_datetime(
            [
                "2026-02-02",
                "2026-02-03",
                "2026-02-04",
                "2026-02-05",
                "2026-02-06",
                "2026-02-09",
                "2026-02-10",
            ]
        )
        return pd.DataFrame(
            {
                "return": [0.0, -0.01, -0.02, 0.01, 0.01, 0.01, 0.01],
                "holding": ["cash", POSITION, POSITION, POSITION, POSITION, POSITION, POSITION],
                "next_holding": [POSITION, POSITION, POSITION, POSITION, POSITION, POSITION, POSITION],
                "signal_on": [True, True, True, True, True, True, True],
                "momentum_gap": [0.02, 0.03, 0.01, 0.02, 0.03, 0.04, 0.05],
            },
            index=index,
        )

    def make_turnover(self, gross_result: pd.DataFrame) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "rebalance_date": gross_result.index,
                "execution_timing": ["close"] * len(gross_result),
                "two_side_cost_rate": [0.0] * len(gross_result),
            }
        )

    def make_peak_drawdown_result(self) -> pd.DataFrame:
        index = pd.to_datetime(
            [
                "2026-03-02",
                "2026-03-03",
                "2026-03-04",
                "2026-03-05",
                "2026-03-06",
                "2026-03-09",
                "2026-03-10",
            ]
        )
        return pd.DataFrame(
            {
                "return": [0.0, 0.03, -0.02, 0.01, 0.01, 0.01, 0.01],
                "holding": ["cash", POSITION, POSITION, POSITION, POSITION, POSITION, POSITION],
                "next_holding": [POSITION, POSITION, POSITION, POSITION, POSITION, POSITION, POSITION],
                "signal_on": [True, True, True, True, True, True, True],
                "momentum_gap": [0.01, 0.03, 0.02, 0.03, 0.04, 0.05, 0.06],
            },
            index=index,
        )

    def make_peak_drawdown_stop_level_gate_result(self) -> pd.DataFrame:
        index = pd.to_datetime(
            [
                "2026-03-16",
                "2026-03-17",
                "2026-03-18",
                "2026-03-19",
                "2026-03-20",
                "2026-03-23",
                "2026-03-24",
                "2026-03-25",
            ]
        )
        return pd.DataFrame(
            {
                "return": [0.0, 0.03, -0.02, 0.01, 0.01, 0.01, 0.01, 0.01],
                "holding": ["cash", POSITION, POSITION, POSITION, POSITION, POSITION, POSITION, POSITION],
                "next_holding": [POSITION, POSITION, POSITION, POSITION, POSITION, POSITION, POSITION, POSITION],
                "signal_on": [True, True, True, True, True, True, True, True],
                "momentum_gap": [0.01, 0.07, 0.06, 0.03, 0.04, 0.05, 0.07, 0.08],
            },
            index=index,
        )

    def make_peak_drawdown_two_hit_window_result(self) -> pd.DataFrame:
        index = pd.to_datetime(
            [
                "2026-04-06",
                "2026-04-07",
                "2026-04-08",
                "2026-04-09",
                "2026-04-10",
                "2026-04-13",
            ]
        )
        return pd.DataFrame(
            {
                "return": [0.0, 0.03, -0.011, 0.0, -0.011, 0.01],
                "holding": ["cash", POSITION, POSITION, POSITION, POSITION, POSITION],
                "next_holding": [POSITION, POSITION, POSITION, POSITION, POSITION, POSITION],
                "signal_on": [True, True, True, True, True, True],
                "momentum_gap": [0.01, 0.03, 0.02, 0.025, 0.015, 0.02],
            },
            index=index,
        )

    def make_ratio_bias_take_profit_result(self) -> pd.DataFrame:
        index = pd.to_datetime(
            [
                "2026-05-04",
                "2026-05-05",
                "2026-05-06",
                "2026-05-07",
                "2026-05-08",
                "2026-05-11",
            ]
        )
        return pd.DataFrame(
            {
                "return": [0.0, 0.02, 0.01, 0.02, 0.0, 0.01],
                "holding": ["cash", POSITION, POSITION, POSITION, POSITION, "cash"],
                "next_holding": [POSITION, POSITION, POSITION, POSITION, "cash", POSITION],
                "signal_on": [True, True, True, True, False, True],
                "momentum_gap": [0.01, 0.02, 0.03, 0.03, -0.01, 0.02],
                "microcap_close": [100.0, 102.0, 108.0, 109.0, 108.0, 110.0],
                "hedge_close": [100.0, 100.0, 100.0, 100.0, 100.0, 100.0],
            },
            index=index,
        )

    def make_ratio_bias_non_positive_trade_result(self) -> pd.DataFrame:
        index = pd.to_datetime(
            [
                "2026-05-18",
                "2026-05-19",
                "2026-05-20",
            ]
        )
        return pd.DataFrame(
            {
                "return": [0.0, 0.0, 0.0],
                "holding": ["cash", POSITION, POSITION],
                "next_holding": [POSITION, POSITION, POSITION],
                "signal_on": [True, True, True],
                "momentum_gap": [0.01, 0.02, 0.02],
                "microcap_close": [100.0, 106.0, 107.0],
                "hedge_close": [100.0, 100.0, 100.0],
            },
            index=index,
        )

    def make_momentum_gap_peak_decay_result(self) -> pd.DataFrame:
        index = pd.to_datetime(
            [
                "2026-06-01",
                "2026-06-02",
                "2026-06-03",
                "2026-06-04",
                "2026-06-05",
                "2026-06-08",
            ]
        )
        return pd.DataFrame(
            {
                "return": [0.0, 0.02, 0.01, 0.02, 0.0, 0.01],
                "holding": ["cash", POSITION, POSITION, POSITION, POSITION, "cash"],
                "next_holding": [POSITION, POSITION, POSITION, POSITION, "cash", POSITION],
                "signal_on": [True, True, True, True, False, True],
                "momentum_gap": [0.02, 0.05, 0.10, 0.04, -0.01, 0.03],
                "microcap_close": [100.0, 102.0, 103.0, 104.0, 104.0, 105.0],
                "hedge_close": [100.0, 100.0, 100.0, 100.0, 100.0, 100.0],
            },
            index=index,
        )

    def make_momentum_gap_peak_decay_derisk_result(self) -> pd.DataFrame:
        index = pd.to_datetime(
            [
                "2026-06-15",
                "2026-06-16",
                "2026-06-17",
                "2026-06-18",
                "2026-06-19",
            ]
        )
        return pd.DataFrame(
            {
                "return": [0.0, 0.02, 0.03, 0.02, 0.0],
                "holding": ["cash", POSITION, POSITION, POSITION, "cash"],
                "next_holding": [POSITION, POSITION, POSITION, POSITION, "cash"],
                "signal_on": [True, True, True, True, False],
                "momentum_gap": [0.02, 0.10, 0.05, 0.04, -0.01],
                "microcap_close": [100.0, 101.0, 102.0, 103.0, 103.0],
                "hedge_close": [100.0, 100.0, 100.0, 100.0, 100.0],
            },
            index=index,
        )

    def make_momentum_gap_peak_decay_derisk_recovery_result(self) -> pd.DataFrame:
        index = pd.to_datetime(
            [
                "2026-06-22",
                "2026-06-23",
                "2026-06-24",
                "2026-06-25",
                "2026-06-26",
                "2026-06-29",
            ]
        )
        return pd.DataFrame(
            {
                "return": [0.0, 0.02, 0.03, 0.02, 0.02, 0.0],
                "holding": ["cash", POSITION, POSITION, POSITION, POSITION, "cash"],
                "next_holding": [POSITION, POSITION, POSITION, POSITION, POSITION, "cash"],
                "signal_on": [True, True, True, True, True, False],
                "momentum_gap": [0.02, 0.10, 0.015, 0.07, 0.09, -0.01],
                "microcap_close": [100.0, 101.0, 102.0, 103.0, 104.0, 104.0],
                "hedge_close": [100.0, 100.0, 100.0, 100.0, 100.0, 100.0],
            },
            index=index,
        )

    def make_momentum_gap_peak_decay_derisk_rearm_result(self) -> pd.DataFrame:
        index = pd.to_datetime(
            [
                "2026-07-01",
                "2026-07-02",
                "2026-07-03",
                "2026-07-06",
                "2026-07-07",
                "2026-07-08",
                "2026-07-09",
                "2026-07-10",
                "2026-07-13",
            ]
        )
        return pd.DataFrame(
            {
                "return": [0.0, 0.02, 0.03, 0.02, 0.02, 0.02, 0.02, 0.03, 0.0],
                "holding": ["cash", POSITION, POSITION, POSITION, POSITION, POSITION, POSITION, POSITION, "cash"],
                "next_holding": [POSITION, POSITION, POSITION, POSITION, POSITION, POSITION, POSITION, POSITION, "cash"],
                "signal_on": [True, True, True, True, True, True, True, True, False],
                "momentum_gap": [0.02, 0.10, 0.015, 0.05, 0.018, 0.12, 0.11, 0.02, -0.01],
                "microcap_close": [100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 106.0, 107.0, 107.0],
                "hedge_close": [100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0],
            },
            index=index,
        )

    def test_forced_stop_loss_blocks_reentry_until_base_signal_resets(self) -> None:
        gross = self.make_gross_result()
        turnover = self.make_turnover(gross)

        result = live_mod.apply_single_trade_forced_stop_loss(
            gross_result=gross,
            turnover_df=turnover,
            stop_loss_threshold=0.02,
        )

        self.assertTrue(bool(result.loc[pd.Timestamp("2026-01-07"), "forced_stop_triggered"]))
        self.assertEqual(result.loc[pd.Timestamp("2026-01-08"), "holding"], "cash")
        self.assertEqual(result.loc[pd.Timestamp("2026-01-08"), "next_holding"], "cash")
        self.assertTrue(bool(result.loc[pd.Timestamp("2026-01-09"), "signal_reset_seen"]))
        self.assertEqual(result.loc[pd.Timestamp("2026-01-12"), "next_holding"], POSITION)
        self.assertEqual(result.loc[pd.Timestamp("2026-01-13"), "holding"], POSITION)

    def test_forced_stop_loss_does_not_trigger_when_threshold_is_wider(self) -> None:
        gross = self.make_gross_result()
        turnover = self.make_turnover(gross)

        result = live_mod.apply_single_trade_forced_stop_loss(
            gross_result=gross,
            turnover_df=turnover,
            stop_loss_threshold=0.04,
        )

        self.assertFalse(bool(result["forced_stop_triggered"].any()))
        self.assertEqual(result.loc[pd.Timestamp("2026-01-08"), "holding"], POSITION)
        self.assertEqual(result.loc[pd.Timestamp("2026-01-08"), "next_holding"], POSITION)

    def test_scan_uses_fixed_threshold_grid_from_two_to_five_percent(self) -> None:
        gross = self.make_gross_result()
        turnover = self.make_turnover(gross)

        summary = live_mod.run_forced_stop_loss_scan(
            gross_result=gross,
            turnover_df=turnover,
        )

        self.assertEqual(summary["threshold_pct"].tolist(), [2, 3, 4, 5])
        stop_counts = dict(zip(summary["threshold_pct"], summary["forced_stop_count"]))
        self.assertGreater(stop_counts[2], 0)
        self.assertGreater(stop_counts[3], 0)
        self.assertEqual(stop_counts[4], 0)
        self.assertEqual(stop_counts[5], 0)

    def test_two_day_momentum_strength_can_unlock_reentry_without_cash_reset(self) -> None:
        gross = self.make_blocked_reentry_result()
        turnover = self.make_turnover(gross)

        result = live_mod.apply_single_trade_forced_stop_loss(
            gross_result=gross,
            turnover_df=turnover,
            stop_loss_threshold=0.02,
            reentry_momentum_strength_days=2,
        )

        self.assertTrue(bool(result.loc[pd.Timestamp("2026-02-04"), "forced_stop_triggered"]))
        self.assertEqual(result.loc[pd.Timestamp("2026-02-05"), "next_holding"], "cash")
        self.assertEqual(result.loc[pd.Timestamp("2026-02-06"), "next_holding"], POSITION)
        self.assertEqual(result.loc[pd.Timestamp("2026-02-09"), "holding"], POSITION)

    def test_three_day_momentum_strength_waits_one_more_day_before_reentry(self) -> None:
        gross = self.make_blocked_reentry_result()
        turnover = self.make_turnover(gross)

        result = live_mod.apply_single_trade_forced_stop_loss(
            gross_result=gross,
            turnover_df=turnover,
            stop_loss_threshold=0.02,
            reentry_momentum_strength_days=3,
        )

        self.assertTrue(bool(result.loc[pd.Timestamp("2026-02-04"), "forced_stop_triggered"]))
        self.assertEqual(result.loc[pd.Timestamp("2026-02-06"), "next_holding"], "cash")
        self.assertEqual(result.loc[pd.Timestamp("2026-02-09"), "next_holding"], POSITION)
        self.assertEqual(result.loc[pd.Timestamp("2026-02-10"), "holding"], POSITION)

    def test_peak_drawdown_stop_uses_trade_peak_instead_of_entry_loss(self) -> None:
        gross = self.make_peak_drawdown_result()
        turnover = self.make_turnover(gross)

        result = live_mod.apply_peak_drawdown_forced_stop_loss(
            gross_result=gross,
            turnover_df=turnover,
            stop_loss_threshold=0.015,
        )

        self.assertTrue(bool(result.loc[pd.Timestamp("2026-03-04"), "forced_stop_triggered"]))
        self.assertGreater(float(result.loc[pd.Timestamp("2026-03-04"), "trade_return_net"]), 0.0)
        self.assertLessEqual(float(result.loc[pd.Timestamp("2026-03-04"), "trade_drawdown_from_peak"]), -0.015)
        self.assertEqual(result.loc[pd.Timestamp("2026-03-05"), "holding"], "cash")

    def test_peak_drawdown_two_day_momentum_strength_can_reenter_without_reset(self) -> None:
        gross = self.make_peak_drawdown_result()
        turnover = self.make_turnover(gross)

        result = live_mod.apply_peak_drawdown_forced_stop_loss(
            gross_result=gross,
            turnover_df=turnover,
            stop_loss_threshold=0.015,
            reentry_momentum_strength_days=2,
        )

        self.assertTrue(bool(result.loc[pd.Timestamp("2026-03-04"), "forced_stop_triggered"]))
        self.assertEqual(result.loc[pd.Timestamp("2026-03-05"), "next_holding"], "cash")
        self.assertEqual(result.loc[pd.Timestamp("2026-03-06"), "next_holding"], POSITION)
        self.assertEqual(result.loc[pd.Timestamp("2026-03-09"), "holding"], POSITION)

    def test_peak_drawdown_three_day_momentum_strength_waits_longer_before_reentry(self) -> None:
        gross = self.make_peak_drawdown_result()
        turnover = self.make_turnover(gross)

        result = live_mod.apply_peak_drawdown_forced_stop_loss(
            gross_result=gross,
            turnover_df=turnover,
            stop_loss_threshold=0.015,
            reentry_momentum_strength_days=3,
        )

        self.assertTrue(bool(result.loc[pd.Timestamp("2026-03-04"), "forced_stop_triggered"]))
        self.assertEqual(result.loc[pd.Timestamp("2026-03-06"), "next_holding"], "cash")
        self.assertEqual(result.loc[pd.Timestamp("2026-03-09"), "next_holding"], POSITION)
        self.assertEqual(result.loc[pd.Timestamp("2026-03-10"), "holding"], POSITION)

    def test_peak_drawdown_reentry_can_require_gap_to_reclaim_stop_day_level(self) -> None:
        gross = self.make_peak_drawdown_stop_level_gate_result()
        turnover = self.make_turnover(gross)

        result = live_mod.apply_peak_drawdown_forced_stop_loss(
            gross_result=gross,
            turnover_df=turnover,
            stop_loss_threshold=0.015,
            reentry_momentum_strength_days=2,
            require_reentry_gap_above_stop_level=True,
        )

        self.assertTrue(bool(result.loc[pd.Timestamp("2026-03-18"), "forced_stop_triggered"]))
        self.assertEqual(result.loc[pd.Timestamp("2026-03-20"), "next_holding"], "cash")
        self.assertEqual(result.loc[pd.Timestamp("2026-03-23"), "next_holding"], "cash")
        self.assertEqual(result.loc[pd.Timestamp("2026-03-24"), "next_holding"], POSITION)
        self.assertEqual(result.loc[pd.Timestamp("2026-03-25"), "holding"], POSITION)

    def test_peak_drawdown_can_stop_after_two_one_percent_events_within_five_days(self) -> None:
        gross = self.make_peak_drawdown_two_hit_window_result()
        turnover = self.make_turnover(gross)

        result = live_mod.apply_peak_drawdown_forced_stop_loss(
            gross_result=gross,
            turnover_df=turnover,
            stop_loss_threshold=0.01,
            stop_trigger_window_days=5,
            stop_trigger_event_count=2,
        )

        self.assertFalse(bool(result.loc[pd.Timestamp("2026-04-08"), "forced_stop_triggered"]))
        self.assertEqual(int(result.loc[pd.Timestamp("2026-04-08"), "drawdown_event_count_in_window"]), 1)
        self.assertTrue(bool(result.loc[pd.Timestamp("2026-04-10"), "forced_stop_triggered"]))
        self.assertEqual(int(result.loc[pd.Timestamp("2026-04-10"), "drawdown_event_count_in_window"]), 2)
        self.assertEqual(result.loc[pd.Timestamp("2026-04-13"), "holding"], "cash")

    def test_ratio_bias_take_profit_blocks_until_signal_reset(self) -> None:
        gross = self.make_ratio_bias_take_profit_result()
        turnover = self.make_turnover(gross)

        result = live_mod.apply_ratio_bias_take_profit(
            gross_result=gross,
            turnover_df=turnover,
            bias_window=2,
            take_profit_threshold=0.02,
        )

        self.assertTrue(bool(result.loc[pd.Timestamp("2026-05-06"), "take_profit_triggered"]))
        self.assertGreater(float(result.loc[pd.Timestamp("2026-05-06"), "trade_return_net"]), 0.0)
        self.assertGreaterEqual(float(result.loc[pd.Timestamp("2026-05-06"), "ratio_bias"]), 0.02)
        self.assertEqual(result.loc[pd.Timestamp("2026-05-07"), "holding"], "cash")
        self.assertEqual(result.loc[pd.Timestamp("2026-05-07"), "next_holding"], "cash")
        self.assertEqual(float(result.loc[pd.Timestamp("2026-05-07"), "return_net"]), 0.0)
        self.assertTrue(bool(result.loc[pd.Timestamp("2026-05-08"), "signal_reset_seen"]))
        self.assertEqual(result.loc[pd.Timestamp("2026-05-11"), "next_holding"], POSITION)

    def test_ratio_bias_take_profit_requires_positive_trade_return(self) -> None:
        gross = self.make_ratio_bias_non_positive_trade_result()
        turnover = self.make_turnover(gross)

        result = live_mod.apply_ratio_bias_take_profit(
            gross_result=gross,
            turnover_df=turnover,
            bias_window=2,
            take_profit_threshold=0.02,
        )

        self.assertFalse(bool(result["take_profit_triggered"].any()))
        self.assertLess(float(result.loc[pd.Timestamp("2026-05-19"), "trade_return_net"]), 0.0)
        self.assertGreaterEqual(float(result.loc[pd.Timestamp("2026-05-19"), "ratio_bias"]), 0.02)

    def test_single_trade_stop_cash_days_do_not_inherit_base_returns(self) -> None:
        gross = self.make_blocked_reentry_result()
        turnover = self.make_turnover(gross)

        result = live_mod.apply_single_trade_forced_stop_loss(
            gross_result=gross,
            turnover_df=turnover,
            stop_loss_threshold=0.02,
        )

        self.assertTrue(bool(result.loc[pd.Timestamp("2026-02-04"), "forced_stop_triggered"]))
        self.assertEqual(result.loc[pd.Timestamp("2026-02-05"), "holding"], "cash")
        self.assertEqual(float(result.loc[pd.Timestamp("2026-02-05"), "return_net"]), 0.0)

    def test_peak_drawdown_stop_cash_days_do_not_inherit_base_returns(self) -> None:
        gross = self.make_peak_drawdown_result()
        turnover = self.make_turnover(gross)

        result = live_mod.apply_peak_drawdown_forced_stop_loss(
            gross_result=gross,
            turnover_df=turnover,
            stop_loss_threshold=0.015,
        )

        self.assertTrue(bool(result.loc[pd.Timestamp("2026-03-04"), "forced_stop_triggered"]))
        self.assertEqual(result.loc[pd.Timestamp("2026-03-05"), "holding"], "cash")
        self.assertEqual(float(result.loc[pd.Timestamp("2026-03-05"), "return_net"]), 0.0)

    def test_gap_peak_decay_exit_triggers_when_gap_drops_below_trade_peak_ratio(self) -> None:
        gross = self.make_momentum_gap_peak_decay_result()
        turnover = self.make_turnover(gross)

        result = live_mod.apply_momentum_gap_peak_decay_exit(
            gross_result=gross,
            turnover_df=turnover,
            decay_ratio_threshold=0.5,
        )

        self.assertTrue(bool(result.loc[pd.Timestamp("2026-06-04"), "signal_quality_exit_triggered"]))
        self.assertEqual(float(result.loc[pd.Timestamp("2026-06-04"), "gap_peak"]), 0.10)
        self.assertAlmostEqual(float(result.loc[pd.Timestamp("2026-06-04"), "gap_decay_ratio"]), 0.4, places=9)

    def test_gap_peak_decay_exit_blocks_until_base_signal_resets(self) -> None:
        gross = self.make_momentum_gap_peak_decay_result()
        turnover = self.make_turnover(gross)

        result = live_mod.apply_momentum_gap_peak_decay_exit(
            gross_result=gross,
            turnover_df=turnover,
            decay_ratio_threshold=0.5,
        )

        self.assertEqual(result.loc[pd.Timestamp("2026-06-05"), "holding"], "cash")
        self.assertTrue(bool(result.loc[pd.Timestamp("2026-06-05"), "signal_reset_seen"]))
        self.assertEqual(result.loc[pd.Timestamp("2026-06-08"), "next_holding"], POSITION)

    def test_gap_peak_decay_cash_days_have_zero_return_net_after_exit(self) -> None:
        gross = self.make_momentum_gap_peak_decay_result()
        turnover = self.make_turnover(gross)

        result = live_mod.apply_momentum_gap_peak_decay_exit(
            gross_result=gross,
            turnover_df=turnover,
            decay_ratio_threshold=0.5,
        )

        self.assertEqual(result.loc[pd.Timestamp("2026-06-05"), "holding"], "cash")
        self.assertEqual(float(result.loc[pd.Timestamp("2026-06-05"), "return_net"]), 0.0)

    def test_gap_peak_decay_derisk_switches_execution_scale_after_threshold(self) -> None:
        gross = self.make_momentum_gap_peak_decay_derisk_result()
        turnover = self.make_turnover(gross)

        result = live_mod.apply_momentum_gap_peak_decay_derisk(
            gross_result=gross,
            turnover_df=turnover,
            decay_ratio_threshold=0.6,
            derisk_scale=0.5,
        )

        self.assertFalse(bool(result.loc[pd.Timestamp("2026-06-16"), "signal_quality_derisk_triggered"]))
        self.assertTrue(bool(result.loc[pd.Timestamp("2026-06-17"), "signal_quality_derisk_triggered"]))
        self.assertEqual(float(result.loc[pd.Timestamp("2026-06-16"), "execution_scale"]), 1.0)
        self.assertEqual(float(result.loc[pd.Timestamp("2026-06-17"), "execution_scale"]), 0.5)

    def test_gap_peak_decay_derisk_scales_return_net_on_derisked_days(self) -> None:
        gross = self.make_momentum_gap_peak_decay_derisk_result()
        turnover = self.make_turnover(gross)

        result = live_mod.apply_momentum_gap_peak_decay_derisk(
            gross_result=gross,
            turnover_df=turnover,
            decay_ratio_threshold=0.6,
            derisk_scale=0.5,
        )

        self.assertAlmostEqual(float(result.loc[pd.Timestamp("2026-06-17"), "return_net"]), 0.015, places=9)
        self.assertAlmostEqual(float(result.loc[pd.Timestamp("2026-06-18"), "return_net"]), 0.01, places=9)

    def test_gap_peak_decay_derisk_does_not_use_full_return_after_scale_cut(self) -> None:
        gross = self.make_momentum_gap_peak_decay_derisk_result()
        turnover = self.make_turnover(gross)

        result = live_mod.apply_momentum_gap_peak_decay_derisk(
            gross_result=gross,
            turnover_df=turnover,
            decay_ratio_threshold=0.6,
            derisk_scale=0.5,
        )

        self.assertLess(float(result.loc[pd.Timestamp("2026-06-18"), "return_net"]), float(gross.loc[pd.Timestamp("2026-06-18"), "return"]))
        self.assertEqual(result.loc[pd.Timestamp("2026-06-19"), "next_holding"], "cash")

    def test_gap_peak_decay_derisk_can_recover_to_full_scale_at_sixty_percent(self) -> None:
        gross = self.make_momentum_gap_peak_decay_derisk_recovery_result()
        turnover = self.make_turnover(gross)

        result = live_mod.apply_momentum_gap_peak_decay_derisk(
            gross_result=gross,
            turnover_df=turnover,
            decay_ratio_threshold=0.2,
            derisk_scale=0.7,
            recovery_ratio_threshold=0.6,
        )

        self.assertTrue(bool(result.loc[pd.Timestamp("2026-06-24"), "signal_quality_derisk_triggered"]))
        self.assertEqual(float(result.loc[pd.Timestamp("2026-06-24"), "execution_scale"]), 0.7)
        self.assertEqual(float(result.loc[pd.Timestamp("2026-06-25"), "execution_scale"]), 1.0)
        self.assertAlmostEqual(float(result.loc[pd.Timestamp("2026-06-25"), "gap_decay_ratio"]), 0.7, places=9)

    def test_gap_peak_decay_derisk_stays_cut_until_eighty_percent_recovery(self) -> None:
        gross = self.make_momentum_gap_peak_decay_derisk_recovery_result()
        turnover = self.make_turnover(gross)

        result = live_mod.apply_momentum_gap_peak_decay_derisk(
            gross_result=gross,
            turnover_df=turnover,
            decay_ratio_threshold=0.2,
            derisk_scale=0.7,
            recovery_ratio_threshold=0.8,
        )

        self.assertEqual(float(result.loc[pd.Timestamp("2026-06-24"), "execution_scale"]), 0.7)
        self.assertEqual(float(result.loc[pd.Timestamp("2026-06-25"), "execution_scale"]), 0.7)
        self.assertEqual(float(result.loc[pd.Timestamp("2026-06-26"), "execution_scale"]), 1.0)

    def test_gap_peak_decay_derisk_does_not_retrigger_after_recovery_without_new_peak(self) -> None:
        gross = self.make_momentum_gap_peak_decay_derisk_rearm_result()
        turnover = self.make_turnover(gross)

        result = live_mod.apply_momentum_gap_peak_decay_derisk(
            gross_result=gross,
            turnover_df=turnover,
            decay_ratio_threshold=0.2,
            derisk_scale=0.3,
            recovery_ratio_threshold=0.4,
        )

        self.assertTrue(bool(result.loc[pd.Timestamp("2026-07-03"), "signal_quality_derisk_triggered"]))
        self.assertEqual(float(result.loc[pd.Timestamp("2026-07-06"), "execution_scale"]), 1.0)
        self.assertFalse(bool(result.loc[pd.Timestamp("2026-07-07"), "signal_quality_derisk_triggered"]))
        self.assertEqual(float(result.loc[pd.Timestamp("2026-07-07"), "execution_scale"]), 1.0)

    def test_gap_peak_decay_derisk_can_retrigger_after_recovery_once_trade_sets_new_peak(self) -> None:
        gross = self.make_momentum_gap_peak_decay_derisk_rearm_result()
        turnover = self.make_turnover(gross)

        result = live_mod.apply_momentum_gap_peak_decay_derisk(
            gross_result=gross,
            turnover_df=turnover,
            decay_ratio_threshold=0.2,
            derisk_scale=0.3,
            recovery_ratio_threshold=0.4,
        )

        self.assertEqual(float(result.loc[pd.Timestamp("2026-07-08"), "gap_peak"]), 0.12)
        self.assertTrue(bool(result.loc[pd.Timestamp("2026-07-10"), "signal_quality_derisk_triggered"]))
        self.assertEqual(float(result.loc[pd.Timestamp("2026-07-10"), "execution_scale"]), 0.3)


if __name__ == "__main__":
    unittest.main()
