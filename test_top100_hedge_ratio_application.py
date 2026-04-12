from __future__ import annotations

import unittest
from unittest.mock import patch

import numpy as np
import pandas as pd

import analyze_microcap_zz1000_hedge as hedge_mod
import microcap_top100_mom16_biweekly_live as live_mod


class Top100HedgeRatioApplicationTests(unittest.TestCase):
    def test_run_backtest_scales_hedge_leg_by_hedge_ratio(self) -> None:
        close_df = pd.DataFrame(
            {
                "microcap": [100.0, 110.0, 121.0, 133.1, 146.41, 161.051],
                "hedge": [100.0, 101.0, 102.01, 103.0301, 104.060401, 105.10100501],
            },
            index=pd.to_datetime(
                [
                    "2026-01-05",
                    "2026-01-06",
                    "2026-01-07",
                    "2026-01-08",
                    "2026-01-09",
                    "2026-01-12",
                ]
            ),
        )

        with patch.object(hedge_mod, "calc_bias_momentum", return_value=pd.Series(np.zeros(len(close_df)), index=close_df.index)):
            with patch.object(hedge_mod, "calc_rolling_r2", return_value=pd.Series(np.ones(len(close_df)), index=close_df.index)):
                full_hedge = hedge_mod.run_backtest(
                    close_df=close_df,
                    signal_model="momentum",
                    lookback=2,
                    bias_n=2,
                    bias_mom_day=1,
                    futures_drag=0.0,
                    require_positive_microcap_mom=True,
                    r2_window=2,
                    r2_threshold=0.0,
                    vol_scale_enabled=False,
                    target_vol=0.0,
                    vol_window=2,
                    max_lev=1.0,
                    min_lev=1.0,
                    scale_threshold=0.0,
                    hedge_ratio=1.0,
                )
                partial_hedge = hedge_mod.run_backtest(
                    close_df=close_df,
                    signal_model="momentum",
                    lookback=2,
                    bias_n=2,
                    bias_mom_day=1,
                    futures_drag=0.0,
                    require_positive_microcap_mom=True,
                    r2_window=2,
                    r2_threshold=0.0,
                    vol_scale_enabled=False,
                    target_vol=0.0,
                    vol_window=2,
                    max_lev=1.0,
                    min_lev=1.0,
                    scale_threshold=0.0,
                    hedge_ratio=0.8,
                )

        active_dates = partial_hedge.index[partial_hedge["holding"] == "long_microcap_short_zz1000"]
        self.assertGreater(len(active_dates), 0)
        test_date = active_dates[0]

        microcap_ret = float(partial_hedge.loc[test_date, "microcap_ret"])
        hedge_ret = float(partial_hedge.loc[test_date, "hedge_ret"])
        self.assertAlmostEqual(
            float(full_hedge.loc[test_date, "return_raw"]),
            microcap_ret - hedge_ret,
            places=12,
        )
        self.assertAlmostEqual(
            float(partial_hedge.loc[test_date, "return_raw"]),
            microcap_ret - 0.8 * hedge_ret,
            places=12,
        )

    def test_run_signal_passes_fixed_hedge_ratio_to_backtest(self) -> None:
        close_df = pd.DataFrame(
            {
                "microcap": [100.0, 101.0, 102.0],
                "hedge": [100.0, 100.5, 101.0],
            },
            index=pd.to_datetime(["2026-01-05", "2026-01-06", "2026-01-07"]),
        )
        fake_result = pd.DataFrame(
            {
                "return_raw": [0.0],
                "holding": ["cash"],
                "next_holding": ["cash"],
                "signal_on": [False],
                "microcap_close": [102.0],
                "hedge_close": [101.0],
                "microcap_ret": [0.0],
                "hedge_ret": [0.0],
                "microcap_mom": [0.0],
                "hedge_mom": [0.0],
                "momentum_gap": [0.0],
                "ratio_bias_mom": [0.0],
                "ratio_r2": [0.0],
                "futures_drag": [0.0],
                "active_spread_ret": [0.0],
                "weight": [1.0],
                "realized_vol": [0.0],
                "scale_raw": [0.0],
                "return": [0.0],
                "nav": [1.0],
            },
            index=pd.to_datetime(["2026-01-07"]),
        )

        with patch.object(live_mod.hedge_mod, "run_backtest", return_value=fake_result) as run_backtest_mock:
            live_mod.run_signal(close_df)

        self.assertEqual(run_backtest_mock.call_args.kwargs["hedge_ratio"], live_mod.FIXED_HEDGE_RATIO)


if __name__ == "__main__":
    unittest.main()
