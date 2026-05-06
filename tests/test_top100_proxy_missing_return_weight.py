import unittest
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import analyze_top100_rebalance_frequency as freq


class Top100ProxyMissingReturnWeightTest(unittest.TestCase):
    def test_missing_held_return_keeps_zero_weight_in_proxy_portfolio(self):
        trading_dates = pd.DatetimeIndex(
            [pd.Timestamp("2026-04-23"), pd.Timestamp("2026-04-24"), pd.Timestamp("2026-04-27")]
        )
        rebalance_dates = pd.DatetimeIndex([trading_dates[0]])
        target_members_map = {trading_dates[0]: ["000001", "000002"]}
        returns_df = pd.DataFrame(
            {
                "000001": [np.nan, 0.10, 0.00],
                "000002": [np.nan, np.nan, 0.00],
            },
            index=trading_dates,
        )
        buyable_df = pd.DataFrame(True, index=trading_dates, columns=returns_df.columns)
        sellable_df = pd.DataFrame(True, index=trading_dates, columns=returns_df.columns)

        index_df, _, _ = freq.simulate_rebalance_path(
            trading_dates=trading_dates,
            returns_df=returns_df,
            target_members_map=target_members_map,
            rebalance_dates=rebalance_dates,
            buyable_df=buyable_df,
            sellable_df=sellable_df,
            one_side_cost_rate=0.0,
            top_n=2,
            execution_timing=freq.EXECUTION_TIMING_CLOSE,
        )

        first_holding_return = index_df.loc[index_df["date"].eq(trading_dates[1]), "daily_return"].iloc[0]
        self.assertEqual(first_holding_return, 0.05)


if __name__ == "__main__":
    unittest.main()
