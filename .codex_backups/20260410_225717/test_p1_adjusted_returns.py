from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

import analyze_top100_rebalance_frequency as freq_mod


class AdjustedReturnCacheTests(unittest.TestCase):
    def test_load_symbol_cache_prefers_adjusted_prices_for_returns(self) -> None:
        trading_dates = pd.DatetimeIndex([pd.Timestamp("2024-01-02"), pd.Timestamp("2024-01-03")])
        cap_dates = trading_dates

        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            price_dir = root / "prices_raw"
            adj_dir = root / "prices_qfq"
            share_dir = root / "share_change"
            price_dir.mkdir()
            adj_dir.mkdir()
            share_dir.mkdir()

            pd.DataFrame(
                {
                    "date": ["2024-01-02", "2024-01-03"],
                    "close_raw": [10.0, 20.0],
                }
            ).to_csv(price_dir / "000001.csv", index=False)
            pd.DataFrame(
                {
                    "date": ["2024-01-02", "2024-01-03"],
                    "close_qfq": [10.0, 10.0],
                }
            ).to_csv(adj_dir / "000001.csv", index=False)
            pd.DataFrame(
                {
                    "change_date": ["2024-01-02"],
                    "total_shares_10k": [100.0],
                    "reason": ["定期报告"],
                }
            ).to_csv(share_dir / "000001.csv", index=False)

            with (
                patch.object(freq_mod, "PRICE_DIR", price_dir),
                patch.object(freq_mod, "ADJ_PRICE_DIR", adj_dir),
                patch.object(freq_mod, "SHARE_DIR", share_dir),
                patch.object(freq_mod, "SHARED_PRICE_DIR", None),
                patch.object(freq_mod, "SHARED_ADJ_PRICE_DIR", None),
                patch.object(freq_mod, "SHARED_SHARE_DIR", None),
            ):
                result = freq_mod.load_symbol_cache(
                    symbol="000001",
                    trading_dates=trading_dates,
                    cap_dates=cap_dates,
                )

        self.assertIsNotNone(result)
        _, ret_series, cap_series, _, _ = result
        self.assertEqual(float(ret_series.loc[pd.Timestamp("2024-01-03")]), 0.0)
        self.assertEqual(float(cap_series.loc[pd.Timestamp("2024-01-03")]), 20.0 * 100.0 * 10000.0)


if __name__ == "__main__":
    unittest.main()
