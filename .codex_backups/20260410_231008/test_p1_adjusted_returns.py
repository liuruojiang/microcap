from __future__ import annotations

import shutil
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

import pandas as pd

import analyze_top100_rebalance_frequency as freq_mod
import fetch_wind_microcap_index as index_mod


class AdjustedReturnCacheTests(unittest.TestCase):
    def _build_cache_root(self) -> Path:
        return freq_mod.ROOT / "_tmp_test_cache" / uuid.uuid4().hex

    def test_load_symbol_cache_prefers_adjusted_prices_for_returns(self) -> None:
        trading_dates = pd.DatetimeIndex([pd.Timestamp("2024-01-02"), pd.Timestamp("2024-01-03")])
        cap_dates = trading_dates

        root = self._build_cache_root()
        try:
            price_dir = root / "prices_raw"
            adj_dir = root / "prices_qfq"
            share_dir = root / "share_change"
            price_dir.mkdir(parents=True)
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
        finally:
            shutil.rmtree(root, ignore_errors=True)

        self.assertIsNotNone(result)
        _, ret_series, cap_series, _, _ = result
        self.assertEqual(float(ret_series.loc[pd.Timestamp("2024-01-03")]), 0.0)
        self.assertEqual(float(cap_series.loc[pd.Timestamp("2024-01-03")]), 20.0 * 100.0 * 10000.0)

    def test_load_symbol_cache_falls_back_to_raw_prices_when_adjusted_cache_missing(self) -> None:
        trading_dates = pd.DatetimeIndex([pd.Timestamp("2024-01-02"), pd.Timestamp("2024-01-03")])
        cap_dates = trading_dates

        root = self._build_cache_root()
        try:
            price_dir = root / "prices_raw"
            adj_dir = root / "prices_qfq"
            share_dir = root / "share_change"
            price_dir.mkdir(parents=True)
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
                    "change_date": ["2024-01-02"],
                    "total_shares_10k": [100.0],
                    "reason": ["瀹氭湡鎶ュ憡"],
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
        finally:
            shutil.rmtree(root, ignore_errors=True)

        self.assertIsNotNone(result)
        _, ret_series, _, _, _ = result
        self.assertEqual(float(ret_series.loc[pd.Timestamp("2024-01-03")]), 1.0)


class AdjustedPriceFetchTests(unittest.TestCase):
    def _build_cache_root(self) -> Path:
        return index_mod.ROOT / "_tmp_test_cache" / uuid.uuid4().hex

    def test_fetch_adjusted_price_history_uses_tencent_source_and_writes_cache(self) -> None:
        root = self._build_cache_root()
        try:
            adj_dir = root / "prices_qfq"
            adj_dir.mkdir(parents=True)
            expected = pd.DataFrame(
                {
                    "date": pd.to_datetime(["2024-01-02", "2024-01-03"]),
                    "close_qfq": [10.0, 10.5],
                }
            )
            with (
                patch.object(index_mod, "ADJ_PRICE_CACHE_DIR", adj_dir),
                patch.object(index_mod, "SHARED_ADJ_PRICE_CACHE_DIR", None),
                patch.object(index_mod, "_fetch_adjusted_price_history_tx", return_value=expected.copy()) as tx_mock,
                patch.object(index_mod, "get_akshare") as ak_mock,
            ):
                result = index_mod.fetch_adjusted_price_history(
                    symbol="000001",
                    start_date="2024-01-02",
                    end_date="2024-01-03",
                    force_refresh=True,
                )
        finally:
            shutil.rmtree(root, ignore_errors=True)

        self.assertEqual(tx_mock.call_count, 1)
        self.assertEqual(ak_mock.call_count, 0)
        self.assertEqual(result["close_qfq"].tolist(), [10.0, 10.5])


if __name__ == "__main__":
    unittest.main()
