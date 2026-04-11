from __future__ import annotations

import shutil
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

import pandas as pd

import analyze_top100_rebalance_frequency as freq_mod


class HistoricalUniverseTests(unittest.TestCase):
    def _build_cache_root(self) -> Path:
        return freq_mod.ROOT / "_tmp_test_cache" / uuid.uuid4().hex

    def test_list_backtest_universe_symbols_uses_price_share_intersection(self) -> None:
        root = self._build_cache_root()
        try:
            price_dir = root / "prices_raw"
            share_dir = root / "share_change"
            shared_price_dir = root / "shared_prices_raw"
            shared_share_dir = root / "shared_share_change"
            price_dir.mkdir(parents=True)
            share_dir.mkdir()
            shared_price_dir.mkdir()
            shared_share_dir.mkdir()

            (price_dir / "000001.csv").write_text("date,close_raw\n2024-01-02,10\n", encoding="utf-8")
            (price_dir / "000002.csv").write_text("date,close_raw\n2024-01-02,11\n", encoding="utf-8")
            (share_dir / "000001.csv").write_text("change_date,total_shares_10k\n2024-01-02,100\n", encoding="utf-8")
            (shared_share_dir / "000002.csv").write_text(
                "change_date,total_shares_10k\n2024-01-02,100\n",
                encoding="utf-8",
            )
            (shared_price_dir / "000003.csv").write_text("date,close_raw\n2024-01-02,12\n", encoding="utf-8")

            with (
                patch.object(freq_mod, "PRICE_DIR", price_dir),
                patch.object(freq_mod, "SHARE_DIR", share_dir),
                patch.object(freq_mod, "SHARED_PRICE_DIR", shared_price_dir),
                patch.object(freq_mod, "SHARED_SHARE_DIR", shared_share_dir),
            ):
                symbols = freq_mod.list_backtest_universe_symbols()
        finally:
            shutil.rmtree(root, ignore_errors=True)

        self.assertEqual(symbols, ["000001", "000002"])

    def test_list_backtest_universe_symbols_intersects_security_master(self) -> None:
        root = self._build_cache_root()
        try:
            price_dir = root / "prices_raw"
            share_dir = root / "share_change"
            price_dir.mkdir(parents=True)
            share_dir.mkdir()

            (price_dir / "000001.csv").write_text("date,close_raw\n2024-01-02,10\n", encoding="utf-8")
            (price_dir / "000002.csv").write_text("date,close_raw\n2024-01-02,11\n", encoding="utf-8")
            (share_dir / "000001.csv").write_text("change_date,total_shares_10k\n2024-01-02,100\n", encoding="utf-8")
            (share_dir / "000002.csv").write_text("change_date,total_shares_10k\n2024-01-02,100\n", encoding="utf-8")

            master = pd.DataFrame(
                {
                    "symbol": ["000001", "000003"],
                    "list_date": ["2010-01-01", "2011-01-01"],
                    "delist_date": [None, None],
                }
            )

            with (
                patch.object(freq_mod, "PRICE_DIR", price_dir),
                patch.object(freq_mod, "SHARE_DIR", share_dir),
                patch.object(freq_mod, "SHARED_PRICE_DIR", None),
                patch.object(freq_mod, "SHARED_SHARE_DIR", None),
                patch.object(freq_mod, "load_security_master", return_value=master),
            ):
                symbols = freq_mod.list_backtest_universe_symbols()
        finally:
            shutil.rmtree(root, ignore_errors=True)

        self.assertEqual(symbols, ["000001"])

    def test_build_master_rows_from_cache_does_not_infer_delist_date(self) -> None:
        root = self._build_cache_root()
        try:
            price_dir = root / "prices_raw"
            price_dir.mkdir(parents=True)
            (price_dir / "688001.csv").write_text(
                "date,close_raw\n2024-01-02,10\n2024-01-03,11\n",
                encoding="utf-8",
            )

            with (
                patch.object(freq_mod, "PRICE_DIR", price_dir),
                patch.object(freq_mod, "SHARED_PRICE_DIR", None),
            ):
                frame = freq_mod._build_master_rows_from_cache({"688001"})
        finally:
            shutil.rmtree(root, ignore_errors=True)

        self.assertEqual(frame.loc[0, "symbol"], "688001")
        self.assertTrue(pd.isna(frame.loc[0, "delist_date"]))

    def test_build_st_intervals_from_name_changes_handles_initial_exit_and_reentry(self) -> None:
        changes = pd.DataFrame(
            {
                "change_date": pd.to_datetime(["2012-05-03", "2021-04-29", "2022-06-30", "2023-04-29"]),
                "old_name": ["ST零七", "全新好", "*ST全新", "全新好"],
                "new_name": ["零七股份", "*ST全新", "全新好", "*ST全新"],
            }
        )

        intervals = freq_mod.build_st_intervals_from_name_changes(
            first_trade_date=pd.Timestamp("2010-01-04"),
            last_trade_date=pd.Timestamp("2026-04-10"),
            changes=changes,
        )

        self.assertEqual(
            intervals,
            [
                {"start": "2010-01-04", "end": "2012-05-03", "source": "name_change"},
                {"start": "2021-04-29", "end": "2022-06-30", "source": "name_change"},
                {"start": "2023-04-29", "end": None, "source": "name_change"},
            ],
        )

    def test_load_symbol_cache_excludes_st_dates_from_cap_series(self) -> None:
        trading_dates = pd.DatetimeIndex([pd.Timestamp("2024-01-02"), pd.Timestamp("2024-01-03")])
        cap_dates = trading_dates

        root = self._build_cache_root()
        try:
            price_dir = root / "prices_raw"
            share_dir = root / "share_change"
            price_dir.mkdir(parents=True)
            share_dir.mkdir()

            pd.DataFrame(
                {
                    "date": ["2024-01-02", "2024-01-03"],
                    "close_raw": [10.0, 11.0],
                }
            ).to_csv(price_dir / "000001.csv", index=False)
            pd.DataFrame(
                {
                    "change_date": ["2024-01-02"],
                    "total_shares_10k": [100.0],
                }
            ).to_csv(share_dir / "000001.csv", index=False)

            meta = {
                "symbol": "000001",
                "first_trade_date": "2024-01-02",
                "last_trade_date": "2024-01-03",
                "st_intervals": [{"start": "2024-01-03", "end": None, "source": "name_change"}],
            }

            with (
                patch.object(freq_mod, "PRICE_DIR", price_dir),
                patch.object(freq_mod, "SHARE_DIR", share_dir),
                patch.object(freq_mod, "SHARED_PRICE_DIR", None),
                patch.object(freq_mod, "SHARED_SHARE_DIR", None),
                patch.object(freq_mod, "load_security_meta", return_value=meta),
            ):
                result = freq_mod.load_symbol_cache(
                    symbol="000001",
                    trading_dates=trading_dates,
                    cap_dates=cap_dates,
                )
        finally:
            shutil.rmtree(root, ignore_errors=True)

        self.assertIsNotNone(result)
        _, _, cap_series, _, _ = result
        self.assertTrue(pd.notna(cap_series.loc[pd.Timestamp("2024-01-02")]))
        self.assertTrue(pd.isna(cap_series.loc[pd.Timestamp("2024-01-03")]))

    def test_load_symbol_cache_excludes_inactive_dates_from_cap_series(self) -> None:
        trading_dates = pd.DatetimeIndex(
            [
                pd.Timestamp("2024-01-01"),
                pd.Timestamp("2024-01-02"),
                pd.Timestamp("2024-01-03"),
                pd.Timestamp("2024-01-04"),
            ]
        )
        cap_dates = trading_dates

        root = self._build_cache_root()
        try:
            price_dir = root / "prices_raw"
            share_dir = root / "share_change"
            price_dir.mkdir(parents=True)
            share_dir.mkdir()

            pd.DataFrame(
                {
                    "date": ["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04"],
                    "close_raw": [9.0, 10.0, 11.0, 12.0],
                }
            ).to_csv(price_dir / "000001.csv", index=False)
            pd.DataFrame(
                {
                    "change_date": ["2024-01-01"],
                    "total_shares_10k": [100.0],
                }
            ).to_csv(share_dir / "000001.csv", index=False)

            meta = {
                "symbol": "000001",
                "first_trade_date": "2024-01-01",
                "last_trade_date": "2024-01-04",
                "list_date": "2024-01-02",
                "delist_date": "2024-01-03",
                "st_intervals": [],
            }

            with (
                patch.object(freq_mod, "PRICE_DIR", price_dir),
                patch.object(freq_mod, "SHARE_DIR", share_dir),
                patch.object(freq_mod, "SHARED_PRICE_DIR", None),
                patch.object(freq_mod, "SHARED_SHARE_DIR", None),
                patch.object(freq_mod, "load_security_meta", return_value=meta),
            ):
                result = freq_mod.load_symbol_cache(
                    symbol="000001",
                    trading_dates=trading_dates,
                    cap_dates=cap_dates,
                )
        finally:
            shutil.rmtree(root, ignore_errors=True)

        self.assertIsNotNone(result)
        _, _, cap_series, _, _ = result
        self.assertTrue(pd.isna(cap_series.loc[pd.Timestamp("2024-01-01")]))
        self.assertTrue(pd.notna(cap_series.loc[pd.Timestamp("2024-01-02")]))
        self.assertTrue(pd.notna(cap_series.loc[pd.Timestamp("2024-01-03")]))
        self.assertTrue(pd.isna(cap_series.loc[pd.Timestamp("2024-01-04")]))

    def test_build_st_intervals_from_name_changes_handles_pre_start_history(self) -> None:
        changes = pd.DataFrame(
            {
                "change_date": pd.to_datetime(["2002-04-12", "2012-05-04"]),
                "old_name": ["ST达声", "ST零七"],
                "new_name": ["深达声A", "零七股份"],
            }
        )

        intervals = freq_mod.build_st_intervals_from_name_changes(
            first_trade_date=pd.Timestamp("2010-01-04"),
            last_trade_date=pd.Timestamp("2026-04-10"),
            changes=changes,
        )

        self.assertEqual(
            intervals,
            [{"start": "2010-01-04", "end": "2012-05-04", "source": "name_change"}],
        )


if __name__ == "__main__":
    unittest.main()
