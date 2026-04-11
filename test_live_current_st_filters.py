from __future__ import annotations

import shutil
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

import pandas as pd

import analyze_top100_rebalance_frequency as freq_mod
import microcap_top100_mom16_biweekly_live as live_mod
import poe_bots.microcap_top100_poe_bot_autorebuild_singlefile as bot_mod


class LiveCurrentStFilterTests(unittest.TestCase):
    def _build_cache_root(self) -> Path:
        return freq_mod.ROOT / "_tmp_test_cache" / uuid.uuid4().hex

    def test_load_symbol_cache_keeps_current_non_st_symbol_for_live_caps(self) -> None:
        trading_dates = pd.DatetimeIndex([pd.Timestamp("2026-04-02")])
        cap_dates = trading_dates
        root = self._build_cache_root()

        try:
            price_dir = root / "prices_raw"
            share_dir = root / "share_change"
            current_st_path = root / "current_st.csv"
            price_dir.mkdir(parents=True)
            share_dir.mkdir()

            pd.DataFrame(
                {
                    "date": ["2026-04-02"],
                    "close_raw": [12.59],
                }
            ).to_csv(price_dir / "600847.csv", index=False)
            pd.DataFrame(
                {
                    "change_date": ["2024-12-31"],
                    "total_shares_10k": [15328.74],
                }
            ).to_csv(share_dir / "600847.csv", index=False)
            pd.DataFrame({"code": ["000001"]}).to_csv(current_st_path, index=False)

            meta = {
                "symbol": "600847",
                "first_trade_date": "2010-01-04",
                "last_trade_date": "2026-04-10",
                "list_date": "1994-03-24",
                "delist_date": None,
                "st_intervals": [{"start": "2017-04-29", "end": None, "source": "cninfo_notice"}],
            }

            with (
                patch.object(freq_mod, "PRICE_DIR", price_dir),
                patch.object(freq_mod, "SHARE_DIR", share_dir),
                patch.object(freq_mod, "SHARED_PRICE_DIR", None),
                patch.object(freq_mod, "SHARED_SHARE_DIR", None),
                patch.object(freq_mod, "CURRENT_ST", current_st_path),
                patch.object(freq_mod, "load_security_meta", return_value=meta),
            ):
                result = freq_mod.load_symbol_cache(
                    symbol="600847",
                    trading_dates=trading_dates,
                    cap_dates=cap_dates,
                    exclude_historical_st_from_caps=False,
                )
        finally:
            shutil.rmtree(root, ignore_errors=True)

        self.assertIsNotNone(result)
        _, _, cap_series, _, _ = result
        self.assertTrue(pd.notna(cap_series.loc[pd.Timestamp("2026-04-02")]))

    def test_select_live_target_members_backfills_after_name_filter(self) -> None:
        rebalance_date = pd.Timestamp("2026-04-02")
        caps_by_date = {
            rebalance_date: {
                "300391": 100.0,
                **{f"{i:06d}": 1000.0 + i for i in range(1, 100)},
                "001366": 2000.0,
            }
        }
        rebalance_dates = pd.DatetimeIndex([rebalance_date])
        name_map = {"300391": "长药退", "001366": "播恩集团"}
        for i in range(1, 100):
            name_map[f"{i:06d}"] = f"样本{i:03d}"

        members_map = live_mod.build_live_target_members_map(
            caps_by_date=caps_by_date,
            rebalance_dates=rebalance_dates,
            name_map=name_map,
            top_n=100,
        )

        members = members_map[rebalance_date]
        self.assertEqual(len(members), 100)
        self.assertNotIn("300391", members)
        self.assertIn("001366", members)

    def test_poe_bot_effective_date_aligns_with_close_execution(self) -> None:
        trading_dates = pd.DatetimeIndex(
            [
                pd.Timestamp("2026-04-01"),
                pd.Timestamp("2026-04-02"),
                pd.Timestamp("2026-04-03"),
            ]
        )
        effective_date = bot_mod.resolve_rebalance_effective_date(
            trading_dates=trading_dates,
            latest_rebalance=pd.Timestamp("2026-04-02"),
        )
        self.assertEqual(effective_date, "2026-04-02")


if __name__ == "__main__":
    unittest.main()
