from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import pandas as pd

import microcap_top100_mom16_biweekly_live_v2_0 as live


class RealtimeRefreshCacheDirTests(unittest.TestCase):
    def test_refresh_price_cache_tail_creates_cache_dirs_before_fetch(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cache_dir = root / ".microcap_index_cache"
            price_dir = cache_dir / "prices_raw"
            adjusted_dir = cache_dir / "prices_qfq"
            share_dir = cache_dir / "share_change"

            original_values = {
                "CACHE_DIR": getattr(live.fetch_mod, "CACHE_DIR"),
                "PRICE_CACHE_DIR": getattr(live.fetch_mod, "PRICE_CACHE_DIR"),
                "ADJ_PRICE_CACHE_DIR": getattr(live.fetch_mod, "ADJ_PRICE_CACHE_DIR"),
                "SHARE_CACHE_DIR": getattr(live.fetch_mod, "SHARE_CACHE_DIR"),
                "fetch_price_history": getattr(live.fetch_mod, "fetch_price_history"),
                "fetch_share_change": getattr(live.fetch_mod, "fetch_share_change", None),
            }

            def fake_price_history(symbol: str, start_date: str, end_date: str, force_refresh: bool = False) -> pd.DataFrame:
                self.assertTrue(price_dir.is_dir())
                return pd.DataFrame({"date": [pd.Timestamp(end_date)], "close_raw": [1.0]})

            def fake_share_change(symbol: str, start_date: str, end_date: str, force_refresh: bool = False) -> pd.DataFrame:
                self.assertTrue(share_dir.is_dir())
                return pd.DataFrame(
                    {
                        "change_date": [pd.Timestamp(end_date)],
                        "total_shares_10k": [1.0],
                        "reason": ["test"],
                    }
                )

            try:
                for name, value in {
                    "CACHE_DIR": cache_dir,
                    "PRICE_CACHE_DIR": price_dir,
                    "ADJ_PRICE_CACHE_DIR": adjusted_dir,
                    "SHARE_CACHE_DIR": share_dir,
                }.items():
                    setattr(live.fetch_mod, name, value)
                    live._fetch_ns[name] = value
                setattr(live.fetch_mod, "fetch_price_history", fake_price_history)
                setattr(live.fetch_mod, "fetch_share_change", fake_share_change)

                live.base_mod.refresh_price_cache_tail(
                    pd.Timestamp("2026-05-25"),
                    max_workers=1,
                    symbols=["000001"],
                    force_refresh=True,
                )

                self.assertTrue(price_dir.is_dir())
                self.assertTrue(adjusted_dir.is_dir())
                self.assertTrue(share_dir.is_dir())
            finally:
                for name, value in original_values.items():
                    if value is not None:
                        setattr(live.fetch_mod, name, value)
                        live._fetch_ns[name] = value


if __name__ == "__main__":
    unittest.main()
