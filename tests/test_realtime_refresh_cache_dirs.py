from __future__ import annotations

import tempfile
import unittest
from types import SimpleNamespace
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

    def test_state_only_context_uses_existing_panel_without_refreshing_shadow(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            panel_shadow = root / "panel_shadow.csv"
            index_csv = root / "proxy_index.csv"
            costed_nav_csv = root / "costed_nav.csv"
            for path in (panel_shadow, index_csv, costed_nav_csv):
                path.write_text("date,close\n2026-05-25,1.0\n", encoding="utf-8")

            original_build_shadow = live.base_mod.build_refreshed_panel_shadow
            original_build_context = live.base_mod.build_realtime_context_from_cached_proxy
            try:
                def fail_refresh(*args, **kwargs):
                    raise AssertionError("state-only mode must not refresh panel shadow")

                def fake_context(args, base_paths, panel_path, target_end_date, reason):
                    return {"close_df": pd.DataFrame(index=[pd.Timestamp("2026-05-25")])}

                live.base_mod.build_refreshed_panel_shadow = fail_refresh
                live.base_mod.build_realtime_context_from_cached_proxy = fake_context

                panel_path, target_end_date, context = live._cached_realtime_context_from_existing_state(
                    SimpleNamespace(index_csv=index_csv, costed_nav_csv=costed_nav_csv),
                    {"panel_shadow": panel_shadow},
                    "production state-only mode avoids implicit cache rebuilds",
                    refresh_panel=False,
                )

                self.assertEqual(panel_path, panel_shadow)
                self.assertEqual(pd.Timestamp(target_end_date).date().isoformat(), "2026-05-25")
                self.assertEqual(context["close_df"].index[-1].date().isoformat(), "2026-05-25")
            finally:
                live.base_mod.build_refreshed_panel_shadow = original_build_shadow
                live.base_mod.build_realtime_context_from_cached_proxy = original_build_context


if __name__ == "__main__":
    unittest.main()
