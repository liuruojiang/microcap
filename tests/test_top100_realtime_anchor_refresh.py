import tempfile
import unittest
from pathlib import Path

import pandas as pd


class Top100RealtimeAnchorRefreshTest(unittest.TestCase):
    def test_intraday_history_anchor_ignores_same_day_hedge_history(self):
        import microcap_top100_mom16_biweekly_live as base

        hedge_hist = pd.DataFrame(
            [
                {"date": "2026-05-11", "close": 8866.78},
                {"date": "2026-05-12", "close": 8790.34},
            ]
        )

        latest = base.latest_closed_history_date(hedge_hist, now=pd.Timestamp("2026-05-12 14:55:00"))

        self.assertEqual(latest, pd.Timestamp("2026-05-11"))

    def test_after_close_history_anchor_can_use_same_day_hedge_history(self):
        import microcap_top100_mom16_biweekly_live as base

        hedge_hist = pd.DataFrame(
            [
                {"date": "2026-05-11", "close": 8866.78},
                {"date": "2026-05-12", "close": 8790.34},
            ]
        )

        latest = base.latest_closed_history_date(hedge_hist, now=pd.Timestamp("2026-05-12 16:05:00"))

        self.assertEqual(latest, pd.Timestamp("2026-05-12"))

    def test_realtime_query_base_context_refreshes_strategy_files_before_loading_index(self):
        import microcap_top100_mom16_biweekly_live as base

        original_refresh = base.ensure_strategy_nav_fresh
        original_load = base.load_close_df
        original_build = base.build_base_signal_context
        calls = []

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            index_csv = root / "stale_index.csv"
            panel_path = root / "panel.csv"
            index_csv.write_text("date,close\n2026-05-08,1.0\n", encoding="utf-8")
            target_end_date = pd.Timestamp("2026-05-11")
            args = type("Args", (), {"index_csv": index_csv})()
            paths = {"sentinel": root / "sentinel.csv"}

            try:
                base.ensure_strategy_nav_fresh = lambda _args, _paths, _panel, _target: calls.append("refresh")
                base.load_close_df = lambda _panel, _index: calls.append("load") or pd.DataFrame(
                    [{"microcap": 1.0, "hedge": 1.0}],
                    index=[target_end_date],
                )
                base.build_base_signal_context = (
                    lambda _args, _paths, _panel, _target, close_df: calls.append("build") or {"close_df": close_df}
                )

                result = base.ensure_realtime_query_base_context(args, paths, panel_path, target_end_date)
            finally:
                base.ensure_strategy_nav_fresh = original_refresh
                base.load_close_df = original_load
                base.build_base_signal_context = original_build

        self.assertEqual(calls, ["refresh", "load", "build"])
        self.assertEqual(pd.Timestamp(result["close_df"].index[-1]), target_end_date)


if __name__ == "__main__":
    unittest.main()
