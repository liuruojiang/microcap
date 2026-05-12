import tempfile
import unittest
from pathlib import Path

import pandas as pd


class Top100RealtimeAnchorRefreshTest(unittest.TestCase):
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
