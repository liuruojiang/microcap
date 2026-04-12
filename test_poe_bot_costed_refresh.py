from __future__ import annotations

import base64
import shutil
import unittest
import zlib
from pathlib import Path
from unittest.mock import patch

import pandas as pd

import poe_bots.microcap_top100_poe_bot as bot


class PoeBotCostedRefreshTests(unittest.TestCase):
    def setUp(self) -> None:
        self.work_dir = Path(__file__).resolve().parent / "_tmp_poe_bot_costed_refresh"
        shutil.rmtree(self.work_dir, ignore_errors=True)
        self.work_dir.mkdir(parents=True)

    def tearDown(self) -> None:
        shutil.rmtree(self.work_dir, ignore_errors=True)

    def test_rebuild_costed_nav_from_proxy_turnover_writes_latest_date(self) -> None:
        turnover_path = self.work_dir / "proxy_turnover.csv"
        costed_path = self.work_dir / "costed_nav.csv"
        close_df = pd.DataFrame(
            {
                "microcap": [1.0, 1.01],
                "hedge": [1.0, 0.99],
            },
            index=pd.to_datetime(["2026-04-09", "2026-04-10"]),
        )
        gross = pd.DataFrame(
            {
                "return": [0.0, 0.02],
                "holding": ["cash", "long_microcap_short_zz1000"],
                "next_holding": ["long_microcap_short_zz1000", "long_microcap_short_zz1000"],
            },
            index=pd.to_datetime(["2026-04-09", "2026-04-10"]),
        )

        pd.DataFrame(
            {
                "rebalance_date": ["2026-04-10"],
                "turnover_frac_one_side": [0.2],
            }
        ).to_csv(turnover_path, index=False, encoding="utf-8")

        with patch.object(bot, "run_backtest", return_value=gross):
            bot.rebuild_costed_nav_from_proxy_turnover(close_df, turnover_path, costed_path)

        saved = pd.read_csv(costed_path)
        self.assertEqual(saved["date"].iloc[-1], "2026-04-10")
        self.assertIn("nav_net", saved.columns)

    def test_try_extend_costed_nav_without_turnover_extends_non_rebalance_tail(self) -> None:
        costed_path = self.work_dir / "costed_nav.csv"
        close_df = pd.DataFrame(
            {
                "microcap": [1.0, 1.01],
                "hedge": [1.0, 0.99],
            },
            index=pd.to_datetime(["2026-04-09", "2026-04-10"]),
        )
        gross = pd.DataFrame(
            {
                "return": [0.01, 0.02],
                "holding": ["long_microcap_short_zz1000", "long_microcap_short_zz1000"],
                "next_holding": ["long_microcap_short_zz1000", "long_microcap_short_zz1000"],
            },
            index=pd.to_datetime(["2026-04-09", "2026-04-10"]),
        )

        pd.DataFrame(
            {
                "date": ["2026-04-09"],
                "holding": ["long_microcap_short_zz1000"],
                "next_holding": ["long_microcap_short_zz1000"],
                "return": [0.01],
                "entry_exit_cost": [0.0],
                "rebalance_cost": [0.0],
                "total_cost": [0.0],
                "return_net": [0.01],
                "nav_net": [1.01],
            }
        ).to_csv(costed_path, index=False, encoding="utf-8")

        with patch.object(bot, "run_backtest", return_value=gross):
            ok = bot.try_extend_costed_nav_without_turnover(
                close_df,
                costed_path,
                pd.Timestamp("2026-04-10"),
            )

        self.assertTrue(ok)
        saved = pd.read_csv(costed_path)
        self.assertEqual(saved["date"].iloc[-1], "2026-04-10")
        self.assertAlmostEqual(float(saved["nav_net"].iloc[-1]), 1.0302, places=10)

    def test_load_performance_source_skips_costed_when_turnover_history_is_missing(self) -> None:
        costed_path = self.work_dir / "costed_nav.csv"
        gross_path = self.work_dir / "live_nav.csv"
        turnover_path = self.work_dir / "proxy_turnover.csv"

        pd.DataFrame(
            {
                "date": ["2026-04-09", "2026-04-10"],
                "return_net": [0.01, -0.02],
                "nav_net": [1.01, 0.9898],
            }
        ).to_csv(costed_path, index=False, encoding="utf-8")
        pd.DataFrame(
            {
                "date": ["2026-04-09", "2026-04-10"],
                "return": [0.01, 0.02],
                "nav": [1.01, 1.0302],
            }
        ).to_csv(gross_path, index=False, encoding="utf-8")

        strategy = {
            "performance_costed_nav_csv": costed_path,
            "performance_live_nav_csv": gross_path,
            "performance_proxy_turnover_csv": turnover_path,
            "embedded_performance_b64": "",
        }

        with patch.object(bot, "load_embedded_performance_source", return_value=None):
            with patch.object(bot, "get_strategy", return_value=strategy):
                perf_df, ret_col, nav_col, source_label, source_path = bot.load_performance_source()

        self.assertEqual(source_label, "gross")
        self.assertEqual(ret_col, "return")
        self.assertEqual(nav_col, "nav")
        self.assertEqual(Path(source_path), gross_path)
        self.assertAlmostEqual(float(perf_df.iloc[-1]["nav"]), 1.0302, places=10)

    def test_load_performance_source_prefers_local_files_over_stale_embedded_payload(self) -> None:
        gross_path = self.work_dir / "live_nav.csv"
        turnover_path = self.work_dir / "proxy_turnover.csv"

        pd.DataFrame(
            {
                "date": ["2026-04-09", "2026-04-10"],
                "return": [0.0, 0.0],
                "nav": [1.0, 1.0],
            }
        ).to_csv(gross_path, index=False, encoding="utf-8")

        embedded_df = pd.DataFrame(
            {
                "date": ["2026-04-09", "2026-04-10"],
                "return_net": [0.0, -0.02],
                "nav_net": [1.0, 0.98],
            }
        )
        embedded_b64 = base64.b64encode(
            zlib.compress(embedded_df.to_csv(index=False).encode("utf-8"))
        ).decode("ascii")

        strategy = {
            "performance_costed_nav_csv": self.work_dir / "missing_costed_nav.csv",
            "performance_live_nav_csv": gross_path,
            "performance_proxy_turnover_csv": turnover_path,
            "embedded_performance_b64": embedded_b64,
        }

        with patch.object(bot, "get_strategy", return_value=strategy):
            perf_df, ret_col, nav_col, source_label, source_path = bot.load_performance_source()

        self.assertEqual(source_label, "gross")
        self.assertEqual(ret_col, "return")
        self.assertEqual(nav_col, "nav")
        self.assertEqual(Path(source_path), gross_path)
        self.assertAlmostEqual(float(perf_df.iloc[-1]["nav"]), 1.0, places=10)


if __name__ == "__main__":
    unittest.main()
