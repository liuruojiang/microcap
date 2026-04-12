from __future__ import annotations

import unittest

import pandas as pd

import poe_bots.microcap_top100_poe_bot as bot


class PoeBotPerformanceNaturalLanguageTests(unittest.TestCase):
    def setUp(self) -> None:
        self.now = pd.Timestamp("2026-04-12")

    def test_bare_performance_defaults_to_last_one_year(self) -> None:
        start, end, label = bot.resolve_performance_date_range("表现", now=self.now)
        self.assertEqual(start, pd.Timestamp("2025-04-12"))
        self.assertEqual(end, pd.Timestamp("2026-04-12"))
        self.assertEqual(label, "last_1_year")

    def test_this_year_to_date_phrase_is_supported(self) -> None:
        start, end, label = bot.resolve_performance_date_range("今年以来表现", now=self.now)
        self.assertEqual(start, pd.Timestamp("2026-01-01"))
        self.assertEqual(end, pd.Timestamp("2026-04-12"))
        self.assertEqual(label, "2026_ytd")

    def test_last_month_phrase_is_supported(self) -> None:
        start, end, label = bot.resolve_performance_date_range("上个月表现", now=self.now)
        self.assertEqual(start, pd.Timestamp("2026-03-01"))
        self.assertEqual(end, pd.Timestamp("2026-03-31"))
        self.assertEqual(label, "2026-03")

    def test_recent_days_phrase_is_supported(self) -> None:
        start, end, label = bot.resolve_performance_date_range("最近90天表现", now=self.now)
        self.assertEqual(start, pd.Timestamp("2026-01-12"))
        self.assertEqual(end, pd.Timestamp("2026-04-12"))
        self.assertEqual(label, "last_90_days")

    def test_net_value_trend_routes_to_performance_command(self) -> None:
        self.assertEqual(bot.normalize_command("净值走势"), bot.CMD_PERFORMANCE)


if __name__ == "__main__":
    unittest.main()
