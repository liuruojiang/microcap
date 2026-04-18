from __future__ import annotations

import unittest
from unittest.mock import patch

from poe_bots import microcap_top100_poe_bot as bot


class PoeBotIndexHistorySourceTests(unittest.TestCase):
    def test_index_history_prefers_tencent_before_eastmoney(self) -> None:
        requested_urls: list[str] = []

        def fake_fetch_json(url: str, headers=None, timeout=None, retries=None):
            requested_urls.append(url)
            if "ifzq.gtimg.cn" in url:
                return {
                    "data": {
                        "sh000852": {
                            "day": [
                                ["2026-04-16", "8232.98", "8232.98"],
                                ["2026-04-17", "8307.44", "8307.44"],
                            ]
                        }
                    }
                }
            raise AssertionError(f"unexpected source requested: {url}")

        with patch.object(bot, "read_json_cache", return_value=None):
            with patch.object(bot, "write_json_cache", return_value=None):
                with patch.object(bot, "fetch_json", side_effect=fake_fetch_json):
                    frame = bot.eastmoney_index_history(bot.HEDGE_SECID, 35, force_refresh=True)

        self.assertEqual(len(frame), 2)
        self.assertEqual(str(frame.iloc[-1]["date"].date()), "2026-04-17")
        self.assertEqual(float(frame.iloc[-1]["close"]), 8307.44)
        self.assertEqual(len(requested_urls), 1)
        self.assertIn("ifzq.gtimg.cn", requested_urls[0])

    def test_index_history_falls_back_to_sina_after_tencent_failure(self) -> None:
        requested_urls: list[str] = []

        def fake_fetch_json(url: str, headers=None, timeout=None, retries=None):
            requested_urls.append(url)
            if "ifzq.gtimg.cn" in url:
                raise RuntimeError("tencent unavailable")
            if "money.finance.sina.com.cn" in url:
                return [
                    {"day": "2026-04-16", "close": "8232.983"},
                    {"day": "2026-04-17", "close": "8307.436"},
                ]
            raise AssertionError(f"unexpected source requested: {url}")

        with patch.object(bot, "read_json_cache", return_value=None):
            with patch.object(bot, "write_json_cache", return_value=None):
                with patch.object(bot, "fetch_json", side_effect=fake_fetch_json):
                    frame = bot.eastmoney_index_history(bot.HEDGE_SECID, 35, force_refresh=True)

        self.assertEqual(len(frame), 2)
        self.assertEqual(str(frame.iloc[-1]["date"].date()), "2026-04-17")
        self.assertAlmostEqual(float(frame.iloc[-1]["close"]), 8307.436, places=6)
        self.assertEqual(len(requested_urls), 2)
        self.assertIn("ifzq.gtimg.cn", requested_urls[0])
        self.assertIn("money.finance.sina.com.cn", requested_urls[1])


if __name__ == "__main__":
    unittest.main()
