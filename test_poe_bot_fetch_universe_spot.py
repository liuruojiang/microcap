from __future__ import annotations

import unittest
from urllib.parse import parse_qs, urlparse
from unittest.mock import patch

from poe_bots import microcap_top100_poe_bot as bot


def build_page_payload(page: int, total: int, page_size: int = 100) -> dict[str, object]:
    rows = []
    start = (page - 1) * page_size
    end = min(start + page_size, total)
    for idx in range(start, end):
        rows.append(
            {
                "f12": f"{idx + 1:06d}",
                "f14": f"Stock{idx + 1}",
                "f2": 10.0 + idx,
                "f17": 9.5 + idx,
                "f18": 9.0 + idx,
                "f20": 100000000.0 + idx,
            }
        )
    return {"data": {"total": total, "diff": rows}}


class PoeBotFetchUniverseSpotTests(unittest.TestCase):
    def test_fetch_eastmoney_universe_rows_fetches_only_required_pages(self) -> None:
        requested_pages: list[int] = []

        def fake_fetch_json(url: str, headers=None, timeout=None, retries=None):
            query = parse_qs(urlparse(url).query)
            page = int(query["pn"][0])
            requested_pages.append(page)
            return build_page_payload(page, total=1000)

        with patch.object(bot, "fetch_json", side_effect=fake_fetch_json):
            rows = bot.fetch_eastmoney_universe_rows(target_count=250)

        self.assertEqual(requested_pages, [1, 2, 3])
        self.assertEqual(len(rows), 300)
        self.assertEqual(rows[0]["code"], "000001")
        self.assertEqual(rows[-1]["code"], "000300")

    def test_fetch_eastmoney_universe_rows_skips_invalid_first_page(self) -> None:
        requested_pages: list[int] = []

        def fake_fetch_json(url: str, headers=None, timeout=None, retries=None):
            query = parse_qs(urlparse(url).query)
            page = int(query["pn"][0])
            requested_pages.append(page)
            if page == 1:
                return {
                    "data": {
                        "total": 300,
                        "diff": [
                            {
                                "f12": "000001",
                                "f14": "Bad",
                                "f2": "-",
                                "f17": "-",
                                "f18": 10.0,
                                "f20": "-",
                            }
                        ]
                        * 100,
                    }
                }
            return build_page_payload(page, total=300)

        with patch.object(bot, "fetch_json", side_effect=fake_fetch_json):
            rows = bot.fetch_eastmoney_universe_rows(target_count=150)

        self.assertEqual(requested_pages, [1, 2, 3])
        self.assertEqual(len(rows), 200)
        self.assertEqual(rows[0]["code"], "000101")


if __name__ == "__main__":
    unittest.main()
