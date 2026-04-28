from __future__ import annotations

import unittest
import base64
import gzip
import json
from urllib.parse import parse_qs, urlparse
from unittest.mock import patch

import pandas as pd

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

    def test_select_candidate_pool_falls_back_to_freshest_snapshot_date(self) -> None:
        rows = []
        for idx in range(150):
            rows.append(
                {
                    "code": f"{idx + 1:06d}",
                    "name": f"Name{idx + 1}",
                    "latest_date": "2026-04-27",
                    "latest_price": 10.0,
                    "market_cap": 1000.0 + idx,
                }
            )
        for idx in range(150, 700):
            rows.append(
                {
                    "code": f"{idx + 1:06d}",
                    "name": f"Name{idx + 1}",
                    "latest_date": "2026-04-10",
                    "latest_price": 10.0,
                    "market_cap": 1.0 + idx,
                }
            )
        universe = pd.DataFrame(rows)

        with patch.object(bot, "load_current_st_codes", return_value=set()):
            selected = bot.select_candidate_pool(universe, 600, min_latest_date="2026-04-28")

        self.assertEqual(len(selected), 150)
        self.assertEqual(set(selected["latest_date"]), {"2026-04-27"})

    def test_fetch_universe_spot_uses_akshare_fallback_when_primary_empty(self) -> None:
        rows = []
        for idx in range(700):
            rows.append(
                {
                    "代码": f"{idx + 1:06d}",
                    "名称": f"Name{idx + 1}",
                    "最新价": 10.0,
                    "总市值": 1000.0 + idx,
                    "昨收": 9.9,
                    "今开": 10.1,
                }
            )
        fake_ak = type("FakeAk", (), {"stock_zh_a_spot_em": staticmethod(lambda: pd.DataFrame(rows))})

        with patch.object(bot, "read_json_cache", return_value=None):
            with patch.object(bot, "write_json_cache"):
                with patch.object(bot, "fetch_eastmoney_universe_rows", return_value=[]):
                    with patch.object(bot, "get_akshare", return_value=fake_ak):
                        with patch.object(bot, "load_current_st_codes", return_value=set()):
                            universe = bot.fetch_universe_spot(cache_seconds=0)

        self.assertEqual(len(universe), 700)
        self.assertEqual(universe.iloc[0]["code"], "000001")
        self.assertEqual(float(universe.iloc[0]["latest_price"]), 10.0)

    def test_fetch_universe_spot_uses_embedded_snapshot_when_network_empty(self) -> None:
        rows = [
            {
                "code": f"{idx + 1:06d}",
                "name": f"Name{idx + 1}",
                "latest_price": 10.0,
                "market_cap": 1000.0 + idx,
                "prev_close": 9.9,
                "open_price": 10.1,
            }
            for idx in range(700)
        ]
        blob = base64.b64encode(gzip.compress(json.dumps(rows).encode("utf-8"))).decode("ascii")

        with patch.object(bot, "read_json_cache", return_value=None):
            with patch.object(bot, "write_json_cache"):
                with patch.object(bot, "fetch_eastmoney_universe_rows", return_value=[]):
                    with patch.object(bot, "fetch_akshare_universe_rows", return_value=[]):
                        with patch.object(bot, "EMBEDDED_UNIVERSE_B64", blob):
                            with patch.object(bot, "load_current_st_codes", return_value=set()):
                                universe = bot.fetch_universe_spot(cache_seconds=0)

        self.assertEqual(len(universe), 700)
        self.assertEqual(universe.iloc[-1]["code"], "000700")


if __name__ == "__main__":
    unittest.main()
