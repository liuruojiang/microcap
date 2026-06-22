import unittest
import sys
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import microcap_top100_mom16_biweekly_live_v2_0 as v2_0

base_mod = v2_0.embedded_context.base_mod


class FakeEastmoneyResponse:
    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict[str, object]:
        return {
            "data": {
                "f43": 886568,
                "f60": 877102,
                "f86": 1782112422,
            }
        }


class RealtimeHedgeQuoteDateParsingTests(unittest.TestCase):
    @patch.object(base_mod.requests, "get")
    def test_hedge_quote_epoch_f86_is_interpreted_as_cn_trade_date(self, mock_get) -> None:
        mock_get.return_value = FakeEastmoneyResponse()

        price, source, trade_date = base_mod.fetch_hedge_realtime_quote_fast()

        self.assertEqual(price, 8865.68)
        self.assertEqual(source, "eastmoney_stock_get")
        self.assertEqual(trade_date, "2026-06-22")


if __name__ == "__main__":
    unittest.main()
