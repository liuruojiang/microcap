"""Check the first-day boundary stock against SSE's historical turnover endpoint."""

from __future__ import annotations

import hashlib
import json
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path

import requests


HERE = Path(__file__).resolve().parent
URL = "https://query.sse.com.cn/security/fund/queryNewAllQuatAbel.do"
CALLBACK = "jsonpCallbackProbe"
HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Referer": "https://www.sse.com.cn/assortment/stock/list/info/turnover/index.shtml?COMPANY_CODE=600137",
}
DATES = {"2010-01-04": "18.90", "2010-01-14": "20.05"}
SHARES = Decimal("97217588")


def load(day: str) -> tuple[dict, bytes, str]:
    path = HERE / f"sse_600137_{day.replace('-', '')}_raw.txt"
    params = {"jsonCallBack": CALLBACK, "FUNDID": "600137",
              "inMonth": "201001", "inYear": "2010", "searchDate": day}
    request = requests.Request("GET", URL, params=params).prepare()
    if path.exists():
        raw = path.read_bytes()
    else:
        response = requests.get(URL, params=params, headers=HEADERS, timeout=45)
        response.raise_for_status()
        raw = response.content
    prefix = f"{CALLBACK}(".encode()
    assert raw.startswith(prefix) and raw.rstrip().endswith(b")")
    payload = json.loads(raw[len(prefix):raw.rfind(b")")])
    assert payload.get("actionErrors") == []
    if not path.exists():
        path.write_bytes(raw)
    return payload, raw, request.url


def main() -> None:
    out = []
    for day, expected in DATES.items():
        payload, raw, request_url = load(day)
        matches = [x for x in payload["result"]
                   if x.get("id") == "600137" and x.get("closeTxDate") == day]
        assert len(matches) == 1
        row = matches[0]
        price = Decimal(str(row["closePrice"]))
        cap = SHARES * price
        assert price == Decimal(expected)
        assert Decimal(str(row["closeMarketValue"])) == (
            cap / Decimal("10000")).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        out.append({"day": day, "request_url": request_url,
                    "response_sha256": hashlib.sha256(raw).hexdigest(),
                    "response_bytes": len(raw), "price": str(price),
                    "issuer_q3_shares": str(SHARES), "calculated_cap_rmb": str(cap),
                    "sse_close_market_value_10k_rmb": row["closeMarketValue"]})
    (HERE / "sse_600137_price_manifest.json").write_text(
        json.dumps(out, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps(out, ensure_ascii=False))


if __name__ == "__main__":
    main()
