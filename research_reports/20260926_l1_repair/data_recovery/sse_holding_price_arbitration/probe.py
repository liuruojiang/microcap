"""Read-only SSE historical close arbitration for a frozen research CSV."""
from __future__ import annotations

import csv
import hashlib
import json
from decimal import Decimal
from pathlib import Path
from urllib.parse import urlencode

import requests


ROOT = Path(__file__).resolve().parents[1]
INPUT = ROOT / "price_challenge" / "possible_holding_date_differences.csv"
OUT = Path(__file__).resolve().parent
URL = "https://query.sse.com.cn/security/fund/queryNewAllQuatAbel.do"
REFERER = "https://www.sse.com.cn/assortment/stock/list/info/turnover/index.shtml?COMPANY_CODE="
CALLBACK = "jsonpCallbackProbe"


def equal_price(a: str, b: str) -> str:
    return "" if a == "" or b == "" else str(Decimal(a) == Decimal(b)).lower()


def main() -> None:
    with INPUT.open(encoding="utf-8-sig", newline="") as fh:
        input_rows = list(csv.DictReader(fh))
    session = requests.Session()
    out_rows: list[dict[str, str]] = []
    responses: list[dict] = []
    for row in input_rows:
        code, day = row["symbol"], row["date"]
        out = {
            "symbol": code,
            "date": day,
            "sina_close_raw": row["close_raw"],
            "tencent_close_raw": row["close_tencent"],
            "sse_id": "",
            "sse_closeTxDate": "",
            "sse_closePrice": "",
            "sse_closeMarketValue_10000yuan": "",
            "sse_equals_sina": "",
            "sse_equals_tencent": "",
            "status": "out_of_scope_sz" if not code.startswith("6") else "unqueried",
            "response_file": "",
            "response_sha256": "",
            "request_url": "",
            "http_status": "",
            "result_count": "",
            "exact_match_count": "",
        }
        if not code.startswith("6"):
            out_rows.append(out)
            continue
        params = {
            "jsonCallBack": CALLBACK,
            "FUNDID": code,
            "inMonth": day[:4] + day[5:7],
            "inYear": day[:4],
            "searchDate": day,
        }
        requested_url = URL + "?" + urlencode(params)
        resp = session.get(
            URL,
            params=params,
            headers={"Referer": REFERER + code, "User-Agent": "Mozilla/5.0"},
            timeout=30,
        )
        filename = f"sse_{code}_{day.replace('-', '')}.jsonp"
        raw_path = OUT / filename
        raw_path.write_bytes(resp.content)
        digest = hashlib.sha256(resp.content).hexdigest()
        out.update(
            response_file=f"sse_holding_price_arbitration/{filename}",
            response_sha256=digest,
            request_url=requested_url,
            http_status=str(resp.status_code),
        )
        response_record = {
            "symbol": code,
            "date": day,
            "request_url": requested_url,
            "http_status": resp.status_code,
            "response_file": filename,
            "response_sha256": digest,
            "response_bytes": len(resp.content),
        }
        if resp.status_code != 200:
            out["status"] = "http_error"
        else:
            content = resp.text
            prefix = CALLBACK + "("
            if not (content.startswith(prefix) and content.endswith(")")):
                out["status"] = "invalid_jsonp"
            else:
                obj = json.loads(content[len(prefix) : -1])
                result = obj.get("result") or []
                matches = [
                    item
                    for item in result
                    if str(item.get("id")) == code and item.get("closeTxDate") == day
                ]
                out["result_count"] = str(len(result))
                out["exact_match_count"] = str(len(matches))
                response_record["result_date_keys"] = [
                    {"id": item.get("id"), "closeTxDate": item.get("closeTxDate")}
                    for item in result
                ]
                if len(matches) == 0:
                    out["status"] = "no_exact_day_row"
                elif len(matches) > 1:
                    out["status"] = "duplicate_exact_day_rows"
                else:
                    item = matches[0]
                    price = str(item.get("closePrice", ""))
                    out.update(
                        sse_id=str(item.get("id", "")),
                        sse_closeTxDate=str(item.get("closeTxDate", "")),
                        sse_closePrice=price,
                        sse_closeMarketValue_10000yuan=str(item.get("closeMarketValue", "")),
                        sse_equals_sina=equal_price(price, row["close_raw"]),
                        sse_equals_tencent=equal_price(price, row["close_tencent"]),
                        status="exact_day_match",
                    )
        responses.append(response_record)
        out_rows.append(out)
    csv_path = ROOT / "SSE_POSSIBLE_HOLDING_PRICE_ARBITRATION.csv"
    with csv_path.open("w", encoding="utf-8-sig", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(out_rows[0]))
        writer.writeheader()
        writer.writerows(out_rows)
    manifest = {
        "source_csv": str(INPUT.relative_to(ROOT)),
        "source_csv_sha256": hashlib.sha256(INPUT.read_bytes()).hexdigest(),
        "sse_endpoint": URL,
        "callback": CALLBACK,
        "user_agent": "Mozilla/5.0",
        "referer_template": REFERER + "{symbol}",
        "responses": responses,
        "comparison_csv": csv_path.name,
        "comparison_csv_sha256": hashlib.sha256(csv_path.read_bytes()).hexdigest(),
    }
    (OUT / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps(out_rows, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
