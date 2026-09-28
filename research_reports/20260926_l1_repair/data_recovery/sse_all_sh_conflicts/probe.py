"""Sequential official SSE quote arbitration for frozen Shanghai price conflicts."""
from __future__ import annotations

import csv
import hashlib
import json
from decimal import Decimal
from pathlib import Path
from urllib.parse import urlencode

import requests


ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "price_challenge" / "differences_with_possible_member_window.csv"
OUT = Path(__file__).resolve().parent
CSV_OUT = ROOT / "SSE_ALL_SH_PRICE_CONFLICTS.csv"
BASE = "https://query.sse.com.cn/security/fund/queryNewAllQuatAbel.do"
REFERER = "https://www.sse.com.cn/assortment/stock/list/info/turnover/index.shtml?COMPANY_CODE="
CALLBACK = "jsonpCallbackProbe"
LAST_PAGE_DATE = "2022-01-07"


def read_jsonp(raw: bytes) -> dict:
    s = raw.decode("utf-8-sig")
    prefix = CALLBACK + "("
    if not (s.startswith(prefix) and s.endswith(")")):
        raise ValueError("response is not expected JSONP callback")
    return json.loads(s[len(prefix) : -1])


def match_label(sse: str, sina: str, tencent: str) -> str:
    a = sina != "" and Decimal(sse) == Decimal(sina)
    b = tencent != "" and Decimal(sse) == Decimal(tencent)
    if a and b:
        return "both"
    if a:
        return "sina_only"
    if b:
        return "tencent_only"
    return "neither"


def main() -> None:
    with SOURCE.open(encoding="utf-8-sig", newline="") as fh:
        rows = list(csv.DictReader(fh))
    sh = sorted((x for x in rows if x["symbol"].startswith("6")), key=lambda x: (x["date"], x["symbol"]))
    session = requests.Session()
    output: list[dict[str, str]] = []
    manifest: list[dict] = []
    for index, original in enumerate(sh, 1):
        code, day = original["symbol"], original["date"]
        row = {
            "date": day,
            "symbol": code,
            "sina_close_raw": original["close_raw"],
            "tencent_close_raw": original["close_tencent"],
            "original_merge": original["_merge"],
            "is_rebalance_date": original["is_rebalance_date"],
            "last_signal_date": original["last_signal_date"],
            "under_old_rank100_at_last_signal": original["under_old_rank100_at_last_signal"],
            "possible_holding_period_price_effect": original["possible_holding_period_price_effect"],
            "sse_id": "",
            "sse_closeTxDate": "",
            "sse_closePrice": "",
            "sse_closeMarketValue_10000yuan": "",
            "sse_match": "",
            "status": "outside_page_supported_date_range" if day > LAST_PAGE_DATE else "unqueried",
            "http_status": "",
            "result_count": "",
            "exact_match_count": "",
            "request_url": "",
            "response_file": "",
            "response_sha256": "",
            "response_bytes": "",
        }
        if day > LAST_PAGE_DATE:
            output.append(row)
            continue
        params = {
            "jsonCallBack": CALLBACK,
            "FUNDID": code,
            "inMonth": day[:4] + day[5:7],
            "inYear": day[:4],
            "searchDate": day,
        }
        request_url = BASE + "?" + urlencode(params)
        row["request_url"] = request_url
        name = f"sse_{code}_{day.replace('-', '')}.jsonp"
        raw_file = OUT / name
        try:
            resp = session.get(
                BASE,
                params=params,
                headers={"Referer": REFERER + code, "User-Agent": "Mozilla/5.0"},
                timeout=30,
            )
            raw_file.write_bytes(resp.content)
            digest = hashlib.sha256(resp.content).hexdigest()
            row.update(
                http_status=str(resp.status_code),
                response_file=f"sse_all_sh_conflicts/{name}",
                response_sha256=digest,
                response_bytes=str(len(resp.content)),
            )
            if resp.status_code != 200:
                row["status"] = "http_error"
            else:
                data = read_jsonp(resp.content)
                result = data.get("result") or []
                exact = [x for x in result if str(x.get("id")) == code and x.get("closeTxDate") == day]
                row["result_count"] = str(len(result))
                row["exact_match_count"] = str(len(exact))
                if not exact:
                    row["status"] = "no_exact_day_row"
                elif len(exact) > 1:
                    row["status"] = "duplicate_exact_day_rows"
                else:
                    item = exact[0]
                    price = str(item.get("closePrice", ""))
                    row.update(
                        sse_id=str(item.get("id", "")),
                        sse_closeTxDate=str(item.get("closeTxDate", "")),
                        sse_closePrice=price,
                        sse_closeMarketValue_10000yuan=str(item.get("closeMarketValue", "")),
                        sse_match=match_label(price, row["sina_close_raw"], row["tencent_close_raw"]),
                        status="exact_day_match",
                    )
        except (requests.RequestException, UnicodeError, ValueError, json.JSONDecodeError) as exc:
            row["status"] = "request_or_parse_error"
            row["error"] = f"{type(exc).__name__}: {exc}"
        output.append(row)
        manifest.append({
            "date": day,
            "symbol": code,
            "request_url": request_url,
            "status": row["status"],
            "http_status": row["http_status"],
            "response_file": row["response_file"],
            "response_sha256": row["response_sha256"],
            "response_bytes": row["response_bytes"],
            "result_count": row["result_count"],
            "exact_match_count": row["exact_match_count"],
        })
        if index % 20 == 0:
            print(f"processed {index}/{len(sh)}", flush=True)
    fieldnames = [k for k in output[0] if k != "error"]
    with CSV_OUT.open("w", encoding="utf-8-sig", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(output)
    meta = {
        "source_csv": str(SOURCE.relative_to(ROOT)),
        "source_csv_sha256": hashlib.sha256(SOURCE.read_bytes()).hexdigest(),
        "last_page_supported_date": LAST_PAGE_DATE,
        "endpoint": BASE,
        "callback": CALLBACK,
        "referer_template": REFERER + "{symbol}",
        "user_agent": "Mozilla/5.0",
        "comparison_csv": CSV_OUT.name,
        "comparison_csv_sha256": hashlib.sha256(CSV_OUT.read_bytes()).hexdigest(),
        "responses": manifest,
    }
    (OUT / "manifest.json").write_text(json.dumps(meta, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(json.dumps({
        "rows": len(output),
        "statuses": {s: sum(x["status"] == s for x in output) for s in sorted({x["status"] for x in output})},
        "matches": {s: sum(x["sse_match"] == s for x in output) for s in sorted({x["sse_match"] for x in output}) if s},
        "errors": [x for x in output if x.get("error")],
    }, ensure_ascii=False, indent=2), flush=True)


if __name__ == "__main__":
    main()
