"""Isolated, read-only source probe for two historical Shenzhen prices."""

import csv
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import baostock as bs
import requests


ROOT = Path(__file__).resolve().parent
FILES = {
    "002473_20190318_issuer_suspension.pdf": "https://disc.static.szse.cn/download/disc/disk01/finalpage/2019-03-18/654b32b2-7b01-4ef2-bfa5-cb037c5091ed.PDF",
    "300362_20210806_issuer_trading_period.pdf": "https://static.cninfo.com.cn/finalpage/2021-08-06/1210685888.PDF",
    "szse_202108_month_section.html": "https://docs.static.szse.cn/www/market/periodical/month/W020210907547542491329.html",
}
API = "https://www.szse.cn/api/market/ssjjhq/getHistoryData"


def digest(b):
    return hashlib.sha256(b).hexdigest()


def main():
    out = {"retrieved_utc": datetime.now(timezone.utc).isoformat(), "files": [], "api_attempts": []}
    for name, url in FILES.items():
        response = requests.get(url, timeout=20)
        raw = response.content
        (ROOT / name).write_bytes(raw)
        out["files"].append({"name": name, "url": url, "status": response.status_code, "bytes": len(raw), "sha256": digest(raw), "content_type": response.headers.get("Content-Type")})
    for code in ("002473", "300362"):
        params = {"cycleType": "32", "marketId": "1", "code": code}
        item = {"url": requests.Request("GET", API, params=params).prepare().url, "code": code}
        try:
            response = requests.get(API, params=params, headers={"User-Agent": "Mozilla/5.0", "Referer": "https://www.szse.cn/market/trend/index.html"}, timeout=8)
            raw = response.content
            name = f"szse_api_{code}.raw"
            (ROOT / name).write_bytes(raw)
            item.update({"status": response.status_code, "bytes": len(raw), "sha256": digest(raw), "saved": name})
        except requests.RequestException as exc:
            item.update({"error_type": type(exc).__name__, "error": str(exc)[:500]})
        out["api_attempts"].append(item)
    bs.login()
    rows = []
    for code, start, end in (("sz.002473", "2019-03-15", "2019-03-20"), ("sz.300362", "2021-08-03", "2021-08-06")):
        query = bs.query_history_k_data_plus(code, "date,code,open,high,low,close,preclose,volume,amount,tradestatus,isST", start_date=start, end_date=end, frequency="d", adjustflag="3")
        if query.error_code != "0":
            raise RuntimeError((code, query.error_code, query.error_msg))
        while query.next():
            rows.append(dict(zip(query.fields, query.get_row_data())))
    bs.logout()
    name = "baostock_diagnostic_daily.csv"
    with (ROOT / name).open("w", encoding="utf-8-sig", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=["date", "code", "open", "high", "low", "close", "preclose", "volume", "amount", "tradestatus", "isST"])
        writer.writeheader()
        writer.writerows(rows)
    raw = (ROOT / name).read_bytes()
    out["files"].append({"name": name, "source": "BaoStock anonymous query_history_k_data_plus", "rows": len(rows), "bytes": len(raw), "sha256": digest(raw), "adjustflag": "3"})
    (ROOT / "manifest.json").write_text(json.dumps(out, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
