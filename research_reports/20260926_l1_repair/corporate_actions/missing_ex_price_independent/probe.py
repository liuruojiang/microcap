"""Independent read-only suspension challenge for ledger's eight no-price ex dates."""

import csv
import hashlib
import json
from datetime import date, timedelta, datetime, timezone
from pathlib import Path

import baostock as bs
import requests


BASE = Path(__file__).resolve().parent
LEDGER = BASE.parent / "rights_event_ledger_2010_2014.csv"
PDFS = {
    "600211_20130905_retrospective.pdf": "https://static.cninfo.com.cn/finalpage/2013-09-05/63049912.PDF",
    "600758_20140606_issuer_status.pdf": "https://static.cninfo.com.cn/finalpage/2014-06-06/64106790.PDF",
}


def sha(b):
    return hashlib.sha256(b).hexdigest()


def main():
    events = [r for r in csv.DictReader(LEDGER.open(encoding="utf-8-sig", newline="")) if "missing_raw_price_pair" in r["unresolved_reasons"]]
    assert len(events) == 8
    evidence = {"retrieved_utc": datetime.now(timezone.utc).isoformat(), "ledger_path": str(LEDGER), "events": [], "files": []}
    for filename, url in PDFS.items():
        response = requests.get(url, timeout=20)
        response.raise_for_status()
        raw = response.content
        (BASE / filename).write_bytes(raw)
        evidence["files"].append({"name": filename, "url": url, "status": response.status_code, "bytes": len(raw), "sha256": sha(raw)})
    result = bs.login()
    if result.error_code != "0":
        raise RuntimeError((result.error_code, result.error_msg))
    rows = []
    fields = "date,code,open,high,low,close,volume,amount,tradestatus"
    for event in events:
        symbol = event["symbol"]
        code = ("sh." if symbol.startswith("6") else "sz.") + symbol
        day = date.fromisoformat(event["ex_date"])
        query = bs.query_history_k_data_plus(code, fields, start_date=(day-timedelta(days=2)).isoformat(), end_date=(day+timedelta(days=2)).isoformat(), frequency="d", adjustflag="3")
        if query.error_code != "0":
            raise RuntimeError((code, query.error_code, query.error_msg))
        item = {"symbol": symbol, "ex_date": event["ex_date"], "rows": 0, "target_row": None}
        while query.next():
            row = dict(zip(query.fields, query.get_row_data()))
            rows.append(row)
            item["rows"] += 1
            if row["date"] == event["ex_date"]:
                item["target_row"] = row
        evidence["events"].append(item)
    bs.logout()
    assert all(x["target_row"] for x in evidence["events"])
    name = "baostock_eight_exdate_window.csv"
    with (BASE / name).open("w", encoding="utf-8-sig", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields.split(","))
        writer.writeheader()
        writer.writerows(rows)
    raw = (BASE / name).read_bytes()
    evidence["files"].append({"name": name, "source": "BaoStock anonymous history_k_data_plus", "rows": len(rows), "sha256": sha(raw), "adjustflag": "3"})
    (BASE / "manifest.json").write_text(json.dumps(evidence, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
