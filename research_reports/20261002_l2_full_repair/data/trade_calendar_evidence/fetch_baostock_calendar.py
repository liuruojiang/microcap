"""Freeze independent BaoStock exchange trading dates for the isolated L2 ledger."""

import csv
import hashlib
import json
from datetime import date
from pathlib import Path

import baostock as bs


HERE = Path(__file__).resolve().parent
START = date(2010, 2, 22)
END = date(2026, 9, 30)


def main():
    login = bs.login()
    if login.error_code != "0":
        raise RuntimeError((login.error_code, login.error_msg))
    all_rows = []
    spans = []
    try:
        for year in range(START.year, END.year + 1):
            start = max(START, date(year, 1, 1)).isoformat()
            end = min(END, date(year, 12, 31)).isoformat()
            result = bs.query_trade_dates(start_date=start, end_date=end)
            if result.error_code != "0" or result.fields != ["calendar_date", "is_trading_day"]:
                raise RuntimeError((year, result.error_code, result.error_msg, result.fields))
            batch = []
            while result.next():
                values = result.get_row_data()
                if len(values) != 2 or values[1] not in {"0", "1"}:
                    raise ValueError((year, values))
                batch.append(dict(zip(result.fields, values)))
            if not batch or batch[0]["calendar_date"] != start or batch[-1]["calendar_date"] != end:
                raise ValueError((year, "incomplete response", len(batch)))
            all_rows.extend(batch)
            spans.append({"start": start, "end": end, "rows": len(batch),
                          "trading": sum(r["is_trading_day"] == "1" for r in batch)})
    finally:
        bs.logout()
    if len({r["calendar_date"] for r in all_rows}) != len(all_rows):
        raise ValueError("duplicate calendar dates")
    output = HERE / "baostock_trade_calendar_20100222_20260930.csv"
    with output.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["calendar_date", "is_trading_day"])
        writer.writeheader()
        writer.writerows(all_rows)
    digest = hashlib.sha256(output.read_bytes()).hexdigest()
    (HERE / "manifest.json").write_text(json.dumps({
        "source": "BaoStock anonymous query_trade_dates; exchange calendar",
        "date_range": [START.isoformat(), END.isoformat()], "spans": spans,
        "rows": len(all_rows), "trading_rows": sum(r["is_trading_day"] == "1" for r in all_rows),
        "sha256": digest,
    }, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"rows": len(all_rows), "trading": sum(r["is_trading_day"] == "1" for r in all_rows),
                      "sha256": digest}, ensure_ascii=False))


if __name__ == "__main__":
    main()
