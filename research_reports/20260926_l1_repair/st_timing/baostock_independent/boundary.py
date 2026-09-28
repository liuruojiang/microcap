"""Delisting and trading-calendar boundary probes in a fresh BaoStock session."""

import csv
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import baostock as bs


HERE = Path(__file__).resolve().parent
PROBES = [
    ("sz.000787", "2013-01-01", "2013-03-15"),
    ("sz.002071", "2021-04-20", "2021-06-01"),
    ("sh.600070", "2025-04-10", "2025-06-01"),
]


def write(path, fields, rows):
    with path.open("w", encoding="utf-8-sig", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(fields)
        writer.writerows(rows)
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    login = bs.login()
    if login.error_code != "0":
        raise RuntimeError(login.error_msg)
    result_summary = {"retrieved_utc": datetime.now(timezone.utc).isoformat(), "queries": []}
    try:
        for code, start, end in PROBES:
            result = bs.query_history_k_data_plus(code, "date,code,close,volume,tradestatus,isST", start_date=start, end_date=end, frequency="d", adjustflag="3")
            if result.error_code != "0":
                raise RuntimeError((code, result.error_msg))
            rows = []
            while result.next():
                rows.append(result.get_row_data())
            path = HERE / f"boundary_{code.replace('.', '_')}.csv"
            digest = write(path, result.fields, rows)
            result_summary["queries"].append({"code": code, "start": start, "end": end, "rows": len(rows), "first": rows[0][0] if rows else None, "last": rows[-1][0] if rows else None, "sha256": digest})
            print(code, len(rows), rows[0] if rows else None, rows[-1] if rows else None, flush=True)
        calendar = bs.query_trade_dates(start_date="2003-01-01", end_date="2016-03-31")
        if calendar.error_code != "0":
            raise RuntimeError(calendar.error_msg)
        rows = []
        while calendar.next():
            rows.append(calendar.get_row_data())
        digest = write(HERE / "trade_calendar.csv", calendar.fields, rows)
        result_summary["calendar"] = {"fields": calendar.fields, "rows": len(rows), "sha256": digest}
        print("calendar", len(rows), flush=True)
    finally:
        bs.logout()
    (HERE / "boundary_manifest.json").write_text(json.dumps(result_summary, ensure_ascii=False, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
