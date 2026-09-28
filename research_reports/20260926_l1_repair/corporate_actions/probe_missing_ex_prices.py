"""Check whether eight unresolved ex-date price gaps were traded or suspended.

Isolated BaoStock cross-check only; does not patch the official price cache.
"""

from __future__ import annotations

import csv
import hashlib
import json
from datetime import date, timedelta
from pathlib import Path

import baostock as bs


ROOT = Path(__file__).resolve().parent
LEDGER = ROOT / "rights_event_ledger_2010_2014.csv"
FIELDS = "date,code,open,high,low,close,preclose,volume,tradestatus,isST"


def main() -> None:
    with LEDGER.open(newline="", encoding="utf-8-sig") as handle:
        missing = [
            row
            for row in csv.DictReader(handle)
            if "missing_raw_price_pair" in row["unresolved_reasons"]
        ]
    assert len(missing) == 8
    login = bs.login()
    if login.error_code != "0":
        raise RuntimeError(f"BaoStock login: {login.error_code} {login.error_msg}")
    output: list[dict[str, str]] = []
    try:
        for event in missing:
            day = date.fromisoformat(event["ex_date"])
            symbol = event["symbol"]
            code = ("sh." if symbol.startswith("6") else "sz.") + symbol
            result = bs.query_history_k_data_plus(
                code,
                FIELDS,
                start_date=(day - timedelta(days=5)).isoformat(),
                end_date=(day + timedelta(days=5)).isoformat(),
                frequency="d",
                adjustflag="3",
            )
            if result.error_code != "0":
                raise RuntimeError(f"{code} {day}: {result.error_code} {result.error_msg}")
            rows = []
            while result.next():
                values = result.get_row_data()
                rows.append(dict(zip(result.fields, values, strict=True)))
            on_date = [row for row in rows if row["date"] == event["ex_date"]]
            assert len(on_date) <= 1
            output.append(
                {
                    "symbol": symbol,
                    "ex_date": event["ex_date"],
                    "record_date": event["record_date"],
                    "ledger_previous_raw_close": event["previous_raw_close"],
                    "ledger_ex_raw_close": event["ex_date_raw_close"],
                    "bao_ex_row_count": str(len(on_date)),
                    "bao_ex_close": on_date[0]["close"] if on_date else "",
                    "bao_ex_volume": on_date[0]["volume"] if on_date else "",
                    "bao_ex_tradestatus": on_date[0]["tradestatus"] if on_date else "",
                    "bao_nearby_rows_json": json.dumps(rows, ensure_ascii=False),
                }
            )
    finally:
        bs.logout()
    target = ROOT / "missing_ex_price_baostock_probe.csv"
    with target.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=output[0].keys())
        writer.writeheader()
        writer.writerows(output)
    print("rows", len(output), "sha256", hashlib.sha256(target.read_bytes()).hexdigest())
    for row in output:
        print(row["symbol"], row["ex_date"], row["bao_ex_close"], row["bao_ex_volume"], row["bao_ex_tradestatus"])


if __name__ == "__main__":
    main()
