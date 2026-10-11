"""Fetch two February 2010 bars absent from prior frozen BaoStock window."""

import csv
import hashlib
import json

import baostock as bs

from candidate import OUT


def main():
    fields = "date,code,open,close,preclose,volume,tradestatus,isST"
    wanted = ["002066", "002074"]
    date = "2010-02-12"
    login = bs.login()
    if login.error_code != "0":
        raise RuntimeError(f"login {login.error_code} {login.error_msg}")
    records = []
    try:
        for symbol in wanted:
            result = bs.query_history_k_data_plus("sz." + symbol, fields, start_date=date,
                                                  end_date=date, frequency="d", adjustflag="3")
            if result.error_code != "0":
                raise RuntimeError(result.error_msg)
            while result.next():
                records.append({**dict(zip(fields.split(","), result.get_row_data())), "symbol": symbol})
    finally:
        bs.logout()
    if len(records) != len(wanted):
        raise RuntimeError(f"expected {len(wanted)} bars, got {len(records)}")
    path = OUT / "extra_bars_20100212.csv"
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields.split(",") + ["symbol"])
        writer.writeheader()
        writer.writerows(records)
    (OUT / "extra_bars_manifest.json").write_text(json.dumps({
        "source": "BaoStock anonymous query_history_k_data_plus", "date": date,
        "symbols": wanted, "fields": fields, "adjustflag": "3", "frequency": "d",
        "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(records, ensure_ascii=False))


if __name__ == "__main__":
    main()
