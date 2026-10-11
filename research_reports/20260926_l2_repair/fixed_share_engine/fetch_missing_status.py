"""Fetch only absent raw-close held-symbol days for an early paper slice."""

import csv
import hashlib
import json
from pathlib import Path

import baostock as bs

from candidate import ROOT, OUT, CALENDAR, PRICE_DIR, rows


def main():
    order_path = OUT / "opening_orders.csv"
    if not order_path.exists():
        raise RuntimeError("Run candidate.py once to create opening_orders.csv")
    held = [r["symbol"] for r in rows(order_path) if r["conditional_booked"] == "true"]
    dates = [r["date"] for r in rows(CALENDAR) if "2010-01-06" <= r["date"] <= "2010-01-15"]
    gaps = []
    for symbol in held:
        present = {r["date"] for r in rows(PRICE_DIR / f"{symbol}.csv")}
        gaps.extend((symbol, date) for date in dates if date not in present)
    login = bs.login()
    if login.error_code != "0":
        raise RuntimeError(f"BaoStock login {login.error_code}: {login.error_msg}")
    fields = "date,code,close,preclose,volume,tradestatus,isST"
    output = []
    errors = []
    try:
        for symbol, date in gaps:
            code = ("sh." if symbol.startswith("6") else "sz.") + symbol
            query = bs.query_history_k_data_plus(code, fields, start_date=date, end_date=date,
                                                 frequency="d", adjustflag="3")
            if query.error_code != "0":
                errors.append({"symbol": symbol, "date": date, "error": query.error_msg})
                continue
            seen = 0
            while query.next():
                output.append({**dict(zip(fields.split(","), query.get_row_data())),
                               "symbol": symbol})
                seen += 1
            if seen != 1:
                errors.append({"symbol": symbol, "date": date, "error": f"rows={seen}"})
    finally:
        bs.logout()
    if errors:
        (OUT / "missing_status_errors.json").write_text(json.dumps(errors, ensure_ascii=False, indent=2), encoding="utf-8")
        raise RuntimeError(f"Missing status rows for {len(errors)} of {len(gaps)} requests")
    path = OUT / "missing_raw_status_20100106_15.csv"
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields.split(",") + ["symbol"])
        writer.writeheader()
        writer.writerows(sorted(output, key=lambda x: (x["date"], x["symbol"])))
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    (OUT / "missing_raw_status_manifest.json").write_text(json.dumps({
        "source": "BaoStock anonymous query_history_k_data_plus", "adjustflag": "3",
        "frequency": "d", "fields": fields, "window": [dates[0], dates[-1]],
        "opening_orders_sha256": hashlib.sha256(order_path.read_bytes()).hexdigest(),
        "requested_missing_symbol_days": len(gaps), "returned_rows": len(output),
        "sha256": digest}, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"requested": len(gaps), "rows": len(output), "sha256": digest}))


if __name__ == "__main__":
    main()
