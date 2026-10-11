"""Read-only independent 002624 suspension/dividend counterexample."""

from __future__ import annotations

import csv
import hashlib
import json
from decimal import Decimal as D, getcontext
from pathlib import Path

import baostock as bs


getcontext().prec = 50
HERE = Path(__file__).resolve().parent
ENGINE = HERE.parent / "engine"
FIELDS = ["date", "preclose", "close", "volume", "tradestatus"]


def run() -> None:
    login = bs.login()
    if login.error_code != "0":
        raise RuntimeError(login.error_msg)
    try:
        query = bs.query_history_k_data_plus(
            "sz.002624",
            ",".join(FIELDS),
            start_date="2014-04-01",
            end_date="2014-09-03",
            frequency="d",
            adjustflag="3",
        )
        if query.error_code != "0":
            raise RuntimeError(query.error_msg)
        rows = []
        while query.next():
            rows.append(dict(zip(FIELDS, query.get_row_data())))
    finally:
        bs.logout()

    raw_path = HERE / "002624_baostock_raw.csv"
    with raw_path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(rows)

    by_date = {r["date"]: r for r in rows}
    last_real = max(
        (r for r in rows if r["date"] < "2014-05-23" and r["tradestatus"] == "1" and D(r["volume"]) > 0),
        key=lambda r: r["date"],
    )
    next_real = min(
        (r for r in rows if r["date"] >= "2014-05-23" and r["tradestatus"] == "1" and D(r["volume"]) > 0),
        key=lambda r: r["date"],
    )
    record = by_date["2014-05-22"]
    ex = by_date["2014-05-23"]
    assert last_real["date"] == "2014-04-04" and D(last_real["close"]) == D("8.10")
    assert next_real["date"] == "2014-09-01" and D(next_real["preclose"]) == D("8.05")
    assert record["tradestatus"] == ex["tradestatus"] == "0"
    assert D(record["volume"]) == D(ex["volume"]) == 0
    assert D(record["close"]) == D("8.10") and D(ex["close"]) == D("8.05")

    rights_path = ENGINE / "full_history_rights_events.csv"
    with rights_path.open(encoding="utf-8-sig", newline="") as stream:
        eligible = [
            r for r in csv.DictReader(stream)
            if r["branch"] == "netted" and r["date"] == "2014-05-22"
            and r["phase"] == "record" and r["symbol"] == "002624"
        ]
    assert len(eligible) == 1
    q = D(eligible[0]["record_shares"])
    due = D(eligible[0]["cash_due"])
    assert abs(due - q * D("0.05")) < D("1e-20")
    prior = q * D("8.10")
    theoretical = q * D("8.05") + due
    assert abs(theoretical - prior) < D("1e-20")
    doubled = q * D("8.10") + due
    omitted = q * D("8.05")
    assert abs((doubled - prior) - due) < D("1e-20")
    assert abs((prior - omitted) - due) < D("1e-20")

    result = {
        "status": "PASS_theoretical_mark_only",
        "raw_sha256": hashlib.sha256(raw_path.read_bytes()).hexdigest(),
        "rights_sha256_at_read": hashlib.sha256(rights_path.read_bytes()).hexdigest(),
        "bao_rows": len(rows),
        "last_real": last_real,
        "record": record,
        "ex": ex,
        "next_real": next_real,
        "record_shares": str(q),
        "gross_cash_due": str(due),
        "prior_stock_value": str(prior),
        "correct_paper_value": str(theoretical),
        "double_count_injection_value": str(doubled),
        "double_count_excess": str(doubled - prior),
        "omit_cash_injection_value": str(omitted),
        "omit_cash_shortfall": str(prior - omitted),
    }
    (HERE / "002624_cash_mark_audit.json").write_text(
        json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    run()
