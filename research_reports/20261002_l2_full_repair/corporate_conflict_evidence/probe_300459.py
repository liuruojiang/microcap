"""Isolated issuer-verified 300459 suspended cash-ex event probe."""

import csv
import hashlib
import json
from datetime import datetime, timezone
from decimal import Decimal, getcontext
from pathlib import Path

import baostock as bs
from pypdf import PdfReader

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
RIGHTS = ROOT / "research_reports/20261002_l2_full_repair/engine/full_history_rights_events.csv"
SOURCES = {
    "distribution": ("300459_20150707_issuer_distribution.pdf", "https://static.cninfo.com.cn/finalpage/2015-07-07/1201247517.PDF", "26d6cac21df0023a3600a36a9f51cb272e12f0d90192764821122b750006250f"),
    "suspension": ("300459_20150708_issuer_suspension.pdf", "https://static.cninfo.com.cn/finalpage/2015-07-08/1201258846.PDF", "ccb8fe0bc09a32b2901a2b634eeec06c40a6116c72afc65794f57464c8c91e4d"),
    "resumption": ("300459_20150720_issuer_resumption.pdf", "https://static.cninfo.com.cn/finalpage/2015-07-20/1201316997.PDF", "f5c168dcdf29f0f88ca4c8901a6f3430a95cc9080581cdce71178a60730f6150"),
}
FIELDS = "date,code,open,high,low,close,preclose,volume,tradestatus,isST".split(",")


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    getcontext().prec = 64
    texts = {}
    for key, (filename, _url, digest) in SOURCES.items():
        path = HERE / filename
        assert sha(path) == digest
        texts[key] = "".join("\n".join(page.extract_text() or "" for page in PdfReader(path).pages).split())
    for fragment in ("每10股派1.000000元", "2015年7月13日", "2015年7月14日", "直接划入其资金账户"):
        assert fragment in texts["distribution"], fragment
    assert "2015年7月8日下午开市起停牌" in texts["suspension"]
    assert "2015年7月20日（星期一）开市起复牌" in texts["resumption"]

    login = bs.login()
    assert login.error_code == "0", login.error_msg
    try:
        response = bs.query_history_k_data_plus(
            "sz.300459", ",".join(FIELDS), start_date="2015-07-06",
            end_date="2015-07-22", frequency="d", adjustflag="3",
        )
        assert response.error_code == "0", response.error_msg
        bars = []
        while response.next():
            bars.append(dict(zip(FIELDS, response.get_row_data())))
    finally:
        bs.logout()
    by_date = {row["date"]: row for row in bars}
    traded = [row for row in bars if row["tradestatus"] == "1" and Decimal(row["volume"] or "0") > 0]
    last = max((row for row in traded if row["date"] < "2015-07-14"), key=lambda row: row["date"])
    first = min((row for row in traded if row["date"] > "2015-07-14"), key=lambda row: row["date"])
    ex = by_date["2015-07-14"]
    assert (last["date"], last["close"], last["volume"]) == ("2015-07-08", "33.2100", "116700")
    assert (by_date["2015-07-13"]["volume"], ex["close"], ex["volume"], ex["tradestatus"]) == ("0", "33.1100", "0", "0")
    assert (first["date"], first["preclose"], first["close"]) == ("2015-07-20", "33.1100", "36.4200")
    assert Decimal(last["close"]) - Decimal("0.10") == Decimal(ex["close"])

    csv_path = HERE / "300459_baostock_unadjusted_window.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, FIELDS)
        writer.writeheader()
        writer.writerows(bars)
    records = {}
    with RIGHTS.open(encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            if row["date"] == "2015-07-13" and row["symbol"] == "300459" and row["phase"] == "record":
                shares = Decimal(row["record_shares"])
                cash = Decimal(row["cash_due"])
                assert abs(cash - shares * Decimal("0.10")) < Decimal("1e-30")
                pre = shares * Decimal("33.21")
                correct = shares * Decimal("33.11") + cash
                assert abs(correct - pre) < Decimal("1e-28")
                missing = shares * Decimal("33.11")
                double = shares * Decimal("33.11") + cash * 2
                records[row["branch"]] = {
                    "eligible_shares": str(shares), "gross_cash_due_yuan": str(cash),
                    "pre_ex_mark_yuan": str(pre), "correct_ex_mark_yuan": str(correct),
                    "missing_cash_mark_yuan": str(missing),
                    "missing_cash_false_return": str(missing / pre - 1),
                    "double_cash_mark_yuan": str(double),
                    "double_cash_false_return": str(double / pre - 1),
                }
    assert set(records) == {"prefunded", "netted"}
    output = {
        "queried_utc": datetime.now(timezone.utc).isoformat(),
        "issuer_sources": {key: {"url": url, "sha256": digest} for key, (_, url, digest) in SOURCES.items()},
        "candidate_source_status": "cninfo=1;sina=0; single_source_only",
        "event_scope": "exact symbol, record date, ex date, cash date, gross ratio only; no blanket single-source admission",
        "record_date": "2015-07-13", "ex_and_cash_payment_date": "2015-07-14",
        "gross_cash_yuan_per_share": "0.10",
        "suspension_start": "2015-07-08 afternoon after morning trading",
        "vendor_query": "BaoStock sz.300459 daily adjustflag=3 (unadjusted), 2015-07-06..2015-07-22",
        "vendor_rows": len(bars), "last_actual_trade_before_ex": last,
        "ex_date_zero_volume_row": ex, "first_actual_trade_after_ex": first,
        "vendor_csv_sha256": sha(csv_path),
        "account_rights_events_sha256": sha(RIGHTS),
        "account_record_entitlements": records,
        "assessment": "33.11 = last partial-session traded close 33.21 minus gross dividend 0.10 and is resumed first true trade preclose. Admit only this issuer-documented event; value ex-date suspended position at reference plus cash/receivable exactly once, never as a fill.",
    }
    (HERE / "300459_probe.json").write_text(json.dumps(output, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(output, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
