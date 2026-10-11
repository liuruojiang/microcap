"""Isolated issuer-verified 600778 suspended cash-ex event probe."""

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
    "distribution": ("600778_20150624_issuer_distribution.pdf", "https://static.cninfo.com.cn/finalpage/2015-06-24/1201187285.PDF", "4665a8fae757f04fce8e4bd11713d6b6b485bf7737c1aa60efc43aad0189bb81"),
    "suspension": ("600778_20150602_issuer_suspension.pdf", "https://static.cninfo.com.cn/finalpage/2015-06-02/1201089575.PDF", "44d8ca4dcb888e98e6dd2566ad9882f1e97fd0f89d29831614cfd9960f0efd34"),
    "resumption": ("600778_20151013_issuer_resumption.pdf", "https://static.cninfo.com.cn/finalpage/2015-10-13/1201685612.PDF", "1a69f55aed1a45fd3d098db9d1216545f20023c5aa2693c8348ef0e17094bae5"),
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
    for fragment in ("每股现金红利（扣税前）0.09", "2015年6月30日", "2015年7月1日", "每10股派发现金红利0.9元"):
        assert fragment in texts["distribution"], fragment
    assert "2015年6月2日起停牌" in texts["suspension"]
    assert "2015年10月13日开市起复牌" in texts["resumption"]

    login = bs.login()
    assert login.error_code == "0", login.error_msg
    try:
        response = bs.query_history_k_data_plus(
            "sh.600778", ",".join(FIELDS), start_date="2015-05-28",
            end_date="2015-10-15", frequency="d", adjustflag="3",
        )
        assert response.error_code == "0", response.error_msg
        bars = []
        while response.next():
            bars.append(dict(zip(FIELDS, response.get_row_data())))
    finally:
        bs.logout()
    by_date = {row["date"]: row for row in bars}
    traded = [row for row in bars if row["tradestatus"] == "1" and Decimal(row["volume"] or "0") > 0]
    last = max((row for row in traded if row["date"] < "2015-07-01"), key=lambda row: row["date"])
    first = min((row for row in traded if row["date"] > "2015-07-01"), key=lambda row: row["date"])
    ex = by_date["2015-07-01"]
    assert (last["date"], last["close"], last["volume"]) == ("2015-06-01", "16.1000", "21916633")
    assert (by_date["2015-06-30"]["volume"], ex["close"], ex["volume"], ex["tradestatus"]) == ("0", "16.0100", "0", "0")
    assert (first["date"], first["preclose"], first["close"]) == ("2015-10-13", "16.0100", "14.4100")
    assert Decimal(last["close"]) - Decimal("0.09") == Decimal(ex["close"])

    csv_path = HERE / "600778_baostock_unadjusted_window.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, FIELDS)
        writer.writeheader()
        writer.writerows(bars)
    records = {}
    with RIGHTS.open(encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            if row["date"] == "2015-06-30" and row["symbol"] == "600778" and row["phase"] == "record":
                shares = Decimal(row["record_shares"])
                cash = Decimal(row["cash_due"])
                assert abs(cash - shares * Decimal("0.09")) < Decimal("1e-30")
                pre = shares * Decimal("16.10")
                correct = shares * Decimal("16.01") + cash
                assert abs(correct - pre) < Decimal("1e-28")
                missing = shares * Decimal("16.01")
                double = shares * Decimal("16.01") + cash * 2
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
        "record_date": "2015-06-30", "ex_and_cash_payment_date": "2015-07-01",
        "gross_cash_yuan_per_share": "0.09",
        "vendor_query": "BaoStock sh.600778 daily adjustflag=3 (unadjusted), 2015-05-28..2015-10-15",
        "vendor_rows": len(bars), "last_actual_trade_before_ex": last,
        "ex_date_zero_volume_row": ex, "first_actual_trade_after_ex": first,
        "vendor_csv_sha256": sha(csv_path),
        "account_rights_events_sha256": sha(RIGHTS),
        "account_record_entitlements": records,
        "assessment": "16.01 = last traded 16.10 minus gross dividend 0.09 and is resumed first true trade preclose. Admit only this issuer-documented event; value suspended position at reference plus cash/receivable exactly once, never as a fill.",
        "tax_limit": "Paper account gross cash is diagnostic. Issuer reports 0.0855/share provisional payment for individuals/funds, 0.081 for QFII, and gross 0.09 for other specified categories; account tax status must be modeled separately.",
    }
    (HERE / "600778_probe.json").write_text(json.dumps(output, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(output, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
