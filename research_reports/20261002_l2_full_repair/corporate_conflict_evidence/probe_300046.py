"""Isolated exact-source dividend check for suspended 300046 on 2016-06-15."""

import csv
import hashlib
import json
from datetime import datetime, timezone
from decimal import Decimal, getcontext
from pathlib import Path

import baostock as bs
import requests
from pypdf import PdfReader

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
ISSUER = ROOT / "research_reports/20261002_l2_full_repair/data/suspension_ex_evidence/300046_20160606_issuer_dividend.pdf"
RIGHTS = ROOT / "research_reports/20261002_l2_full_repair/engine/full_history_rights_events.csv"
URL = "https://static.cninfo.com.cn/finalpage/2016-06-06/1202358106.PDF"
SHA = "0104460007b40cb68b31983a7cf580600cc0fb5f39e8d96b4c91e91a3a6ffb51"
FIELDS = "date,code,open,high,low,close,preclose,volume,tradestatus,isST".split(",")


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    getcontext().prec = 64
    assert sha(ISSUER) == SHA
    response = requests.get(URL, timeout=30)
    response.raise_for_status()
    assert hashlib.sha256(response.content).hexdigest() == SHA
    text = "".join("\n".join(p.extract_text() or "" for p in PdfReader(ISSUER).pages).split())
    for fragment in ("每10股派2.50元", "2016年6月14日", "2016年6月15日", "直接划入其资金账户"):
        assert fragment in text, fragment

    login = bs.login()
    assert login.error_code == "0", login.error_msg
    try:
        result = bs.query_history_k_data_plus("sz.300046", ",".join(FIELDS), start_date="2016-03-03", end_date="2016-09-23", frequency="d", adjustflag="3")
        assert result.error_code == "0", result.error_msg
        bars = []
        while result.next():
            bars.append(dict(zip(FIELDS, result.get_row_data())))
    finally:
        bs.logout()
    actual = [r for r in bars if r["tradestatus"] == "1" and Decimal(r["volume"]) > 0]
    before = max((r for r in actual if r["date"] < "2016-06-15"), key=lambda r: r["date"])
    after = min((r for r in actual if r["date"] > "2016-06-15"), key=lambda r: r["date"])
    ex = next(r for r in bars if r["date"] == "2016-06-15")
    assert (before["date"], before["close"]) == ("2016-03-04", "17.8400")
    assert (ex["close"], ex["volume"], ex["tradestatus"]) == ("17.5900", "0", "0")
    assert (after["date"], after["preclose"]) == ("2016-09-20", "17.5900")
    assert Decimal(before["close"]) - Decimal("0.25") == Decimal(ex["close"])
    csv_path = HERE / "300046_baostock_unadjusted_window.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, FIELDS)
        writer.writeheader()
        writer.writerows(bars)

    accounts = {}
    with RIGHTS.open(encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            if row["date"] == "2016-06-14" and row["phase"] == "record" and row["symbol"] == "300046":
                shares = Decimal(row["record_shares"])
                cash = Decimal(row["cash_due"])
                assert abs(cash - shares * Decimal("0.25")) < Decimal("1e-30")
                previous = shares * Decimal("17.84")
                correct = shares * Decimal("17.59") + cash
                assert abs(correct - previous) < Decimal("1e-30")
                accounts[row["branch"]] = {
                    "record_shares": str(shares), "gross_cash_due_yuan": str(cash),
                    "pre_ex_mark_yuan": str(previous), "correct_ex_mark_yuan": str(correct),
                    "missing_cash_false_return": str(-cash / previous),
                    "double_cash_false_return": str(cash / previous),
                }
    assert set(accounts) == {"prefunded", "netted"}
    output = {
        "queried_utc": datetime.now(timezone.utc).isoformat(),
        "issuer_url": URL, "issuer_pdf_sha256": SHA, "live_pdf_sha256_matched": True,
        "issuer_template_typo": "Opening paragraph mentions 2012/2013; actual 2015 implementation section, code, amount and 2016 dates are explicit.",
        "candidate_source_status": "cninfo=1;sina=0; single_source_only",
        "event_scope": "300046 only; record=2016-06-14; ex=pay=2016-06-15; gross cash 0.25/share",
        "vendor_query": "BaoStock sz.300046 daily adjustflag=3, 2016-03-03..2016-09-23",
        "vendor_rows": len(bars), "last_actual_trade_before_ex": before,
        "ex_date_zero_volume_reference": ex, "first_actual_trade_after_ex": after,
        "vendor_csv_sha256": sha(csv_path), "account_rights_events_sha256": sha(RIGHTS),
        "account_record_entitlements": accounts,
        "price_rounding_note": "17.84 - 0.25 = 17.59 exactly at cent precision; no rounding residual.",
        "assessment": "Use zero-volume 17.59 only to value suspension with 0.25/share gross cash once. It is not an executable fill. Exact event only, no blanket single-source admission.",
    }
    (HERE / "300046_probe.json").write_text(json.dumps(output, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(output, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
