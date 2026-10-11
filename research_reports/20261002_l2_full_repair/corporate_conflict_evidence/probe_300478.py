"""Independent exact-event proof for 300478 suspended cash dividend in 2017."""

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
ISSUER = ROOT / "research_reports/20261002_l2_full_repair/data/suspension_ex_evidence/300478_20170601_issuer_dividend.pdf"
RIGHTS = ROOT / "research_reports/20261002_l2_full_repair/engine/full_history_rights_events.csv"
URL = "https://static.cninfo.com.cn/finalpage/2017-06-01/1203577004.PDF"
SHA = "6ce5cfc87620e740b380f09b2ccaccdd3ca6b4b8ab3a90e4229d382a861dcc5b"
FIELDS = "date,code,open,high,low,close,preclose,volume,tradestatus,isST".split(",")


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    getcontext().prec = 64
    assert sha(ISSUER) == SHA
    response = requests.get(URL, timeout=30)
    response.raise_for_status()
    assert hashlib.sha256(response.content).hexdigest() == SHA
    issuer_text = "".join("\n".join(p.extract_text() or "" for p in PdfReader(ISSUER).pages).split())
    for fragment in ("每10股派1.5元", "2017年6月6日", "2017年6月7日", "直接划入其资金账户"):
        assert fragment in issuer_text, fragment

    login = bs.login()
    assert login.error_code == "0", login.error_msg
    try:
        result = bs.query_history_k_data_plus("sz.300478", ",".join(FIELDS), start_date="2017-03-16", end_date="2017-08-09", frequency="d", adjustflag="3")
        assert result.error_code == "0", result.error_msg
        bars = []
        while result.next():
            bars.append(dict(zip(FIELDS, result.get_row_data())))
    finally:
        bs.logout()
    actual = [r for r in bars if r["tradestatus"] == "1" and Decimal(r["volume"]) > 0]
    before = max((r for r in actual if r["date"] < "2017-06-07"), key=lambda r: r["date"])
    after = min((r for r in actual if r["date"] > "2017-06-07"), key=lambda r: r["date"])
    ex = next(r for r in bars if r["date"] == "2017-06-07")
    assert (before["date"], before["close"]) == ("2017-03-17", "55.7500")
    assert (ex["close"], ex["volume"], ex["tradestatus"]) == ("55.6000", "0", "0")
    assert (after["date"], after["preclose"]) == ("2017-08-07", "55.6000")
    assert Decimal(before["close"]) - Decimal("0.15") == Decimal(ex["close"])
    csv_path = HERE / "300478_baostock_unadjusted_window.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, FIELDS)
        writer.writeheader()
        writer.writerows(bars)

    accounts = {}
    with RIGHTS.open(encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            if row["date"] == "2017-06-06" and row["phase"] == "record" and row["symbol"] == "300478":
                shares = Decimal(row["record_shares"])
                cash = Decimal(row["cash_due"])
                assert abs(cash - shares * Decimal("0.15")) < Decimal("1e-30")
                prior = shares * Decimal("55.75")
                correct = shares * Decimal("55.60") + cash
                assert abs(correct - prior) < Decimal("1e-30")
                accounts[row["branch"]] = {
                    "record_shares": str(shares), "gross_cash_due_yuan": str(cash),
                    "correct_ex_mark_yuan": str(correct),
                    "missing_cash_mark_yuan": str(correct - cash),
                    "double_cash_mark_yuan": str(correct + cash),
                    "missing_cash_false_return": str(-cash / prior),
                    "double_cash_false_return": str(cash / prior),
                }
    assert set(accounts) == {"prefunded", "netted"}
    output = {
        "queried_utc": datetime.now(timezone.utc).isoformat(),
        "issuer_url": URL, "issuer_pdf_sha256": SHA, "live_pdf_sha256_matched": True,
        "candidate_source_status": "cninfo=1;sina=0; single_source_only",
        "event_scope": "300478 only; record=2017-06-06; ex=pay=2017-06-07; gross cash 0.15/share",
        "vendor_query": "BaoStock sz.300478 daily adjustflag=3, 2017-03-16..2017-08-09",
        "vendor_rows": len(bars), "last_actual_trade_before_ex": before,
        "ex_date_zero_volume_reference": ex, "first_actual_trade_after_ex": after,
        "vendor_csv_sha256": sha(csv_path), "account_rights_events_sha256": sha(RIGHTS),
        "account_record_entitlements": accounts,
        "assessment": "55.60 is zero-volume suspended valuation only, paired with 0.15/share gross cash exactly once; not a fill. Exact event only.",
    }
    (HERE / "300478_probe.json").write_text(json.dumps(output, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(output, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
