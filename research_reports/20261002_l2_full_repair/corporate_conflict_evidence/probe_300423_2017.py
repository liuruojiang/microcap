"""Independent exact-event proof for 300423 suspended cash dividend in 2017."""

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
ISSUER = ROOT / "research_reports/20261002_l2_full_repair/data/suspension_ex_evidence/300423_20170519_issuer_dividend.pdf"
RIGHTS = ROOT / "research_reports/20261002_l2_full_repair/engine/full_history_rights_events.csv"
URL = "https://static.cninfo.com.cn/finalpage/2017-05-19/1203543718.PDF"
SHA = "aad834fa11a91d266fc32c0423546f648ed5548df15ff43c4488cee0ca8f330e"
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
    for fragment in ("每10股派0.500000元", "2017年5月25日", "2017年5月26日", "直接划入其资金账户"):
        assert fragment in issuer_text, fragment

    login = bs.login()
    assert login.error_code == "0", login.error_msg
    try:
        result = bs.query_history_k_data_plus("sz.300423", ",".join(FIELDS), start_date="2017-02-23", end_date="2017-08-02", frequency="d", adjustflag="3")
        assert result.error_code == "0", result.error_msg
        bars = []
        while result.next():
            bars.append(dict(zip(FIELDS, result.get_row_data())))
    finally:
        bs.logout()
    actual = [r for r in bars if r["tradestatus"] == "1" and Decimal(r["volume"]) > 0]
    before = max((r for r in actual if r["date"] < "2017-05-26"), key=lambda r: r["date"])
    after = min((r for r in actual if r["date"] > "2017-05-26"), key=lambda r: r["date"])
    ex = next(r for r in bars if r["date"] == "2017-05-26")
    assert (before["date"], before["close"]) == ("2017-02-24", "34.0000")
    assert (ex["close"], ex["volume"], ex["tradestatus"]) == ("33.9500", "0", "0")
    assert (after["date"], after["preclose"]) == ("2017-07-31", "33.9500")
    assert Decimal(before["close"]) - Decimal("0.05") == Decimal(ex["close"])
    csv_path = HERE / "300423_2017_baostock_unadjusted_window.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, FIELDS)
        writer.writeheader()
        writer.writerows(bars)

    accounts = {}
    with RIGHTS.open(encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            if row["date"] == "2017-05-25" and row["phase"] == "record" and row["symbol"] == "300423":
                shares = Decimal(row["record_shares"])
                cash = Decimal(row["cash_due"])
                assert abs(cash - shares * Decimal("0.05")) < Decimal("1e-30")
                prior = shares * Decimal("34.00")
                correct = shares * Decimal("33.95") + cash
                assert abs(correct - prior) < Decimal("1e-30")
                accounts[row["branch"]] = {
                    "record_shares": str(shares), "gross_cash_due_yuan": str(cash),
                    "correct_ex_mark_yuan": str(correct),
                    "missing_cash_false_return": str(-cash / prior),
                    "double_cash_false_return": str(cash / prior),
                }
    assert set(accounts) == {"prefunded", "netted"}
    output = {
        "queried_utc": datetime.now(timezone.utc).isoformat(),
        "issuer_url": URL, "issuer_pdf_sha256": SHA, "live_pdf_sha256_matched": True,
        "issuer_notice_number_note": "PDF header labels notice 2016-037 despite 2017-05-19 publication; implementation body, fiscal year, dates and ratio are consistent.",
        "candidate_source_status": "cninfo=1;sina=0; single_source_only",
        "event_scope": "300423 only; record=2017-05-25; ex=pay=2017-05-26; gross cash 0.05/share",
        "vendor_query": "BaoStock sz.300423 daily adjustflag=3, 2017-02-23..2017-08-02",
        "vendor_rows": len(bars), "last_actual_trade_before_ex": before,
        "ex_date_zero_volume_reference": ex, "first_actual_trade_after_ex": after,
        "vendor_csv_sha256": sha(csv_path), "account_rights_events_sha256": sha(RIGHTS),
        "account_record_entitlements": accounts,
        "assessment": "Use zero-volume 33.95 only for suspended valuation with 0.05/share gross cash once; never as fill. Exact issuer-documented event only.",
    }
    (HERE / "300423_2017_probe.json").write_text(json.dumps(output, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(output, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
