"""Isolated issuer and account-rights check for 002735 on 2016-06-03."""

import csv
import hashlib
import json
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP, getcontext
from pathlib import Path

import baostock as bs
import requests
from pypdf import PdfReader

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
ISSUER = ROOT / "research_reports/20261002_l2_full_repair/data/suspension_ex_evidence/002735_20160527_issuer_dividend.pdf"
RIGHTS = ROOT / "research_reports/20261002_l2_full_repair/engine/full_history_rights_events.csv"
URL = "https://static.cninfo.com.cn/finalpage/2016-05-27/1202340844.PDF"
SHA = "eeed3d9fecd6dd8ee69d8ad462d92fac76dd557880ceea5176a13fd8cfec951a"
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
    for fragment in ("每10股派0.82元", "2016年6月2日", "2016年6月3日", "直接划入其资金账户"):
        assert fragment in issuer_text, fragment

    login = bs.login()
    assert login.error_code == "0", login.error_msg
    try:
        result = bs.query_history_k_data_plus("sz.002735", ",".join(FIELDS), start_date="2016-03-31", end_date="2016-11-07", frequency="d", adjustflag="3")
        assert result.error_code == "0", result.error_msg
        bars = []
        while result.next():
            bars.append(dict(zip(FIELDS, result.get_row_data())))
    finally:
        bs.logout()
    actual = [row for row in bars if row["tradestatus"] == "1" and Decimal(row["volume"]) > 0]
    before = max((row for row in actual if row["date"] < "2016-06-03"), key=lambda row: row["date"])
    after = min((row for row in actual if row["date"] > "2016-06-03"), key=lambda row: row["date"])
    ex = next(row for row in bars if row["date"] == "2016-06-03")
    assert (before["date"], before["close"]) == ("2016-04-01", "41.2700")
    assert (ex["close"], ex["volume"], ex["tradestatus"]) == ("41.1900", "0", "0")
    assert (after["date"], after["preclose"]) == ("2016-11-04", "41.1900")
    assert (Decimal(before["close"]) - Decimal("0.082")).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP) == Decimal(ex["close"])
    csv_path = HERE / "002735_baostock_unadjusted_window.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, FIELDS)
        writer.writeheader()
        writer.writerows(bars)

    eligible = {}
    with RIGHTS.open(encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            if row["date"] == "2016-06-02" and row["phase"] == "record" and row["symbol"] == "002735":
                old = Decimal(row["record_shares"])
                cash = Decimal(row["cash_due"])
                assert abs(cash - old * Decimal("0.082")) < Decimal("1e-30")
                pre = old * Decimal("41.27")
                correct = old * Decimal("41.19") + cash
                eligible[row["branch"]] = {
                    "record_shares": str(old), "gross_cash_due_yuan": str(cash),
                    "pre_ex_mark_yuan": str(pre), "correct_ex_mark_yuan": str(correct),
                    "missing_cash_mark_yuan": str(correct - cash),
                    "double_cash_mark_yuan": str(correct + cash),
                    "missing_cash_false_return": str(-cash / pre),
                    "double_cash_false_return": str(cash / pre),
                }
    assert set(eligible) == {"prefunded", "netted"}
    output = {
        "queried_utc": datetime.now(timezone.utc).isoformat(),
        "issuer_url": URL, "issuer_pdf_sha256": SHA, "live_pdf_sha256_matched": True,
        "candidate_source_status": "cninfo=1;sina=0; single_source_only",
        "event_scope": "002735 only; record=2016-06-02; ex=pay=2016-06-03; gross cash 0.082/share",
        "vendor_query": "BaoStock sz.002735 daily adjustflag=3, 2016-03-31..2016-11-07",
        "vendor_rows": len(bars), "last_actual_trade_before_ex": before,
        "ex_date_zero_volume_reference": ex, "first_actual_trade_after_ex": after,
        "vendor_csv_sha256": sha(csv_path), "account_rights_events_sha256": sha(RIGHTS),
        "account_record_entitlements": eligible,
        "rounding_note": "41.27 - 0.082 = 41.188, quoted to cents as 41.19; 0.002/share mechanical rounding difference, not alpha.",
        "assessment": "Issuer original validates exact event. 41.19 suspended zero-volume quote can value the position with 0.082/share gross cash booked once; cannot fill an order. Do not admit other single-source events by analogy.",
    }
    (HERE / "002735_probe.json").write_text(json.dumps(output, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(output, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
