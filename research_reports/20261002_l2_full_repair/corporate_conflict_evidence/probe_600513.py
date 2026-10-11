"""Isolated paper-release boundary for 600513 mixed rights on 2016-07-06."""

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
ISSUER = ROOT / "research_reports/20261002_l2_full_repair/data/suspension_ex_evidence/600513_20160630_issuer_mixed.pdf"
POSITIONS = ROOT / "research_reports/20261002_l2_full_repair/engine/full_history_daily_positions.csv"
ORDERS = ROOT / "research_reports/20261002_l2_full_repair/engine/settled_orders.csv"
URL = "https://static.cninfo.com.cn/finalpage/2016-06-30/1202437128.PDF"
SHA = "afe4005e2a3182d819343fb0b990504335fa6126458b5bc0561ab67d4850464a"
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
    for fragment in ("每10股派发现金红利0.6元", "每10股转增3股", "2016年7月5日", "2016年7月6日", "2016年7月7日", "直接记入股东账户"):
        assert fragment in text, fragment
    assert "股份到账日" not in text

    login = bs.login()
    assert login.error_code == "0", login.error_msg
    try:
        result = bs.query_history_k_data_plus("sh.600513", ",".join(FIELDS), start_date="2016-07-01", end_date="2016-07-11", frequency="d", adjustflag="3")
        assert result.error_code == "0", result.error_msg
        bars = []
        while result.next():
            bars.append(dict(zip(FIELDS, result.get_row_data())))
    finally:
        bs.logout()
    by_date = {row["date"]: row for row in bars}
    record, ex, release = [by_date[d] for d in ("2016-07-05", "2016-07-06", "2016-07-07")]
    assert (record["close"], record["tradestatus"]) == ("19.7700", "1")
    assert (ex["preclose"], ex["close"], ex["tradestatus"]) == ("15.1600", "15.6100", "1")
    assert (release["preclose"], release["close"], release["tradestatus"]) == ("15.6100", "15.4400", "1")
    adjusted_reference = (Decimal(record["close"]) - Decimal("0.06")) / Decimal("1.3")
    assert adjusted_reference.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP) == Decimal(ex["preclose"])
    csv_path = HERE / "600513_baostock_unadjusted_window.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, FIELDS)
        writer.writeheader()
        writer.writerows(bars)

    prior = {}
    with POSITIONS.open(encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            if row["date"] == "2016-07-04" and row["symbol"] == "600513" and row["state"] == "settled_shares":
                old = Decimal(row["shares"])
                bonus = old * Decimal("0.3")
                cash = old * Decimal("0.06")
                prior[row["branch"]] = {
                    "last_settled_shares_2016_07_04": str(old),
                    "conditional_record_shares_if_unchanged": str(old),
                    "conditional_bonus_due": str(bonus),
                    "conditional_gross_cash_due": str(cash),
                    "correct_ex_close_mark": str((old + bonus) * Decimal("15.61") + cash),
                    "omitting_pending_bonus_mark": str(old * Decimal("15.61") + cash),
                }
    assert set(prior) == {"prefunded", "netted"}
    record_orders = []
    with ORDERS.open(encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            if row["symbol"] == "600513" and row["execution_date"] == "2016-07-05":
                record_orders.append(row)
    assert not record_orders
    output = {
        "queried_utc": datetime.now(timezone.utc).isoformat(),
        "issuer_url": URL, "issuer_pdf_sha256": SHA, "live_pdf_sha256_matched": True,
        "record_date": "2016-07-05", "ex_and_cash_payment_date": "2016-07-06",
        "new_unrestricted_shares_first_listing_date": "2016-07-07",
        "actual_csdc_share_credit_date": None,
        "paper_release_date": "2016-07-07",
        "paper_release_status": "conservative first-tradable date, not proven actual CSDC credit timestamp",
        "bonus_new_shares_per_old_share": "0.3", "gross_cash_per_old_share": "0.06",
        "candidate_source_status": "two_source_match; normalized share_credit_date blank",
        "vendor_query": "BaoStock sh.600513 daily adjustflag=3, 2016-07-01..2016-07-11",
        "record_traded_bar": record, "ex_traded_bar": ex, "release_traded_bar": release,
        "theoretical_ex_reference_unrounded": str(adjusted_reference),
        "vendor_rows": len(bars), "vendor_csv_sha256": sha(csv_path),
        "stopped_account_positions_sha256": sha(POSITIONS), "stopped_account_orders_sha256": sha(ORDERS),
        "conditional_record_entitlements": prior,
        "entitlement_limit": "Engine stopped at 2016-07-04; no 2016-07-05 record line. No 600513 order recorded on 07-05. Replace with rerun record rows.",
        "valuation_counterexample": "At 07-06 true close 15.61, one old share plus pending 0.3 share plus 0.06 cash = 20.353; omitting pending bonus gives 15.67. Both differ from 07-05 true close 19.77 because 07-06 truly traded; preserve that real move.",
        "execution_boundary": "Existing shares trade on 07-06; pending new shares economically valued then but no paper sale before 07-07 listed session. Actual CSDC credit time is unknown.",
    }
    (HERE / "600513_probe.json").write_text(json.dumps(output, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(output, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
