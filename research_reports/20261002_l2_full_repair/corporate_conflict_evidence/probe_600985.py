"""Isolated issuer-credit-date boundary probe; never infer actual CSDC credit date."""

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
ISSUER = HERE / "600985_20150703_issuer_distribution.pdf"
ISSUER_URL = "https://static.cninfo.com.cn/finalpage/2015-07-03/1201230308.PDF"
ISSUER_SHA = "8dd86ad3ca9344131809e1a38c1e0dd181b9426c0448a8de4d50f08980946f7c"
POSITIONS = ROOT / "research_reports/20261002_l2_full_repair/engine/full_history_daily_positions.csv"
ORDERS = ROOT / "research_reports/20261002_l2_full_repair/engine/settled_orders.csv"
FIELDS = "date,code,open,high,low,close,preclose,volume,tradestatus,isST".split(",")


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    getcontext().prec = 64
    assert sha(ISSUER) == ISSUER_SHA
    text = "".join("\n".join(page.extract_text() or "" for page in PdfReader(ISSUER).pages).split())
    for fragment in ("2015年7月9日", "2015年7月10日", "2015年7月13日", "每10股派发现金红利1.7元", "每10股转增5股", "将所分派股份直接记入股东账户"):
        assert fragment in text, fragment
    assert "股份到账日" not in text

    login = bs.login()
    assert login.error_code == "0", login.error_msg
    try:
        response = bs.query_history_k_data_plus(
            "sh.600985", ",".join(FIELDS), start_date="2015-07-08",
            end_date="2015-07-15", frequency="d", adjustflag="3",
        )
        assert response.error_code == "0", response.error_msg
        bars = []
        while response.next():
            bars.append(dict(zip(FIELDS, response.get_row_data())))
    finally:
        bs.logout()
    by_date = {row["date"]: row for row in bars}
    record, ex, first_listed = (by_date[date] for date in ("2015-07-09", "2015-07-10", "2015-07-13"))
    assert (record["close"], record["volume"], record["tradestatus"]) == ("14.7600", "5766024", "1")
    assert (ex["preclose"], ex["close"], ex["volume"], ex["tradestatus"]) == ("9.7300", "10.7000", "2950679", "1")
    assert (first_listed["preclose"], first_listed["close"], first_listed["volume"], first_listed["tradestatus"]) == ("10.7000", "11.7700", "6839410", "1")
    assert (Decimal(record["close"]) - Decimal("0.17")) / Decimal("1.5") > Decimal("9.72")
    assert ((Decimal(record["close"]) - Decimal("0.17")) / Decimal("1.5")).quantize(Decimal("0.01")) == Decimal(ex["preclose"])

    csv_path = HERE / "600985_baostock_unadjusted_window.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, FIELDS)
        writer.writeheader()
        writer.writerows(bars)

    prior = {}
    with POSITIONS.open(encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            if row["date"] == "2015-07-08" and row["symbol"] == "600985" and row["state"] == "settled_shares":
                s = Decimal(row["shares"])
                prior[row["branch"]] = {
                    "last_settled_shares_2015_07_08": str(s),
                    "conditional_2015_07_09_bonus_due_0p5": str(s * Decimal("0.5")),
                    "conditional_2015_07_09_gross_cash_due": str(s * Decimal("0.17")),
                }
    assert set(prior) == {"prefunded", "netted"}
    orders_on_record = []
    with ORDERS.open(encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            if row["symbol"] == "600985" and row["execution_date"] == "2015-07-09":
                orders_on_record.append(row)
    assert not orders_on_record

    output = {
        "queried_utc": datetime.now(timezone.utc).isoformat(),
        "issuer_url": ISSUER_URL, "issuer_pdf_sha256": ISSUER_SHA,
        "record_date": "2015-07-09", "ex_and_cash_payment_date": "2015-07-10",
        "new_unrestricted_shares_first_listing_date": "2015-07-13",
        "actual_share_credit_date": None,
        "share_credit_date_status": "not stated in issuer implementation notice; cannot infer from listing date",
        "bonus_new_shares_per_old_share": "0.5", "gross_cash_per_old_share": "0.17",
        "vendor_query": "BaoStock sh.600985 daily adjustflag=3 (unadjusted), 2015-07-08..2015-07-15",
        "vendor_rows": len(bars), "record_date_traded_bar": record,
        "ex_date_traded_bar": ex, "first_listing_date_traded_bar": first_listed,
        "vendor_csv_sha256": sha(csv_path),
        "stopped_account_prior_positions_sha256": sha(POSITIONS),
        "stopped_account_settled_orders_sha256": sha(ORDERS),
        "provisional_record_entitlements_from_last_settled_no_record_day_orders": prior,
        "entitlement_limit": "No 2015-07-09 record event exists in the stopped account output yet; shares above are conditional on no other record-day state mutation and must be replaced with rerun ledger record shares.",
        "valuation_counterexample": "At 2015-07-10 true traded close 10.70, per old share equity is 1.5*10.70+0.17=16.22 vs 14.76 at record close; omitting pending 0.5 share would value 10.87 instead of 16.22. The 07-10 trading move is real; 9.73 is adjusted preclose, not actual close.",
        "execution_boundary": "Existing shares can trade on 07-10; newly entitled shares must not be sold before 07-13 listed session. Pending economic right must be marked on 07-10, and actual CSDC credit timestamp remains unknown.",
    }
    (HERE / "600985_probe.json").write_text(json.dumps(output, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(output, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
