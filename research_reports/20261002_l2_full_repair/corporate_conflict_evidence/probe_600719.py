"""Isolated 600719 share-right and first-tradable-date check; no engine writes."""

import csv
import hashlib
import json
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import baostock as bs
import requests
from pypdf import PdfReader

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
ISSUER = ROOT / "research_reports/20261002_l2_full_repair/data/suspension_ex_evidence/600719_20151124_issuer_bonus.pdf"
URL = "https://static.cninfo.com.cn/finalpage/2015-11-24/1201785739.PDF"
SHA = "91c32510cbabeb5c1a599c4b900d6c817da1b1503a83f1e40b2e786f7ee75248"
POSITIONS = ROOT / "research_reports/20261002_l2_full_repair/engine/full_history_daily_positions.csv"
ORDERS = ROOT / "research_reports/20261002_l2_full_repair/engine/settled_orders.csv"
FIELDS = "date,code,open,high,low,close,preclose,volume,tradestatus,isST".split(",")


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    assert sha(ISSUER) == SHA
    response = requests.get(URL, timeout=30)
    response.raise_for_status()
    assert hashlib.sha256(response.content).hexdigest() == SHA
    pdf_text = "".join("\n".join(page.extract_text() or "" for page in PdfReader(ISSUER).pages).split())
    for fragment in (
        "每10股转增10股", "2015年11月27日", "2015年11月30日",
        "2015年12月1日", "将所转增股份直接记入公司股东账户",
    ):
        assert fragment in pdf_text, fragment
    assert "股份到账日" not in pdf_text

    login = bs.login()
    assert login.error_code == "0", login.error_msg
    try:
        result = bs.query_history_k_data_plus(
            "sh.600719", ",".join(FIELDS), start_date="2015-11-24",
            end_date="2015-12-04", frequency="d", adjustflag="3",
        )
        assert result.error_code == "0", result.error_msg
        bars = []
        while result.next():
            bars.append(dict(zip(FIELDS, result.get_row_data())))
    finally:
        bs.logout()
    by_date = {row["date"]: row for row in bars}
    record, ex, release = [by_date[date] for date in ("2015-11-27", "2015-11-30", "2015-12-01")]
    assert (record["close"], record["tradestatus"]) == ("15.2200", "1")
    assert (ex["preclose"], ex["close"], ex["tradestatus"]) == ("7.6100", "7.6400", "1")
    assert (release["preclose"], release["close"], release["tradestatus"]) == ("7.6400", "7.9900", "1")
    assert Decimal(record["close"]) / 2 == Decimal(ex["preclose"])
    csv_path = HERE / "600719_baostock_unadjusted_window.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, FIELDS)
        writer.writeheader()
        writer.writerows(bars)

    prior = {}
    with POSITIONS.open(encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            if row["date"] == "2015-11-26" and row["symbol"] == "600719" and row["state"] == "settled_shares":
                s = Decimal(row["shares"])
                prior[row["branch"]] = {
                    "last_settled_shares_2015_11_26": str(s),
                    "conditional_record_shares_if_unchanged": str(s),
                    "conditional_bonus_due": str(s),
                    "correct_2015_11_30_ex_close_mark": str(s * 2 * Decimal("7.64")),
                    "omitting_pending_bonus_mark": str(s * Decimal("7.64")),
                }
    assert set(prior) == {"prefunded", "netted"}
    record_orders = []
    with ORDERS.open(encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            if row["symbol"] == "600719" and row["execution_date"] == "2015-11-27":
                record_orders.append(row)
    assert not record_orders

    output = {
        "queried_utc": datetime.now(timezone.utc).isoformat(),
        "issuer_url": URL, "issuer_pdf_sha256": SHA, "live_pdf_sha256_matched": True,
        "record_date": "2015-11-27", "ex_date": "2015-11-30",
        "new_unrestricted_shares_first_listing_date": "2015-12-01",
        "actual_csdc_share_credit_date": None,
        "paper_release_date": "2015-12-01",
        "paper_release_status": "conservative first-tradable date; not a claim about actual CSDC credit timestamp",
        "bonus_new_shares_per_old_share": "1", "gross_cash_per_old_share": "0",
        "candidate_source_status": "two_source_match; normalized share_credit_date blank",
        "vendor_query": "BaoStock sh.600719 daily adjustflag=3, 2015-11-24..2015-12-04",
        "record_traded_bar": record, "ex_traded_bar": ex, "release_traded_bar": release,
        "vendor_rows": len(bars), "vendor_csv_sha256": sha(csv_path),
        "stopped_account_positions_sha256": sha(POSITIONS),
        "stopped_account_orders_sha256": sha(ORDERS),
        "conditional_record_entitlements": prior,
        "entitlement_limit": "Engine stopped at 2015-11-26; no 2015-11-27 record line. No recorded 600719 execution order on record day. Replace conditional shares with rerun ledger rights record.",
        "valuation_counterexample": "2015-11-30 true traded close 7.64: one old share plus one pending new share = 15.28; omitting pending new share yields 7.64 (false -50% mark versus correct ex-date mark). Counting new share twice yields 22.92 (false +50%).",
        "execution_boundary": "Original share is traded on ex date. Pending new share earns value on ex date but is paper-tradable only from the issuer-stated 2015-12-01 listing date; actual CSDC credit time unknown.",
    }
    (HERE / "600719_probe.json").write_text(json.dumps(output, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(output, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
