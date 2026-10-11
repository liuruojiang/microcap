"""Independent evidence for branch-divergent 2016/2017 account rights; no engine writes."""

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
SOURCE = ROOT / "research_reports/20261002_l2_full_repair/data/suspension_ex_evidence"
RIGHTS = ROOT / "research_reports/20261002_l2_full_repair/engine/full_history_rights_events.csv"
POSITIONS = ROOT / "research_reports/20261002_l2_full_repair/engine/full_history_daily_positions.csv"
ORDERS = ROOT / "research_reports/20261002_l2_full_repair/engine/settled_orders.csv"
FIELDS = "date,code,open,high,low,close,preclose,volume,tradestatus,isST".split(",")


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def issuer(name, url, expected, fragments):
    path = SOURCE / name
    assert sha(path) == expected
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    assert hashlib.sha256(response.content).hexdigest() == expected
    content = "".join("\n".join(p.extract_text() or "" for p in PdfReader(path).pages).split())
    for fragment in fragments:
        assert fragment in content, fragment
    return {"url": url, "pdf_sha256": expected, "live_pdf_sha256_matched": True}


def bars_for(code, start, end):
    login = bs.login()
    assert login.error_code == "0", login.error_msg
    try:
        result = bs.query_history_k_data_plus(code, ",".join(FIELDS), start_date=start, end_date=end, frequency="d", adjustflag="3")
        assert result.error_code == "0", result.error_msg
        bars = []
        while result.next():
            bars.append(dict(zip(FIELDS, result.get_row_data())))
    finally:
        bs.logout()
    return bars


def save_bars(name, bars):
    path = HERE / name
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, FIELDS)
        writer.writeheader()
        writer.writerows(bars)
    return sha(path)


def main():
    getcontext().prec = 64
    rights_sha = sha(RIGHTS)
    positions_sha = sha(POSITIONS)
    orders_sha = sha(ORDERS)
    queried = datetime.now(timezone.utc).isoformat()

    issuer189 = issuer(
        "600189_20160708_issuer_dividend.pdf",
        "https://static.cninfo.com.cn/finalpage/2016-07-08/1202458737.PDF",
        "87429d77b24a9652d8c2d689396dde11852c90f6d9689647021265d6f5b80143",
        ("每股现金分红税前0.050元", "2016年7月15日", "2016年7月18日", "本年度不进行公积金转增股本"),
    )
    bars189 = bars_for("sh.600189", "2016-07-06", "2017-01-06")
    by189 = {r["date"]: r for r in bars189}
    actual189 = [r for r in bars189 if r["tradestatus"] == "1" and Decimal(r["volume"]) > 0]
    before189 = max((r for r in actual189 if r["date"] < "2016-07-18"), key=lambda r: r["date"])
    after189 = min((r for r in actual189 if r["date"] > "2016-07-18"), key=lambda r: r["date"])
    ex189 = by189["2016-07-18"]
    assert (before189["date"], before189["close"]) == ("2016-07-07", "12.1000")
    assert (ex189["close"], ex189["volume"], ex189["tradestatus"]) == ("12.0500", "0", "0")
    assert (after189["date"], after189["preclose"]) == ("2017-01-04", "12.0500")
    assert Decimal(before189["close"]) - Decimal("0.05") == Decimal(ex189["close"])
    record189 = []
    with RIGHTS.open(encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            if row["branch"] == "netted" and row["date"] == "2016-07-15" and row["phase"] == "record" and row["symbol"] == "600189":
                record189.append(row)
    assert len(record189) == 1
    old189 = Decimal(record189[0]["record_shares"])
    cash189 = Decimal(record189[0]["cash_due"])
    assert abs(cash189 - old189 * Decimal("0.05")) < Decimal("1e-30")
    out189 = {
        "queried_utc": queried, "issuer": issuer189,
        "source_status": "cninfo=1;sina=0; single_source_only",
        "event_scope": "netted 600189 only; record=2016-07-15; ex=pay=2016-07-18; gross cash 0.05/share",
        "vendor_query": "BaoStock sh.600189 daily adjustflag=3, 2016-07-06..2017-01-06",
        "last_actual_trade_before_ex": before189, "ex_zero_volume_reference": ex189,
        "first_actual_trade_after_ex": after189, "vendor_rows": len(bars189),
        "vendor_csv_sha256": save_bars("600189_baostock_unadjusted_window.csv", bars189),
        "account_rights_events_sha256": rights_sha,
        "netted_record_shares": str(old189), "netted_gross_cash_due": str(cash189),
        "missing_or_duplicate_cash_false_return": str(cash189 / (old189 * Decimal("12.10"))),
        "assessment": "12.05 is a zero-volume estimate for suspended valuation with 0.05/share gross cash booked once; never an execution price. Admit exact event only.",
    }
    (HERE / "600189_probe.json").write_text(json.dumps(out189, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    issuer513 = issuer(
        "600513_20170512_issuer_mixed.pdf",
        "https://static.cninfo.com.cn/finalpage/2017-05-12/1203504732.PDF",
        "5ed5f2220804d8d71ac39061e9e73f3f4680c25b6c998122c018a777201d9338",
        ("每股现金红利0.086元", "每股转增股份0.3股", "2017/5/18", "2017/5/19", "2017/5/22", "直接计入股东账户"),
    )
    bars513 = bars_for("sh.600513", "2017-05-16", "2017-05-25")
    by513 = {r["date"]: r for r in bars513}
    record513, ex513, release513 = [by513[d] for d in ("2017-05-18", "2017-05-19", "2017-05-22")]
    assert (record513["close"], record513["tradestatus"]) == ("13.3900", "1")
    assert (ex513["preclose"], ex513["close"], ex513["tradestatus"]) == ("10.2300", "10.2900", "1")
    assert (release513["preclose"], release513["close"], release513["tradestatus"]) == ("10.2900", "9.9500", "1")
    reference513 = (Decimal("13.39") - Decimal("0.086")) / Decimal("1.3")
    assert reference513.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP) == Decimal(ex513["preclose"])
    prior513 = []
    with POSITIONS.open(encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            if row["branch"] == "prefunded" and row["date"] == "2017-05-17" and row["state"] == "settled_shares" and row["symbol"] == "600513":
                prior513.append(row)
    assert len(prior513) == 1
    old513 = Decimal(prior513[0]["shares"])
    orders513 = []
    with ORDERS.open(encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            if row["branch"] == "prefunded" and row["execution_date"] == "2017-05-18" and row["symbol"] == "600513":
                orders513.append(row)
    assert not orders513
    out513 = {
        "queried_utc": queried, "issuer": issuer513,
        "source_status": "cninfo=1;sina=1; two_source_match; normalized share_credit_date blank",
        "event_scope": "prefunded 600513; record=2017-05-18; ex=pay=2017-05-19; bonus=0.3 and gross cash=0.086 per old share",
        "new_unrestricted_shares_first_listing_and_paper_release_date": "2017-05-22",
        "actual_csdc_share_credit_date": None,
        "credit_boundary": "Issuer states direct account credit, no precise CSDC timestamp. Pending economic value starts 05-19; conservatively paper-tradable 05-22.",
        "vendor_query": "BaoStock sh.600513 daily adjustflag=3, 2017-05-16..2017-05-25",
        "record_traded_bar": record513, "ex_traded_bar": ex513,
        "release_traded_bar": release513, "theoretical_ex_reference_unrounded": str(reference513),
        "vendor_rows": len(bars513), "vendor_csv_sha256": save_bars("600513_2017_baostock_unadjusted_window.csv", bars513),
        "stopped_account_positions_sha256": positions_sha, "stopped_account_orders_sha256": orders_sha,
        "conditional_prefunded_record_shares_from_2017_05_17": str(old513),
        "conditional_bonus_due": str(old513 * Decimal("0.3")),
        "conditional_gross_cash_due": str(old513 * Decimal("0.086")),
        "entitlement_limit": "No 2017-05-18 record line in stopped account. Last settled position and no recorded 05-18 order imply conditional shares only; replace with rerun rights ledger.",
        "valuation_counterexample": "At 05-19 true close 10.29, each old share plus pending 0.3 and cash 0.086 is worth 1.3*10.29+0.086=13.463. Omitting pending share yields 10.376; preserving previous day's 13.39 would also conceal true market move.",
    }
    (HERE / "600513_2017_probe.json").write_text(json.dumps(out513, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"600189": out189, "600513_2017": out513}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
