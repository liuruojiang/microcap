"""Isolated split-on-suspension adversarial probe; no engine mutation."""

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
    "distribution": ("300319_20140910_issuer_distribution.pdf", "https://static.cninfo.com.cn/finalpage/2014-09-10/1200220201.PDF", "965d399e1d7c53952a5a4d060e8a0cdc597960a66bbf00650ee2a39218823d6c"),
    "suspension": ("300319_20140725_issuer_suspension.pdf", "https://static.cninfo.com.cn/finalpage/2014-07-25/1200078636.PDF", "5174f302b251a3147d5c207d89b366a50abaf69e4f7442b026c78d00a1fc7c48"),
    "resumption": ("300319_20150105_issuer_resumption.pdf", "https://static.cninfo.com.cn/finalpage/2015-01-05/1200511737.PDF", "71ad81837155b7e86c15b92b27e85480e049acfe17a88943d1a3eadf2f2aca0d"),
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
    for fragment in ("每10股转增15股", "2014年9月17日", "2014年9月18日", "直接记入股东证券账户", "起始交易日为2014年9月18日"):
        assert fragment in texts["distribution"], fragment
    assert "2014年7月21日上午开市起停牌" in texts["suspension"]
    assert "2015年1月5日开市起复牌" in texts["resumption"]

    login = bs.login()
    assert login.error_code == "0", login.error_msg
    try:
        response = bs.query_history_k_data_plus(
            "sz.300319", ",".join(FIELDS), start_date="2014-07-14",
            end_date="2015-01-06", frequency="d", adjustflag="3",
        )
        assert response.error_code == "0", response.error_msg
        bars = []
        while response.next():
            bars.append(dict(zip(FIELDS, response.get_row_data())))
    finally:
        bs.logout()
    by_date = {row["date"]: row for row in bars}
    traded = [row for row in bars if row["tradestatus"] == "1" and Decimal(row["volume"] or "0") > 0]
    last = max((row for row in traded if row["date"] < "2014-09-18"), key=lambda row: row["date"])
    first = min((row for row in traded if row["date"] > "2014-09-18"), key=lambda row: row["date"])
    ex = by_date["2014-09-18"]
    assert (last["date"], last["close"], last["volume"]) == ("2014-07-18", "26.5900", "2020252")
    assert (ex["close"], ex["volume"], ex["tradestatus"]) == ("10.6400", "0", "0")
    assert (first["date"], first["preclose"], first["close"]) == ("2015-01-05", "10.6400", "11.7000")
    assert (Decimal(last["close"]) / Decimal("2.5")).quantize(Decimal("0.01")) == Decimal(ex["close"])

    csv_path = HERE / "300319_baostock_unadjusted_window.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, FIELDS)
        writer.writeheader()
        writer.writerows(bars)

    records = {}
    with RIGHTS.open(encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            if row["date"] == "2014-09-17" and row["symbol"] == "300319" and row["phase"] == "record":
                shares = Decimal(row["record_shares"])
                bonus = Decimal(row["bonus_due"])
                assert abs(bonus - shares * Decimal("1.5")) < Decimal("1e-30")
                pre_value = shares * Decimal("26.59")
                correct_ex_value = (shares + bonus) * Decimal("10.64")
                price_only_value = shares * Decimal("10.64")
                early_bonus_value = (shares + bonus) * Decimal("26.59")
                records[row["branch"]] = {
                    "eligible_shares_at_record": str(shares),
                    "bonus_due_1p5_per_share": str(bonus),
                    "total_shares_from_ex_date": str(shares + bonus),
                    "pre_split_mark_yuan": str(pre_value),
                    "correct_ex_mark_with_all_shares_yuan": str(correct_ex_value),
                    "price_only_missing_bonus_mark_yuan": str(price_only_value),
                    "early_bonus_unadjusted_price_mark_yuan": str(early_bonus_value),
                    "price_only_false_return": str(price_only_value / pre_value - 1),
                    "early_bonus_false_return": str(early_bonus_value / pre_value - 1),
                    "rounded_reference_mark_delta_yuan": str(correct_ex_value - pre_value),
                }
    assert set(records) == {"prefunded", "netted"}
    output = {
        "queried_utc": datetime.now(timezone.utc).isoformat(),
        "issuer_sources": {key: {"url": url, "sha256": digest} for key, (_, url, digest) in SOURCES.items()},
        "record_date": "2014-09-17",
        "ex_and_share_credit_date": "2014-09-18",
        "new_unrestricted_shares_legal_trade_start": "2014-09-18",
        "actual_trade_unavailable_until": "2015-01-05",
        "vendor_query": "BaoStock sz.300319 daily adjustflag=3 (unadjusted), 2014-07-14..2015-01-06",
        "vendor_rows": len(bars),
        "last_actual_trade_before_ex": last,
        "ex_date_zero_volume_row": ex,
        "first_actual_trade_after_ex": first,
        "vendor_csv_sha256": sha(csv_path),
        "account_rights_events_sha256": sha(RIGHTS),
        "account_record_entitlements": records,
        "invariant": "On ex date, 2.5 total shares per eligible share must be valued at adjusted ex reference; newly credited shares cannot fill an order while trading remains suspended. If held as pending entitlement, include its value exactly once.",
        "source_limit": "BaoStock is vendor data and 10.64 is a zero-volume reference, not an executable close; the 0.004/share price-rounding uplift after the 2.5 split is reference rounding, not earned trading return.",
    }
    (HERE / "300319_probe.json").write_text(json.dumps(output, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(output, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
