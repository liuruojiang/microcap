"""Isolated mixed bonus-and-cash ex-date adversarial probe."""

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
    "distribution": ("002667_20150525_issuer_distribution.pdf", "https://static.cninfo.com.cn/finalpage/2015-05-25/1201050530.PDF", "aedeeefccb6425f994f84ebe63941f490810ebc04e9326611d914c79d3ace09c"),
    "suspension": ("002667_20150526_issuer_suspension_progress.pdf", "https://static.cninfo.com.cn/finalpage/2015-05-26/1201060652.PDF", "b39ba172bfb4df76945afc470bd75983a6f06325d00b98e1c47304c37410dd54"),
    "resumption": ("002667_20151126_issuer_resumption.pdf", "https://static.cninfo.com.cn/finalpage/2015-11-26/1201788818.PDF", "1604f22bc5eeb5032788d1cbfbf1e6d9f03a5996afe8aa6b80193b04cc9005ff"),
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
    for fragment in ("每10股派0.700000元", "每10股转增10.000000股", "2015年5月28日", "2015年5月29日", "直接记入股东证券账户", "直接划入其资金账户", "无限售流通股的起始交易日为2015年5月29日"):
        assert fragment in texts["distribution"], fragment
    assert "2015年4月7日" in texts["suspension"] and "继续停牌" in texts["suspension"]
    assert "2015年11月26日开市起复牌" in texts["resumption"]

    login = bs.login()
    assert login.error_code == "0", login.error_msg
    try:
        response = bs.query_history_k_data_plus(
            "sz.002667", ",".join(FIELDS), start_date="2015-04-01",
            end_date="2015-11-30", frequency="d", adjustflag="3",
        )
        assert response.error_code == "0", response.error_msg
        bars = []
        while response.next():
            bars.append(dict(zip(FIELDS, response.get_row_data())))
    finally:
        bs.logout()
    by_date = {row["date"]: row for row in bars}
    traded = [row for row in bars if row["tradestatus"] == "1" and Decimal(row["volume"] or "0") > 0]
    last = max((row for row in traded if row["date"] < "2015-05-29"), key=lambda row: row["date"])
    first = min((row for row in traded if row["date"] > "2015-05-29"), key=lambda row: row["date"])
    ex = by_date["2015-05-29"]
    assert (last["date"], last["close"], last["volume"]) == ("2015-04-03", "47.6500", "4627889")
    assert (by_date["2015-05-28"]["volume"], ex["close"], ex["volume"], ex["tradestatus"]) == ("0", "23.7900", "0", "0")
    assert (first["date"], first["preclose"], first["close"]) == ("2015-11-26", "23.7900", "26.1700")
    assert (Decimal(last["close"]) - Decimal("0.07")) / 2 == Decimal(ex["close"])

    csv_path = HERE / "002667_baostock_unadjusted_window.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, FIELDS)
        writer.writeheader()
        writer.writerows(bars)
    records = {}
    with RIGHTS.open(encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            if row["date"] == "2015-05-28" and row["symbol"] == "002667" and row["phase"] == "record":
                shares = Decimal(row["record_shares"])
                bonus = Decimal(row["bonus_due"])
                cash = Decimal(row["cash_due"])
                assert abs(bonus - shares) < Decimal("1e-30")
                assert abs(cash - shares * Decimal("0.07")) < Decimal("1e-30")
                old_mark = shares * Decimal("47.65")
                correct = (shares + bonus) * Decimal("23.79") + cash
                missing_bonus = shares * Decimal("23.79") + cash
                missing_cash = (shares + bonus) * Decimal("23.79")
                bonus_double = (shares + 2 * bonus) * Decimal("23.79") + cash
                cash_double = (shares + bonus) * Decimal("23.79") + 2 * cash
                assert abs(correct - old_mark) < Decimal("1e-28")
                records[row["branch"]] = {
                    "eligible_shares": str(shares), "new_shares_due": str(bonus),
                    "gross_cash_due_yuan": str(cash), "pre_ex_mark_yuan": str(old_mark),
                    "correct_ex_mark_yuan": str(correct),
                    "missing_bonus_mark_yuan": str(missing_bonus),
                    "missing_bonus_false_return": str(missing_bonus / old_mark - 1),
                    "missing_cash_mark_yuan": str(missing_cash),
                    "missing_cash_false_return": str(missing_cash / old_mark - 1),
                    "double_bonus_mark_yuan": str(bonus_double),
                    "double_bonus_false_return": str(bonus_double / old_mark - 1),
                    "double_cash_mark_yuan": str(cash_double),
                    "double_cash_false_return": str(cash_double / old_mark - 1),
                }
    assert set(records) == {"prefunded", "netted"}
    output = {
        "queried_utc": datetime.now(timezone.utc).isoformat(),
        "issuer_sources": {key: {"url": url, "sha256": digest} for key, (_, url, digest) in SOURCES.items()},
        "record_date": "2015-05-28",
        "ex_bonus_credit_and_cash_payment_date": "2015-05-29",
        "new_unrestricted_shares_legal_trade_start": "2015-05-29",
        "actual_trade_unavailable_until": "2015-11-26",
        "gross_cash_yuan_per_share": "0.07",
        "new_shares_per_old_share": "1",
        "vendor_query": "BaoStock sz.002667 daily adjustflag=3 (unadjusted), 2015-04-01..2015-11-30",
        "vendor_rows": len(bars), "last_actual_trade_before_ex": last,
        "ex_date_zero_volume_row": ex, "first_actual_trade_after_ex": first,
        "vendor_csv_sha256": sha(csv_path),
        "account_rights_events_sha256": sha(RIGHTS),
        "account_record_entitlements": records,
        "invariant": "From ex date count original plus credited bonus (or one pending entitlement) exactly once, and cash paid/receivable exactly once; compare to ex reference (old true close minus 0.07)/2. Neither ex row nor legally tradable new shares are executable during suspension.",
        "source_limit": "Paper account holds fractional shares; issuer allocates insufficient-one-share remainders by CSDC procedure, so these are diagnostic entitlements, not verified broker positions.",
    }
    (HERE / "002667_probe.json").write_text(json.dumps(output, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(output, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
