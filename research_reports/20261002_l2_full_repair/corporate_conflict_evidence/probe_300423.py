"""Independent frozen-source check for suspended mixed rights on 300423."""

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
    "distribution": ("300423_20160514_issuer_distribution.pdf", "2016-05-14/1202321214", "6aaca9ae25b55c8153cf299bea517ee1f30745445f2dad6fc0b30564e8950188"),
    "suspension": ("300423_20160322_issuer_suspension.pdf", "2016-03-22/1202063402", "e0dfbbc4f126772a550deda69ae187c50588c641b5f7222f6f31f7f5e84d88c6"),
    "resumption": ("300423_20160930_issuer_resumption.pdf", "2016-09-30/1202736676", "8d1f22b078c7b51846326d23cd64627cf16a95e9320543b7a716e208655fc155"),
}
FIELDS = "date,code,open,high,low,close,preclose,volume,tradestatus,isST".split(",")


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    getcontext().prec = 64
    source_output = {}
    texts = {}
    for kind, (name, slug, expected) in SOURCES.items():
        path = HERE / name
        assert sha(path) == expected, name
        texts[kind] = "".join("\n".join(p.extract_text() or "" for p in PdfReader(path).pages).split())
        source_output[kind] = {"url": "https://static.cninfo.com.cn/finalpage/" + slug + ".PDF", "sha256": expected}
    for fragment in ("每10股派1.000000元", "每10股转增2.000000股", "2016年5月19日", "2016年5月20日", "直接记入股东证券账户", "起始交易日为2016年5月20日"):
        assert fragment in texts["distribution"], fragment
    assert "自2016年3月21日开市起停牌" in texts["suspension"]
    assert "自2016年9月30日开市起复牌" in texts["resumption"]

    login = bs.login()
    assert login.error_code == "0", login.error_msg
    try:
        result = bs.query_history_k_data_plus("sz.300423", ",".join(FIELDS), start_date="2016-03-17", end_date="2016-10-10", frequency="d", adjustflag="3")
        assert result.error_code == "0", result.error_msg
        bars = []
        while result.next():
            bars.append(dict(zip(FIELDS, result.get_row_data())))
    finally:
        bs.logout()
    actual = [row for row in bars if row["tradestatus"] == "1" and Decimal(row["volume"]) > 0]
    before = max((row for row in actual if row["date"] < "2016-05-20"), key=lambda row: row["date"])
    after = min((row for row in actual if row["date"] > "2016-05-20"), key=lambda row: row["date"])
    ex = next(row for row in bars if row["date"] == "2016-05-20")
    assert (before["date"], before["close"]) == ("2016-03-18", "34.3600")
    assert (ex["close"], ex["volume"], ex["tradestatus"]) == ("28.5500", "0", "0")
    assert (after["date"], after["preclose"], after["close"]) == ("2016-09-30", "28.5500", "31.4100")
    assert (Decimal(before["close"]) - Decimal("0.10")) / Decimal("1.2") == Decimal(ex["close"])
    csv_path = HERE / "300423_baostock_unadjusted_window.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, FIELDS)
        writer.writeheader()
        writer.writerows(bars)

    eligible = {}
    with RIGHTS.open(encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            if row["date"] == "2016-05-19" and row["phase"] == "record" and row["symbol"] == "300423":
                old = Decimal(row["record_shares"])
                cash = Decimal(row["cash_due"])
                bonus = Decimal(row["bonus_due"])
                assert cash == old * Decimal("0.10")
                assert bonus == old * Decimal("0.2")
                prior = old * Decimal("34.36")
                correct = (old + bonus) * Decimal("28.55") + cash
                assert prior == correct
                eligible[row["branch"]] = {
                    "record_shares": str(old), "new_shares_due": str(bonus), "gross_cash_due_yuan": str(cash),
                    "correct_ex_mark_yuan": str(correct),
                    "missing_bonus_false_return": str(-bonus * Decimal("28.55") / prior),
                    "missing_cash_false_return": str(-cash / prior),
                    "double_bonus_false_return": str(bonus * Decimal("28.55") / prior),
                    "double_cash_false_return": str(cash / prior),
                }
    assert set(eligible) == {"prefunded", "netted"}
    output = {
        "queried_utc": datetime.now(timezone.utc).isoformat(),
        "issuer_sources": source_output,
        "candidate_source_status": "cninfo=1;sina=0; single_source_only",
        "event_scope": "exact symbol, 2016-05-19 record, 2016-05-20 ex/credit/cash, 2 bonus and 1 gross cash per 10 only",
        "suspension_start": "2016-03-21", "resumption_date": "2016-09-30",
        "record_date": "2016-05-19", "ex_share_credit_cash_date": "2016-05-20",
        "new_unrestricted_shares_legal_trading_start": "2016-05-20 but actual stock remains suspended until 2016-09-30",
        "vendor_query": "BaoStock sz.300423 daily adjustflag=3, 2016-03-17..2016-10-10",
        "vendor_rows": len(bars), "last_actual_trade_before_ex": before,
        "ex_date_zero_volume_reference": ex, "first_actual_trade_after_ex": after,
        "vendor_csv_sha256": sha(csv_path), "account_rights_events_sha256": sha(RIGHTS),
        "account_record_entitlements": eligible,
        "assessment": "On ex date, 28.55 is a zero-volume valuation reference, not fill. Each old share plus 0.2 new share plus 0.10 gross cash marks at 34.36, exactly preserving pre-ex value. Cash and bonus must each be booked once. Shares credited on ex date but cannot transact during suspension.",
    }
    (HERE / "300423_probe.json").write_text(json.dumps(output, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(output, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
