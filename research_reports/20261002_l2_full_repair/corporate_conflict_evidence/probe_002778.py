"""Independent exact-event proof for 002778 suspended 2018-06-15 dividend."""

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
FIELDS = "date,code,open,high,low,close,preclose,volume,tradestatus,isST".split(",")
SOURCES = {
    "implementation": ("002778_20180607_issuer_dividend.pdf", "2018-06-07/1205041201", "eb75b5d9518345cfd50222cdabe7a33b5482cac198f0cf3a18501916de8a8388"),
    "suspension": ("002778_20180423_issuer_suspension.pdf", "2018-04-23/1204678618", "0660f681c3b73573685c0ff17148a540106694a74e0db7e350fb71803d5420de"),
    "resumption": ("002778_20180720_issuer_resumption.pdf", "2018-07-20/1205207361", "9b0e8d7b2feee5c77d4e0466c2c5ee29776dad3bc83dfbe4ad6daaa15353f8ea"),
}


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    getcontext().prec = 64
    sources, texts = {}, {}
    for kind, (name, slug, expected) in SOURCES.items():
        path = HERE / name
        assert sha(path) == expected
        texts[kind] = "".join("\n".join(p.extract_text() or "" for p in PdfReader(path).pages).split())
        sources[kind] = {"url": "https://static.cninfo.com.cn/finalpage/" + slug + ".PDF", "sha256": expected}
    for fragment in ("每10股派0.700000元", "2018年6月14日", "2018年6月15日", "直接划入其资金账户"):
        assert fragment in texts["implementation"], fragment
    assert "自2018年4月23日开市时起开始停牌" in texts["suspension"]
    assert "2018年7月20日（星期五）开市起复牌" in texts["resumption"]

    login = bs.login()
    assert login.error_code == "0", login.error_msg
    try:
        result = bs.query_history_k_data_plus("sz.002778", ",".join(FIELDS), start_date="2018-04-19", end_date="2018-07-23", frequency="d", adjustflag="3")
        assert result.error_code == "0", result.error_msg
        bars = []
        while result.next():
            bars.append(dict(zip(FIELDS, result.get_row_data())))
    finally:
        bs.logout()
    actual = [r for r in bars if r["tradestatus"] == "1" and Decimal(r["volume"]) > 0]
    before = max((r for r in actual if r["date"] < "2018-06-15"), key=lambda r: r["date"])
    after = min((r for r in actual if r["date"] > "2018-06-15"), key=lambda r: r["date"])
    ex = next(r for r in bars if r["date"] == "2018-06-15")
    assert (before["date"], before["close"]) == ("2018-04-20", "19.0900")
    assert (ex["close"], ex["volume"], ex["tradestatus"]) == ("19.0200", "0", "0")
    assert (after["date"], after["preclose"], after["close"]) == ("2018-07-20", "19.0200", "19.3000")
    assert Decimal(before["close"]) - Decimal("0.07") == Decimal(ex["close"])
    csv_path = HERE / "002778_baostock_unadjusted_window.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, FIELDS)
        writer.writeheader()
        writer.writerows(bars)

    accounts = {}
    with RIGHTS.open(encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            if row["date"] == "2018-06-14" and row["phase"] == "record" and row["symbol"] == "002778":
                shares = Decimal(row["record_shares"])
                cash = Decimal(row["cash_due"])
                assert abs(cash - shares * Decimal("0.07")) < Decimal("1e-30")
                prior = shares * Decimal("19.09")
                correct = shares * Decimal("19.02") + cash
                assert abs(correct - prior) < Decimal("1e-30")
                accounts[row["branch"]] = {
                    "record_shares": str(shares), "gross_cash_due_yuan": str(cash),
                    "pre_ex_mark_yuan": str(prior), "correct_ex_mark_yuan": str(correct),
                    "missing_cash_mark_yuan": str(correct - cash),
                    "double_cash_mark_yuan": str(correct + cash),
                    "missing_cash_false_return": str(-cash / prior),
                    "double_cash_false_return": str(cash / prior),
                }
    assert set(accounts) == {"prefunded", "netted"}
    output = {
        "queried_utc": datetime.now(timezone.utc).isoformat(),
        "issuer_sources": sources,
        "candidate_source_status": "cninfo=1;sina=0; single_source_only",
        "event_scope": "002778 only; record=2018-06-14; ex=pay=2018-06-15; gross cash 0.07/share",
        "vendor_query": "BaoStock sz.002778 daily adjustflag=3, 2018-04-19..2018-07-23",
        "vendor_rows": len(bars), "last_actual_trade_before_ex": before,
        "ex_date_zero_volume_reference": ex, "first_actual_trade_after_ex": after,
        "vendor_csv_sha256": sha(csv_path), "account_rights_events_sha256": sha(RIGHTS),
        "account_record_entitlements": accounts,
        "price_precision_note": "19.09 - 0.07 = 19.02 exactly to cents; no rounding residual.",
        "assessment": "19.02 is suspended zero-volume valuation with gross cash booked once, never an executable fill. Only this exact source-backed event is admitted.",
    }
    (HERE / "002778_probe.json").write_text(json.dumps(output, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(output, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
