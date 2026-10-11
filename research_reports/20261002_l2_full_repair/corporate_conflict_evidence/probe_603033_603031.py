"""Independent issuer, quote and account clock proof for divergent 2018 rights."""

import csv
import hashlib
import json
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP, getcontext
from pathlib import Path

import baostock as bs
from pypdf import PdfReader

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
RIGHTS = ROOT / "research_reports/20261002_l2_full_repair/engine/full_history_rights_events.csv"
POSITIONS = ROOT / "research_reports/20261002_l2_full_repair/engine/full_history_daily_positions.csv"
ORDERS = ROOT / "research_reports/20261002_l2_full_repair/engine/settled_orders.csv"
FIELDS = "date,code,open,high,low,close,preclose,volume,tradestatus,isST".split(",")
SOURCES = {
    "603033_implementation": ("603033_20180511_issuer_dividend.pdf", "2018-05-11/1204930567", "36cb0ce8869ac318b36a1615561eaca1982a30dbd55946f67e0f7bb498b0f4f8"),
    "603033_suspension": ("603033_20180201_issuer_suspension.pdf", "2018-02-01/1204379748", "10c3815a866ceb23016e7a3e0361ee631e68ba8c240d41e3d44f5d3a2a0971fe"),
    "603033_resumption": ("603033_20180526_issuer_resumption.pdf", "2018-05-26/1205007433", "cc6c9e19dd6499f85e08d7ed3294b9bd9eee55608550a2ec18c9f3cc5c831446"),
    "603031_implementation": ("603031_20180601_issuer_mixed.pdf", "2018-06-01/1205021532", "0705aaf7dc9595ef0ea1bb178bad26916caae0e1eb32f3b33508c787b18150d4"),
}


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def source_proof():
    source = {}
    texts = {}
    for key, (name, slug, expected) in SOURCES.items():
        path = HERE / name
        assert sha(path) == expected, name
        texts[key] = "".join("\n".join(p.extract_text() or "" for p in PdfReader(path).pages).split())
        source[key] = {"url": "https://static.cninfo.com.cn/finalpage/" + slug + ".PDF", "sha256": expected}
    for fragment in ("每股现金红利0.15元", "2018/5/18", "2018/5/21"):
        assert fragment in texts["603033_implementation"], fragment
    assert "自2018年2月1日起停牌" in texts["603033_suspension"]
    assert "于2018年5月28日起复牌" in texts["603033_resumption"]
    for fragment in ("每股现金红利0.15元", "每股转增股份0.4股", "2018/6/6", "2018/6/7", "2018/6/8", "直接计入股东账户"):
        assert fragment in texts["603031_implementation"], fragment
    assert "股份到账日" not in texts["603031_implementation"]
    return source


def query(code, start, end):
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
    source = source_proof()
    queried = datetime.now(timezone.utc).isoformat()
    rights_sha, positions_sha, orders_sha = sha(RIGHTS), sha(POSITIONS), sha(ORDERS)

    bars033 = query("sh.603033", "2018-01-30", "2018-05-30")
    actual033 = [r for r in bars033 if r["tradestatus"] == "1" and Decimal(r["volume"]) > 0]
    before033 = max((r for r in actual033 if r["date"] < "2018-05-21"), key=lambda r: r["date"])
    after033 = min((r for r in actual033 if r["date"] > "2018-05-21"), key=lambda r: r["date"])
    ex033 = next(r for r in bars033 if r["date"] == "2018-05-21")
    assert (before033["date"], before033["close"]) == ("2018-01-31", "18.8500")
    assert (ex033["close"], ex033["volume"], ex033["tradestatus"]) == ("18.7000", "0", "0")
    assert (after033["date"], after033["preclose"]) == ("2018-05-28", "18.7000")
    assert Decimal("18.85") - Decimal("0.15") == Decimal(ex033["close"])
    record033 = []
    with RIGHTS.open(encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            if row["branch"] == "netted" and row["date"] == "2018-05-18" and row["phase"] == "record" and row["symbol"] == "603033":
                record033.append(row)
    assert len(record033) == 1
    shares033 = Decimal(record033[0]["record_shares"])
    cash033 = Decimal(record033[0]["cash_due"])
    assert abs(cash033 - shares033 * Decimal("0.15")) < Decimal("1e-30")
    out033 = {
        "queried_utc": queried,
        "issuer_sources": {k: v for k, v in source.items() if k.startswith("603033")},
        "source_status": "cninfo=1;sina=0; single_source_only",
        "event_scope": "netted 603033 only; record=2018-05-18; ex=pay=2018-05-21; gross cash 0.15/share",
        "vendor_query": "BaoStock sh.603033 daily adjustflag=3, 2018-01-30..2018-05-30",
        "last_actual_trade_before_ex": before033, "ex_zero_volume_reference": ex033,
        "first_actual_trade_after_ex": after033,
        "vendor_rows": len(bars033), "vendor_csv_sha256": save_bars("603033_baostock_unadjusted_window.csv", bars033),
        "account_rights_events_sha256": rights_sha,
        "netted_record_shares": str(shares033), "netted_gross_cash_due": str(cash033),
        "missing_cash_mark_delta_yuan": str(-cash033), "double_cash_mark_delta_yuan": str(cash033),
        "missing_or_duplicate_cash_false_return": str(cash033 / (shares033 * Decimal("18.85"))),
        "assessment": "Use 18.70 only as suspended zero-volume valuation with gross cash once. No fill before 05-28 actual resumption.",
    }
    (HERE / "603033_probe.json").write_text(json.dumps(out033, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    bars031 = query("sh.603031", "2018-06-04", "2018-06-12")
    by031 = {r["date"]: r for r in bars031}
    record031, ex031, release031 = [by031[d] for d in ("2018-06-06", "2018-06-07", "2018-06-08")]
    assert (record031["close"], record031["tradestatus"]) == ("23.0000", "1")
    assert (ex031["preclose"], ex031["close"], ex031["tradestatus"]) == ("16.3200", "15.8700", "1")
    assert (release031["preclose"], release031["close"], release031["tradestatus"]) == ("15.8700", "15.3500", "1")
    reference031 = (Decimal("23.00") - Decimal("0.15")) / Decimal("1.4")
    assert reference031.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP) == Decimal(ex031["preclose"])
    prior031 = []
    with POSITIONS.open(encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            if row["branch"] == "prefunded" and row["date"] == "2018-06-05" and row["state"] == "settled_shares" and row["symbol"] == "603031":
                prior031.append(row)
    assert len(prior031) == 1
    old031 = Decimal(prior031[0]["shares"])
    orders031 = []
    with ORDERS.open(encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            if row["branch"] == "prefunded" and row["execution_date"] == "2018-06-06" and row["symbol"] == "603031":
                orders031.append(row)
    assert not orders031
    bonus031, cash031 = old031 * Decimal("0.4"), old031 * Decimal("0.15")
    out031 = {
        "queried_utc": queried, "issuer_source": source["603031_implementation"],
        "source_status": "cninfo=1;sina=0; single_source_only; normalized share_credit_date blank",
        "event_scope": "prefunded 603031; record=2018-06-06; ex=pay=2018-06-07; bonus=0.4 and gross cash=0.15 per old share",
        "new_unrestricted_shares_first_listing_and_paper_release_date": "2018-06-08",
        "actual_csdc_share_credit_date": None,
        "credit_boundary": "Issuer says CSDC directly credits shares but gives no exact credit timestamp. New rights valued pending from 06-07; paper-tradable from listed 06-08.",
        "vendor_query": "BaoStock sh.603031 daily adjustflag=3, 2018-06-04..2018-06-12",
        "record_traded_bar": record031, "ex_traded_bar": ex031, "release_traded_bar": release031,
        "theoretical_ex_reference_unrounded": str(reference031),
        "vendor_rows": len(bars031), "vendor_csv_sha256": save_bars("603031_baostock_unadjusted_window.csv", bars031),
        "stopped_account_positions_sha256": positions_sha, "stopped_account_orders_sha256": orders_sha,
        "conditional_prefunded_record_shares_from_2018_06_05": str(old031),
        "conditional_bonus_due": str(bonus031), "conditional_gross_cash_due": str(cash031),
        "entitlement_limit": "Stopped account has no 06-06 record line. Last settled shares with no recorded 06-06 order remain conditional until rerun rights record.",
        "valuation_counterexample": "At 06-07 true close 15.87, each old share plus pending 0.4 and cash 0.15 is worth 1.4*15.87+0.15=22.368. Omitting pending bonus yields 16.02; treating 06-07 new shares as sellable precedes 06-08 listing. 23.00 previous close to 22.368 includes true market move.",
    }
    (HERE / "603031_probe.json").write_text(json.dumps(out031, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"603033": out033, "603031": out031}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
