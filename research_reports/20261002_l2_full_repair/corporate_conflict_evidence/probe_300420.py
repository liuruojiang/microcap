"""Isolated issuer-verified 300420 single-source suspended cash-ex probe."""

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
    "distribution": ("300420_20150512_issuer_distribution.pdf", "https://static.cninfo.com.cn/finalpage/2015-05-12/1200999076.PDF", "8bd77e924c04cea16d9ac2cb85e36719cd0b455e5fc67e4930e607b80e22080a"),
    "suspension": ("300420_20150512_issuer_suspension.pdf", "https://static.cninfo.com.cn/finalpage/2015-05-12/1200999077.PDF", "1d086ce181ca3a1f07654d30117773c7c0221cdb8466f4c70e81e25636e39368"),
    "resumption": ("300420_20150811_issuer_resumption.pdf", "https://static.cninfo.com.cn/finalpage/2015-08-11/1201418415.PDF", "dbd91cf09b20c29824fa91903ba56fa123f1e9209420d5f5a4c7fbc0bf637175"),
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
    for fragment in ("每10股派1.200000元", "2015年5月18日", "2015年5月19日", "直接划入其资金账户"):
        assert fragment in texts["distribution"], fragment
    assert "自2015年5月12日开市起停牌" in texts["suspension"]
    assert "2015年8月11日开市起复牌" in texts["resumption"]

    login = bs.login()
    assert login.error_code == "0", login.error_msg
    try:
        response = bs.query_history_k_data_plus(
            "sz.300420", ",".join(FIELDS), start_date="2015-05-06",
            end_date="2015-08-13", frequency="d", adjustflag="3",
        )
        assert response.error_code == "0", response.error_msg
        bars = []
        while response.next():
            bars.append(dict(zip(FIELDS, response.get_row_data())))
    finally:
        bs.logout()
    by_date = {row["date"]: row for row in bars}
    traded = [row for row in bars if row["tradestatus"] == "1" and Decimal(row["volume"] or "0") > 0]
    last = max((row for row in traded if row["date"] < "2015-05-19"), key=lambda row: row["date"])
    first = min((row for row in traded if row["date"] > "2015-05-19"), key=lambda row: row["date"])
    ex = by_date["2015-05-19"]
    assert (last["date"], last["close"], last["volume"]) == ("2015-05-11", "38.4000", "4151603")
    assert (by_date["2015-05-18"]["volume"], ex["close"], ex["volume"], ex["tradestatus"]) == ("0", "38.2800", "0", "0")
    assert (first["date"], first["preclose"], first["close"]) == ("2015-08-11", "38.2800", "42.1100")
    assert Decimal(last["close"]) - Decimal("0.12") == Decimal(ex["close"])

    csv_path = HERE / "300420_baostock_unadjusted_window.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, FIELDS)
        writer.writeheader()
        writer.writerows(bars)
    records = {}
    with RIGHTS.open(encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            if row["date"] == "2015-05-18" and row["symbol"] == "300420" and row["phase"] == "record":
                shares = Decimal(row["record_shares"])
                gross_cash = Decimal(row["cash_due"])
                assert abs(gross_cash - shares * Decimal("0.12")) < Decimal("1e-30")
                records[row["branch"]] = {"eligible_shares": str(shares), "gross_cash_due_yuan": str(gross_cash)}
    assert set(records) == {"prefunded", "netted"}
    output = {
        "queried_utc": datetime.now(timezone.utc).isoformat(),
        "issuer_sources": {key: {"url": url, "sha256": digest} for key, (_, url, digest) in SOURCES.items()},
        "candidate_source_status": "cninfo=1;sina=0; single_source_only",
        "event_scope": "exact symbol, record date, ex date, gross ratio only; no blanket single-source admission",
        "record_date": "2015-05-18",
        "ex_and_cash_credit_date": "2015-05-19",
        "gross_cash_yuan_per_share": "0.12",
        "vendor_query": "BaoStock sz.300420 daily adjustflag=3 (unadjusted), 2015-05-06..2015-08-13",
        "vendor_rows": len(bars),
        "last_actual_trade_before_ex": last,
        "ex_date_zero_volume_row": ex,
        "first_actual_trade_after_ex": first,
        "vendor_csv_sha256": sha(csv_path),
        "account_rights_events_sha256": sha(RIGHTS),
        "account_record_entitlements": records,
        "assessment": "38.28 = last traded 38.40 minus gross dividend 0.12 and is resumed first true trade preclose. Eligible only as an issuer-documented single-event exception and zero-volume valuation reference with cash/receivable; never an executable close or generic source admission.",
    }
    (HERE / "300420_probe.json").write_text(json.dumps(output, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(output, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
