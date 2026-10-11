"""Isolated issuer-verified single-source suspended cash-ex event probe."""

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
    "distribution": ("300141_20150429_issuer_distribution.pdf", "https://static.cninfo.com.cn/finalpage/2015-04-29/1200933106.PDF", "319387b47ec4ba41ee70ad868fc783190f9b0352b5d26f042ec8db2051b50533"),
    "suspension": ("300141_20150506_issuer_suspension.pdf", "https://static.cninfo.com.cn/finalpage/2015-05-06/1200967113.PDF", "78b77d2c9ba05cb7c8ae3a81a39c2f16bac2b343bdf90c0f9dbb8df7436e555d"),
    "resumption": ("300141_20150520_issuer_resumption.pdf", "https://static.cninfo.com.cn/finalpage/2015-05-20/1201036277.PDF", "c8e5c7c17f3dbc3139e3a6893a4626756694ed618d14feb069e9e30a7289ae22"),
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
    for fragment in ("每10股派1.000000元", "2015年05月06日", "2015年05月07日", "直接划入其资金账户"):
        assert fragment in texts["distribution"], fragment
    assert "公司股票继续停牌" in texts["suspension"]
    assert "2015年5月20日开市起复牌" in texts["resumption"]

    login = bs.login()
    assert login.error_code == "0", login.error_msg
    try:
        response = bs.query_history_k_data_plus(
            "sz.300141", ",".join(FIELDS), start_date="2015-01-20",
            end_date="2015-05-22", frequency="d", adjustflag="3",
        )
        assert response.error_code == "0", response.error_msg
        bars = []
        while response.next():
            bars.append(dict(zip(FIELDS, response.get_row_data())))
    finally:
        bs.logout()
    by_date = {row["date"]: row for row in bars}
    traded = [row for row in bars if row["tradestatus"] == "1" and Decimal(row["volume"] or "0") > 0]
    last = max((row for row in traded if row["date"] < "2015-05-07"), key=lambda row: row["date"])
    first = min((row for row in traded if row["date"] > "2015-05-07"), key=lambda row: row["date"])
    ex = by_date["2015-05-07"]
    assert (last["date"], last["close"], last["volume"]) == ("2015-01-22", "13.9100", "3082283")
    assert (by_date["2015-05-06"]["volume"], ex["close"], ex["volume"], ex["tradestatus"]) == ("0", "13.8100", "0", "0")
    assert (first["date"], first["preclose"], first["close"]) == ("2015-05-20", "13.8100", "15.1900")
    assert Decimal(last["close"]) - Decimal("0.10") == Decimal(ex["close"])

    csv_path = HERE / "300141_baostock_unadjusted_window.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, FIELDS)
        writer.writeheader()
        writer.writerows(bars)

    records = {}
    with RIGHTS.open(encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            if row["date"] == "2015-05-06" and row["symbol"] == "300141" and row["phase"] == "record":
                shares = Decimal(row["record_shares"])
                gross_cash = Decimal(row["cash_due"])
                assert abs(gross_cash - shares * Decimal("0.1")) < Decimal("1e-30")
                records[row["branch"]] = {"eligible_shares": str(shares), "gross_cash_due_yuan": str(gross_cash)}
    assert set(records) == {"prefunded", "netted"}
    output = {
        "queried_utc": datetime.now(timezone.utc).isoformat(),
        "issuer_sources": {key: {"url": url, "sha256": digest} for key, (_, url, digest) in SOURCES.items()},
        "candidate_source_status": "cninfo=1;sina=0; single_source_only",
        "event_scope": "exact symbol, record date, ex date, gross ratio only; no blanket single-source admission",
        "record_date": "2015-05-06",
        "ex_and_cash_credit_date": "2015-05-07",
        "gross_cash_yuan_per_share": "0.10",
        "vendor_query": "BaoStock sz.300141 daily adjustflag=3 (unadjusted), 2015-01-20..2015-05-22",
        "vendor_rows": len(bars),
        "last_actual_trade_before_ex": last,
        "ex_date_zero_volume_row": ex,
        "first_actual_trade_after_ex": first,
        "vendor_csv_sha256": sha(csv_path),
        "account_rights_events_sha256": sha(RIGHTS),
        "account_record_entitlements": records,
        "assessment": "13.81 = last traded 13.91 minus gross dividend 0.10 and is resumed first true trade preclose. Eligible only as an issuer-documented single-event exception and a zero-volume valuation reference with cash/receivable; never an executable close or generic source admission.",
    }
    (HERE / "300141_probe.json").write_text(json.dumps(output, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(output, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
