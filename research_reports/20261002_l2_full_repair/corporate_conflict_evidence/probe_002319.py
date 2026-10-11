"""Isolated 002319 suspended cash-ex date evidence probe."""

import csv
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import baostock as bs
from pypdf import PdfReader

HERE = Path(__file__).resolve().parent
SOURCES = {
    "distribution": ("002319_20140614_issuer_distribution.pdf", "https://static.cninfo.com.cn/finalpage/2014-06-14/64135847.PDF", "47e4c62a2a734602db9b7135ffeb4ee723c22491a467ca8c6b2e3db4079473f2"),
    "suspension": ("002319_20140621_issuer_suspension.pdf", "https://static.cninfo.com.cn/finalpage/2014-06-21/64159039.PDF", "21fe0e008445d9c391aaae8ef54581ed48fb259a96a91fc0375c17e9f46737f1"),
    "resumption": ("002319_20140710_issuer_resumption.pdf", "https://static.cninfo.com.cn/finalpage/2014-07-10/1200043606.PDF", "fba4d5429d2e0a0f146f383fb4d754a626fda00bbea9bfc3b1e1ba0b4e7fe377"),
}
FIELDS = "date,code,open,high,low,close,preclose,volume,tradestatus,isST".split(",")


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    texts = {}
    for key, (filename, _url, expected) in SOURCES.items():
        path = HERE / filename
        assert sha(path) == expected
        texts[key] = "\n".join(page.extract_text() or "" for page in PdfReader(path).pages)
    compact_distribution = "".join(texts["distribution"].split())
    for fragment in ("2014年6月19日", "2014年6月20日", "每10股派0.300000元"):
        assert fragment in compact_distribution, fragment
    assert "2014年6月18日13:00起停牌" in texts["suspension"]
    assert "2014年7月10日开市起复牌" in texts["resumption"]

    login = bs.login()
    assert login.error_code == "0", login.error_msg
    try:
        response = bs.query_history_k_data_plus(
            "sz.002319", ",".join(FIELDS), start_date="2014-06-10",
            end_date="2014-07-15", frequency="d", adjustflag="3",
        )
        assert response.error_code == "0", response.error_msg
        rows = []
        while response.next():
            rows.append(dict(zip(FIELDS, response.get_row_data())))
    finally:
        bs.logout()
    by_date = {row["date"]: row for row in rows}
    actual = [row for row in rows if row["tradestatus"] == "1" and float(row["volume"] or 0) > 0]
    last_before = max((row for row in actual if row["date"] < "2014-06-20"), key=lambda row: row["date"])
    next_after = min((row for row in actual if row["date"] > "2014-06-20"), key=lambda row: row["date"])
    ex = by_date["2014-06-20"]
    assert (last_before["date"], last_before["close"]) == ("2014-06-18", "8.1900")
    assert (by_date["2014-06-19"]["volume"], ex["close"], ex["volume"], ex["tradestatus"]) == ("0", "8.1600", "0", "0")
    assert (next_after["date"], next_after["preclose"], next_after["close"]) == ("2014-07-10", "8.1600", "8.3200")
    assert round(float(last_before["close"]) - 0.03, 2) == float(ex["close"])

    csv_path = HERE / "002319_baostock_unadjusted_window.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, FIELDS)
        writer.writeheader()
        writer.writerows(rows)
    result = {
        "query_utc": datetime.now(timezone.utc).isoformat(),
        "sources": {key: {"url": url, "sha256": digest} for key, (_, url, digest) in SOURCES.items()},
        "record_date": "2014-06-19",
        "ex_date": "2014-06-20",
        "cash_payment_date": "2014-06-20",
        "gross_cash_yuan_per_share": "0.03",
        "vendor_query": "BaoStock sz.002319 daily adjustflag=3 (unadjusted), 2014-06-10..2014-07-15",
        "vendor_rows": len(rows),
        "last_actual_trade_before_ex": last_before,
        "record_date_zero_volume_row": by_date["2014-06-19"],
        "ex_date_zero_volume_row": ex,
        "next_actual_trade_after_ex": next_after,
        "vendor_csv_sha256": sha(csv_path),
        "assessment": "8.16 = last true close 8.19 - gross dividend 0.03 and equals resumed first true trade preclose; eligible only as explicitly flagged suspended ex-reference mark with dividend cash/receivable credited, never as traded close or fill price.",
        "source_limit": "BaoStock is a vendor; this does not prove an exchange-issued ex-reference quote or executable price on suspension day.",
    }
    (HERE / "002319_probe.json").write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
