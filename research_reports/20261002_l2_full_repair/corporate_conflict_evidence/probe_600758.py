"""Isolated, reproducible 600758 suspended ex-dividend price probe."""

import csv
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import baostock as bs
from pypdf import PdfReader

HERE = Path(__file__).resolve().parent
PDF = HERE / "600758_20140611_issuer_distribution.pdf"
PDF_URL = "https://static.cninfo.com.cn/finalpage/2014-06-11/64123464.PDF"
PDF_SHA256 = "874b2bc8726666ef3aedcc1773a9d66d4cae20368bffe56f19de614d50e19e0d"
SUSPENSION_PDF = HERE / "600758_20140611_issuer_suspension.pdf"
SUSPENSION_URL = "https://static.cninfo.com.cn/finalpage/2014-06-11/64123465.PDF"
SUSPENSION_SHA256 = "144187ace2d223f860b316a2bf8e6f1b5561027bc0cf7370a46aa9974564560f"
FIELDS = "date,code,open,high,low,close,preclose,volume,tradestatus,isST".split(",")


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    assert sha(PDF) == PDF_SHA256
    assert sha(SUSPENSION_PDF) == SUSPENSION_SHA256
    pdf_text = "\n".join(page.extract_text() or "" for page in PdfReader(PDF).pages)
    suspension_text = "\n".join(page.extract_text() or "" for page in PdfReader(SUSPENSION_PDF).pages)
    assert "2014 年 1 月 29 日起停牌" in suspension_text
    assert "继续停牌期间" in suspension_text
    for fragment in ("2014 年 6 月 16 日", "2014 年 6 月 17 日", "每  10 股派发现金红利  0.3 元", "税前每股现金红利 0.03 元"):
        assert fragment in pdf_text, fragment

    login = bs.login()
    assert login.error_code == "0", login.error_msg
    try:
        response = bs.query_history_k_data_plus(
            "sh.600758", ",".join(FIELDS), start_date="2014-01-20",
            end_date="2014-07-18", frequency="d", adjustflag="3",
        )
        assert response.error_code == "0", response.error_msg
        rows = []
        while response.next():
            rows.append(dict(zip(FIELDS, response.get_row_data())))
    finally:
        bs.logout()

    assert len(rows) == 121, len(rows)
    by_date = {row["date"]: row for row in rows}
    actual = [row for row in rows if row["tradestatus"] == "1" and float(row["volume"] or 0) > 0]
    last_before = max((row for row in actual if row["date"] < "2014-06-17"), key=lambda row: row["date"])
    next_after = min((row for row in actual if row["date"] > "2014-06-17"), key=lambda row: row["date"])
    ex = by_date["2014-06-17"]
    assert (last_before["date"], last_before["close"]) == ("2014-01-28", "7.2300")
    assert (ex["close"], ex["preclose"], ex["volume"], ex["tradestatus"]) == ("7.2000", "7.2000", "0", "0")
    assert (next_after["date"], next_after["preclose"], next_after["close"]) == ("2014-07-14", "7.2000", "7.9200")
    assert round(float(last_before["close"]) - 0.03, 2) == float(ex["close"])

    csv_path = HERE / "600758_baostock_unadjusted_window.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, FIELDS)
        writer.writeheader()
        writer.writerows(rows)
    result = {
        "query_utc": datetime.now(timezone.utc).isoformat(),
        "issuer_pdf_url": PDF_URL,
        "issuer_pdf_sha256": PDF_SHA256,
        "issuer_suspension_pdf_url": SUSPENSION_URL,
        "issuer_suspension_pdf_sha256": SUSPENSION_SHA256,
        "issuer_notice_date": "2014-06-10",
        "issuer_publication_date": "2014-06-11",
        "record_date": "2014-06-16",
        "ex_date": "2014-06-17",
        "cash_payment_date": "2014-06-17",
        "gross_cash_yuan_per_share": "0.03",
        "vendor_query": "BaoStock sh.600758 daily adjustflag=3 (unadjusted), 2014-01-20..2014-07-18",
        "vendor_rows": len(rows),
        "last_actual_trade_before_ex": last_before,
        "ex_date_zero_volume_row": ex,
        "next_actual_trade_after_ex": next_after,
        "vendor_csv_sha256": sha(csv_path),
        "assessment": "7.20 = last true close 7.23 - gross dividend 0.03 and equals next true trade preclose; eligible only as explicitly flagged suspended ex-reference mark with dividend cash/receivable credited, never as traded close or fill price.",
        "source_limit": "BaoStock is a vendor; this probe does not prove an exchange-issued ex-reference quote or executable price on suspension day.",
    }
    (HERE / "600758_probe.json").write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
