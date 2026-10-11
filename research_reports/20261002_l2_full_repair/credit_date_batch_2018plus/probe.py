"""Read-only primary-source probe for frozen held-member missing-credit candidates."""
from __future__ import annotations

import csv
import hashlib
import json
import re
import time
from datetime import date, timedelta
from pathlib import Path

import fitz
import requests

BASE = Path(__file__).resolve().parents[1]
HERE = Path(__file__).resolve().parent
FROZEN = BASE / "data" / "old_path_event_overlaps.csv"
API = "https://www.cninfo.com.cn/new/hisAnnouncement/query"
HEADERS = {"User-Agent": "Mozilla/5.0", "Referer": "https://www.cninfo.com.cn/"}


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> None:
    rows = list(csv.DictReader(FROZEN.open(encoding="utf-8-sig", newline="")))
    candidates = [
        r for r in rows
        if r["ex_date"] > "2018-06-25"
        and r["bonus_per_10"] not in ("", "0", "0.0")
        and not r["share_credit_date"]
        and r["old_held_at_record_snapshot"] == "True"
    ]
    session = requests.Session()
    result = []
    for i, r in enumerate(candidates, 1):
        symbol = r["symbol"]
        notice = date.fromisoformat(r["announcement_date"])
        start = (notice - timedelta(days=2)).isoformat()
        end = (notice + timedelta(days=3)).isoformat()
        data = {
            "pageNum": 1, "pageSize": 100, "column": "sse", "tabName": "fulltext",
            "stock": "", "searchkey": symbol, "secid": "", "plate": "sh",
            "category": "", "trade": "", "seDate": start + "~" + end,
            "sortName": "time", "sortType": "asc",
        }
        item = {k: r[k] for k in ["symbol", "record_date", "ex_date", "announcement_date", "bonus_per_10", "cash_per_10_gross", "source_status"]}
        item["frozen_input_sha256"] = sha(FROZEN)
        try:
            response = session.post(API, data=data, headers=HEADERS, timeout=25)
            response.raise_for_status()
            found = [a for a in response.json().get("announcements", [])
                     if a["secCode"] == symbol and "权益分派实施公告" in a["announcementTitle"]]
            if not found:
                item["status"] = "primary_implementation_pdf_not_found_in_window"
                item["api_result_count"] = response.json().get("totalAnnouncement")
            else:
                a = min(found, key=lambda x: abs(date.fromtimestamp(x["announcementTime"] / 1000).toordinal() - notice.toordinal()))
                url = "https://static.cninfo.com.cn/" + a["adjunctUrl"]
                pdf = HERE / f"{symbol}_{r['ex_date'].replace('-', '')}_issuer.pdf"
                rr = session.get(url, headers=HEADERS, timeout=30)
                rr.raise_for_status()
                if not rr.content.startswith(b"%PDF"):
                    raise ValueError("not a PDF")
                pdf.write_bytes(rr.content)
                body = "\n".join(p.get_text() for p in fitz.open(pdf))
                txt = pdf.with_suffix(".txt")
                txt.write_text(body, encoding="utf-8")
                text_norm = re.sub(r"\s+", "", body)
                # A date by itself is not a claim; the issuer's header/table must label the share listing date.
                marker = "新增无限售条件流通股份上市日" in text_norm or "新增无限售条件流通股上市日" in text_norm
                date_strings = sorted(set(re.findall(r"20\d{2}[./年]\d{1,2}[./月]\d{1,2}", body)))
                item.update({
                    "status": "issuer_pdf_header_marker_present" if marker else "issuer_pdf_downloaded_no_listing_marker",
                    "source_url": url,
                    "source_title": a["announcementTitle"],
                    "source_notice_date": a["adjunctUrl"].split("/")[1],
                    "pdf_file": pdf.name,
                    "pdf_sha256": sha(pdf),
                    "text_sha256": sha(txt),
                    "listing_date_marker_present": marker,
                    "date_strings_for_manual_review": date_strings,
                })
        except Exception as exc:
            item["status"] = "fetch_or_extract_failed"
            item["error"] = type(exc).__name__ + ": " + str(exc)[:180]
        result.append(item)
        print(i, len(candidates), symbol, r["ex_date"], item["status"], item.get("source_url", ""), flush=True)
        (HERE / "results.json").write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
        time.sleep(0.2)
    print("candidates", len(candidates), "frozen_sha", sha(FROZEN))


if __name__ == "__main__":
    main()
