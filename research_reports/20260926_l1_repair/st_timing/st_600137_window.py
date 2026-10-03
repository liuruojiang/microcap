"""Archive CNInfo 600137 filings around the first L1 rebalance; research only."""

from __future__ import annotations

import csv
import gzip
import hashlib
import json
import math
from datetime import datetime, timedelta, timezone
from pathlib import Path

import fitz
import requests


OUT = Path(__file__).resolve().parent / "600137_2008_2010_window"
URL = "https://www.cninfo.com.cn/new/hisAnnouncement/query"
HEADERS = {"User-Agent": "Mozilla/5.0", "Referer": "https://www.cninfo.com.cn/new/index"}
PAYLOAD = {
    "pageNum": "1", "pageSize": "30", "column": "sse", "tabName": "fulltext",
    "plate": "", "stock": "600137,gssh0600137", "searchkey": "", "secid": "",
    "category": "", "trade": "", "seDate": "2008-06-12~2010-01-04",
    "sortName": "", "sortType": "", "isHLtitle": "true",
}


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def get_page(n: int) -> tuple[dict, bytes]:
    path = OUT / f"page_{n:03d}.json.gz"
    if path.exists():
        raw = gzip.decompress(path.read_bytes())
    else:
        response = requests.post(URL, data={**PAYLOAD, "pageNum": str(n)}, headers=HEADERS, timeout=30)
        response.raise_for_status()
        raw = response.content
        path.write_bytes(gzip.compress(raw, mtime=0))
    return json.loads(raw), raw


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    first, _ = get_page(1)
    total = int(first["totalAnnouncement"])
    pages = math.ceil(total / 30)
    page_log, notices = [], []
    for n in range(1, pages + 1):
        body, raw = get_page(n)
        items = body["announcements"]
        assert int(body["totalAnnouncement"]) == total
        page_log.append({"page": n, "rows": len(items), "raw_sha256": sha(raw), "bytes": len(raw)})
        for item in items:
            day = datetime.fromtimestamp(item["announcementTime"] / 1000, tz=timezone.utc)
            day = day.astimezone(timezone(timedelta(hours=8))).date().isoformat()
            notices.append({
                "date": day, "id": str(item["announcementId"]),
                "title": item["announcementTitle"],
                "url": "https://static.cninfo.com.cn/" + item["adjunctUrl"],
            })
    assert len(notices) == total and len({x["id"] for x in notices}) == total
    with (OUT / "all_notices.csv").open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["date", "id", "title", "url"])
        writer.writeheader()
        writer.writerows(notices)
    (OUT / "manifest.json").write_text(json.dumps({
        "query_url": URL, "payload": PAYLOAD, "total": total, "pages": page_log,
        "unique_ids": len(notices), "complete_within_cninfo_query": True,
    }, ensure_ascii=False, indent=2), encoding="utf-8")

    risk_words = ("特别处理", "风险警示", "证券简称", "股票简称", "股票交易", "年度报告", "季度报告", "半年度报告", "提示性公告", "异常波动")
    selected = [x for x in notices if any(word in x["title"] for word in risk_words)]
    (OUT / "pdfs").mkdir(exist_ok=True)
    pdf_log = []
    for item in selected:
        path = OUT / "pdfs" / f"{item['id']}.pdf"
        if path.exists():
            raw = path.read_bytes()
        else:
            response = requests.get(item["url"], headers=HEADERS, timeout=30)
            response.raise_for_status()
            raw = response.content
            path.write_bytes(raw)
        try:
            doc = fitz.open(stream=raw, filetype="pdf")
            first_page = doc[0].get_text() if len(doc) else ""
            all_text = "\n".join(page.get_text() for page in doc)
            (OUT / "pdfs" / f"{item['id']}.txt").write_text(all_text, encoding="utf-8")
            error = ""
        except Exception as exc:
            first_page, all_text, error = "", "", f"{type(exc).__name__}: {exc}"
        pdf_log.append({**item, "sha256": sha(raw), "bytes": len(raw),
                        "first_page_chars": len(first_page), "all_text_chars": len(all_text),
                        "first_page_st": "ST" in first_page.upper(), "error": error})
    with (OUT / "selected_source_manifest.csv").open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(pdf_log[0]))
        writer.writeheader()
        writer.writerows(pdf_log)
    print(json.dumps({"total": total, "pages": pages, "selected": len(selected),
                      "selected_saved": len(pdf_log), "errors": sum(bool(x["error"]) for x in pdf_log)}, ensure_ascii=False))


if __name__ == "__main__":
    main()
