"""Read CNInfo issuer filing titles near the initial 2010 Top100 dates."""

from __future__ import annotations

import gzip
import hashlib
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests


ROOT = Path(__file__).resolve().parent
URL = "https://www.cninfo.com.cn/new/hisAnnouncement/query"
HEADERS = {"User-Agent": "Mozilla/5.0", "Referer": "https://www.cninfo.com.cn/new/index"}
STOCKS = {"600633": "gssh0600633", "600617": "gssh0600617", "600792": "gssh0600792"}
BASE = {"pageNum": "1", "pageSize": "30", "column": "sse", "tabName": "fulltext",
        "plate": "", "searchkey": "", "secid": "", "category": "", "trade": "",
        "seDate": "2009-09-01~2010-01-14", "sortName": "", "sortType": "",
        "isHLtitle": "true"}


def one(code: str, secid: str) -> dict:
    payload = {**BASE, "stock": f"{code},{secid}"}
    rows, pages = [], []
    n = 1
    while True:
        response = requests.post(URL, data={**payload, "pageNum": str(n)}, headers=HEADERS, timeout=45)
        response.raise_for_status()
        raw = response.content
        body = response.json()
        path = ROOT / f"cninfo_{code}_20090901_20100114_page{n}.json.gz"
        path.write_bytes(gzip.compress(raw, mtime=0))
        pages.append({"page": n, "raw_file": path.name, "sha256": hashlib.sha256(raw).hexdigest(),
                      "bytes": len(raw), "rows": len(body.get("announcements") or [])})
        for x in body.get("announcements") or []:
            date = datetime.fromtimestamp(x["announcementTime"] / 1000, tz=timezone.utc).astimezone(
                timezone(timedelta(hours=8))).date().isoformat()
            rows.append({"date": date, "id": str(x["announcementId"]),
                         "title": x["announcementTitle"],
                         "url": "https://static.cninfo.com.cn/" + x["adjunctUrl"]})
        if not body.get("hasMore"):
            break
        n += 1
    assert len(rows) == int(body["totalAnnouncement"])
    return {"code": code, "payload": payload, "total": len(rows), "pages": pages, "notices": rows}


if __name__ == "__main__":
    out = [one(k, v) for k, v in STOCKS.items()]
    (ROOT / "three_issuer_notice_manifest.json").write_text(
        json.dumps(out, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    for stock in out:
        print(stock["code"], stock["total"], "pages", len(stock["pages"]))
        for row in stock["notices"][:10]:
            print(" ", row["date"], row["id"], row["title"])
