"""Find original issuer notices at the three annual-table ST transitions."""

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
WINDOWS = {"600633": ("gssh0600633", "2009-03-20~2009-03-25"),
           "600617": ("gssh0600617", "2009-04-27~2009-05-01"),
           "600792": ("gssh0600792", "2009-03-17~2009-03-21")}


if __name__ == "__main__":
    out = []
    for code, (secid, window) in WINDOWS.items():
        payload = {"pageNum": "1", "pageSize": "30", "column": "sse", "tabName": "fulltext",
                   "plate": "", "stock": f"{code},{secid}", "searchkey": "", "secid": "",
                   "category": "", "trade": "", "seDate": window, "sortName": "",
                   "sortType": "", "isHLtitle": "true"}
        response = requests.post(URL, data=payload, headers=HEADERS, timeout=45)
        response.raise_for_status()
        raw = response.content
        body = response.json()
        path = ROOT / f"cninfo_{code}_entry_window.json.gz"
        path.write_bytes(gzip.compress(raw, mtime=0))
        rows = [{"id": str(x["announcementId"]),
                 "date": datetime.fromtimestamp(x["announcementTime"] / 1000, tz=timezone.utc)
                 .astimezone(timezone(timedelta(hours=8))).date().isoformat(),
                 "title": x["announcementTitle"],
                 "url": "https://static.cninfo.com.cn/" + x["adjunctUrl"]}
                for x in body.get("announcements") or []]
        if len(rows) != int(body["totalAnnouncement"]) or body.get("hasMore"):
            raise ValueError(f"{code}: incomplete entry-window query")
        out.append({"code": code, "payload": payload, "raw_file": path.name,
                    "raw_sha256": hashlib.sha256(raw).hexdigest(), "notices": rows})
    (ROOT / "entry_window_query_manifest.json").write_text(
        json.dumps(out, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    for x in out:
        print(x["code"], x["payload"]["seDate"], x["notices"])
