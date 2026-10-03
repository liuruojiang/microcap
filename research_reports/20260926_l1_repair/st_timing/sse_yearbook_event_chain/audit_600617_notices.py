"""Archive all CNInfo 600617 issuer notice index pages over the first ST spell."""

from __future__ import annotations

import csv
import gzip
import hashlib
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests


ROOT = Path(__file__).resolve().parent / "600617_chain"
URL = "https://www.cninfo.com.cn/new/hisAnnouncement/query"
HEADERS = {"User-Agent": "Mozilla/5.0", "Referer": "https://www.cninfo.com.cn/new/index"}
BASE = {"pageNum": "1", "pageSize": "30", "column": "sse", "tabName": "fulltext",
        "plate": "", "stock": "600617,gssh0600617", "searchkey": "", "secid": "",
        "category": "", "trade": "", "seDate": "2009-04-29~2014-03-25",
        "sortName": "", "sortType": "", "isHLtitle": "true"}


if __name__ == "__main__":
    ROOT.mkdir(exist_ok=True)
    page_log, rows = [], []
    n = 1
    total = None
    while True:
        path = ROOT / f"page_{n:03d}.json.gz"
        if path.exists():
            raw = gzip.decompress(path.read_bytes())
            body = json.loads(raw)
        else:
            response = requests.post(URL, data={**BASE, "pageNum": str(n)}, headers=HEADERS, timeout=45)
            response.raise_for_status()
            raw = response.content
            body = response.json()
            path.write_bytes(gzip.compress(raw, mtime=0))
        if total is None:
            total = int(body["totalAnnouncement"])
        assert int(body["totalAnnouncement"]) == total
        items = body.get("announcements") or []
        page_log.append({"page": n, "rows": len(items), "raw_sha256": hashlib.sha256(raw).hexdigest(),
                         "raw_bytes": len(raw), "file": path.name})
        for x in items:
            stamp = datetime.fromtimestamp(x["announcementTime"] / 1000, tz=timezone.utc).astimezone(
                timezone(timedelta(hours=8)))
            rows.append({"date": stamp.date().isoformat(), "time": stamp.strftime("%H:%M:%S"),
                         "id": str(x["announcementId"]), "title": x["announcementTitle"],
                         "url": "https://static.cninfo.com.cn/" + x["adjunctUrl"]})
        if not body.get("hasMore"):
            break
        n += 1
    assert len(rows) == total and len({x["id"] for x in rows}) == total
    with (ROOT / "all_notices.csv").open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    (ROOT / "notice_manifest.json").write_text(json.dumps({
        "url": URL, "payload": BASE, "total": total, "unique_ids": total, "pages": page_log,
        "all_notices_sha256": hashlib.sha256((ROOT / "all_notices.csv").read_bytes()).hexdigest(),
    }, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print("total", total, "pages", n)
    for x in rows:
        if any(k in x["title"] for k in ("特别处理", "风险警示", "简称", "摘帽", "摘星", "退市")):
            print(x["date"], x["id"], x["title"])
