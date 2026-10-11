"""Archive unfiltered CNInfo issuer filings for two January 2010 L1 names."""

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
BASE = {
    "pageNum": "1", "pageSize": "30", "column": "sse", "tabName": "fulltext",
    "plate": "", "searchkey": "", "secid": "", "category": "", "trade": "",
    "seDate": "2010-01-01~2010-01-14", "sortName": "", "sortType": "",
    "isHLtitle": "true",
}
STOCKS = {"600137": "gssh0600137", "600603": "gssh0600603"}


def one(code: str, secid: str) -> dict:
    payload = {**BASE, "stock": f"{code},{secid}"}
    response = requests.post(URL, data=payload, headers=HEADERS, timeout=45)
    response.raise_for_status()
    raw = response.content
    body = response.json()
    total = int(body["totalAnnouncement"])
    if total > 30 or bool(body.get("hasMore")):
        raise ValueError(f"{code}: pagination required; this narrow query is not complete")
    rows = body.get("announcements") or []
    if len(rows) != total:
        raise ValueError(f"{code}: {len(rows)} returned of {total}")
    path = ROOT / f"cninfo_{code}_20100101_20100114.json.gz"
    path.write_bytes(gzip.compress(raw, mtime=0))
    return {
        "code": code, "query_url": URL, "payload": payload, "raw_gzip_file": path.name,
        "raw_sha256": hashlib.sha256(raw).hexdigest(), "raw_bytes": len(raw),
        "total": total, "hasMore": body.get("hasMore"),
        "notices": [
            {"id": str(x["announcementId"]),
             "date": datetime.fromtimestamp(x["announcementTime"] / 1000, tz=timezone.utc)
             .astimezone(timezone(timedelta(hours=8))).date().isoformat(),
             "title": x["announcementTitle"],
             "url": "https://static.cninfo.com.cn/" + x["adjunctUrl"]}
            for x in rows
        ],
    }


if __name__ == "__main__":
    result = [one(k, v) for k, v in STOCKS.items()]
    (ROOT / "jan2010_query_manifest.json").write_text(
        json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps([{"code": x["code"], "total": x["total"],
                       "sha256": x["raw_sha256"], "notices": x["notices"]}
                      for x in result], ensure_ascii=False))
