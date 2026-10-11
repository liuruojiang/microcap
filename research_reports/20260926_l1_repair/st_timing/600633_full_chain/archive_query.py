"""Archive all CNInfo issuer notices across 600633's first ST spell.

Research only; does not write formal security metadata, members, or NAV.
"""

from __future__ import annotations

import csv
import hashlib
import json
import math
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests


HERE = Path(__file__).resolve().parent
URL = "https://www.cninfo.com.cn/new/hisAnnouncement/query"
HEADERS = {"User-Agent": "Mozilla/5.0", "Referer": "https://www.cninfo.com.cn/new/index"}
PAYLOAD = {
    "pageNum": "1", "pageSize": "30", "column": "sse", "tabName": "fulltext",
    "plate": "", "stock": "600633,gssh0600633", "searchkey": "", "secid": "",
    "category": "", "trade": "", "seDate": "2009-03-23~2011-09-29",
    "sortName": "", "sortType": "", "isHLtitle": "true",
}


def read_page(page: int) -> tuple[dict, bytes]:
    path = HERE / f"page_{page:03d}.json"
    if path.exists():
        raw = path.read_bytes()
    else:
        response = requests.post(URL, data={**PAYLOAD, "pageNum": str(page)},
                                 headers=HEADERS, timeout=45)
        response.raise_for_status()
        raw = response.content
        path.write_bytes(raw)
    return json.loads(raw), raw


def main() -> None:
    HERE.mkdir(exist_ok=True)
    first, _ = read_page(1)
    total = int(first["totalAnnouncement"])
    pages = math.ceil(total / 30)
    notices = []
    page_manifest = []
    for page in range(1, pages + 1):
        body, raw = read_page(page)
        items = body["announcements"]
        assert int(body["totalAnnouncement"]) == total
        assert len(items) == (30 if page < pages else total - 30 * (pages - 1))
        page_manifest.append({"page": page, "rows": len(items), "bytes": len(raw),
                              "sha256": hashlib.sha256(raw).hexdigest()})
        for item in items:
            assert item["secCode"] == "600633"
            announced = datetime.fromtimestamp(item["announcementTime"] / 1000,
                                                tz=timezone.utc)
            local = announced.astimezone(timezone(timedelta(hours=8)))
            notices.append({
                "date": local.date().isoformat(), "time_bjt": local.isoformat(),
                "id": item["announcementId"], "api_sec_name": item["secName"],
                "title": item["announcementTitle"],
                "url": "https://static.cninfo.com.cn/" + item["adjunctUrl"],
            })
    assert len(notices) == total
    assert len({x["id"] for x in notices}) == total
    with (HERE / "all_notices.csv").open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(notices[0]))
        writer.writeheader()
        writer.writerows(notices)
    (HERE / "query_manifest.json").write_text(
        json.dumps({"url": URL, "payload": PAYLOAD, "total": total,
                    "unique_ids": total, "pages": page_manifest},
                   ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps({"total": total, "pages": pages, "unique_ids": total}))


if __name__ == "__main__":
    main()
