"""Archive CNInfo's full-title index for the disputed 600792 interval."""
from __future__ import annotations

import csv
import gzip
import hashlib
import json
import math
from datetime import datetime, timezone, timedelta
from pathlib import Path

import requests

HERE = Path(__file__).resolve().parent
URL = "https://www.cninfo.com.cn/new/hisAnnouncement/query"
PARAMS = dict(pageNum="1", pageSize="30", column="sse", tabName="fulltext", plate="",
              stock="600792,gssh0600792", searchkey="", secid="", category="", trade="",
              seDate="2008-01-01~2012-12-31", sortName="", sortType="", isHLtitle="true")
HEADERS = {"User-Agent": "Mozilla/5.0", "Referer": "https://www.cninfo.com.cn/new/index"}
CST = timezone(timedelta(hours=8))


def read_page(number: int) -> tuple[dict, bytes]:
    path = HERE / f"page_{number:03d}.json.gz"
    if path.exists():
        raw = gzip.decompress(path.read_bytes())
    else:
        response = requests.post(URL, data={**PARAMS, "pageNum": str(number)}, headers=HEADERS, timeout=30)
        response.raise_for_status()
        raw = response.content
        path.write_bytes(gzip.compress(raw))
    data = json.loads(raw)
    assert isinstance(data["announcements"], list)
    return data, raw


def main() -> None:
    first, _ = read_page(1)
    total = int(first["totalAnnouncement"])
    pages = math.ceil(total / 30)
    rows, manifest = [], []
    for number in range(1, pages + 1):
        data, raw = read_page(number)
        assert int(data["totalAnnouncement"]) == total
        manifest.append(dict(page=number, rows=len(data["announcements"]), sha256=hashlib.sha256(raw).hexdigest(), bytes=len(raw)))
        for item in data["announcements"]:
            date = datetime.fromtimestamp(item["announcementTime"] / 1000, CST).date().isoformat()
            rows.append(dict(date=date, id=str(item["announcementId"]), title=item["announcementTitle"],
                             url="https://static.cninfo.com.cn/" + item["adjunctUrl"]))
    assert len(rows) == total and len({row["id"] for row in rows}) == total
    with (HERE / "all_notices.csv").open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0])); writer.writeheader(); writer.writerows(rows)
    (HERE / "manifest.json").write_text(json.dumps(dict(params=PARAMS, total=total, pages=manifest), indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(total, pages)


if __name__ == "__main__":
    main()
