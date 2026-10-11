"""Archive issuer filings for first-two-date BaoStock ST triage leads."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import fitz
import requests


HERE = Path(__file__).resolve().parent
OUT = HERE / "issuer_three"
URL = "https://www.cninfo.com.cn/new/hisAnnouncement/query"
HEADERS = {"User-Agent": "Mozilla/5.0", "Referer": "https://www.cninfo.com.cn/new/index"}
SELECTED = {
    "600633": ("57435883", "57472798", "57494686", "57616191"),
    "600617": ("57457285", "57507089"),
    "600792": ("57443562", "57491007", "57599574"),
}


def sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def fetch(path: Path, url: str, *, data: dict | None = None) -> bytes:
    if path.exists():
        return path.read_bytes()
    if data is None:
        response = requests.get(url, headers=HEADERS, timeout=45)
    else:
        response = requests.post(url, data=data, headers=HEADERS, timeout=45)
    response.raise_for_status()
    path.write_bytes(response.content)
    return response.content


def main() -> None:
    OUT.mkdir(exist_ok=True)
    manifest = {}
    for symbol, selected in SELECTED.items():
        target = OUT / symbol
        target.mkdir(exist_ok=True)
        payload = {
            "pageNum": "1", "pageSize": "30", "column": "sse", "tabName": "fulltext",
            "plate": "", "stock": f"{symbol},gssh0{symbol}", "searchkey": "", "secid": "",
            "category": "", "trade": "", "seDate": "2009-12-01~2010-02-28",
            "sortName": "", "sortType": "", "isHLtitle": "true",
        }
        raw = fetch(target / "page_1.json", URL, data=payload)
        body = json.loads(raw)
        assert body["totalAnnouncement"] == len(body["announcements"])
        assert not body["hasMore"]
        notices = body["announcements"]
        assert len({x["announcementId"] for x in notices}) == len(notices)
        assert all(x["secCode"] == symbol for x in notices)
        indexed = {x["announcementId"]: x for x in notices}
        assert set(selected) <= indexed.keys()
        rows = []
        for announcement_id in selected:
            item = indexed[announcement_id]
            url = "https://static.cninfo.com.cn/" + item["adjunctUrl"]
            blob = fetch(target / f"{announcement_id}.pdf", url)
            doc = fitz.open(stream=blob, filetype="pdf")
            first = doc[0].get_text() if len(doc) else ""
            assert symbol in first[:1000]
            content = "\n".join(page.get_text() for page in doc)
            (target / f"{announcement_id}.txt").write_text(content, encoding="utf-8")
            day = datetime.fromtimestamp(item["announcementTime"] / 1000, tz=timezone.utc)
            day = day.astimezone(timezone(timedelta(hours=8))).date().isoformat()
            rows.append({"announcement_id": announcement_id, "day": day,
                         "title": item["announcementTitle"],
                         "api_sec_name": item["secName"], "url": url,
                         "pdf_sha256": sha(blob), "pdf_bytes": len(blob),
                         "first_page_text": first[:500], "pdf_pages": len(doc)})
        manifest[symbol] = {"query_payload": payload, "query_total": len(notices),
                            "query_sha256": sha(raw), "files": rows}
    (OUT / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps({s: {"announcements": m["query_total"], "pdfs": len(m["files"])}
                      for s, m in manifest.items()}, ensure_ascii=False))


if __name__ == "__main__":
    main()
