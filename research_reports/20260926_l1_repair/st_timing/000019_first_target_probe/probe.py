"""Archive issuer evidence for 000019 around the first Top100 target date.

Research only. Never writes formal inputs, members, or NAV.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import fitz
import requests


HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[3]
QUERY_URL = "https://www.cninfo.com.cn/new/hisAnnouncement/query"
HEADERS = {"User-Agent": "Mozilla/5.0", "Referer": "https://www.cninfo.com.cn/new/index"}
QUERY = {
    "pageNum": "1", "pageSize": "30", "column": "szse", "tabName": "fulltext",
    "plate": "", "stock": "000019,gssz0000019", "searchkey": "", "secid": "",
    "category": "", "trade": "", "seDate": "2009-10-01~2010-02-28",
    "sortName": "", "sortType": "", "isHLtitle": "true",
}
SELECTED = {
    "57186212": "2009-10-23",  # Quarter report: total shares.
    "57335885": "2009-11-27",  # Risk notice: ordinary name, not ST entry.
    "57456706": "2009-12-31",  # Shareholder resolution: total shares and name.
    "57483178": "2010-01-08",  # Latest known issuer name before target.
    "57535691": "2010-01-25",  # Later corroboration only.
}


def digest(blob: bytes) -> str:
    return hashlib.sha256(blob).hexdigest()


def cached_get(path: Path, url: str) -> bytes:
    if path.exists():
        return path.read_bytes()
    response = requests.get(url, headers=HEADERS, timeout=45)
    response.raise_for_status()
    path.write_bytes(response.content)
    return response.content


def main() -> None:
    raw_path = HERE / "cninfo_query_raw.json"
    if raw_path.exists():
        raw = raw_path.read_bytes()
    else:
        response = requests.post(QUERY_URL, data=QUERY, headers=HEADERS, timeout=45)
        response.raise_for_status()
        raw = response.content
        raw_path.write_bytes(raw)
    body = json.loads(raw)
    notices = body["announcements"]
    assert body["totalAnnouncement"] == 23 and not body["hasMore"]
    assert len(notices) == 23 and len({x["announcementId"] for x in notices}) == 23
    assert all(x["secCode"] == "000019" for x in notices)
    notice_by_id = {x["announcementId"]: x for x in notices}
    assert set(SELECTED) <= notice_by_id.keys()

    source_rows = []
    for announcement_id, day in SELECTED.items():
        notice = notice_by_id[announcement_id]
        url = "https://static.cninfo.com.cn/" + notice["adjunctUrl"]
        assert f"/{day}/{announcement_id}.PDF" in url
        blob = cached_get(HERE / f"{announcement_id}.pdf", url)
        doc = fitz.open(stream=blob, filetype="pdf")
        content = "\n".join(page.get_text() for page in doc)
        (HERE / f"{announcement_id}.txt").write_text(content, encoding="utf-8")
        assert "000019" in content[:800] or announcement_id == "57186212"
        source_rows.append({
            "announcement_id": announcement_id, "day": day, "url": url,
            "announcement_time_ms": notice["announcementTime"],
            "sec_name": notice["secName"], "sha256": digest(blob),
            "bytes": len(blob), "pdf_pages": len(doc), "text_chars": len(content),
        })
    names_file = ROOT / ".microcap_index_cache/sz_name_change_short.csv"
    names = [line for line in names_file.read_text(encoding="utf-8-sig").splitlines()
             if ",000019," in line]
    manifest = {
        "scope": "research_only_first_target_000019",
        "query_url": QUERY_URL, "query_payload": QUERY,
        "query_sha256": digest(raw), "query_bytes": len(raw),
        "query_total": 23, "query_unique_ids": 23,
        "selected_sources": source_rows,
        "szse_name_history_local_path": str(names_file),
        "szse_name_history_sha256": digest(names_file.read_bytes()),
        "szse_000019_rows": names,
    }
    (HERE / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps({"query_total": 23, "sources": len(source_rows),
                      "query_sha256": manifest["query_sha256"]}))


if __name__ == "__main__":
    main()
