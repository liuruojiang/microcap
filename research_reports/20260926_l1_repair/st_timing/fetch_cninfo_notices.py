"""Fetch official CNInfo ST titles for the frozen 57-symbol L1 gap, read-only to strategy state."""

from __future__ import annotations

import json
import hashlib
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd
import requests


OUT = Path(__file__).resolve().parent
ROOT = OUT.parents[2]
ORG_MAP = ROOT / ".microcap_index_cache/cninfo_a_org_map.csv"
QUERY_URL = "https://www.cninfo.com.cn/new/hisAnnouncement/query"
SPECS = [
    ("category_tbclts_szsh", ""),
    ("", "风险警示"),
    ("", "特别处理"),
    ("", "撤销风险警示"),
    ("", "撤销特别处理"),
    ("", "摘帽"),
]
HEADERS = {"User-Agent": "Mozilla/5.0", "Referer": "https://www.cninfo.com.cn/new/index"}


def fetch_page(payload: dict) -> tuple[dict, str]:
    response = requests.post(QUERY_URL, headers=HEADERS, data=payload, timeout=30)
    response.raise_for_status()
    digest = hashlib.sha256(response.content).hexdigest()
    data = response.json()
    if not isinstance(data, dict) or "totalAnnouncement" not in data:
        raise ValueError("CNInfo response missing totalAnnouncement")
    return data, digest


def fetch_symbol(row: dict, org_map: dict[str, str]) -> dict:
    symbol = row["symbol"]
    path = OUT / "cninfo_raw" / f"{symbol}.json"
    if path.exists():
        saved = json.loads(path.read_text(encoding="utf-8"))
        if all("first_page_sha256" in spec for spec in saved.get("specs", [])):
            return saved
    org_id = org_map.get(symbol)
    query_start_date = "1990-01-01"
    query_end_date = "2026-09-24"
    column = "sse" if symbol.startswith(("6", "5", "9")) else "szse"
    result = {
        "symbol": symbol,
        "org_id": org_id,
        "query_url": QUERY_URL,
        "query_start_date": query_start_date,
        "query_end_date": query_end_date,
        "column": column,
        "specs": [],
        "announcements": [],
    }
    if org_id is None:
        result["error"] = "org_id_missing_in_frozen_map"
    else:
        for category, searchkey in SPECS:
            payload = {
                "pageNum": "1", "pageSize": "30", "column": column, "tabName": "fulltext",
                "plate": "", "stock": f"{symbol},{org_id}", "searchkey": searchkey,
                "secid": "", "category": category, "trade": "",
                "seDate": f"{query_start_date}~{query_end_date}",
                "sortName": "", "sortType": "", "isHLtitle": "true",
            }
            spec_result = {"category": category, "searchkey": searchkey, "totalAnnouncement": None, "pages_fetched": 0, "first_page_sha256": None, "response_sha256_pages": [], "error": None}
            for attempt in range(2):
                try:
                    first, first_digest = fetch_page(payload)
                    spec_result["first_page_sha256"] = first_digest
                    total = int(first.get("totalAnnouncement") or 0)
                    spec_result["totalAnnouncement"] = total
                    pages = (total + 29) // 30
                    for page in range(1, pages + 1):
                        data, digest = (first, first_digest) if page == 1 else fetch_page({**payload, "pageNum": str(page)})
                        spec_result["pages_fetched"] += 1
                        spec_result["response_sha256_pages"].append({"pageNum": page, "sha256": digest})
                        for item in data.get("announcements") or []:
                            ts = pd.to_datetime(item.get("announcementTime"), unit="ms", utc=True, errors="coerce")
                            title = re.sub(r"<[^>]+>", "", str(item.get("announcementTitle") or ""))
                            result["announcements"].append({
                                "notice_time_shanghai": None if pd.isna(ts) else ts.tz_convert("Asia/Shanghai").isoformat(),
                                "title": title,
                                "announcementId": item.get("announcementId"),
                                "adjunctUrl": item.get("adjunctUrl"),
                                "secCode": item.get("secCode"),
                                "query_category": category,
                                "query_searchkey": searchkey,
                            })
                    break
                except (requests.RequestException, ValueError) as exc:
                    spec_result["error"] = f"{type(exc).__name__}: {exc}"
                    if attempt == 0:
                        time.sleep(1)
            result["specs"].append(spec_result)
            if spec_result["error"]:
                # A failed query means coverage is incomplete. Continue to
                # preserve any independently successful search terms.
                continue
    dedup = {}
    for item in result["announcements"]:
        dedup[(item["notice_time_shanghai"], item["title"])] = item
    result["announcements"] = sorted(dedup.values(), key=lambda x: str(x["notice_time_shanghai"]))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    return result


def main() -> None:
    coverage = pd.read_csv(OUT / "st_57_coverage.csv", dtype={"symbol": str}).fillna("")
    org = pd.read_csv(ORG_MAP, dtype=str)
    org_map = dict(zip(org["code"], org["org_id"]))
    rows = coverage.to_dict("records")
    print(f"fetch symbols={len(rows)} workers=2", flush=True)
    results = []
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = {pool.submit(fetch_symbol, row, org_map): row["symbol"] for row in rows}
        for i, fut in enumerate(as_completed(futures), start=1):
            result = fut.result()
            results.append(result)
            failures = sum(bool(x.get("error")) for x in result["specs"])
            print(f"{i}/{len(rows)} {result['symbol']} notices={len(result['announcements'])} failed_specs={failures}", flush=True)
    summary = pd.DataFrame([{
        "symbol": x["symbol"],
        "org_id": x["org_id"],
        "announcement_count": len(x["announcements"]),
        "successful_specs": sum(not y["error"] for y in x["specs"]),
        "failed_specs": sum(bool(y["error"]) for y in x["specs"]),
        "errors": " | ".join(y["error"] for y in x["specs"] if y["error"]),
    } for x in results]).sort_values("symbol")
    summary.to_csv(OUT / "cninfo_fetch_summary.csv", index=False, encoding="utf-8-sig")
    print(summary[["successful_specs", "failed_specs"]].sum().to_dict(), flush=True)


if __name__ == "__main__":
    main()
