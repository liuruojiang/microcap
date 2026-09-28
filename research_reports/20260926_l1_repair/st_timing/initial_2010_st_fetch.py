"""Archive all CNInfo notices for the 29 first-session delisted candidates."""

from __future__ import annotations

import gzip
import hashlib
import json
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd
import requests


OUT = Path(__file__).resolve().parent / "initial_2010_st"
ROOT = OUT.parents[3]
URL = "https://www.cninfo.com.cn/new/hisAnnouncement/query"
HEADERS = {"User-Agent": "Mozilla/5.0", "Referer": "https://www.cninfo.com.cn/new/index"}


def target_symbols() -> tuple[set[str], set[str]]:
    recovery = ROOT / "research_reports/20260926_l1_repair/data_recovery"
    initial = pd.read_csv(recovery / "initial_2010_01_04_candidate_caps.csv", dtype={"symbol": str})
    first = pd.read_csv(recovery / "candidate_cap_comparisons.csv", dtype={"symbol": str})
    a = set(initial.loc[initial["possible_below_rank100"].fillna(False), "symbol"].str.zfill(6))
    b = set(first.loc[(first["rebalance_date"] == "2010-01-14") &
                      first["possible_below_rank100"].fillna(False), "symbol"].str.zfill(6))
    assert len(a) == 24 and len(b) == 27 and len(a | b) == 29
    return a, b


def get_page(payload: dict) -> tuple[dict, bytes]:
    response = requests.post(URL, data=payload, headers=HEADERS, timeout=30)
    response.raise_for_status()
    data = response.json()
    assert isinstance(data, dict) and "totalAnnouncement" in data
    return data, response.content


def fetch(symbol: str, org_id: str, after: bool = False) -> dict:
    directory = OUT / ("all_notices_20100115_to_20100228" if after else "all_notices_2009_to_20100114") / symbol
    directory.mkdir(parents=True, exist_ok=True)
    manifest_path = directory / "manifest.json"
    if manifest_path.exists():
        saved = json.loads(manifest_path.read_text(encoding="utf-8"))
        if saved.get("complete"):
            return saved
    payload = {
        "pageNum": "1", "pageSize": "30",
        "column": "sse" if symbol.startswith("6") else "szse",
        "tabName": "fulltext", "plate": "", "stock": f"{symbol},{org_id}",
        "searchkey": "", "secid": "", "category": "", "trade": "",
        "seDate": "2010-01-15~2010-02-28" if after else "2009-01-01~2010-01-14",
        "sortName": "", "sortType": "", "isHLtitle": "true",
    }
    first, raw_first = get_page(payload)
    total = int(first["totalAnnouncement"] or 0)
    page_count = (total + 29) // 30
    result = {"symbol": symbol, "org_id": org_id, "query_url": URL, "payload": payload,
              "totalAnnouncement": total, "page_count": page_count, "pages": [], "notices": [],
              "zero_result_response_sha256": hashlib.sha256(raw_first).hexdigest() if not page_count else None,
              "complete": False}
    for number in range(1, page_count + 1):
        data, raw = (first, raw_first) if number == 1 else get_page({**payload, "pageNum": str(number)})
        (directory / f"page_{number:04d}.json.gz").write_bytes(gzip.compress(raw, mtime=0))
        items = data.get("announcements") or []
        result["pages"].append({"page": number, "raw_sha256": hashlib.sha256(raw).hexdigest(),
                                "reported_total": int(data["totalAnnouncement"] or 0), "rows": len(items)})
        for item in items:
            result["notices"].append({
                "announcementId": item.get("announcementId"),
                "announcementTime": item.get("announcementTime"),
                "title": item.get("announcementTitle"),
                "adjunctUrl": item.get("adjunctUrl"),
            })
    result["complete"] = sum(x["rows"] for x in result["pages"]) == total and all(
        x["reported_total"] == total for x in result["pages"])
    manifest_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--after", action="store_true")
    args = parser.parse_args()
    initial, first = target_symbols()
    org = pd.read_csv(ROOT / ".microcap_index_cache/cninfo_a_org_map.csv", dtype=str).set_index("code")
    assert all(symbol in org.index for symbol in initial | first)
    results = []
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = {pool.submit(fetch, symbol, org.loc[symbol, "org_id"], args.after): symbol for symbol in sorted(initial | first)}
        for i, future in enumerate(as_completed(futures), 1):
            symbol = futures[future]
            try:
                result = future.result()
                results.append(result)
                print(f"{i}/29 {symbol} notices={result['totalAnnouncement']} complete={result['complete']}", flush=True)
            except Exception as exc:
                print(f"{i}/29 {symbol} ERROR {type(exc).__name__}: {exc}", flush=True)
    rows = [{"symbol": x["symbol"], "initial_2010_01_04": x["symbol"] in initial,
             "first_2010_01_14": x["symbol"] in first, "notices": x["totalAnnouncement"],
             "pages": x["page_count"], "complete": x["complete"]} for x in results]
    pd.DataFrame(rows).sort_values("symbol").to_csv(
        OUT / ("fetch_after_summary.csv" if args.after else "fetch_summary.csv"), index=False, encoding="utf-8-sig")


if __name__ == "__main__":
    main()
