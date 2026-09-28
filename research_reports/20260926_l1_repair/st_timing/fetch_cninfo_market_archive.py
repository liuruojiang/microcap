"""Read-only CNInfo market-wide yearly ST archive, with page-level raw hashes."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd
import requests


OUT = Path(__file__).resolve().parent
ROOT = OUT.parents[2]
URL = "https://www.cninfo.com.cn/new/hisAnnouncement/query"
HEADERS = {"User-Agent": "Mozilla/5.0", "Referer": "https://www.cninfo.com.cn/new/index"}
SPECS = [
    ("category", "category_tbclts_szsh", ""),
    ("risk", "", "风险警示"),
    ("special", "", "特别处理"),
    ("remove_risk", "", "撤销风险警示"),
    ("remove_special", "", "撤销特别处理"),
    ("remove_hat", "", "摘帽"),
]


def query_page(payload: dict) -> tuple[dict, bytes]:
    last_error = None
    for attempt in range(2):
        try:
            response = requests.post(URL, headers=HEADERS, data=payload, timeout=30)
            response.raise_for_status()
            data = response.json()
            if not isinstance(data, dict) or "totalAnnouncement" not in data:
                raise ValueError("missing totalAnnouncement")
            return data, response.content
        except (requests.RequestException, ValueError) as exc:
            last_error = exc
            if attempt == 0:
                time.sleep(1)
    raise RuntimeError(str(last_error))


def run_spec(year: int, spec: tuple[str, str, str], candidate_symbols: set[str]) -> dict:
    label, category, searchkey = spec
    end = f"{year}-09-24" if year == 2026 else f"{year}-12-31"
    base = {
        "pageNum": "1", "pageSize": "30", "column": "szse", "tabName": "fulltext",
        "plate": "", "stock": "", "searchkey": searchkey, "secid": "",
        "category": category, "trade": "", "seDate": f"{year}-01-01~{end}",
        "sortName": "", "sortType": "", "isHLtitle": "true",
    }
    directory = OUT / "market_archive" / str(year) / label
    directory.mkdir(parents=True, exist_ok=True)
    manifest_path = directory / "manifest.json"
    if manifest_path.exists():
        saved = json.loads(manifest_path.read_text(encoding="utf-8"))
        if saved.get("complete"):
            return saved
    result = {"year": year, "label": label, "category": category, "searchkey": searchkey,
              "query_url": URL, "seDate": base["seDate"], "page_size_requested": 30,
              "totalAnnouncement": None, "page_manifests": [], "candidate_announcements": [], "complete": False}
    first, first_bytes = query_page(base)
    total = int(first.get("totalAnnouncement") or 0)
    result["totalAnnouncement"] = total
    pages = (total + 29) // 30
    if pages == 0:
        result["first_page_sha256"] = hashlib.sha256(first_bytes).hexdigest()
    for page in range(1, pages + 1):
        data, raw = (first, first_bytes) if page == 1 else query_page({**base, "pageNum": str(page)})
        page_path = directory / f"page_{page:04d}.json.gz"
        page_path.write_bytes(gzip.compress(raw, mtime=0))
        announcements = data.get("announcements") or []
        result["page_manifests"].append({
            "page": page, "sha256_raw_response": hashlib.sha256(raw).hexdigest(),
            "response_bytes": len(raw), "row_count": len(announcements),
            "reported_total": int(data.get("totalAnnouncement") or 0),
        })
        for item in announcements:
            symbol = str(item.get("secCode") or "").zfill(6)
            if symbol in candidate_symbols:
                result["candidate_announcements"].append({
                    "symbol": symbol, "announcementId": item.get("announcementId"),
                    "announcementTime": item.get("announcementTime"),
                    "title": item.get("announcementTitle"),
                    "adjunctUrl": item.get("adjunctUrl"),
                })
    result["complete"] = len(result["page_manifests"]) == pages and all(
        x["reported_total"] == total for x in result["page_manifests"]
    ) and sum(x["row_count"] for x in result["page_manifests"]) == total
    manifest_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--years", nargs="+", type=int, required=True)
    args = parser.parse_args()
    members = pd.read_csv(ROOT / "outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_members.csv", dtype={"symbol": str})
    extra = pd.read_csv(ROOT / "research_reports/20260926_l1_repair/data_recovery/coverage_by_symbol.csv", dtype={"symbol": str})
    extra = extra.loc[pd.to_numeric(extra["price_rows"], errors="coerce") > 0]
    candidate_symbols = set(members["symbol"].str.zfill(6)) | set(extra["symbol"].str.zfill(6))
    print(f"candidate symbols={len(candidate_symbols)}", flush=True)
    jobs = [(year, spec) for year in args.years for spec in SPECS]
    results = []
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = {pool.submit(run_spec, year, spec, candidate_symbols): (year, spec[0]) for year, spec in jobs}
        for i, future in enumerate(as_completed(futures), start=1):
            year, label = futures[future]
            try:
                result = future.result()
                results.append(result)
                print(f"{i}/{len(jobs)} {year} {label} total={result['totalAnnouncement']} pages={len(result['page_manifests'])} candidates={len(result['candidate_announcements'])} complete={result['complete']}", flush=True)
            except Exception as exc:
                print(f"{i}/{len(jobs)} {year} {label} ERROR {type(exc).__name__}: {exc}", flush=True)
    summary = [{
        "year": x["year"], "label": x["label"], "totalAnnouncement": x["totalAnnouncement"],
        "pages": len(x["page_manifests"]), "candidate_rows": len(x["candidate_announcements"]),
        "complete": x["complete"],
    } for x in results]
    pd.DataFrame(summary).sort_values(["year", "label"]).to_csv(
        OUT / "market_archive_probe_summary.csv", index=False, encoding="utf-8-sig"
    )


if __name__ == "__main__":
    main()
