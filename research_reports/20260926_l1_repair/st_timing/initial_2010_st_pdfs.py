"""Download near-date issuer PDFs and preserve body snippets/hash for ST triage."""

from __future__ import annotations

import hashlib
import json
import re
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import fitz
import pandas as pd
import requests


OUT = Path(__file__).resolve().parent / "initial_2010_st"
HEADERS = {"User-Agent": "Mozilla/5.0", "Referer": "https://www.cninfo.com.cn/new/index"}


def select(after: bool = False) -> list[dict]:
    selected = {}
    directory = "all_notices_20100115_to_20100228" if after else "all_notices_2009_to_20100114"
    for manifest in (OUT / directory).glob("*/manifest.json"):
        data = json.loads(manifest.read_text(encoding="utf-8"))
        symbol = data["symbol"]
        docs = []
        for item in data["notices"]:
            dt = pd.to_datetime(item["announcementTime"], unit="ms", utc=True).tz_convert("Asia/Shanghai")
            docs.append({"symbol": symbol, "announcement_date": dt.date().isoformat(),
                         "announcementId": str(item["announcementId"]), "title": re.sub(r"<[^>]*>", "", str(item["title"])),
                         "url": "https://static.cninfo.com.cn/" + str(item["adjunctUrl"])})
        docs.sort(key=lambda x: (x["announcement_date"], x["announcementId"]))
        if after:
            for item in docs[:3]:
                selected[(symbol, item["announcementId"])] = item
        else:
            for cutoff in ("2010-01-04", "2010-01-14"):
                eligible = [x for x in docs if x["announcement_date"] <= cutoff]
                for item in eligible[-4:]:
                    selected[(symbol, item["announcementId"])] = item
    return list(selected.values())


def download(item: dict) -> dict:
    directory = OUT / "near_date_pdfs"
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{item['symbol']}_{item['announcementId']}.pdf"
    if path.exists():
        raw = path.read_bytes()
    else:
        response = requests.get(item["url"], headers=HEADERS, timeout=35)
        response.raise_for_status()
        raw = response.content
        assert raw.startswith(b"%PDF")
        path.write_bytes(raw)
    result = dict(item)
    result.update({"sha256": hashlib.sha256(raw).hexdigest(), "bytes": len(raw), "path": str(path),
                   "text_excerpt": "", "header_name_candidates": "", "error": ""})
    try:
        doc = fitz.open(stream=raw, filetype="pdf")
        first = "\n".join(doc[index].get_text() for index in range(min(2, len(doc))))
        result["text_excerpt"] = first[:4500].replace("\n", " ")
        matches = re.findall(r"(?:证券(?:简称|名称)|股票简称)\s*[：:]\s*([^\s，。；;]{2,20})", first)
        result["header_name_candidates"] = "|".join(dict.fromkeys(matches))
    except Exception as exc:
        result["error"] = f"{type(exc).__name__}: {exc}"
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--after", action="store_true")
    args = parser.parse_args()
    items = select(args.after)
    print(f"selected unique PDFs={len(items)}", flush=True)
    rows = []
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = {pool.submit(download, item): item for item in items}
        for i, future in enumerate(as_completed(futures), 1):
            item = futures[future]
            try:
                row = future.result()
            except Exception as exc:
                row = {**item, "sha256": "", "bytes": 0, "path": "", "text_excerpt": "",
                       "header_name_candidates": "", "error": f"{type(exc).__name__}: {exc}"}
            rows.append(row)
            print(f"{i}/{len(items)} {item['symbol']} {item['announcement_date']} {row['header_name_candidates']} {row['error']}", flush=True)
    pd.DataFrame(rows).sort_values(["symbol", "announcement_date", "announcementId"]).to_csv(
        OUT / ("post_date_pdf_evidence.csv" if args.after else "near_date_pdf_evidence.csv"),
        index=False, encoding="utf-8-sig")


if __name__ == "__main__":
    main()
