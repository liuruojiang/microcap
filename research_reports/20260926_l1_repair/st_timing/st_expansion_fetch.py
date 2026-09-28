"""Freeze and fetch all CNInfo ST-title leads for top 30 unproved old targets."""

from __future__ import annotations

import hashlib
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import fitz
import pandas as pd
import requests
from bs4 import BeautifulSoup


OUT = Path(__file__).resolve().parent
ROOT = OUT.parents[2]
DEST = OUT / "expansion_top30"
HEADERS = {"User-Agent": "Mozilla/5.0", "Referer": "https://www.cninfo.com.cn/new/index"}


def freeze() -> pd.DataFrame:
    members = pd.read_csv(ROOT / "outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_members.csv", dtype={"symbol": str})
    titles = pd.read_csv(OUT / "market_archive_title_triage.csv", dtype={"symbol": str, "announcementId": str}).fillna("")
    proved = pd.read_csv(OUT / "certified_st_target_lower_bound.csv", dtype={"symbol": str})
    candidates = set(titles.loc[titles["title_class"].isin(["entry", "remain_st", "needs_review"]), "symbol"]) - set(proved["symbol"])
    rank = members.loc[members["symbol"].isin(candidates)].groupby("symbol").agg(
        old_target_rows=("rebalance_date", "size"), first_target_date=("rebalance_date", "min"),
        last_target_date=("rebalance_date", "max")).reset_index()
    rank = rank.sort_values(["old_target_rows", "symbol"], ascending=[False, True]).head(30)
    assert len(rank) == 30 and rank["old_target_rows"].sum() == 4421
    rank.insert(0, "priority", range(1, 31))
    rank.to_csv(DEST / "frozen_top30.csv", index=False, encoding="utf-8-sig")
    selected = titles.loc[titles["symbol"].isin(rank["symbol"])].copy()
    assert len(selected) == 262
    selected.to_csv(DEST / "frozen_notice_leads.csv", index=False, encoding="utf-8-sig")
    return selected


def fetch(row: dict) -> dict:
    symbol = row["symbol"]
    notice_id = str(row["announcementId"])
    url = row["source_url"]
    directory = DEST / "pdfs" / symbol
    directory.mkdir(parents=True, exist_ok=True)
    is_html = url.lower().endswith((".html", ".htm"))
    source_path = directory / f"{notice_id}.{'html' if is_html else 'pdf'}"
    if source_path.exists():
        raw = source_path.read_bytes()
    else:
        response = requests.get(url, headers=HEADERS, timeout=45)
        response.raise_for_status()
        raw = response.content
        if not is_html and not raw.startswith(b"%PDF"):
            raise ValueError("unexpected non-PDF response")
        source_path.write_bytes(raw)
    digest = hashlib.sha256(raw).hexdigest()
    if is_html:
        decoded = raw.decode("gbk", errors="replace")
        body = BeautifulSoup(decoded, "html.parser").get_text(" ", strip=True)
        page_count = 1
    else:
        doc = fitz.open(stream=raw, filetype="pdf")
        body = "\n".join(doc[i].get_text() for i in range(len(doc)))
        page_count = len(doc)
    text_path = directory / f"{notice_id}.txt"
    text_path.write_text(body, encoding="utf-8")
    return {"symbol": symbol, "announcementId": notice_id, "year": row["year"],
            "title": row["title"], "title_class": row["title_class"],
            "announcementTime": row["announcementTime"], "source_url": url,
            "sha256": digest, "bytes": len(raw), "pages": page_count, "format": "html" if is_html else "pdf",
            "extracted_chars": len(body), "local_source_path": str(source_path),
            "local_text_path": str(text_path), "error": ""}


def main() -> None:
    DEST.mkdir(parents=True, exist_ok=True)
    selected = freeze()
    rows = selected.to_dict("records")
    results = []
    with ThreadPoolExecutor(max_workers=3) as pool:
        futures = {pool.submit(fetch, row): row for row in rows}
        for i, future in enumerate(as_completed(futures), 1):
            row = futures[future]
            try:
                result = future.result()
            except Exception as exc:
                result = {**row, "sha256": "", "bytes": 0, "pages": 0, "format": "",
                          "extracted_chars": 0, "local_source_path": "", "local_text_path": "",
                          "error": f"{type(exc).__name__}: {exc}"}
            results.append(result)
            print(f"{i}/262 {row['symbol']} {row['announcementId']} {result['error']}", flush=True)
    pd.DataFrame(results).sort_values(["symbol", "announcementTime", "announcementId"]).to_csv(
        DEST / "notice_pdf_manifest.csv", index=False, encoding="utf-8-sig")
    summary = {"symbols": 30, "old_target_rows": 4421, "notice_leads": len(results),
               "source_fetched": sum(not x["error"] for x in results),
               "pdf_fetched": sum(x.get("format") == "pdf" and not x["error"] for x in results),
               "html_fetched": sum(x.get("format") == "html" and not x["error"] for x in results),
               "failed": [x for x in results if x["error"]]}
    (DEST / "fetch_summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2), flush=True)


if __name__ == "__main__":
    main()
