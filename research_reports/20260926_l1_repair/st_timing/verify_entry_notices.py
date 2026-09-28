"""Read effective-session wording from official entry notices touching target rows."""

from __future__ import annotations

import hashlib
import json
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd
import pdfplumber
import requests


OUT = Path(__file__).resolve().parent


def work(item: dict) -> dict:
    symbol, start, url = item["symbol"], item.get("entry_notice_date") or item.get("exit_notice_date"), item["source_url"]
    ext = ".pdf" if url.lower().endswith(".pdf") else ".html"
    action = item.get("action", "entry")
    path = OUT / "evidence" / f"{symbol}_{start.replace('-', '')}_{action}{ext}"
    if not path.exists():
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        if ext == ".pdf" and not response.content.startswith(b"%PDF"):
            raise ValueError("non-PDF response for PDF attachment")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(response.content)
    raw = path.read_bytes()
    if ext == ".pdf":
        with pdfplumber.open(path) as pdf:
            body = "\n".join(page.extract_text() or "" for page in pdf.pages)
    else:
        body = re.sub(r"<[^>]+>", " ", raw.decode("utf-8", errors="replace"))
    text_path = path.with_suffix(".txt")
    text_path.write_text(body, encoding="utf-8")
    snippets = []
    for match in re.finditer(r".{0,90}(?:起实施|起实行|起被实施|起实施|起复牌|起恢复|起停牌|起.*?风险警示|证券简称|股票简称).{0,90}", body):
        snippet = re.sub(r"\s+", " ", match.group(0))
        if snippet not in snippets:
            snippets.append(snippet)
    return {**item, "local_path": str(path), "sha256": hashlib.sha256(raw).hexdigest(),
            "bytes": len(raw), "extracted_chars": len(body),
            "candidate_effective_date_snippets": " | ".join(snippets[:12]), "error": ""}


def main() -> None:
    symbols = pd.read_csv(OUT / "strict_symbol_coverage.csv", dtype={"symbol": str})
    targets = pd.read_csv(OUT / "strict_target_status.csv", dtype={"symbol": str})
    notices = pd.read_csv(OUT / "strict_notice_classification.csv", dtype={"symbol": str})
    items = []
    for row in symbols.itertuples(index=False):
        intervals = json.loads(row.strict_intervals)
        target_dates = targets.loc[(targets["symbol"] == row.symbol) &
                                   (targets["strict_title_status"] == "explicit_entry_interior"), "target_date"].tolist()
        for interval in intervals:
            if interval["basis"] != "explicit_entry":
                continue
            selected = [d for d in target_dates if interval["start_notice_date"] < d and
                        (interval["end_notice_date"] is None or d < interval["end_notice_date"])]
            if not selected:
                continue
            match = notices.loc[(notices["symbol"] == row.symbol) &
                                (notices["notice_date"] == interval["start_notice_date"]) &
                                (notices["strict_action"] == "entry")]
            if match.empty:
                continue
            source = match.iloc[0]
            items.append({"symbol": row.symbol, "entry_notice_date": interval["start_notice_date"],
                          "title": source.title, "source_url": source.source_url,
                          "first_target_date": min(selected), "last_target_date": max(selected),
                          "target_rows_inside_interval": len(selected)})
    print(f"entry notices needed={len(items)}", flush=True)
    results = []
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = {pool.submit(work, item): item for item in items}
        for i, fut in enumerate(as_completed(futures), start=1):
            try:
                result = fut.result()
            except Exception as exc:
                result = {**futures[fut], "local_path": "", "sha256": "", "bytes": 0,
                          "extracted_chars": 0, "candidate_effective_date_snippets": "",
                          "error": f"{type(exc).__name__}: {exc}"}
            results.append(result)
            print(f"{i}/{len(items)} {result['symbol']} {result['entry_notice_date']} rows={result['target_rows_inside_interval']} error={bool(result['error'])}", flush=True)
    pd.DataFrame(results).sort_values(["symbol", "entry_notice_date"]).to_csv(
        OUT / "entry_notice_pdf_evidence.csv", index=False, encoding="utf-8-sig"
    )


if __name__ == "__main__":
    main()
