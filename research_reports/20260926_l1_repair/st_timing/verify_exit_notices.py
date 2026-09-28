"""Inspect official removal notices following title-confirmed target overlap."""

from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd

from verify_entry_notices import OUT, work


def main() -> None:
    symbols = pd.read_csv(OUT / "strict_symbol_coverage.csv", dtype={"symbol": str})
    targets = pd.read_csv(OUT / "strict_target_status.csv", dtype={"symbol": str})
    notices = pd.read_csv(OUT / "strict_notice_classification.csv", dtype={"symbol": str})
    items = []
    for row in symbols.itertuples(index=False):
        target_dates = targets.loc[(targets["symbol"] == row.symbol) &
                                   (targets["strict_title_status"] == "explicit_entry_interior"), "target_date"].tolist()
        for interval in json.loads(row.strict_intervals):
            end = interval["end_notice_date"]
            if interval["basis"] != "explicit_entry" or end is None:
                continue
            selected = [d for d in target_dates if interval["start_notice_date"] < d < end]
            if not selected:
                continue
            match = notices.loc[(notices["symbol"] == row.symbol) &
                                (notices["notice_date"] == end) &
                                (notices["strict_action"] == "exit")]
            if match.empty:
                continue
            source = match.iloc[0]
            items.append({"symbol": row.symbol, "exit_notice_date": end, "action": "exit",
                          "title": source.title, "source_url": source.source_url,
                          "first_target_date": min(selected), "last_target_date": max(selected),
                          "target_rows_inside_interval": len(selected)})
    results = []
    print(f"exit notices needed={len(items)}", flush=True)
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
            print(f"{i}/{len(items)} {result['symbol']} {result['exit_notice_date']} error={bool(result['error'])}", flush=True)
    pd.DataFrame(results).sort_values(["symbol", "exit_notice_date"]).to_csv(
        OUT / "exit_notice_pdf_evidence.csv", index=False, encoding="utf-8-sig"
    )


if __name__ == "__main__":
    main()
