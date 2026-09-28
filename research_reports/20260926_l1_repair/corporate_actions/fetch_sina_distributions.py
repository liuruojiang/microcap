"""Read-only Sina distribution/rights history fallback for recovered delisted names."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import argparse
import hashlib
import json
from pathlib import Path
import time

import akshare as ak
import pandas as pd


ROOT = Path(__file__).resolve().parents[3]
OUT = Path(__file__).resolve().parent
SOURCE_MANIFEST = ROOT / "research_reports/20260926_l1_repair/data_recovery/source_manifest_both.csv"


def fetch_one(symbol: str, indicator: str) -> tuple[dict[str, object], list[dict[str, object]]]:
    started = datetime.now(timezone.utc).isoformat()
    for attempt in range(2):
        try:
            frame = ak.stock_history_dividend_detail(symbol=symbol, indicator=indicator)
            if not isinstance(frame, pd.DataFrame):
                raise TypeError("non-DataFrame response")
            rows: list[dict[str, object]] = []
            for record in frame.to_dict("records"):
                row = {"symbol": symbol, "indicator": indicator}
                for key, value in record.items():
                    row[key] = "" if pd.isna(value) else str(value)
                rows.append(row)
            return ({"symbol": symbol, "indicator": indicator, "status": "ok", "rows": len(rows), "retrieved_utc": started,
                     "source_url": f"https://vip.stock.finance.sina.com.cn/corp/go.php/vISSUE_ShareBonus/stockid/{symbol}.phtml"}, rows)
        except Exception as exc:
            if attempt == 1:
                return ({"symbol": symbol, "indicator": indicator, "status": "error", "error": f"{type(exc).__name__}: {exc}", "retrieved_utc": started}, [])
            time.sleep(0.5)
    raise AssertionError("unreachable")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbols-csv", type=Path, default=SOURCE_MANIFEST)
    parser.add_argument("--tag", default="_recovered_delisted")
    parser.add_argument("--indicator", choices=("both", "分红", "配股"), default="both")
    args = parser.parse_args()
    source = pd.read_csv(args.symbols_csv, dtype={"symbol": str})
    if "prices_raw_status" in source.columns and "share_change_status" in source.columns:
        source = source.loc[source["prices_raw_status"].eq("fetched") & source["share_change_status"].eq("fetched")]
        symbols = sorted(set(source["symbol"].str.zfill(6)) | {"600455", "688316"})
    else:
        symbols = sorted(set(source["symbol"].str.zfill(6)))
    indicators = ("分红", "配股") if args.indicator == "both" else (args.indicator,)
    jobs = [(symbol, indicator) for symbol in symbols for indicator in indicators]
    manifest: list[dict[str, object]] = []
    events: list[dict[str, object]] = []
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = {pool.submit(fetch_one, symbol, indicator): (symbol, indicator) for symbol, indicator in jobs}
        for i, future in enumerate(as_completed(futures), 1):
            status, rows = future.result()
            manifest.append(status)
            events.extend(rows)
            if i % 100 == 0:
                print(f"completed={i}/{len(jobs)} errors={sum(s['status']=='error' for s in manifest)}", flush=True)
    manifest.sort(key=lambda row: (str(row["symbol"]), str(row["indicator"])))
    events.sort(key=lambda row: (str(row["symbol"]), str(row["indicator"]), str(row.get("除权除息日") or row.get("除权日") or "")))
    event_path = OUT / f"sina{args.tag}_distribution_and_rights.csv"
    pd.DataFrame(events).to_csv(event_path, index=False, encoding="utf-8-sig")
    meta = {
        "source": "Sina Finance per-symbol historical dividend and rights tables via akshare.stock_history_dividend_detail",
        "retrieved_utc": datetime.now(timezone.utc).isoformat(),
        "symbols": len(symbols),
        "jobs": len(jobs),
        "successful_jobs": sum(x["status"] == "ok" for x in manifest),
        "failed_jobs": sum(x["status"] == "error" for x in manifest),
        "event_rows": len(events),
        "events_sha256": hashlib.sha256(event_path.read_bytes()).hexdigest(),
        "per_symbol_indicator": manifest,
    }
    path = OUT / f"sina{args.tag}_source_manifest.json"
    path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"success={meta['successful_jobs']} failed={meta['failed_jobs']} rows={len(events)}")


if __name__ == "__main__":
    main()
