"""Fetch full CNInfo distribution history for saved historical target symbols.

Research evidence only; never writes strategy caches or formal output streams.
"""

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
TARGET = ROOT / "outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_members.csv"
WANTED = ["实施方案公告日期", "分红类型", "送股比例", "转增比例", "派息比例", "股权登记日", "除权日", "派息日", "股份到账日", "实施方案分红说明", "报告时间"]


def fetch_one(symbol: str) -> tuple[dict[str, object], list[dict[str, object]]]:
    started = datetime.now(timezone.utc).isoformat()
    for attempt in range(2):
        try:
            frame = ak.stock_dividend_cninfo(symbol)
            if not isinstance(frame, pd.DataFrame):
                raise TypeError("non-DataFrame response")
            missing = set(WANTED).difference(frame.columns)
            if missing:
                raise ValueError(f"missing columns: {sorted(missing)}")
            frame = frame[WANTED].copy()
            for col in ("实施方案公告日期", "股权登记日", "除权日", "派息日", "股份到账日"):
                frame[col] = pd.to_datetime(frame[col], errors="coerce").dt.strftime("%Y-%m-%d")
            rows = [{"symbol": symbol, **row} for row in frame.to_dict("records")]
            return ({"symbol": symbol, "status": "ok", "rows": len(rows), "retrieved_utc": started}, rows)
        except Exception as exc:
            if attempt == 1:
                return ({"symbol": symbol, "status": "error", "error": f"{type(exc).__name__}: {exc}", "retrieved_utc": started}, [])
            time.sleep(0.5)
    raise AssertionError("unreachable")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbols-csv", type=Path, default=TARGET)
    parser.add_argument("--tag", default="")
    args = parser.parse_args()
    source = pd.read_csv(args.symbols_csv, dtype={"symbol": str})
    if "prices_raw_status" in source.columns and "share_change_status" in source.columns:
        source = source.loc[source["prices_raw_status"].eq("fetched") & source["share_change_status"].eq("fetched")]
    symbols = sorted(source["symbol"].str.zfill(6).unique())
    manifest: list[dict[str, object]] = []
    events: list[dict[str, object]] = []
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = {pool.submit(fetch_one, symbol): symbol for symbol in symbols}
        for i, future in enumerate(as_completed(futures), 1):
            status, rows = future.result()
            manifest.append(status)
            events.extend(rows)
            if i % 100 == 0:
                print(f"completed={i}/{len(symbols)} errors={sum(s['status']=='error' for s in manifest)}", flush=True)
    manifest.sort(key=lambda row: str(row["symbol"]))
    events.sort(key=lambda row: (str(row["symbol"]), str(row.get("除权日") or "")))
    event_path = OUT / f"cninfo_distribution_events{args.tag}.csv"
    pd.DataFrame(events, columns=["symbol", *WANTED]).to_csv(event_path, index=False, encoding="utf-8-sig")
    source_manifest = {
        "source": "CNInfo webapi via akshare.stock_dividend_cninfo; full per-symbol history",
        "source_page": "https://webapi.cninfo.com.cn/#/company",
        "retrieved_utc": datetime.now(timezone.utc).isoformat(),
        "requested_symbols": len(symbols),
        "successful_symbols": sum(x["status"] == "ok" for x in manifest),
        "failed_symbols": sum(x["status"] == "error" for x in manifest),
        "event_rows": len(events),
        "events_sha256": hashlib.sha256(event_path.read_bytes()).hexdigest(),
        "per_symbol": manifest,
    }
    manifest_path = OUT / f"cninfo_source_manifest{args.tag}.json"
    manifest_path.write_text(json.dumps(source_manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"success={source_manifest['successful_symbols']} failed={source_manifest['failed_symbols']} event_rows={len(events)}")


if __name__ == "__main__":
    main()
