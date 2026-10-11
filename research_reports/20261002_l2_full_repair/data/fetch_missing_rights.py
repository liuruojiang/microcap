"""Fetch the not-yet-queried old-member rights issue histories from public Sina.

Writes only this diagnostic directory and preserves per-symbol request outcomes.
"""

from __future__ import annotations

import csv
import hashlib
import json
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
OUT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "research_reports/20260926_l1_repair/corporate_actions"))
from fetch_sina_distributions import fetch_one  # noqa: E402


def main() -> None:
    with (OUT / "old_member_source_coverage.csv").open(encoding="utf-8", newline="") as f:
        symbols = [r["symbol"] for r in csv.DictReader(f) if r["sina_rights_job"] != "ok"]
    responses = []
    events = []
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = {pool.submit(fetch_one, s, "配股"): s for s in symbols}
        for i, future in enumerate(as_completed(futures), 1):
            status, rows = future.result()
            responses.append(status)
            events.extend(rows)
            if i % 100 == 0:
                print(f"completed={i}/{len(symbols)} errors={sum(r['status'] != 'ok' for r in responses)}", flush=True)
    responses.sort(key=lambda r: r["symbol"])
    events.sort(key=lambda r: (r["symbol"], str(r.get("除权日", ""))))
    event_path = OUT / "sina_missing_old_member_rights.csv"
    fields = ["symbol", "indicator", "公告日期", "配股方案", "配股价格", "基准股本", "除权日", "股权登记日", "缴款起始日", "缴款终止日", "配股上市日", "募集资金合计"]
    with event_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        w.writerows(events)
    manifest = {
        "source": "Sina Finance per-symbol historical rights issue table via akshare.stock_history_dividend_detail",
        "retrieved_utc": datetime.now(timezone.utc).isoformat(),
        "requested_symbols": len(symbols),
        "successful_jobs": sum(r["status"] == "ok" for r in responses),
        "failed_jobs": sum(r["status"] != "ok" for r in responses),
        "event_rows": len(events),
        "events_sha256": hashlib.sha256(event_path.read_bytes()).hexdigest(),
        "per_symbol_indicator": responses,
    }
    (OUT / "sina_missing_old_member_rights_manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    print({k: manifest[k] for k in ("requested_symbols", "successful_jobs", "failed_jobs", "event_rows")})


if __name__ == "__main__":
    main()
