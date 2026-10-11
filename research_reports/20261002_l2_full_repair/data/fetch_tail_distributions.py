"""Refresh CNInfo implementation history for the latest 100 old effective members."""

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
from fetch_cninfo_distributions import WANTED, fetch_one  # noqa: E402


def main() -> None:
    src = ROOT / "outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_effective_members.csv"
    with src.open(encoding="utf-8-sig", newline="") as f:
        members = list(csv.DictReader(f))
    last = members[-1]["as_of_date"]
    symbols = sorted({r["symbol"].zfill(6) for r in members if r["as_of_date"] == last})
    responses, events = [], []
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = {pool.submit(fetch_one, s): s for s in symbols}
        for i, future in enumerate(as_completed(futures), 1):
            status, rows = future.result()
            responses.append(status)
            events.extend(rows)
            if i % 50 == 0:
                print(f"completed={i}/{len(symbols)} errors={sum(r['status'] != 'ok' for r in responses)}", flush=True)
    responses.sort(key=lambda r: r["symbol"])
    events.sort(key=lambda r: (r["symbol"], str(r.get("除权日") or "")))
    path = OUT / "latest_old_members_cninfo_full_history.csv"
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["symbol", *WANTED], extrasaction="ignore")
        w.writeheader()
        w.writerows(events)
    tail = [r for r in events if "2026-09-25" <= str(r.get("除权日") or "")[:10] <= "2026-09-30"]
    manifest = {
        "source": "CNInfo webapi via akshare.stock_dividend_cninfo",
        "retrieved_utc": datetime.now(timezone.utc).isoformat(),
        "effective_member_date": last,
        "requested_symbols": len(symbols),
        "successful_jobs": sum(r["status"] == "ok" for r in responses),
        "failed_jobs": sum(r["status"] != "ok" for r in responses),
        "all_history_rows": len(events),
        "implementation_rows_ex_date_20260925_30": len(tail),
        "implementation_tail": tail,
        "source_member_sha256": hashlib.sha256(src.read_bytes()).hexdigest(),
        "events_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        "per_symbol": responses,
    }
    (OUT / "latest_old_members_cninfo_manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    print({k: manifest[k] for k in ("requested_symbols", "successful_jobs", "failed_jobs", "implementation_rows_ex_date_20260925_30")})


if __name__ == "__main__":
    main()
