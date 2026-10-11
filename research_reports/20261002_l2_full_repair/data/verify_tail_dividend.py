"""Cross-check 2026-09-30 dividend and CNInfo fallback via Sina."""

import csv
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
OUT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "research_reports/20260926_l1_repair/corporate_actions"))
from fetch_sina_distributions import fetch_one  # noqa: E402


def main():
    statuses, events = [], []
    for symbol in ("003017", "600455", "688316"):
        status, rows = fetch_one(symbol, "分红")
        statuses.append(status)
        events.extend(rows)
    path = OUT / "sina_tail_dividend_full_history.csv"
    fields = ["symbol", "indicator", "公告日期", "送股", "转增", "派息", "进度", "除权除息日", "股权登记日", "红股上市日"]
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        w.writerows(events)
    tail = [r for r in events if "2026-09-25" <= str(r.get("除权除息日") or "")[:10] <= "2026-09-30"]
    manifest = {
        "retrieved_utc": datetime.now(timezone.utc).isoformat(),
        "source": "Sina Finance per-symbol dividend table via akshare.stock_history_dividend_detail",
        "statuses": statuses,
        "tail": tail,
        "events_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
    }
    (OUT / "sina_tail_dividend_manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"statuses": statuses, "tail": tail}, ensure_ascii=False))


if __name__ == "__main__":
    main()
