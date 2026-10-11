"""Intersect normalized event candidates with old effective members.

The old path is a coverage lower bound only; a new fixed-share path may differ.
"""

import bisect
import csv
import hashlib
import json
from collections import Counter, defaultdict
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
OUT = Path(__file__).resolve().parent
OLD = ROOT / "research_reports/20260926_l1_repair/full_replay/members_by_rebalance.csv"
SEED = ROOT / "research_reports/20260926_l1_repair/corporate_actions/initial_2010_01_04_executed_seed.csv"
EVENTS = OUT / "event_candidates_2010_20260930.csv"


def read(path):
    with path.open(encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def main():
    by_date = defaultdict(set)
    for r in read(OLD):
        by_date[r["rebalance_date"]].add(r["symbol"].zfill(6))
    by_date["2010-01-04"] = {r["symbol"].zfill(6) for r in read(SEED)}
    dates = sorted(by_date)
    price_dates = {}
    rows = []
    for r in read(EVENTS):
        ex_date = r["ex_date"]
        record_date = r["record_date"]
        i = bisect.bisect_right(dates, ex_date) - 1
        j = bisect.bisect_right(dates, record_date) - 1 if record_date else -1
        held_ex = i >= 0 and r["symbol"] in by_date[dates[i]]
        held_record = j >= 0 and r["symbol"] in by_date[dates[j]]
        if not (held_ex or held_record):
            continue
        symbol = r["symbol"]
        if symbol not in price_dates:
            price_dates[symbol] = {x["date"] for x in read(ROOT / ".microcap_index_cache/prices_raw" / f"{symbol}.csv")}
        rows.append({
            **r,
            "old_held_at_record_snapshot": held_record,
            "old_held_at_ex_snapshot": held_ex,
            "ex_date_raw_close_present": ex_date in price_dates[symbol],
            "record_day_rebalance_ambiguity": record_date in by_date and held_record,
        })
    rows.sort(key=lambda r: (r["ex_date"], r["symbol"]))
    out = OUT / "old_path_event_overlaps.csv"
    with out.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)
    problematic = [r for r in rows if r["old_held_at_record_snapshot"] and
                   (r["source_status"] != "two_source_match" or
                    not r["ex_date_raw_close_present"] or
                    r["event_type"] == "rights_issue")]
    summary = {
        "scope": "old raw execution path only; not new fixed-share actual holdings",
        "event_candidates_in_old_path": len(rows),
        "status_counts": dict(Counter(f"{r['event_type']}:{r['source_status']}" for r in rows)),
        "old_record_qualified_snapshot_count": sum(r["old_held_at_record_snapshot"] for r in rows),
        "old_record_and_ex_raw_missing_count": sum(r["old_held_at_record_snapshot"] and not r["ex_date_raw_close_present"] for r in rows),
        "first_event_in_old_path": rows[0],
        "first_problematic_old_path_event": problematic[0] if problematic else None,
        "input_sha256": {"events": hashlib.sha256(EVENTS.read_bytes()).hexdigest(),
                         "old_members": hashlib.sha256(OLD.read_bytes()).hexdigest(),
                         "initial_seed": hashlib.sha256(SEED.read_bytes()).hexdigest()},
        "output_sha256": hashlib.sha256(out.read_bytes()).hexdigest(),
    }
    (OUT / "old_path_event_overlap_manifest.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    print(json.dumps({k: summary[k] for k in ("event_candidates_in_old_path", "status_counts", "old_record_qualified_snapshot_count", "old_record_and_ex_raw_missing_count", "first_event_in_old_path", "first_problematic_old_path_event")}, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
