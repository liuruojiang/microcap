"""Priority screen only: old member snapshots x normalized single-source events."""

import csv
import hashlib
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import baostock as bs

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
DATA = ROOT / "research_reports/20261002_l2_full_repair/data"
OVERLAPS = DATA / "old_path_event_overlaps.csv"
EVENTS = DATA / "event_candidates_2010_20260930.csv"


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    overlaps = list(csv.DictReader(OVERLAPS.open(encoding="utf-8-sig", newline="")))
    events = list(csv.DictReader(EVENTS.open(encoding="utf-8-sig", newline="")))
    source_events = [e for e in events if "2015-01-01" <= e["ex_date"] <= "2026-09-30" and e["source_status"] == "single_source_only"]
    snapshot = [e for e in overlaps if "2015-01-01" <= e["ex_date"] <= "2026-09-30" and e["source_status"] == "single_source_only"]
    missing_raw = [e for e in snapshot if e["ex_date_raw_close_present"] == "False"]
    assert len(snapshot) == 543 and len(missing_raw) == 17
    assert all(e["old_held_at_record_snapshot"] == "True" for e in missing_raw)

    login = bs.login()
    assert login.error_code == "0", login.error_msg
    try:
        screened = []
        for e in missing_raw:
            code = ("sh." if e["symbol"].startswith("6") else "sz.") + e["symbol"]
            response = bs.query_history_k_data_plus(
                code, "date,close,preclose,volume,tradestatus",
                start_date=e["ex_date"], end_date=e["ex_date"],
                frequency="d", adjustflag="3",
            )
            assert response.error_code == "0", (code, response.error_msg)
            found = []
            while response.next():
                found.append(response.get_row_data())
            assert len(found) == 1, (code, e["ex_date"], found)
            date, close, preclose, volume, status = found[0]
            assert date == e["ex_date"]
            screened.append({
                "symbol": e["symbol"], "record_date": e["record_date"],
                "ex_date": e["ex_date"], "bonus_per_10": e["bonus_per_10"],
                "cash_per_10_gross": e["cash_per_10_gross"],
                "old_held_at_record_snapshot": e["old_held_at_record_snapshot"],
                "record_day_rebalance_ambiguity": e["record_day_rebalance_ambiguity"],
                "vendor_unadjusted_close": close, "vendor_preclose": preclose,
                "vendor_volume": volume, "vendor_tradestatus": status,
                "vendor_zero_volume_suspended": str(volume == "0" and status == "0"),
            })
    finally:
        bs.logout()
    assert all(row["vendor_zero_volume_suspended"] == "True" for row in screened)

    output = HERE / "single_source_suspended_old_snapshot_priority.csv"
    with output.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, screened[0])
        writer.writeheader()
        writer.writerows(screened)
    manifest = {
        "queried_utc": datetime.now(timezone.utc).isoformat(),
        "scope": "2015-01-01..2026-09-30, normalized events intersected with old effective member snapshots; priority screen only, not actual post-repair account holdings or issuer certification",
        "normalized_all_single_source_events": len(source_events),
        "old_snapshot_single_source_overlaps": len(snapshot),
        "old_snapshot_single_source_missing_raw_close": len(missing_raw),
        "vendor_zero_volume_suspended_confirmed": len(screened),
        "year_counts": dict(sorted(Counter(row["ex_date"][:4] for row in screened).items())),
        "distinct_symbols": len(set(row["symbol"] for row in screened)),
        "input_sha256": {str(EVENTS.relative_to(ROOT)): sha(EVENTS), str(OVERLAPS.relative_to(ROOT)): sha(OVERLAPS)},
        "output_sha256": sha(output),
        "limits": [
            "Only old member snapshot intersections are counted; a corrected account may hold different stocks.",
            "A missing raw bar and vendor zero-volume status prioritize research but do not certify the issuer event or valuation reference.",
            "The count is not a count of future engine blocks; branch holdings, tax, settlement, and other causes differ.",
        ],
    }
    (HERE / "single_source_suspended_old_snapshot_priority_manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps(manifest, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
