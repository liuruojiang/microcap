"""Fail-closed coverage scan for the frozen L1 paper ranking inputs.

This script classifies evidence coverage. It never constructs a corrected
Top100, trades, returns, or NAV. Every input hash is pinned to the 2026-09-24
L1 freeze; a later formal delivery cannot be silently mixed into this scan.
"""

from __future__ import annotations

import csv
import hashlib
import json
from collections import Counter, defaultdict
from pathlib import Path


HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
INPUTS = {
    "old_targets": (ROOT / "outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_members.csv", "04a851bb6e7e67f181607c499a0ca3264443b6e3b655817ac0933532517f7517"),
    "old_initial": (HERE.parent / "data_recovery/initial_formal_seed_caps.csv", "e667587245d9944950d858da78763ba6c4d06accdb083b799052828429acd9f4"),
    "initial_delisted_cap_probe": (HERE.parent / "data_recovery/initial_2010_01_04_candidate_caps.csv", "e115892df1361b190560956462dd162898b0eab8e567b35f13751f31de027176"),
    "delisted_cap_probe": (HERE.parent / "data_recovery/candidate_cap_comparisons.csv", "914e16f122aab7ffe20751564337df92eeab424c604ef7dc683715ea635c2b65"),
    "two_date_closeout": (HERE.parent / "data_recovery/INITIAL_TWO_DATE_CLOSEOUT.csv", "18f3609ea6fd0d9c3b8d271ca0cb7374f292d5f7b8a23dced7faa3e0fe5a5189"),
    "old_st_proof": (HERE.parent / "st_timing/certified_st_target_lower_bound.csv", "7c0f0cab78c93e70cf3c262c258b53e9740ac344a3fe66d7949927480e568da0"),
    "old_st_proof_expansion": (HERE.parent / "st_timing/expansion_top30/new_proven_bad_targets.csv", "08c960ecfa59817c6610ee7191c48fa20d8ebc34e69a1736bab9bac05bf1ca5c"),
}


def read_rows(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8-sig", newline="") as source:
        return list(csv.DictReader(source))


def main() -> None:
    hashes = {}
    for name, (path, expected) in INPUTS.items():
        actual = hashlib.sha256(path.read_bytes()).hexdigest()
        if actual != expected:
            raise RuntimeError(f"Frozen L1 input changed: {name}: {actual}")
        hashes[name] = actual

    targets = read_rows(INPUTS["old_targets"][0])
    initial = read_rows(INPUTS["old_initial"][0])
    initial_probes = read_rows(INPUTS["initial_delisted_cap_probe"][0])
    probes = read_rows(INPUTS["delisted_cap_probe"][0])
    closeout = read_rows(INPUTS["two_date_closeout"][0])
    st_rows = read_rows(INPUTS["old_st_proof"][0]) + read_rows(INPUTS["old_st_proof_expansion"][0])

    target_dates = sorted({row["rebalance_date"] for row in targets})
    assert len(target_dates) == 432 and target_dates[0] == "2010-01-14"
    assert len(initial) == 100 and len(targets) == 432 * 100
    initial_possible_missing = sum(row["possible_below_rank100"] == "True" for row in initial_probes)
    assert initial_possible_missing == 24
    assert all(sum(row["rebalance_date"] == date for row in targets) == 100 for date in target_dates)
    old_st = defaultdict(set)
    for row in st_rows:
        old_st[row["target_date"]].add(row["symbol"].zfill(6))
    assert sum(map(len, old_st.values())) == 287

    probe_count = Counter()
    possible_missing = Counter()
    for row in probes:
        date = row["rebalance_date"]
        probe_count[date] += 1
        if row["possible_below_rank100"] == "True":
            possible_missing[date] += 1
    # The last 11 rebalance dates have no still-listed symbols in this
    # delisted-only probe. Absence from this file is not full-pool certification.
    assert set(probe_count).issubset(set(target_dates))
    assert sum(possible_missing.values()) == 8998

    evidenced_missing = Counter(row["date"] for row in closeout)
    assert evidenced_missing == {"2010-01-04": 17, "2010-01-14": 17}
    dates = ["2010-01-04", *target_dates]
    out = []
    for date in dates:
        formal = date != "2010-01-04"
        reasons = ["historical_security_master_ST_and_share_events_not_exhaustively_certified"]
        if date == "2010-01-04":
            reasons += ["600137_historical_ST_daily_status_not_closed", "conditional_117_rank_not_final_top100"]
        if date == "2010-01-14":
            reasons.append("conditional_117_rank_not_final_top100")
        if old_st[date]:
            reasons.append("officially_proven_ST_in_old_target")
        if possible_missing[date]:
            reasons.append("delisted_candidates_below_old_cutoff_require_PIT_adjudication")
        out.append({
            "date": date,
            "kind": "formal_rebalance" if formal else "initial_executed_seed",
            "old_rows": 100,
            "delisted_symbols_probed": probe_count[date] if formal else "",
            "delisted_below_old_cutoff_raw_probe": possible_missing[date] if formal else initial_possible_missing,
            "evidenced_non_ST_below_old_cutoff": evidenced_missing[date],
            "officially_proven_ST_old_target": len(old_st[date]),
            "certified_full_pool": "False",
            "certified_corrected_top100": "False",
            "block_reasons": "|".join(reasons),
        })

    path = HERE / "per_date_gate.csv"
    with path.open("w", encoding="utf-8-sig", newline="") as target:
        writer = csv.DictWriter(target, fieldnames=list(out[0]))
        writer.writeheader()
        writer.writerows(out)
    summary = {
        "scope": "frozen_2026-09-24_L1_evidence_coverage_only",
        "input_sha256": hashes,
        "formal_rebalance_dates": len(target_dates),
        "initial_seed_dates": 1,
        "dates_with_certified_full_candidate_pool": 0,
        "dates_with_certified_corrected_top100": 0,
        "first_blocked_date": dates[0],
        "first_blocked_formal_rebalance": target_dates[0],
        "possible_delisted_below_old_cutoff_probe_rows": sum(possible_missing.values()),
        "possible_delisted_below_old_cutoff_probe_dates": len(possible_missing),
        "formal_dates_with_any_delisted_probe_rows": len(probe_count),
        "officially_proven_ST_old_target_rows": sum(map(len, old_st.values())),
        "limits": [
            "Zero certified dates means missing exhaustive proof, not proof that every date's ranking differs.",
            "Raw delisted-cap probes are not proven eligible constituents.",
            "The 17/17 two-date closeout is source-limited and conditional, not a final Top100.",
            "No corrected holdings, returns, NAV, costs, or performance were calculated.",
        ],
    }
    (HERE / "summary.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
