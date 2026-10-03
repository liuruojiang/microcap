"""Rank the old 100 plus the 17 independently evidenced omissions.

Research-only conditional scenario. It is not a rebuilt historical Top100.
The source hash guards keep this run tied to the reviewed September 2026 inputs.
"""

from __future__ import annotations

import csv
import hashlib
import json
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path


HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
INPUTS = {
    "old_initial": (
        HERE / "initial_formal_seed_caps.csv",
        "e667587245d9944950d858da78763ba6c4d06accdb083b799052828429acd9f4",
    ),
    "old_first_target": (
        ROOT / "outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_members.csv",
        "04a851bb6e7e67f181607c499a0ca3264443b6e3b655817ac0933532517f7517",
    ),
    "new_candidates": (
        HERE / "INITIAL_TWO_DATE_CLOSEOUT.csv",
        "18f3609ea6fd0d9c3b8d271ca0cb7374f292d5f7b8a23dced7faa3e0fe5a5189",
    ),
}
CENT = Decimal("0.01")


def rows(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def money(value: str) -> Decimal:
    return Decimal(value).quantize(CENT, rounding=ROUND_HALF_UP)


def main() -> None:
    hashes = {}
    for label, (path, expected_hash) in INPUTS.items():
        actual_hash = hashlib.sha256(path.read_bytes()).hexdigest()
        if actual_hash != expected_hash:
            raise ValueError(f"{label} source changed: {actual_hash}")
        hashes[label] = actual_hash

    old_initial = rows(INPUTS["old_initial"][0])
    old_target = [
        row for row in rows(INPUTS["old_first_target"][0])
        if row["rebalance_date"] == "2010-01-14"
    ]
    new = rows(INPUTS["new_candidates"][0])
    source_old = {
        "2010-01-04": old_initial,
        "2010-01-14": old_target,
    }
    output = []
    summary = {"scope": "conditional_117_symbol_paper_ranking_only", "input_sha256": hashes}

    for date, old in source_old.items():
        assert len(old) == 100
        assert sorted(int(row["rank"]) for row in old) == list(range(1, 101))
        old_symbols = {row["symbol"].zfill(6) for row in old}
        assert len(old_symbols) == 100
        old_entries = [
            {
                "date": date,
                "symbol": row["symbol"].zfill(6),
                "source": "old_100",
                "market_cap_rmb": money(row["market_cap"]),
                "old_rank": int(row["rank"]),
            }
            for row in old
        ]
        added = [row for row in new if row["date"] == date]
        assert len(added) == 17
        added_entries = []
        for row in added:
            assert row["st_evidence_status"] == "proven_non_ST"
            assert row["symbol"] not in old_symbols
            calculated_cap = Decimal(row["shares"]) * Decimal(row["raw_close"])
            assert calculated_cap == money(row["market_cap_rmb"])
            added_entries.append(
                {
                    "date": date,
                    "symbol": row["symbol"],
                    "source": "evidenced_addition",
                    "market_cap_rmb": calculated_cap,
                    "old_rank": "",
                }
            )
        old_cutoff = max(row["market_cap_rmb"] for row in old_entries)
        assert all(row["market_cap_rmb"] < old_cutoff for row in added_entries)
        combined = sorted(
            old_entries + added_entries,
            key=lambda row: (row["market_cap_rmb"], row["symbol"]),
        )
        assert len(combined) == 117
        for rank, row in enumerate(combined, 1):
            output.append(
                {
                    **row,
                    "conditional_rank": rank,
                    "conditional_top100": rank <= 100,
                }
            )
        added_in = [r["symbol"] for r in combined[:100] if r["source"] == "evidenced_addition"]
        added_out = [r["symbol"] for r in combined[100:] if r["source"] == "evidenced_addition"]
        old_out = [r["symbol"] for r in combined[100:] if r["source"] == "old_100"]
        assert len(added_in) == len(old_out)
        summary[date] = {
            "old_cutoff_rmb": str(old_cutoff),
            "conditional_cutoff_rmb": str(combined[99]["market_cap_rmb"]),
            "evidenced_additions_below_old_cutoff": len(added),
            "additions_in_conditional_top100": len(added_in),
            "additions_outside_conditional_top100": added_out,
            "displaced_old_symbols": old_out,
            "warning": "Other omitted stocks or invalid old members may change this set.",
        }

    out_csv = HERE / "conditional_two_date_rank.csv"
    with out_csv.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=output[0].keys())
        writer.writeheader()
        for row in output:
            writer.writerow({**row, "market_cap_rmb": f"{row['market_cap_rmb']:.2f}"})
    (HERE / "conditional_two_date_rank_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
