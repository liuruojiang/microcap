"""Research-only 117-stock paper ranking after date-bounded issuer ST exclusions."""

from __future__ import annotations

import csv
import hashlib
import json
from decimal import Decimal
from pathlib import Path


HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[3]
DATA = ROOT / "research_reports/20260926_l1_repair/data_recovery"
OLD_SEED = DATA / "initial_formal_seed_caps.csv"
OLD_TARGET = ROOT / "outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_members.csv"
NEW = DATA / "INITIAL_TWO_DATE_CLOSEOUT.csv"
EXPECTED = {
    OLD_SEED: "e667587245d9944950d858da78763ba6c4d06accdb083b799052828429acd9f4",
    OLD_TARGET: "04a851bb6e7e67f181607c499a0ca3264443b6e3b655817ac0933532517f7517",
    NEW: "18f3609ea6fd0d9c3b8d271ca0cb7374f292d5f7b8a23dced7faa3e0fe5a5189",
}
ST = {
    "2010-01-04": {"600633", "600617", "600792", "600603", "000010", "600080"},
    "2010-01-14": {"600633", "600617", "600792", "600603", "000010", "600080", "600180"},
}


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def main() -> None:
    for path, expected in EXPECTED.items():
        observed = hashlib.sha256(path.read_bytes()).hexdigest()
        assert observed == expected, (path, observed)
    old = {
        "2010-01-04": read_csv(OLD_SEED),
        "2010-01-14": [x for x in read_csv(OLD_TARGET)
                       if x["rebalance_date"] == "2010-01-14"],
    }
    added = read_csv(NEW)
    out = []
    summary = {"scope": "date_bounded_issuer_evidence_117_stock_paper_only",
               "input_sha256": {str(p): h for p, h in EXPECTED.items()}}
    for day, old_rows in old.items():
        assert len(old_rows) == 100
        entries = [{"date": day, "symbol": x["symbol"].zfill(6),
                    "source": "old_100", "old_rank": int(x["rank"]),
                    "market_cap_rmb": Decimal(x["market_cap"])} for x in old_rows]
        old_symbols = {x["symbol"] for x in entries}
        candidates = [x for x in added if x["date"] == day]
        assert len(candidates) == 17
        assert all(x["st_evidence_status"] == "proven_non_ST" for x in candidates)
        entries += [{"date": day, "symbol": x["symbol"],
                     "source": "evidenced_addition", "old_rank": "",
                     "market_cap_rmb": Decimal(x["shares"]) * Decimal(x["raw_close"])}
                    for x in candidates]
        assert len(entries) == len({x["symbol"] for x in entries}) == 117
        base = sorted(entries, key=lambda x: (x["market_cap_rmb"], x["symbol"]))
        removed = sorted((x for x in base if x["symbol"] in ST[day]),
                         key=lambda x: (x["market_cap_rmb"], x["symbol"]))
        assert len(removed) == len(ST[day]) and all(x["source"] == "old_100" for x in removed)
        ranked = [x for x in base if x["symbol"] not in ST[day]]
        assert len(ranked) >= 100
        for rank, row in enumerate(ranked, 1):
            out.append({**row, "rank_after_date_bounded_ST": rank,
                        "in_conditional_top100": rank <= 100})
        top = {x["symbol"] for x in ranked[:100]}
        baseline_top = {x["symbol"] for x in base[:100]}
        summary[day] = {
            "issuer_date_bounded_ST_old_symbols": sorted(ST[day]),
            "baseline_additions_in_117_top100": len(baseline_top - old_symbols),
            "removed_old_ST_in_baseline_top100": sorted(
                baseline_top & ST[day]),
            "old_removed_ST_but_already_outside_baseline_top100": sorted(
                ST[day] - baseline_top),
            "additions_in_conditional_top100": len(top - old_symbols),
            "old_original_symbols_absent": len(old_symbols - top),
            "conditional_cutoff_rmb": str(ranked[99]["market_cap_rmb"]),
            "replacements_entering_vs_baseline": sorted(top - baseline_top),
            "baseline_members_exiting_due_to_ST": sorted(baseline_top - top),
            "external_additions_in_conditional_top100": sorted(top - old_symbols),
            "warning": "Not the complete point-in-time universe or executable NAV.",
        }
    with (HERE / "date_bounded_st_conditional_rank.csv").open(
        "w", encoding="utf-8-sig", newline=""
    ) as handle:
        writer = csv.DictWriter(handle, fieldnames=list(out[0]))
        writer.writeheader()
        for row in out:
            writer.writerow({**row, "market_cap_rmb": f"{row['market_cap_rmb']:.2f}"})
    (HERE / "date_bounded_st_conditional_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps({d: {k: x[k] for k in (
        "additions_in_conditional_top100", "old_original_symbols_absent",
        "conditional_cutoff_rmb", "replacements_entering_vs_baseline")}
        for d, x in summary.items() if d in ST}, ensure_ascii=False))


if __name__ == "__main__":
    main()
