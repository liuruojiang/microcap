"""Read-only audit of the 2026-09-30 delivery tail against the verified backup."""

from __future__ import annotations

import csv
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
HERE = Path(__file__).resolve().parent
BACKUP = HERE / "pre_refresh_backup" / "outputs"
NAMES = [
    "microcap_top100_mom16_biweekly_live_v2_0_base_panel_refreshed.csv",
    "wind_microcap_top_100_biweekly_thursday_16y_cached.csv",
    "microcap_top100_mom16_hedge_zz1000_0p8x_biweekly_thursday_16y_v2_0_base_costed_nav.csv",
    "microcap_top100_mom16_plain_fixed1_v2_0_costed_nav.csv",
    "microcap_top100_mom16_lb25_hl2p5_r2off_eb0p08_vol10_oh26_recovery20_exec0p8_v2_3_costed_nav.csv",
    "microcap_top100_mom16_lb20_hl3_entry0_exit0_no_targetvol_v2_5_costed_nav.csv",
]
ECONOMIC_FIELDS = {
    "return_net", "nav_net", "return", "holding", "next_holding",
    "total_cost", "entry_exit_cost", "rebalance_cost", "current_execution_scale",
    "microcap_close", "hedge_close", "microcap_ret", "hedge_ret",
}


def read(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    with path.open(encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        return list(reader.fieldnames or []), list(reader)


def main() -> None:
    results = []
    for name in NAMES:
        old_header, old = read(BACKUP / name)
        new_header, new = read(ROOT / "outputs" / name)
        shared = min(len(old), len(new))
        mismatch = next((i for i in range(shared) if old[i] != new[i]), None)
        economic_diff = next(
            ((i, key) for i in range(shared) for key in old_header
             if key in ECONOMIC_FIELDS and old[i][key] != new[i][key]),
            None,
        )
        old_dates = [x["date"] for x in old]
        new_dates = [x["date"] for x in new]
        result = {
            "name": name,
            "old_rows": len(old),
            "new_rows": len(new),
            "old_last_date": old_dates[-1] if old_dates else None,
            "new_last_date": new_dates[-1] if new_dates else None,
            "header_identical": old_header == new_header,
            "old_rows_prefix_identical": mismatch is None and len(old) <= len(new),
            "first_mismatch_row_1based": None if mismatch is None else mismatch + 1,
            "first_mismatch_date": None if mismatch is None else old_dates[mismatch],
            "first_mismatch_fields": None if mismatch is None else {
                key: {"old": old[mismatch][key], "new": new[mismatch][key]}
                for key in old_header if old[mismatch][key] != new[mismatch][key]
            },
            "economic_prefix_identical": economic_diff is None and len(old) <= len(new),
            "first_economic_mismatch": None if economic_diff is None else {
                "date": old_dates[economic_diff[0]], "field": economic_diff[1],
                "old": old[economic_diff[0]][economic_diff[1]],
                "new": new[economic_diff[0]][economic_diff[1]],
            },
            "added_dates": new_dates[len(old):],
        }
        results.append(result)
    exact = all(x["header_identical"] and x["old_rows_prefix_identical"] and x["added_dates"] == ["2026-09-30"] for x in results)
    economic = all(x["header_identical"] and x["economic_prefix_identical"] and x["added_dates"] == ["2026-09-30"] for x in results)
    output = {"status": "exact_prefix_pass" if exact else "economic_prefix_pass_with_other_change" if economic else "economic_prefix_mismatch", "streams": results}
    (HERE / "refresh_prefix_audit.json").write_text(json.dumps(output, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(output, ensure_ascii=False))
    if output["status"] == "economic_prefix_mismatch":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
