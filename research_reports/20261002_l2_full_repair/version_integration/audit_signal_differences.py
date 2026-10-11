"""Audit new-proxy signal changes against sliced and historical old signals.

Old streams are read only here, after independent new-signal construction.
They are comparison controls and are never fed into the new signal output.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from rebuild_recent_signals import HERE, OLD_NAV, sha256, v20_module_signal, v23, v23_overheat_next, v25

ROOT = Path(__file__).resolve().parents[3]
OLD_STREAMS = {
    "v2.0": OLD_NAV,
    "v2.3": ROOT / "outputs/microcap_top100_mom16_lb25_hl2p5_r2off_eb0p08_vol10_oh26_recovery20_exec0p8_v2_3_costed_nav.csv",
    "v2.5": ROOT / "outputs/microcap_top100_mom16_lb20_hl3_entry0_exit0_no_targetvol_v2_5_costed_nav.csv",
}


def run(branch: str) -> dict:
    fresh_path = HERE / f"recent_{branch}_rebuilt_signals.csv"
    fresh = pd.read_csv(fresh_path, parse_dates=["date"]).set_index("date").sort_index()
    old_close = pd.read_csv(OLD_NAV, usecols=["date", "microcap_close", "hedge_close"], parse_dates=["date"])
    old_close = old_close.set_index("date").sort_index()
    old_close = old_close.loc[fresh.index, ["microcap_close", "hedge_close"]]
    old_close.columns = ["microcap", "hedge"]
    old_sliced_20 = v20_module_signal(old_close)
    old_sliced_23 = v23.build_spread_log_wls_gross(old_close)
    old_sliced_25 = v25.build_microcap_log_wls_gross(old_close)
    sliced = {
        "v2.0": old_sliced_20.next_holding.ne("cash"),
        "v2.3": v23_overheat_next(old_sliced_23),
        "v2.5": old_sliced_25.next_holding.ne("cash"),
    }
    comparison = []
    summary = {"branch": branch, "new_signal_sha256": sha256(fresh_path),
               "start": str(fresh.index.min().date()), "end": str(fresh.index.max().date()),
               "versions": {}}
    for version, path in OLD_STREAMS.items():
        historical = pd.read_csv(path, usecols=["date", "next_holding"], parse_dates=["date"])
        historical = historical.drop_duplicates("date", keep="last").set_index("date").sort_index()
        historical = historical.next_holding.ne("cash")
        prefix = version.replace(".", "_")
        new_signal = fresh[prefix + "_signal_at_close"].dropna().astype(bool)
        common = new_signal.index.intersection(sliced[version].index).intersection(historical.index)
        changed_new = 0
        changed_sliced = 0
        first = None
        classifications: dict[str, int] = {}
        for date in common:
            new, slice_old, old = bool(new_signal.loc[date]), bool(sliced[version].loc[date]), bool(historical.loc[date])
            if new != old:
                if slice_old == old:
                    category = "new_stock_proxy_path"
                elif slice_old == new:
                    category = "short_recent_seed_or_initial_state"
                else:
                    category = "mixed_state_or_proxy"
                classifications[category] = classifications.get(category, 0) + 1
                first = first or str(date.date())
                changed_new += 1
            if slice_old != old:
                changed_sliced += 1
            comparison.append({"branch": branch, "version": version, "date": str(date.date()),
                               "new_proxy_signal": new, "old_close_short_slice_signal": slice_old,
                               "old_formal_signal": old, "new_vs_old": new != old,
                               "slice_vs_old": slice_old != old,
                               "category": category if new != old else "same"})
        summary["versions"][version] = {
            "compared_sessions": len(common), "new_vs_old_count": changed_new,
            "sliced_old_vs_old_count": changed_sliced, "first_new_difference": first,
            "difference_categories": classifications,
        }
    output = HERE / f"recent_{branch}_signal_comparison.csv"
    pd.DataFrame(comparison).to_csv(output, index=False, encoding="utf-8")
    summary["comparison_output_sha256"] = sha256(output)
    (HERE / f"recent_{branch}_signal_comparison_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--branch", choices=["prefunded", "netted"], required=True)
    args = parser.parse_args()
    print(json.dumps(run(args.branch), ensure_ascii=False, indent=2))
