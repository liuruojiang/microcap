"""Identify member substitutions explaining first daily replay mismatch."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[3]
OUT = Path(__file__).resolve().parent


def main() -> None:
    comp = pd.read_csv(OUT / "daily_replay_comparison_pre_first_turnover_difference.csv")
    comp = comp.loc[comp["difference"].abs().gt(1e-10)].copy()
    dates = pd.DatetimeIndex(pd.to_datetime(comp["date"]))
    previous = pd.Timestamp("2014-09-18")
    members = pd.read_csv(OUT / "replayed_effective_members_by_rebalance.csv", dtype={"symbol": str})
    current = set(members.loc[members["rebalance_date"].eq(str(previous.date())), "symbol"].str.zfill(6))
    targets = pd.read_csv(ROOT / "outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_members.csv", dtype={"symbol": str})
    universe = sorted(set(targets["symbol"].str.zfill(6)) | current)
    needed = pd.DatetimeIndex([previous, *dates.tolist()])
    ret: dict[str, np.ndarray] = {}
    for symbol in universe:
        path = ROOT / ".microcap_index_cache/prices_raw" / f"{symbol}.csv"
        if not path.exists():
            continue
        frame = pd.read_csv(path, usecols=["date", "close_raw"])
        frame["date"] = pd.to_datetime(frame["date"])
        series = frame.drop_duplicates("date", keep="last").set_index("date")["close_raw"].reindex(needed).ffill().pct_change(fill_method=None).loc[dates]
        ret[symbol] = series.fillna(0.0).to_numpy(dtype=float)
    required_delta = (comp["formal_return"] - comp["replay_return"]).to_numpy(dtype=float) * 100
    rows = []
    for removed in sorted(current):
        for added in universe:
            if added in current:
                continue
            error = np.max(np.abs(ret[added] - ret[removed] - required_delta))
            rows.append({"removed_from_replay": removed, "added_to_formal_candidate": added, "max_abs_daily_sum_error": error})
    ranked = pd.DataFrame(rows).sort_values("max_abs_daily_sum_error")
    ranked.head(30).to_csv(OUT / "first_divergence_member_substitution_candidates.csv", index=False, encoding="utf-8-sig")
    print(ranked.head(10).to_string(index=False))


if __name__ == "__main__":
    main()
