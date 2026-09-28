"""Count source-backed corporate actions in the daily-parity replay segment."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[3]
OUT = Path(__file__).resolve().parent
CUTOFF = pd.Timestamp("2014-09-18")


def main() -> None:
    members = pd.read_csv(OUT / "replayed_effective_members_by_rebalance.csv", dtype={"symbol": str})
    members["symbol"] = members["symbol"].str.zfill(6)
    members["rebalance_date"] = pd.to_datetime(members["rebalance_date"])
    member_map = {date: set(group["symbol"]) for date, group in members.groupby("rebalance_date")}
    rebalances = pd.DatetimeIndex(sorted(member_map))
    paths = [OUT / "cninfo_distribution_events.csv", OUT / "cninfo_distribution_events_initial_seed_extra.csv"]
    events = pd.concat([pd.read_csv(path, dtype={"symbol": str}) for path in paths], ignore_index=True)
    events["symbol"] = events["symbol"].str.zfill(6)
    events["ex_date"] = pd.to_datetime(events["除权日"], errors="coerce")
    events = events[events["ex_date"].notna() & events["ex_date"].le(CUTOFF) & events["ex_date"].ge(pd.Timestamp("2010-01-05"))].copy()
    for key in ("送股比例", "转增比例", "派息比例"):
        events[key] = pd.to_numeric(events[key], errors="coerce").fillna(0)
    events = events.loc[events[["送股比例", "转增比例", "派息比例"]].sum(axis=1).gt(0)].copy()
    prior = rebalances.searchsorted(events["ex_date"], side="left") - 1
    events["previous_rebalance"] = [str(rebalances[i].date()) if i >= 0 else "" for i in prior]
    events["held_on_ex_date"] = [bool(i >= 0 and symbol in member_map[rebalances[i]]) for i, symbol in zip(prior, events["symbol"])]
    held = events.loc[events["held_on_ex_date"]].copy()
    held["stock_bonus_per_10"] = held["送股比例"] + held["转增比例"]
    held["cash_per_10"] = held["派息比例"]
    held["record_date"] = pd.to_datetime(held["股权登记日"], errors="coerce")
    held["record_date_at_rebalance"] = held["record_date"].eq(pd.to_datetime(held["previous_rebalance"]))
    fields = ["symbol", "ex_date", "previous_rebalance", "record_date", "record_date_at_rebalance", "stock_bonus_per_10", "cash_per_10", "派息日", "股份到账日", "实施方案公告日期", "实施方案分红说明"]
    held[fields].sort_values(["ex_date", "symbol"]).to_csv(OUT / "source_backed_held_actions_pre_divergence.csv", index=False, encoding="utf-8-sig")
    share = pd.read_csv(OUT / "executed_held_share_events.csv", dtype={"symbol": str})
    share = share.loc[pd.to_datetime(share["price_date"]).le(CUTOFF)]
    summary = {
        "certified_daily_parity_window": ["2010-01-05", str(CUTOFF.date())],
        "cninfo_positive_ex_date_events_all_target_symbols": int(len(events)),
        "cninfo_positive_ex_date_events_held": int(len(held)),
        "held_stock_bonus_events": int(held["stock_bonus_per_10"].gt(0).sum()),
        "held_cash_events": int(held["cash_per_10"].gt(0).sum()),
        "held_record_date_equals_rebalance": int(held["record_date_at_rebalance"].sum()),
        "held_share_change_bonus_candidates": int(len(share)),
        "first_held_action": held[fields].sort_values(["ex_date", "symbol"]).head(1).astype(str).to_dict("records"),
    }
    (OUT / "pre_divergence_actions_summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
