"""Read-only execution-state replay for the corporate-action candidate dates."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
import microcap_top100_mom16_biweekly_live_v2_0 as runtime

OUT = Path(__file__).resolve().parent
TARGET = ROOT / "outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_members.csv"
PROXY = ROOT / "outputs/wind_microcap_top_100_biweekly_thursday_16y_cached.csv"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--trade-calendar", choices=("sparse", "full"), default="sparse")
    args = parser.parse_args()
    targets = pd.read_csv(TARGET, dtype={"symbol": str})
    targets["symbol"] = targets["symbol"].str.zfill(6)
    targets["rebalance_date"] = pd.to_datetime(targets["rebalance_date"]).dt.normalize()
    targets = targets.sort_values(["rebalance_date", "rank"])
    target_map = {pd.Timestamp(date): group["symbol"].tolist() for date, group in targets.groupby("rebalance_date")}
    panel_dates = pd.DatetimeIndex(pd.read_csv(PROXY, usecols=["date"])["date"].pipe(pd.to_datetime)).normalize()
    rebalance_dates = pd.DatetimeIndex(sorted(target_map))
    # Sparse preserves the first diagnostic run and exposes the natural
    # counterexample; full is the proposed correctness fix for long suspensions.
    trade_dates = panel_dates if args.trade_calendar == "full" else runtime.base_mod._minimal_tradeability_dates(panel_dates, rebalance_dates)
    seed_path = OUT / "initial_2010_01_04_executed_seed.csv"
    current: list[str] = pd.read_csv(seed_path, dtype={"symbol": str})["symbol"].str.zfill(6).tolist()
    if len(current) != 100 or len(set(current)) != 100:
        raise AssertionError("initial seed must have 100 unique symbols")
    symbols = sorted(set(targets["symbol"].unique()) | set(current))
    buyable, sellable = runtime.base_mod._load_buy_sell_for_symbols(symbols, trade_dates, 16)
    print(f"trade_dates={len(trade_dates)} symbols={len(symbols)} buyable_columns={len(buyable.columns)}")

    actual_by_rebalance: dict[pd.Timestamp, set[str]] = {}
    replay_counts: list[dict[str, object]] = []
    snapshot_rows: list[dict[str, object]] = [{"rebalance_date": pd.Timestamp("2010-01-04"), "symbol": symbol} for symbol in current]
    for dt in rebalance_dates:
        if dt not in buyable.index:
            raise AssertionError(f"rebalance date missing from tradeability: {dt}")
        result = runtime.freq_mod.apply_trade_constraints(
            current_members=current,
            target_members=target_map[dt],
            trade_date=dt,
            buyable_df=buyable,
            sellable_df=sellable,
            top_n=100,
        )
        current = result["members_after"]
        if len(set(current)) != len(current):
            raise AssertionError(f"duplicate executed members at {dt}")
        actual_by_rebalance[dt] = set(current)
        snapshot_rows.extend({"rebalance_date": dt, "symbol": symbol} for symbol in current)
        replay_counts.append({
            "rebalance_date": dt,
            "entry_count": len(result["entered"]),
            "exit_count": len(result["exited"]),
            "blocked_entry_count": len(result["blocked_entries"]),
            "blocked_exit_count": len(result["blocked_exits"]),
            "holding_count_after": len(current),
        })

    official = pd.read_csv(ROOT / "outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_turnover.csv")
    official["rebalance_date"] = pd.to_datetime(official["rebalance_date"])
    count_columns = ["entry_count", "exit_count", "blocked_entry_count", "blocked_exit_count", "holding_count_after"]
    cmp = pd.DataFrame(replay_counts).merge(official[["rebalance_date", *count_columns]], on="rebalance_date", suffixes=("_replay", "_official"), validate="one_to_one")
    cmp["parity"] = pd.concat([cmp[f"{column}_replay"].eq(cmp[f"{column}_official"]) for column in count_columns], axis=1).all(axis=1)
    mismatch = cmp.loc[~cmp["parity"]]
    cmp.to_csv(OUT / "turnover_replay_comparison.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame(snapshot_rows).to_csv(OUT / "replayed_effective_members_by_rebalance.csv", index=False, encoding="utf-8-sig")

    events = pd.read_csv(OUT / "candidate_share_events.csv", dtype={"symbol": str})
    events["symbol"] = events["symbol"].str.zfill(6)
    dates = pd.to_datetime(events["price_date"])
    prior = rebalance_dates.searchsorted(dates, side="left") - 1
    events["executed_held"] = [bool(i >= 0 and symbol in actual_by_rebalance[rebalance_dates[i]]) for i, symbol in zip(prior, events["symbol"])]
    events["held_status"] = "trade_constraint_replay_check_turnover_parity_before_use"
    held = events.loc[events["executed_held"]].copy()
    path = OUT / "executed_held_share_events.csv"
    held.to_csv(path, index=False, encoding="utf-8-sig")
    print(f"turnover parity={len(cmp)-len(mismatch)}/{len(cmp)} rebalance dates; first mismatches={mismatch[['rebalance_date','entry_count_replay','entry_count_official','exit_count_replay','exit_count_official']].head().to_dict('records')}")
    print(f"candidate events={len(events)} replay-held candidates={len(held)} written={path}")
    official_final = pd.read_csv(ROOT / "outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_effective_members.csv", dtype={"symbol": str})
    official_final_set = set(official_final["symbol"].str.zfill(6))
    replay_final_set = actual_by_rebalance[rebalance_dates[-1]]
    print(f"final effective overlap={len(official_final_set & replay_final_set)} missing={sorted(official_final_set - replay_final_set)} extra={sorted(replay_final_set - official_final_set)}")
    print(held.sort_values("return_difference_diagnostic", ascending=False)[["symbol", "price_date", "reason", "raw_return", "simple_bonus_return_diagnostic"]].head(15).to_string(index=False))


if __name__ == "__main__":
    main()
