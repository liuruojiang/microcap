"""Read-only rank-threshold counterfactual; ST and execution remain unverified."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd


HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]


def main() -> None:
    inventory = pd.read_csv(HERE / "missing_delisted_inventory.csv", dtype={"symbol": str}, parse_dates=["list_date", "delist_date"])
    assert len(inventory) == 254
    targets = pd.read_csv(ROOT / "outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_members.csv", dtype={"symbol": str}, parse_dates=["rebalance_date"])
    rank100 = targets.loc[targets["rank"].eq(100), ["rebalance_date", "market_cap"]].rename(columns={"market_cap": "formal_rank100_cap"})
    assert rank100["rebalance_date"].is_unique and len(rank100) == 432
    output = []
    for item in inventory.itertuples(index=False):
        price_path = HERE / "prices_raw" / f"{item.symbol}.csv"
        share_path = HERE / "share_change" / f"{item.symbol}.csv"
        if not price_path.exists():
            continue
        prices = pd.read_csv(price_path, parse_dates=["date"])
        prices = prices.loc[prices["date"].between(item.list_date, item.delist_date)]
        shares = pd.read_csv(share_path, parse_dates=["change_date", "notice_date", "listing_date"]) if share_path.exists() else pd.DataFrame()
        if not shares.empty:
            # Full source row required. END_DATE records economic as-of, NOTICE_DATE bounds historical
            # availability, LISTING_DATE supplies an independently named listing/effective field.
            shares = shares.dropna(subset=["change_date", "notice_date", "listing_date", "total_shares_10k"])
            shares = shares.loc[
                shares["change_date"].le(item.delist_date)
                & shares["notice_date"].le(item.delist_date)
                & shares["listing_date"].le(item.delist_date)
            ].sort_values("change_date")
        dates = rank100.loc[rank100["rebalance_date"].between(item.list_date, item.delist_date)]
        joined = dates.merge(prices.rename(columns={"date": "rebalance_date"}), on="rebalance_date", how="left")
        if shares.empty:
            joined["total_shares_10k"] = float("nan")
            joined["share_change_date"] = pd.NaT
        else:
            # Each chosen record must satisfy *all three* temporal constraints on the rebalance day.
            share_values = []
            share_dates = []
            for day in joined["rebalance_date"]:
                eligible = shares.loc[
                    shares["change_date"].le(day)
                    & shares["notice_date"].le(day)
                    & shares["listing_date"].le(day)
                ]
                if eligible.empty:
                    share_values.append(float("nan"))
                    share_dates.append(pd.NaT)
                else:
                    chosen = eligible.iloc[-1]
                    share_values.append(float(chosen["total_shares_10k"]))
                    share_dates.append(chosen["change_date"])
            joined["total_shares_10k"] = share_values
            joined["share_change_date"] = share_dates
        joined["symbol"] = item.symbol
        joined["source_name"] = item.name
        joined["candidate_cap"] = joined["close_raw"] * joined["total_shares_10k"] * 10000
        joined["possible_below_rank100"] = joined["candidate_cap"].lt(joined["formal_rank100_cap"])
        joined["st_status"] = "unknown_missing_historical_meta"
        joined["execution_status"] = "unknown_no_volume_or_limit_check"
        output.append(joined)
    detail = pd.concat(output, ignore_index=True)
    detail = detail.sort_values(["rebalance_date", "symbol"])
    detail.to_csv(HERE / "candidate_cap_comparisons.csv", index=False, encoding="utf-8")
    possible = detail.loc[detail["possible_below_rank100"]]
    summary = {
        "missing_delisted_a_symbols": len(inventory),
        "formal_rebalance_dates": len(rank100),
        "listed_symbol_rebalance_rows": len(detail),
        "exact_price_rows": int(detail["close_raw"].notna().sum()),
        "exact_price_and_temporally_available_share_rows": int(detail["candidate_cap"].notna().sum()),
        "unknown_price_rows": int(detail["close_raw"].isna().sum()),
        "unknown_share_given_price_rows": int((detail["close_raw"].notna() & detail["total_shares_10k"].isna()).sum()),
        "possible_below_rank100_rows": len(possible),
        "possible_below_rank100_symbols": int(possible["symbol"].nunique()),
        "possible_below_rank100_dates": int(possible["rebalance_date"].nunique()),
        "first_rebalance_date": rank100["rebalance_date"].min().date().isoformat(),
        "first_formal_rank100_cap": float(rank100.loc[rank100["rebalance_date"].eq(rank100["rebalance_date"].min()), "formal_rank100_cap"].iloc[0]),
        "first_date_possible_symbols": possible.loc[possible["rebalance_date"].eq(rank100["rebalance_date"].min()), "symbol"].tolist(),
        "interpretation": "Only raw-price and time-constrained share-cap threshold comparison; historical ST, liquidity, trade limits and formal membership are unverified.",
    }
    (HERE / "candidate_cap_summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False))


if __name__ == "__main__":
    main()
