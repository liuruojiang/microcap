"""Read-only 2010-01-04 executed-seed cap threshold against missing delisted stocks."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd


HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
DATE = pd.Timestamp("2010-01-04")


def old_cap(symbol: str) -> float:
    price = pd.read_csv(ROOT / ".microcap_index_cache/prices_raw" / f"{symbol}.csv", parse_dates=["date"])
    share = pd.read_csv(ROOT / ".microcap_index_cache/share_change" / f"{symbol}.csv", parse_dates=["change_date"])
    exact = price.loc[price["date"].eq(DATE), "close_raw"]
    known = share.loc[share["change_date"].le(DATE), "total_shares_10k"]
    if len(exact) != 1 or known.empty:
        raise ValueError(f"old seed {symbol} missing exact price/share")
    return float(exact.iloc[0] * known.iloc[-1] * 10000)


def main() -> None:
    seed = pd.read_csv(ROOT / "research_reports/20260926_l1_repair/corporate_actions/initial_2010_01_04_executed_seed.csv", dtype={"symbol": str})
    assert len(seed) == 100 and seed["symbol"].is_unique and seed["rank"].tolist() == list(range(1, 101))
    seed["market_cap"] = seed["symbol"].map(old_cap)
    seed.to_csv(HERE / "initial_formal_seed_caps.csv", index=False, encoding="utf-8")
    threshold = float(seed.loc[seed["rank"].eq(100), "market_cap"].iloc[0])
    inventory = pd.read_csv(HERE / "missing_delisted_inventory.csv", dtype={"symbol": str}, parse_dates=["list_date", "delist_date"])
    rows = []
    for item in inventory.itertuples(index=False):
        if not item.list_date <= DATE <= item.delist_date:
            continue
        p = HERE / "prices_raw" / f"{item.symbol}.csv"
        s = HERE / "share_change" / f"{item.symbol}.csv"
        row = {"symbol": item.symbol, "name": item.name, "list_date": item.list_date.date().isoformat(), "delist_date": item.delist_date.date().isoformat()}
        if p.exists():
            prices = pd.read_csv(p, parse_dates=["date"])
            exact = prices.loc[prices["date"].eq(DATE), "close_raw"]
            row["close_raw"] = float(exact.iloc[0]) if len(exact) == 1 else None
        if s.exists():
            shares = pd.read_csv(s, parse_dates=["change_date", "notice_date", "listing_date"])
            eligible = shares.loc[
                shares["change_date"].le(DATE)
                & shares["notice_date"].le(DATE)
                & shares["listing_date"].le(DATE)
            ].sort_values("change_date")
            row["total_shares_10k"] = float(eligible.iloc[-1]["total_shares_10k"]) if not eligible.empty else None
            row["share_change_date"] = eligible.iloc[-1]["change_date"].date().isoformat() if not eligible.empty else None
        row["initial_formal_rank100_cap"] = threshold
        if row.get("close_raw") and row.get("total_shares_10k"):
            row["candidate_cap"] = row["close_raw"] * row["total_shares_10k"] * 10000
            row["possible_below_rank100"] = row["candidate_cap"] < threshold
        rows.append(row)
    detail = pd.DataFrame(rows).sort_values("symbol")
    detail.to_csv(HERE / "initial_2010_01_04_candidate_caps.csv", index=False, encoding="utf-8")
    summary = {
        "date": "2010-01-04", "old_executed_seed_100th_symbol": seed.iloc[-1]["symbol"],
        "old_executed_seed_100th_cap": threshold,
        "old_seed_cap_nonmonotonic_adjacent_count": int(seed["market_cap"].diff().lt(0).sum()),
        "delisted_listed_symbols": len(detail),
        "delisted_exact_price": int(detail["close_raw"].notna().sum()),
        "delisted_exact_price_with_three_date_shares": int(detail["candidate_cap"].notna().sum()),
        "delisted_possible_below_rank100": int(detail["possible_below_rank100"].fillna(False).sum()),
        "unknown_st_or_executability": True,
        "note": "Executed-seed rank100 cap is a comparator, not a complete reconstructed initial target membership decision.",
    }
    (HERE / "initial_2010_01_04_summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False))


if __name__ == "__main__":
    main()
