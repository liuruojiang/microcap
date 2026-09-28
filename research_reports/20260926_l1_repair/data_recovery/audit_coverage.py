"""Summarize isolated delisted-source coverage, without promoting data."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd


HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]


def main() -> None:
    inventory = pd.read_csv(HERE / "missing_delisted_inventory.csv", dtype={"symbol": str})
    assert len(inventory) == 254 and inventory["symbol"].is_unique
    turnover = pd.read_csv(ROOT / "outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_turnover.csv")
    rebalance_dates = pd.to_datetime(turnover["rebalance_date"])
    results = []
    for item in inventory.itertuples(index=False):
        symbol = item.symbol
        price_path = HERE / "prices_raw" / f"{symbol}.csv"
        share_path = HERE / "share_change" / f"{symbol}.csv"
        record = {"symbol": symbol, "exchange": item.exchange, "list_date": item.list_date, "delist_date": item.delist_date}
        if price_path.exists():
            price = pd.read_csv(price_path, parse_dates=["date"])
            record.update(price_rows=len(price), price_first=price["date"].min().date().isoformat(), price_last=price["date"].max().date().isoformat())
        else:
            price = pd.DataFrame(columns=["date"])
            record["price_rows"] = 0
        if share_path.exists():
            shares = pd.read_csv(share_path, parse_dates=["change_date", "notice_date", "listing_date"])
            record.update(
                share_rows=len(shares),
                share_first=shares["change_date"].min().date().isoformat(),
                share_last=shares["change_date"].max().date().isoformat(),
                notice_after_end_rows=int(shares["notice_date"].gt(shares["change_date"]).sum()),
                notice_missing_rows=int(shares["notice_date"].isna().sum()),
                listing_missing_rows=int(shares["listing_date"].isna().sum()),
                event_rows_without_listing_date=int((shares["listing_date"].isna() & shares["reason"].fillna("").str.contains("转增|送股|配股|增发|首发|上市")).sum()),
            )
        else:
            shares = pd.DataFrame(columns=["change_date", "notice_date"])
            record.update(share_rows=0, notice_after_end_rows=0, notice_missing_rows=0, listing_missing_rows=0)
        if not price.empty:
            listed = pd.DatetimeIndex(rebalance_dates[rebalance_dates.between(pd.Timestamp(item.list_date), pd.Timestamp(item.delist_date))])
            price_dates = pd.DatetimeIndex(price["date"])
            price_rebalance = listed.intersection(price_dates)
            record["eligible_rebalance_dates"] = len(listed)
            record["price_observed_rebalance_dates"] = len(price_rebalance)
            if not shares.empty:
                allowed = shares.dropna(subset=["change_date", "notice_date"]).copy()
                allowed["pit_available_date"] = allowed[["change_date", "notice_date"]].max(axis=1)
                record["price_rebalance_dates_with_pit_shares"] = int(sum(allowed["pit_available_date"].le(day).any() for day in price_rebalance))
            else:
                record["price_rebalance_dates_with_pit_shares"] = 0
        results.append(record)
    summary = pd.DataFrame(results).sort_values("symbol")
    summary.to_csv(HERE / "coverage_by_symbol.csv", index=False, encoding="utf-8")
    checks = {
        "symbols": len(summary),
        "price_fetched": int(summary["price_rows"].gt(0).sum()),
        "share_fetched": int(summary["share_rows"].gt(0).sum()),
        "both_fetched": int((summary["price_rows"].gt(0) & summary["share_rows"].gt(0)).sum()),
        "price_rows_total": int(summary["price_rows"].sum()),
        "share_rows_total": int(summary["share_rows"].sum()),
        "notice_after_end_rows": int(summary["notice_after_end_rows"].sum()),
        "notice_missing_rows": int(summary["notice_missing_rows"].sum()),
        "price_observed_rebalance_dates": int(summary["price_observed_rebalance_dates"].fillna(0).sum()),
        "price_rebalance_dates_with_pit_shares": int(summary["price_rebalance_dates_with_pit_shares"].fillna(0).sum()),
    }
    (HERE / "coverage_summary.json").write_text(json.dumps(checks, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(checks, ensure_ascii=False))


if __name__ == "__main__":
    main()
