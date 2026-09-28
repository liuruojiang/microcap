"""Compare a read-only executed-member replay's raw daily returns with formal proxy."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[3]
OUT = Path(__file__).resolve().parent
PROXY = ROOT / "outputs/wind_microcap_top_100_biweekly_thursday_16y_cached.csv"


def main() -> None:
    member_rows = pd.read_csv(OUT / "replayed_effective_members_by_rebalance.csv", dtype={"symbol": str})
    member_rows["symbol"] = member_rows["symbol"].str.zfill(6)
    member_rows["rebalance_date"] = pd.to_datetime(member_rows["rebalance_date"])
    members = {date: set(group["symbol"]) for date, group in member_rows.groupby("rebalance_date")}
    rebalance_dates = pd.DatetimeIndex(sorted(members))
    proxy = pd.read_csv(PROXY)
    proxy["date"] = pd.to_datetime(proxy["date"])
    proxy = proxy.loc[proxy["date"].le(pd.Timestamp("2014-10-07"))].copy()
    dates = pd.DatetimeIndex([pd.Timestamp("2010-01-04"), *proxy["date"].tolist()])
    all_symbols = sorted({symbol for group in members.values() for symbol in group})
    returns: dict[str, pd.Series] = {}
    for symbol in all_symbols:
        path = ROOT / ".microcap_index_cache/prices_raw" / f"{symbol}.csv"
        if not path.exists():
            continue
        raw = pd.read_csv(path, usecols=["date", "close_raw"])
        raw["date"] = pd.to_datetime(raw["date"])
        raw["close_raw"] = pd.to_numeric(raw["close_raw"], errors="coerce")
        close = raw.dropna().drop_duplicates("date", keep="last").set_index("date")["close_raw"].reindex(dates).ffill()
        returns[symbol] = close.pct_change(fill_method=None)
    return_frame = pd.DataFrame(returns, index=dates)
    rows: list[dict[str, object]] = []
    for dt in proxy["date"]:
        pos = rebalance_dates.searchsorted(dt, side="left") - 1
        if pos < 0:
            continue
        active = members[rebalance_dates[pos]]
        day = return_frame.loc[dt, list(active)]
        replay_return = float(day.fillna(0.0).mean())
        formal_return = float(proxy.loc[proxy["date"].eq(dt), "daily_return"].iloc[0])
        rows.append({"date": dt, "replay_return": replay_return, "formal_return": formal_return,
                     "difference": replay_return - formal_return, "member_count": len(active)})
    comparison = pd.DataFrame(rows)
    comparison.to_csv(OUT / "daily_replay_comparison_pre_first_turnover_difference.csv", index=False, encoding="utf-8-sig")
    print(f"rows={len(comparison)} max_abs_difference={comparison['difference'].abs().max()} nonzero_gt_1e-10={(comparison['difference'].abs()>1e-10).sum()} member_count_min={comparison['member_count'].min()}")
    print(comparison.loc[comparison["difference"].abs().gt(1e-10)].head(5).to_string(index=False))


if __name__ == "__main__":
    main()
