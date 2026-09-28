"""Independently recompute selected official proxy returns from held raw closes."""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
OUT = Path(__file__).resolve().parent
MEMBERS = pd.read_csv(OUT / "members_by_rebalance.csv", dtype={"symbol": str})
MEMBERS["rebalance_date"] = pd.to_datetime(MEMBERS["rebalance_date"])
REBALANCES = pd.DatetimeIndex(sorted(MEMBERS["rebalance_date"].unique()))
PROXY = pd.read_csv(ROOT / "outputs/wind_microcap_top_100_biweekly_thursday_16y_cached.csv")
PROXY["date"] = pd.to_datetime(PROXY["date"])
PROXY = PROXY.sort_values("date").set_index("date")


def prior_and_current_close(symbol: str, prior_day: pd.Timestamp, day: pd.Timestamp) -> tuple[float, float, str]:
    path = ROOT / ".microcap_index_cache/prices_raw" / f"{symbol}.csv"
    if not path.exists():
        raise FileNotFoundError(path)
    price = pd.read_csv(path, usecols=["date", "close_raw"])
    price["date"] = pd.to_datetime(price["date"])
    price["close_raw"] = pd.to_numeric(price["close_raw"], errors="coerce")
    price = price.dropna().sort_values("date")
    if price.empty or day < price["date"].min() or day > price["date"].max():
        return np.nan, np.nan, "outside_price_coverage"
    p = price.loc[price["date"].le(prior_day)]
    c = price.loc[price["date"].le(day)]
    if p.empty or c.empty:
        return np.nan, np.nan, "no_prior_close"
    return float(p.iloc[-1]["close_raw"]), float(c.iloc[-1]["close_raw"]), "raw"


def main() -> None:
    dates = ["2014-09-18", "2015-05-25", "2015-07-10", "2026-09-24"]
    report = []
    all_rows = []
    for date_text in dates:
        day = pd.Timestamp(date_text)
        i = PROXY.index.get_loc(day)
        prior_day = PROXY.index[i - 1]
        rebalance = REBALANCES[REBALANCES < day][-1]
        held = MEMBERS.loc[MEMBERS["rebalance_date"].eq(rebalance), "symbol"].tolist()
        returns = []
        for symbol in held:
            prior, current, source = prior_and_current_close(symbol, prior_day, day)
            ret = current / prior - 1 if np.isfinite(prior) and prior > 0 and np.isfinite(current) else np.nan
            returns.append(0.0 if not np.isfinite(ret) else ret)
            all_rows.append({"date": day, "rebalance_date": rebalance, "symbol": symbol, "previous_close_date": prior_day, "previous_close": prior, "current_close": current, "source": source, "raw_return": ret, "fillna_return": 0.0 if not np.isfinite(ret) else ret})
        actual = float(np.mean(returns))
        official = float(PROXY.at[day, "daily_return"])
        report.append({"date": date_text, "previous_close_date": str(prior_day.date()), "rebalance_date": str(rebalance.date()), "held_count": len(held), "replay_daily_return": actual, "official_daily_return": official, "difference": actual - official})
    pd.DataFrame(all_rows).to_csv(OUT / "key_return_components.csv", index=False, encoding="utf-8-sig")
    (OUT / "key_return_summary.json").write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps(report, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
