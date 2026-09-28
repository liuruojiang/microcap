"""Read-only candidate scan for corporate actions among saved Top100 target names.

This is a diagnostic only. Share-count changes are not automatically shareholder
entitlements: placements, IPOs, unlocks and similar events must be excluded with
issuer notices before constructing a total-return stream.
"""

from __future__ import annotations

import csv
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[3]
OUT = Path(__file__).resolve().parent
TARGET = ROOT / "outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_members.csv"
SHARES = ROOT / ".microcap_index_cache/share_change"
PRICES = ROOT / ".microcap_index_cache/prices_raw"


def main() -> None:
    targets = pd.read_csv(TARGET, dtype={"symbol": str})
    targets["symbol"] = targets["symbol"].str.zfill(6)
    targets["rebalance_date"] = pd.to_datetime(targets["rebalance_date"])
    all_rebalances = pd.DatetimeIndex(sorted(targets["rebalance_date"].unique()))
    symbols = sorted(targets["symbol"].unique())
    events: list[dict[str, object]] = []
    reason_counts: dict[str, int] = {}
    for symbol in symbols:
        share_path = SHARES / f"{symbol}.csv"
        price_path = PRICES / f"{symbol}.csv"
        if not share_path.exists() or not price_path.exists():
            continue
        shares = pd.read_csv(share_path)
        prices = pd.read_csv(price_path)
        if not {"change_date", "total_shares_10k", "reason"}.issubset(shares) or not {"date", "close_raw"}.issubset(prices):
            continue
        shares["change_date"] = pd.to_datetime(shares["change_date"], errors="coerce")
        shares["total_shares_10k"] = pd.to_numeric(shares["total_shares_10k"], errors="coerce")
        shares = shares.dropna(subset=["change_date", "total_shares_10k"]).sort_values("change_date")
        prices["date"] = pd.to_datetime(prices["date"], errors="coerce")
        prices["close_raw"] = pd.to_numeric(prices["close_raw"], errors="coerce")
        prices = prices.dropna(subset=["date", "close_raw"]).sort_values("date")
        prices = prices.drop_duplicates("date", keep="last").set_index("date")["close_raw"]
        for i in range(1, len(shares)):
            row = shares.iloc[i]
            reason = str(row["reason"])
            reason_counts[reason] = reason_counts.get(reason, 0) + 1
            if not any(term in reason for term in ("转增", "送股", "拆细", "拆股", "缩股")):
                continue
            event_date = row["change_date"]
            if event_date < pd.Timestamp("2010-01-05") or event_date > pd.Timestamp("2026-09-24"):
                continue
            prior_shares = float(shares.iloc[i - 1]["total_shares_10k"])
            current_shares = float(row["total_shares_10k"])
            ratio = current_shares / prior_shares if prior_shares > 0 else float("nan")
            before = prices[prices.index < event_date]
            after = prices[prices.index >= event_date]
            if before.empty or after.empty:
                continue
            previous_date, previous_close = before.index[-1], float(before.iloc[-1])
            next_date, next_close = after.index[0], float(after.iloc[0])
            raw_return = next_close / previous_close - 1
            simple_bonus_return = next_close * ratio / previous_close - 1
            prior_rebalances = all_rebalances[all_rebalances < next_date]
            previous_rebalance = prior_rebalances[-1] if len(prior_rebalances) else pd.NaT
            prior_target = bool(pd.notna(previous_rebalance) and ((targets["rebalance_date"].eq(previous_rebalance) & targets["symbol"].eq(symbol)).any()))
            events.append({
                "symbol": symbol,
                "event_date": event_date.date().isoformat(),
                "price_date": next_date.date().isoformat(),
                "previous_price_date": previous_date.date().isoformat(),
                "reason": reason,
                "shares_before_10k": prior_shares,
                "shares_after_10k": current_shares,
                "share_ratio": ratio,
                "previous_raw_close": previous_close,
                "next_raw_close": next_close,
                "raw_return": raw_return,
                "simple_bonus_return_diagnostic": simple_bonus_return,
                "return_difference_diagnostic": simple_bonus_return - raw_return,
                "prior_rebalance_date": previous_rebalance.date().isoformat() if pd.notna(previous_rebalance) else "",
                "target_at_prior_rebalance": prior_target,
                "held_status": "unverified_requires_execution_replay",
            })
    events.sort(key=lambda x: (x["price_date"], x["symbol"]))
    path = OUT / "candidate_share_events.csv"
    if events:
        with path.open("w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=list(events[0]))
            writer.writeheader()
            writer.writerows(events)
    print(f"target symbols={len(symbols)} candidate events={len(events)} written={path}")
    print("reasons:", sorted(reason_counts.items(), key=lambda x: -x[1])[:15])
    for row in sorted(events, key=lambda x: -abs(float(x["return_difference_diagnostic"])))[:10]:
        print(row["symbol"], row["price_date"], row["reason"], round(float(row["raw_return"]), 4), round(float(row["simple_bonus_return_diagnostic"]), 4))


if __name__ == "__main__":
    main()
