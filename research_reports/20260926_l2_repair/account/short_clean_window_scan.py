"""Bounded early-window availability check; no formal backtest or NAV write."""

from __future__ import annotations

import csv
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
HERE = Path(__file__).resolve().parent
MEMBERS = ROOT / "outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_members.csv"
TURNOVER = ROOT / "outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_turnover.csv"
PROXY = ROOT / "outputs/wind_microcap_top_100_biweekly_thursday_16y_cached.csv"


def read(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as file:
        return list(csv.DictReader(file))


def main() -> None:
    members = read(MEMBERS)
    turnover = read(TURNOVER)[:100]
    dates = [r["date"] for r in read(PROXY)]
    eligible = []
    for turn in turnover:
        if int(turn["blocked_entry_count"]) or int(turn["blocked_exit_count"]):
            continue
        d0 = turn["rebalance_date"]
        i = dates.index(d0)
        window = dates[i:i+3]
        symbols = [r["symbol"].zfill(6) for r in members if r["rebalance_date"] == d0]
        if len(symbols) != 100 or len(window) != 3:
            raise ValueError("Unexpected member/window count")
        misses = {d: [] for d in window}
        for symbol in symbols:
            price_path = ROOT / ".microcap_index_cache/prices_raw" / f"{symbol}.csv"
            price_dates = {r["date"] for r in read(price_path)} if price_path.exists() else set()
            for date in window:
                if date not in price_dates:
                    misses[date].append(symbol)
        eligible.append({"rebalance_date": d0, "window": window, "missing_price_symbols": misses})
    result = {
        "scan": "first_100_saved_rebalances_only",
        "date_range": [turnover[0]["rebalance_date"], turnover[-1]["rebalance_date"]],
        "zero_blocked_rebalances": len(eligible),
        "three_day_all_100_raw_close_candidates": sum(not any(row["missing_price_symbols"].values()) for row in eligible),
        "eligible_windows": eligible,
        "further_constraint": "No point-in-time corporate-action ledger exists in these files; price completeness alone would not certify an economic-return window.",
    }
    HERE.mkdir(parents=True, exist_ok=True)
    (HERE / "short_clean_window_scan.json").write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({k: result[k] for k in ("scan", "date_range", "zero_blocked_rebalances", "three_day_all_100_raw_close_candidates", "eligible_windows")}, ensure_ascii=False))


if __name__ == "__main__":
    main()
