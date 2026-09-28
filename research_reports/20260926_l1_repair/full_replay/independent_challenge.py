"""Independent read-only challenge for the frozen Top100 proxy replay.

Uses CSV/JSON/ZIP inputs directly. Does not import replay.py,
check_all_returns.py, or project accounting/tradeability helpers.
"""

from __future__ import annotations

import bisect
import csv
from collections import defaultdict
from decimal import Decimal, ROUND_HALF_UP
import hashlib
import io
import json
import math
from pathlib import Path
import zipfile

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[3]
HERE = Path(__file__).resolve().parent
CACHE = ROOT / ".microcap_index_cache"


def records(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


target: dict[str, list[str]] = defaultdict(list)
for item in records(ROOT / "outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_members.csv"):
    target[item["rebalance_date"]].append(item["symbol"].zfill(6))
turnover = {
    item["rebalance_date"]: item
    for item in records(ROOT / "outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_turnover.csv")
}
initial = [
    item["symbol"].zfill(6)
    for item in records(ROOT / "research_reports/20260926_l1_repair/corporate_actions/initial_2010_01_04_executed_seed.csv")
]
archive = ROOT / "outputs/repair_20260917_cloud_evidence/cloud_20260916_whole.zip"
inner = "outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_effective_members.csv"
with zipfile.ZipFile(archive) as bundle:
    seed_bytes = bundle.read(inner)
approved = [item["symbol"].zfill(6) for item in csv.DictReader(io.StringIO(seed_bytes.decode("utf-8-sig")))]

prices: dict[str, tuple[list[str], list[float]]] = {}
metadata: dict[str, dict] = {}


def symbol_data(symbol: str) -> tuple[list[str], list[float], dict]:
    if symbol not in prices:
        data = sorted(
            (item["date"], float(item["close_raw"]))
            for item in records(CACHE / "prices_raw" / f"{symbol}.csv")
            if item["date"] and item["close_raw"]
        )
        prices[symbol] = ([item[0] for item in data], [item[1] for item in data])
        metadata[symbol] = json.loads((CACHE / "security_meta" / f"{symbol}.json").read_text(encoding="utf-8"))
    return *prices[symbol], metadata[symbol]


def st_at(meta: dict, day: str) -> bool:
    return any(
        period["start"] <= day and (period["end"] is None or day <= period["end"])
        for period in meta.get("st_intervals", [])
    )


def ratio(symbol: str, day: str, st: bool, corrected_2026_rule: bool = False) -> float:
    if symbol.startswith(("4", "8", "920")):
        return .30
    if symbol.startswith(("300", "301")):
        return .05 if st and day < "2020-08-24" else (.20 if day >= "2020-08-24" else .10)
    if symbol.startswith("688"):
        return .20
    if st and corrected_2026_rule and day >= "2026-07-06" and symbol.startswith(("0", "2")):
        return .10
    return .05 if st else .10


def limit_price(previous: float, band: float, buying: bool, *, exact_decimal: bool) -> float:
    sign = 1 if buying else -1
    if exact_decimal:
        value = Decimal(str(previous)) * (Decimal(1) + sign * Decimal(str(band)))
    else:
        # Reproduce the saved strategy's floating point step before Decimal conversion.
        value = Decimal(str(previous * (1.0 + sign * band)))
    return float(value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def can_trade(
    symbol: str, day: str, buying: bool, *, exact_decimal: bool = False,
    corrected_2026_rule: bool = False,
) -> bool:
    days, closes, meta = symbol_data(symbol)
    idx = bisect.bisect_left(days, day)
    if idx >= len(days) or days[idx] != day:
        return False
    if idx == 0:
        return True
    band = ratio(symbol, day, st_at(meta, day), corrected_2026_rule)
    limit = limit_price(closes[idx - 1], band, buying, exact_decimal=exact_decimal)
    return abs(closes[idx] / closes[idx - 1] - limit / closes[idx - 1]) > 1e-6


def replay(*, exact_decimal: bool = False, corrected_2026_rule: bool = False,
           inject_approved_bridge: bool = True) -> tuple[dict, dict]:
    held = initial.copy()
    counts: dict[str, tuple[int, int, int, int, int]] = {}
    snapshots: dict[str, set[str]] = {}
    for day in sorted(target):
        wanted = target[day]
        before = set(held)
        target_set = set(wanted)
        stay = [symbol for symbol in wanted if symbol in before]
        exits = [symbol for symbol in held if symbol not in target_set and can_trade(
            symbol, day, False, exact_decimal=exact_decimal,
            corrected_2026_rule=corrected_2026_rule,
        )]
        blocked_exits = [symbol for symbol in held if symbol not in target_set and not can_trade(
            symbol, day, False, exact_decimal=exact_decimal,
            corrected_2026_rule=corrected_2026_rule,
        )]
        slots = max(100 - len(stay) - len(blocked_exits), 0)
        entered: list[str] = []
        blocked_entries: list[str] = []
        for symbol in wanted:
            if symbol in before:
                continue
            if len(entered) < slots and can_trade(
                symbol, day, True, exact_decimal=exact_decimal,
                corrected_2026_rule=corrected_2026_rule,
            ):
                entered.append(symbol)
            else:
                blocked_entries.append(symbol)
        held = stay + entered + blocked_exits
        if day == "2026-09-03" and inject_approved_bridge:
            held = approved.copy()
        counts[day] = (len(entered), len(exits), len(blocked_entries), len(blocked_exits), len(held))
        snapshots[day] = set(held)
    return counts, snapshots


def returns_check(snapshots: dict[str, set[str]]) -> dict:
    proxy = records(ROOT / "outputs/wind_microcap_top_100_biweekly_thursday_16y_cached.csv")
    days = ["2010-01-04", *(item["date"] for item in proxy)]
    index = pd.Index(days)
    all_symbols = set(initial) | set().union(*snapshots.values())
    changes: dict[str, np.ndarray] = {}
    for symbol in sorted(all_symbols):
        actual_days, closes, meta = symbol_data(symbol)
        series = pd.Series(closes, index=actual_days).reindex(index)
        covered = (index >= actual_days[0]) & (index <= actual_days[-1])
        series = series.where(covered).ffill().where(covered)
        daily = series.pct_change(fill_method=None).to_numpy()
        if meta.get("list_date"):
            daily[index < meta["list_date"]] = np.nan
        if meta.get("delist_date"):
            daily[index > meta["delist_date"]] = np.nan
        changes[symbol] = daily
    rebalance_days = sorted(snapshots)
    gaps: list[float] = []
    missing = 0
    for idx, item in enumerate(proxy, 1):
        date = item["date"]
        prior_rebalance = bisect.bisect_left(rebalance_days, date) - 1
        held = initial if prior_rebalance < 0 else snapshots[rebalance_days[prior_rebalance]]
        values = [changes[symbol][idx] for symbol in sorted(held)]
        missing += sum(not math.isfinite(value) for value in values)
        calculated = sum(value if math.isfinite(value) else 0.0 for value in values) / len(held)
        gaps.append(abs(calculated - float(item["daily_return"])))
    return {
        "days": len(gaps), "max_abs_gap": max(gaps),
        "gaps_gt_1e_10": int(sum(gap > 1e-10 for gap in gaps)),
        "missing_member_returns": missing, "raw_symbols": len(all_symbols),
    }


def main() -> None:
    counts, snapshots = replay()
    expected = {
        day: tuple(int(turnover[day][key]) for key in (
            "entry_count", "exit_count", "blocked_entry_count", "blocked_exit_count", "holding_count_after"
        ))
        for day in turnover
    }
    published: dict[str, set[str]] = defaultdict(set)
    for item in records(HERE / "members_by_rebalance.csv"):
        published[item["rebalance_date"]].add(item["symbol"].zfill(6))
    print("seed_sha256", hashlib.sha256(seed_bytes).hexdigest())
    print("turnover_parity", sum(counts[day] == expected[day] for day in counts), "/", len(counts))
    print("member_snapshot_parity", sum(snapshots[day] == published[day] for day in snapshots), "/", len(snapshots))
    print("returns", json.dumps(returns_check(snapshots)))
    print("2014_600862_buy_allowed", can_trade("600862", "2014-09-18", True))
    print("2014_600862_targeted_but_absent", "600862" in target["2014-09-18"],
          "600862" not in snapshots["2014-09-18"])
    # The 2026-09-03 frozen continuation has a separately approved seed.
    print("sept_03_approved_members", len(approved),
          "sept_17_turnover", counts["2026-09-17"])
    without_bridge_counts, without_bridge_members = replay(inject_approved_bridge=False)
    print("sept_03_without_bridge_symmetric_difference",
          len(without_bridge_members["2026-09-03"] - snapshots["2026-09-03"]),
          len(snapshots["2026-09-03"] - without_bridge_members["2026-09-03"]),
          "sept_17_without_bridge_turnover", without_bridge_counts["2026-09-17"])

    exact_counts, exact_snapshots = replay(exact_decimal=True)
    print("exact_decimal_first_count_difference", next((day for day in counts if counts[day] != exact_counts[day]), None))
    day = "2026-04-30"
    print("april_30_old", counts[day], "exact", exact_counts[day])
    print("april_30_old_only", sorted(snapshots[day] - exact_snapshots[day]))
    print("april_30_exact_only", sorted(exact_snapshots[day] - snapshots[day]))
    print("exact_decimal_changed_member_dates",
          [date for date in snapshots if snapshots[date] != exact_snapshots[date]])
    assert limit_price(4.10, .05, False, exact_decimal=False) == 3.89
    assert limit_price(4.10, .05, False, exact_decimal=True) == 3.90
    assert limit_price(4.00, .05, False, exact_decimal=False) == 3.80
    assert limit_price(4.00, .05, False, exact_decimal=True) == 3.80
    print("rounding_counterexamples", "4.10/3.90 old=3.89 exact=3.90; 4.00/3.85 both=3.80")
    a, b = "000632", "300961"
    def close(symbol: str, date: str) -> float:
        dates, values, _ = symbol_data(symbol)
        return values[bisect.bisect_left(dates, date)]
    impact = ((close(a, "2026-05-06") / close(a, day) - 1)
              - (close(b, "2026-05-06") / close(b, day) - 1)) / 100
    print("may_06_exact_minus_old_return", impact)

    certified = records(ROOT / "research_reports/20260926_l1_repair/st_timing/certified_st_target_lower_bound.csv")
    flags = [item["symbol"].zfill(6) in snapshots[item["target_date"]] for item in certified]
    print("certified_st_target", len(certified), "effective", sum(flags),
          "bridge_non_effective", sum(item["target_date"] == "2026-09-03" and not flag
                                       for item, flag in zip(certified, flags)))

    changed_decisions = []
    events = []
    rebalance_days = sorted(target)
    for date in (day for day in rebalance_days if day >= "2026-07-06"):
        previous = snapshots[rebalance_days[rebalance_days.index(date) - 1]]
        for symbol in sorted(set(target[date]) | previous):
            _, _, meta = symbol_data(symbol)
            if (not symbol.startswith(("0", "2")) or not st_at(meta, date)
                    or (symbol in target[date]) == (symbol in previous)):
                continue
            buying = symbol in target[date]
            old_can = can_trade(symbol, date, buying)
            new_can = can_trade(symbol, date, buying, corrected_2026_rule=True)
            events.append((date, symbol, "buy" if buying else "sell", old_can, new_can))
            if old_can != new_can:
                changed_decisions.append(events[-1])
    revised_counts, revised_snapshots = replay(corrected_2026_rule=True)
    print("post_july_st_action_events", events, "changed_decisions", changed_decisions)
    print("post_july_changed_turnover_days", [day for day in counts if counts[day] != revised_counts[day]])
    print("post_july_changed_member_days", [day for day in counts if snapshots[day] != revised_snapshots[day]])


if __name__ == "__main__":
    main()
