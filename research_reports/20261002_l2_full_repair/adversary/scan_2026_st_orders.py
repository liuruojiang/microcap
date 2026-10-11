"""Read-only independent scan of post-2026-07-06 old rebalance ST orders."""

from __future__ import annotations

import bisect
import csv
import hashlib
import json
from collections import defaultdict
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path


D = Decimal
ROOT = Path(__file__).resolve().parents[3]
HERE = Path(__file__).resolve().parent
TARGET = ROOT / "outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_members.csv"
EFFECTIVE = ROOT / "research_reports/20260926_l1_repair/full_replay/members_by_rebalance.csv"
CACHE = ROOT / ".microcap_index_cache"
START = "2026-07-06"


def rows(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8-sig", newline="") as stream:
        return list(csv.DictReader(stream))


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def mainboard(symbol: str) -> bool:
    return symbol.startswith(("000", "001", "002", "003", "600", "601", "603", "605"))


def raw_close_pair(symbol: str, date: str) -> tuple[D, D] | None:
    r = rows(CACHE / "prices_raw" / f"{symbol}.csv")
    days = [x["date"] for x in r]
    idx = bisect.bisect_left(days, date)
    if idx <= 0 or idx == len(days) or days[idx] != date:
        return None
    return D(r[idx - 1]["close_raw"]), D(r[idx]["close_raw"])


def limit(prev: D, band: D, side: str) -> D:
    sign = D(1) if side == "buy" else D(-1)
    return (prev * (D(1) + sign * band)).quantize(D("0.01"), rounding=ROUND_HALF_UP)


def main() -> None:
    targets: dict[str, list[str]] = defaultdict(list)
    effective: dict[str, set[str]] = defaultdict(set)
    for r in rows(TARGET):
        targets[r["rebalance_date"]].append(r["symbol"].zfill(6))
    for r in rows(EFFECTIVE):
        effective[r["rebalance_date"]].add(r["symbol"].zfill(6))
    dates = sorted(targets)
    cases = []
    all_candidate_orders = 0
    candidate_mainboard_st_orders = 0
    source_hashes = {str(p.relative_to(ROOT)): sha(p) for p in (TARGET, EFFECTIVE)}
    for date in dates:
        if date < START:
            continue
        prevdate = dates[dates.index(date) - 1]
        old, wanted = effective[prevdate], set(targets[date])
        orders = [(s, "sell") for s in sorted(old - wanted)] + [(s, "buy") for s in targets[date] if s not in old]
        all_candidate_orders += len(orders)
        for symbol, side in orders:
            if not mainboard(symbol):
                continue
            meta_path = CACHE / "security_meta" / f"{symbol}.json"
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
            st = any(period["start"] <= date and (period["end"] is None or date <= period["end"])
                     for period in meta.get("st_intervals", []))
            if not st:
                continue
            candidate_mainboard_st_orders += 1
            pair = raw_close_pair(symbol, date)
            if pair is None:
                cases.append({"signal_date": date, "symbol": symbol, "side": side,
                              "st_cached": True, "raw_close_status": "missing_or_first"})
                continue
            prev, close = pair
            old_limit, corrected_limit = limit(prev, D("0.05"), side), limit(prev, D("0.10"), side)
            case = {"signal_date": date, "symbol": symbol, "side": side,
                    "st_cached": True, "previous_true_raw_close": str(prev), "raw_close": str(close),
                    "old_5pct_side_limit": str(old_limit), "corrected_10pct_side_limit": str(corrected_limit),
                    "old_limit_block": close == old_limit, "corrected_limit_block": close == corrected_limit,
                    "in_5_to_10pct_direction_band": (D("0.05") < (close / prev - 1) * (D(1) if side == "buy" else D(-1)) <= D("0.10")),
                    "classification_changed": (close == old_limit) != (close == corrected_limit)}
            cases.append(case)
            source_hashes[str(meta_path.relative_to(ROOT))] = sha(meta_path)
            raw_path = CACHE / "prices_raw" / f"{symbol}.csv"
            source_hashes[str(raw_path.relative_to(ROOT))] = sha(raw_path)
    out = {"status": "old_orders_post_rule_change_scanned",
           "window": {"first_rule_date": START, "last_rebalance": dates[-1],
                      "rebalance_dates": [d for d in dates if d >= START]},
           "old_candidate_order_count": all_candidate_orders,
           "mainboard_cached_ST_candidate_order_count": candidate_mainboard_st_orders,
           "cases": cases,
           "classification_changed_count": sum(c.get("classification_changed", False) for c in cases),
           "side_5_to_10pct_band_count": sum(c.get("in_5_to_10pct_direction_band", False) for c in cases),
           "source_sha256": source_hashes,
           "scope": "Old same-close target minus prior effective candidate orders, not new T+1 fixed-share executed orders."}
    HERE.mkdir(parents=True, exist_ok=True)
    (HERE / "post_20260706_st_orders.json").write_text(json.dumps(out, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({k: out[k] for k in ("old_candidate_order_count", "mainboard_cached_ST_candidate_order_count", "classification_changed_count", "side_5_to_10pct_band_count")}, ensure_ascii=False))


if __name__ == "__main__":
    main()
