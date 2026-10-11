"""Read-only coverage audit for a T-close / T+1-close fixed-quantity paper ledger.

This deliberately does not compute revised holdings, orders, or NAV. It audits
frozen old members and local data availability; L1 membership remains invalid.
"""

from __future__ import annotations

import csv
import hashlib
import json
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
HERE = Path(__file__).resolve().parent
L1 = ROOT / "research_reports/20260926_l1_repair"
RAW = ROOT / ".microcap_index_cache/prices_raw"


def rows(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def members(path: Path, date_field: str) -> dict[str, list[str]]:
    by_date: dict[str, list[str]] = {}
    for row in rows(path):
        by_date.setdefault(row[date_field], []).append(row["symbol"].zfill(6))
    return by_date


def main() -> None:
    seed_path = L1 / "corporate_actions/initial_2010_01_04_executed_seed.csv"
    targets_path = ROOT / "outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_members.csv"
    effective_path = L1 / "full_replay/members_by_rebalance.csv"
    returns_path = L1 / "full_replay/all_daily_returns.csv"
    turnover_path = ROOT / "outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_turnover.csv"
    first_orders_path = ROOT / "research_reports/20260926_l2_repair/clock/next_close_first_rebalance_orders.csv"
    rights_path = L1 / "corporate_actions/rights_event_ledger_2010_2014.csv"
    cninfo_rights_path = L1 / "corporate_actions/cninfo_distribution_events.csv"
    bao_600982_path = L1 / "st_timing/initial_old100_screen/baostock_responses/600982.json"
    shares_600982_path = ROOT / ".microcap_index_cache/share_change/600982.csv"
    checked = [seed_path, targets_path, effective_path, returns_path,
               turnover_path, first_orders_path, rights_path, cninfo_rights_path,
               bao_600982_path, shares_600982_path]
    seed = [row["symbol"].zfill(6) for row in rows(seed_path)]
    target = members(targets_path, "rebalance_date")
    effective = members(effective_path, "rebalance_date")
    dates = sorted({row["date"] for row in rows(returns_path)})
    next_day = {day: dates[index + 1] for index, day in enumerate(dates[:-1])}
    first_target = min(target)
    first_execution = next_day[first_target]
    first_orders = rows(first_orders_path)

    # Scan only the old seed and historical effective members. This is a
    # minimum input-coverage check, not a claim about the corrected L1 pool.
    symbols = set(seed)
    symbols.update(sym for basket in effective.values() for sym in basket)
    symbols.update(sym for basket in target.values() for sym in basket)
    raw_data: dict[str, dict[str, str]] = {}
    missing_raw_files: list[str] = []
    for symbol in sorted(symbols):
        path = RAW / f"{symbol}.csv"
        if not path.exists():
            missing_raw_files.append(symbol)
            continue
        raw_data[symbol] = {row["date"]: row["close_raw"] for row in rows(path)}

    def coverage(day: str, basket: list[str]) -> dict:
        absent = sorted({sym for sym in basket if not raw_data.get(sym, {}).get(day)})
        return {"date": day, "basket_size": len(basket),
                "raw_close_count": len(basket) - len(absent), "missing_raw_close": absent}

    initial_formation = "2010-01-04"
    initial_execution = next((day for day in dates if day > initial_formation), None)
    first_union = sorted(set(seed) | set(target[first_target]))
    first_orders_symbols = sorted({row["symbol"].zfill(6) for row in first_orders})
    event_dates = sorted(effective)
    event_checks = []
    for day in event_dates:
        if day not in next_day:
            continue
        # Current old effective basket was produced at the *same* close.
        # Use it only as a lower-bound list of symbols to source for a new run.
        event_checks.append(coverage(next_day[day], effective[day]))
    first_incomplete_effective = next(
        (item for item in event_checks if item["missing_raw_close"]), None
    )
    rights = rows(rights_path)
    bao_600982 = json.loads(bao_600982_path.read_text(encoding="utf-8"))
    bao_600982_day = next(r for r in bao_600982["rows"] if r["date"] == "2010-01-06")
    cninfo_600982 = [r for r in rows(cninfo_rights_path) if r["symbol"] == "600982"]
    shares_600982 = rows(shares_600982_path)
    result = {
        "scope": "old contaminated members; raw close availability only; no corrected account",
        "source_sha256": {str(p.relative_to(ROOT)): sha256(p) for p in checked},
        "old_target_dates": len(target),
        "old_effective_dates": len(effective),
        "old_return_dates": len(dates),
        "old_seed_unique": len(set(seed)),
        "old_target_first_date": first_target,
        "first_target_t_plus_one_close": first_execution,
        "raw_symbols_scanned": len(symbols),
        "missing_raw_files": missing_raw_files,
        "initial_seed_signal_close": coverage(initial_formation, seed),
        "initial_seed_t_plus_one_close": coverage(initial_execution, seed),
        "initial_seed_2010_01_06_close": coverage("2010-01-06", seed),
        "held_600982_2010_01_06": {
            "in_old_seed": "600982" in seed,
            "prior_real_close": raw_data["600982"]["2010-01-05"],
            "missing_real_close_on_day": "2010-01-06" not in raw_data["600982"],
            "next_real_close": raw_data["600982"]["2010-01-07"],
            "baostock_day": bao_600982_day,
            "cninfo_distribution_rows_for_symbol": len(cninfo_600982),
            "cninfo_ex_event_on_day": [r for r in cninfo_600982 if r["除权日"] == "2010-01-06"],
            "cninfo_ex_dates_2009_2010": [r["除权日"] for r in cninfo_600982 if r["除权日"].startswith(("2009", "2010"))],
            "share_change_on_day": [r for r in shares_600982 if r["change_date"] == "2010-01-06"],
        },
        "first_target_and_seed_union_t_plus_one_close": coverage(first_execution, first_union),
        "first_order_symbols_t_plus_one_close": coverage(first_execution, first_orders_symbols),
        "first_order_rows": len(first_orders),
        "old_effective_next_close_dates_scanned": len(event_checks),
        "old_effective_next_close_dates_with_missing_raw": sum(
            bool(item["missing_raw_close"]) for item in event_checks),
        "first_old_effective_next_close_missing": first_incomplete_effective,
        "rights_event_ledger_rows": len(rights),
        "rights_event_ledger_min_ex_date": min((r["ex_date"] for r in rights), default=None),
        "rights_event_ledger_max_ex_date": max((r["ex_date"] for r in rights), default=None),
        "rights_event_ledger_status_counts": dict(Counter(r.get("status", "") for r in rights)),
        "no_local_full_history_independent_daily_st_trade_status": True,
        "no_local_full_history_contract_settlement_margin_ledger": True,
    }
    HERE.mkdir(parents=True, exist_ok=True)
    (HERE / "coverage.json").write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    with (HERE / "old_effective_next_close_gaps.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["date", "basket_size", "raw_close_count", "missing_raw_close"])
        writer.writeheader()
        for item in event_checks:
            if item["missing_raw_close"]:
                writer.writerow({**item, "missing_raw_close": "|".join(item["missing_raw_close"])})
    print(json.dumps({key: result[key] for key in (
        "old_target_dates", "old_effective_dates", "old_seed_unique", "raw_symbols_scanned",
        "initial_seed_t_plus_one_close", "first_target_and_seed_union_t_plus_one_close",
        "first_order_symbols_t_plus_one_close", "old_effective_next_close_dates_with_missing_raw",
        "first_old_effective_next_close_missing", "rights_event_ledger_rows")}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
