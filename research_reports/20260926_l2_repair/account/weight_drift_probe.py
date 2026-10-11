"""Adversarial fixed-share vs implicit daily equal-weight proxy probe.

The second return date has one missing raw close. It is valued at the last
observed close only for a conditional proxy comparison, never as verified NAV.
"""

from __future__ import annotations

import csv
import hashlib
import json
from decimal import Decimal, getcontext
from pathlib import Path


getcontext().prec = 36
ROOT = Path(__file__).resolve().parents[3]
HERE = Path(__file__).resolve().parent
DATES = ("2010-03-11", "2010-03-12", "2010-03-15")
CAPITAL = Decimal("1000000")
MEMBERS = ROOT / "outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_members.csv"
TURNOVER = ROOT / "outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_turnover.csv"
PROXY = ROOT / "outputs/wind_microcap_top_100_biweekly_thursday_16y_cached.csv"
PRICE_DIR = ROOT / ".microcap_index_cache/prices_raw"


def rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as file:
        return list(csv.DictReader(file))


def date_value(items: list[dict[str, str]], date: str, column: str) -> Decimal | None:
    hit = [item for item in items if item["date"] == date]
    if len(hit) > 1:
        raise ValueError(f"Duplicate date {date}")
    return Decimal(hit[0][column]) if hit and hit[0][column] else None


def main() -> None:
    target = sorted((r for r in rows(MEMBERS) if r["rebalance_date"] == DATES[0]), key=lambda r: int(r["rank"]))
    symbols = [r["symbol"].zfill(6) for r in target]
    if len(symbols) != 100 or len(set(symbols)) != 100:
        raise ValueError("Expected 100 unique members")
    turns = [r for r in rows(TURNOVER) if r["rebalance_date"] == DATES[0]]
    if len(turns) != 1 or any(int(turns[0][k]) != 0 for k in ("blocked_entry_count", "blocked_exit_count")):
        raise ValueError("Initial target not effective")
    old_proxy_rows = rows(PROXY)
    stock_rows = []
    stale = []
    files = {}
    for symbol in symbols:
        path = PRICE_DIR / f"{symbol}.csv"
        prices = rows(path)
        p0, p1, p2 = (date_value(prices, date, "close_raw") for date in DATES)
        if p0 is None or p1 is None:
            raise ValueError(f"Base or first mark missing: {symbol}")
        if p2 is None:
            stale.append(symbol)
        price2_conditional = p2 if p2 is not None else p1
        shares = CAPITAL / Decimal(100) / p0
        stock_rows.append({
            "symbol": symbol, "p0": p0, "p1": p1, "p2_raw": p2,
            "p2_conditional_stale_mark": price2_conditional,
            "shares": shares, "value_1": shares * p1,
            "value_2": shares * price2_conditional,
            "day2_return": price2_conditional / p1 - 1,
        })
        files[str(path.relative_to(ROOT)).replace("\\", "/")] = hashlib.sha256(path.read_bytes()).hexdigest()
    eq1 = sum((r["value_1"] for r in stock_rows), Decimal(0))
    eq2 = sum((r["value_2"] for r in stock_rows), Decimal(0))
    fixed_share_return = eq2 / eq1 - 1
    implicit_equal_return = sum((r["day2_return"] for r in stock_rows), Decimal(0)) / 100
    continuously_quoted = [r for r in stock_rows if r["p2_raw"] is not None]
    clean_eq1 = sum((r["value_1"] for r in continuously_quoted), Decimal(0))
    clean_eq2 = sum((r["value_2"] for r in continuously_quoted), Decimal(0))
    clean_fixed_return = clean_eq2 / clean_eq1 - 1
    clean_equal_return = sum((r["day2_return"] for r in continuously_quoted), Decimal(0)) / len(continuously_quoted)
    old_proxy_return = date_value(old_proxy_rows, DATES[2], "daily_return")
    target_value_each = eq1 / 100
    buy = sum((max(target_value_each - r["value_1"], Decimal(0)) for r in stock_rows), Decimal(0))
    sell = sum((max(r["value_1"] - target_value_each, Decimal(0)) for r in stock_rows), Decimal(0))
    if abs(buy - sell) > Decimal("1e-25"):
        raise AssertionError("Restoring daily equal weights did not net to zero")
    if abs(implicit_equal_return - old_proxy_return) > Decimal("1e-12"):
        raise AssertionError("Proxy does not match equal-weight stale-mark candidate")
    if abs(fixed_share_return - old_proxy_return) < Decimal("1e-12"):
        raise AssertionError("No first-day weight drift found")
    prior_turns = [r for r in rows(TURNOVER) if r["rebalance_date"] == DATES[1]]
    if prior_turns:
        raise AssertionError("Unexpected saved constituent rebalance on day 1")
    result = {
        "status": "conditional_drift_detected_not_economic_nav",
        "dates": DATES,
        "target_symbol_count": 100,
        "full_raw_quote_counts": [sum(r[k] is not None for r in stock_rows) for k in ("p0", "p1", "p2_raw")],
        "missing_raw_close_on_20100315": stale,
        "stale_mark_assumption": "The one unquoted name is carried at its 2010-03-12 raw close to mirror old proxy ffill; issuer action/valuation not verified.",
        "equity_20100312_from_fixed_shares": str(eq1),
        "equity_20100315_conditional_fixed_shares": str(eq2),
        "fixed_share_day2_return": str(fixed_share_return),
        "proxy_implicit_daily_equal_return": str(implicit_equal_return),
        "old_proxy_daily_return": str(old_proxy_return),
        "fixed_minus_proxy_return": str(fixed_share_return - old_proxy_return),
        "continuous_quoted_subset_count": len(continuously_quoted),
        "continuous_quoted_subset_fixed_return": str(clean_fixed_return),
        "continuous_quoted_subset_equal_return": str(clean_equal_return),
        "continuous_quoted_subset_fixed_minus_equal": str(clean_fixed_return - clean_equal_return),
        "daily_equal_restore_buy_value": str(buy),
        "daily_equal_restore_sell_value": str(sell),
        "daily_equal_restore_one_side_turnover": str(buy / eq1),
        "daily_equal_restore_total_two_side_value": str(buy + sell),
        "illustrative_two_side_cost_at_0p003_each_side": str((buy + sell) * Decimal("0.003")),
        "saved_proxy_constituent_rebalance_on_20100312": False,
        "input_price_sha256": files,
    }
    HERE.mkdir(parents=True, exist_ok=True)
    (HERE / "weight_drift_result.json").write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({k: result[k] for k in ("status", "missing_raw_close_on_20100315", "fixed_share_day2_return", "old_proxy_daily_return", "fixed_minus_proxy_return", "daily_equal_restore_one_side_turnover", "daily_equal_restore_total_two_side_value")}, ensure_ascii=False))


if __name__ == "__main__":
    main()
