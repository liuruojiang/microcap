"""Isolated, stock-only cash/quantity ledger on real frozen close data.

This is a fractional-share paper experiment, not a trade replay or certified NAV.
It deliberately never uses old strategy return_net or nav_net to create cash.
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
AS_OF = "2010-03-11"
NEXT = "2010-03-12"
CAPITAL = Decimal("1000000")
MEMBERS = ROOT / "outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_members.csv"
TURNOVER = ROOT / "outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_turnover.csv"
PROXY = ROOT / "outputs/wind_microcap_top_100_biweekly_thursday_16y_cached.csv"
NAV = ROOT / "outputs/microcap_top100_mom16_biweekly_live_v2_0_nav.csv"
PRICE_DIR = ROOT / ".microcap_index_cache/prices_raw"


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def one(rows: list[dict[str, str]], key: str, value: str) -> dict[str, str]:
    selected = [row for row in rows if row[key] == value]
    if len(selected) != 1:
        raise ValueError(f"Expected exactly one {key}={value}; found {len(selected)}")
    return selected[0]


def main() -> None:
    members = [row for row in read_rows(MEMBERS) if row["rebalance_date"] == AS_OF]
    members.sort(key=lambda row: int(row["rank"]))
    symbols = [row["symbol"].zfill(6) for row in members]
    if len(symbols) != 100 or len(set(symbols)) != 100 or [int(r["rank"]) for r in members] != list(range(1, 101)):
        raise ValueError("Target basket must have 100 unique, contiguous ranks")
    turn = one(read_rows(TURNOVER), "rebalance_date", AS_OF)
    if any(int(turn[k]) != 0 for k in ("blocked_entry_count", "blocked_exit_count")):
        raise ValueError("Target basket may differ from effective basket")
    if turn["return_start_date"] != NEXT or turn["execution_timing"] != "close":
        raise ValueError("Unexpected old proxy timing")

    files = {str(p.relative_to(ROOT)).replace("\\", "/"): sha(p) for p in (MEMBERS, TURNOVER, PROXY, NAV)}
    lots = []
    cash = CAPITAL
    initial_price_value = Decimal(0)
    final_price_value = Decimal(0)
    per_stock_allocation = CAPITAL / Decimal(100)
    for rank, symbol in enumerate(symbols, 1):
        price_file = PRICE_DIR / f"{symbol}.csv"
        raw = read_rows(price_file)
        p0 = Decimal(one(raw, "date", AS_OF)["close_raw"])
        p1 = Decimal(one(raw, "date", NEXT)["close_raw"])
        if p0 <= 0 or p1 <= 0:
            raise ValueError(f"Nonpositive raw close: {symbol}")
        shares = per_stock_allocation / p0  # paper fractional shares, zero fees
        debit = shares * p0
        cash -= debit
        initial_price_value += debit
        final_price_value += shares * p1
        files[str(price_file.relative_to(ROOT)).replace("\\", "/")] = sha(price_file)
        lots.append({"rank": rank, "symbol": symbol, "close_0": str(p0), "close_1": str(p1),
                     "shares_fractional": str(shares), "cash_debit_0": str(debit),
                     "mark_1": str(shares * p1)})

    equity_0 = cash + initial_price_value
    equity_1 = cash + final_price_value
    stock_return = equity_1 / equity_0 - 1
    source_proxy = one(read_rows(PROXY), "date", NEXT)
    source_nav = one(read_rows(NAV), "date", NEXT)
    proxy_return = Decimal(source_proxy["daily_return"])
    old_microcap_return = Decimal(source_nav["microcap_ret"])
    if abs(cash) > Decimal("1e-20") or equity_0 != CAPITAL:
        raise AssertionError("Initial stock cash or capital conservation failed")
    if abs(stock_return - proxy_return) > Decimal("1e-12"):
        raise AssertionError(f"Raw stock mark diverges from old proxy: {stock_return} vs {proxy_return}")
    if abs(stock_return - old_microcap_return) > Decimal("1e-12"):
        raise AssertionError(f"Raw stock mark diverges from old microcap_ret: {stock_return} vs {old_microcap_return}")

    HERE.mkdir(parents=True, exist_ok=True)
    with (HERE / "stock_lots_20100311_12.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(lots[0]))
        writer.writeheader()
        writer.writerows(lots)
    result = {
        "status": "diagnostic_stock_only_one_day_pass",
        "source_cutoff": NEXT,
        "start_capital": str(CAPITAL),
        "as_of_close": AS_OF,
        "next_close": NEXT,
        "stock_count": len(lots),
        "position_construction": "100 target symbols; blocked entries/exits both zero; paper fractional shares; equal cash allocation at same-date raw close",
        "cash_0": str(cash),
        "stock_value_0": str(initial_price_value),
        "equity_0": str(equity_0),
        "stock_value_1": str(final_price_value),
        "equity_1": str(equity_1),
        "independent_stock_return": str(stock_return),
        "old_proxy_daily_return": str(proxy_return),
        "old_v20_microcap_return": str(old_microcap_return),
        "proxy_residual": str(stock_return - proxy_return),
        "input_sha256": files,
        "exclusions": ["No actual prior position quantity", "No rebalance trades/fees", "No futures contracts, variation margin or collateral", "No dividends/corporate action cash flow", "No executable same-close order evidence", "L1 candidate/ST defects retained"],
    }
    (HERE / "stock_ledger_result.json").write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({key: result[key] for key in ("status", "stock_count", "equity_0", "equity_1", "independent_stock_return", "proxy_residual")}, ensure_ascii=False))


if __name__ == "__main__":
    main()
