"""Fail-closed fractional-share, stock-only T+1-close paper ledger.

This freezes order QUANTITIES at the signal close. The next close can only
settle a fixed order or reject it; it never determines an order quantity.
Historical targets and raw close files are diagnostic inputs, not authority.
"""

from __future__ import annotations

import csv
import hashlib
import json
from decimal import Decimal, getcontext
from pathlib import Path


getcontext().prec = 40
ROOT = Path(__file__).resolve().parents[3]
OUT = Path(__file__).resolve().parent
SEED = ROOT / "research_reports/20260926_l1_repair/corporate_actions/initial_2010_01_04_executed_seed.csv"
TARGETS = ROOT / "outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_members.csv"
CALENDAR = ROOT / "outputs/wind_microcap_top_100_biweekly_thursday_16y_cached.csv"
RIGHTS = ROOT / "research_reports/20260926_l1_repair/corporate_actions/rights_event_ledger_2010_2014.csv"
PRICE_DIR = ROOT / ".microcap_index_cache/prices_raw"
CAPITAL = Decimal("1000000")  # scale convention only; no old NAV is read
SIDE_FEE = Decimal("0.003")  # old proxy's per-side proportional paper rate
SIGNAL_DATE = "2010-01-04"


def rows(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def price_at(path: Path, date: str) -> Decimal | None:
    found = [x["close_raw"] for x in rows(path) if x["date"] == date]
    if len(found) > 1:
        raise ValueError(f"Duplicate raw close {path.name} {date}")
    if not found:
        return None
    value = Decimal(found[0])
    if not value.is_finite() or value <= 0:
        raise ValueError(f"Bad raw close {path.name} {date}: {value}")
    return value


def main() -> None:
    OUT.mkdir(exist_ok=True)
    seed_rows = rows(SEED)
    seed = [r["symbol"].zfill(6) for r in sorted(seed_rows, key=lambda x: int(x["rank"]))]
    if len(seed) != 100 or len(set(seed)) != 100:
        raise ValueError("Seed is not exactly 100 unique symbols")
    dates = [r["date"] for r in rows(CALENDAR)]
    if len(set(dates)) != len(dates) or dates != sorted(dates):
        raise ValueError("Calendar dates not unique and ordered")
    execution_date = next(d for d in dates if d > SIGNAL_DATE)
    next_date = next(d for d in dates if d > execution_date)
    target_rows = rows(TARGETS)
    if not any(r["rebalance_date"] == "2010-01-14" for r in target_rows):
        raise ValueError("Expected first target not present")
    rights = rows(RIGHTS)
    files = {str(p.relative_to(ROOT)).replace("\\", "/"): sha(p)
             for p in (SEED, TARGETS, CALENDAR, RIGHTS)}
    intended_value = CAPITAL / Decimal(100)
    orders = []
    for rank, symbol in enumerate(seed, 1):
        path = PRICE_DIR / f"{symbol}.csv"
        files[str(path.relative_to(ROOT)).replace("\\", "/")] = sha(path)
        signal_price = price_at(path, SIGNAL_DATE)
        execution_price = price_at(path, execution_date)
        if signal_price is None or execution_price is None:
            raise ValueError(f"Missing opening price {symbol} {SIGNAL_DATE}/{execution_date}")
        # Quantity is fully determined at T; 1+fee avoids known fee overspend
        # at unchanged prices. No T+1 price is used in the order calculation.
        shares = intended_value / (signal_price * (Decimal(1) + SIDE_FEE))
        orders.append({"rank": rank, "symbol": symbol, "side": "buy",
                       "signal_close": SIGNAL_DATE, "execution_close": execution_date,
                       "signal_raw_close": str(signal_price),
                       "frozen_fractional_shares": str(shares),
                       "execution_raw_close": str(execution_price)})

    cash = CAPITAL
    positions: dict[str, Decimal] = {}
    for order in orders:
        symbol = order["symbol"]
        price = Decimal(order["execution_raw_close"])
        quantity = Decimal(order["frozen_fractional_shares"])
        gross = price * quantity
        fee = gross * SIDE_FEE
        debit = gross + fee
        # All-or-none frozen quantity; no partial quantity sized with T+1 price.
        accepted = debit <= cash
        if accepted:
            cash -= debit
            positions[symbol] = quantity
        order.update({"conditional_booked": str(accepted).lower(),
                      "paper_gross": str(gross if accepted else Decimal(0)),
                      "paper_fee": str(fee if accepted else Decimal(0)),
                      "reject_reason": "" if accepted else "insufficient_cash_fixed_order"})
        if cash < 0:
            raise AssertionError("Negative cash")

    stock_value = sum((positions[s] * Decimal(o["execution_raw_close"])
                       for o in orders if (s := o["symbol"]) in positions), Decimal(0))
    total_fee = sum((Decimal(o["paper_fee"]) for o in orders), Decimal(0))
    equity = cash + stock_value
    if abs(equity + total_fee - CAPITAL) > Decimal("1e-25"):
        raise AssertionError("Opening cash/stock/fee conservation")

    # T+1 close positions are held throughout the following session.
    # A missing true raw close or an unverified corporate action stops replay.
    missing = []
    marks = {}
    for symbol, quantity in positions.items():
        close = price_at(PRICE_DIR / f"{symbol}.csv", next_date)
        if close is None:
            missing.append(symbol)
        else:
            marks[symbol] = str(quantity * close)
    possible_rights = [r for r in rights if r.get("symbol", "").zfill(6) in positions
                       and r.get("ex_date") == next_date]
    # RIGHTS is only a partial recovered event table; an empty set is not
    # proof of absence. The price gap alone already blocks this day.
    next_status = "requires_independent_suspension_status" if missing else "requires_rights_completeness_proof"
    result = {
        "status": "candidate_opening_only_not_certified",
        "paper_contract": "T-close fixed fractional quantities; T+1 close all-or-none fixed opening orders; 0.003 fee each conditionally booked side; price is not a fill receipt",
        "signal_date": SIGNAL_DATE, "conditional_execution_date": execution_date,
        "next_mark_date": next_date, "start_cash": str(CAPITAL),
        "opening_frozen_orders": len(orders), "conditional_booked_orders": len(positions),
        "rejected_orders": [{"symbol": o["symbol"], "reason": o["reject_reason"]}
                            for o in orders if o["conditional_booked"] == "false"],
        "cash_after": str(cash), "stock_value_after": str(stock_value),
        "paper_fee_total": str(total_fee), "equity_after": str(equity),
        "conservation_residual": str(equity + total_fee - CAPITAL),
        "next_status": next_status, "next_missing_price_symbols": sorted(missing),
        "next_listed_rights_events": [{"symbol": r["symbol"], "ex_date": r["ex_date"]}
                                      for r in possible_rights],
        "first_rebalance_signal": "2010-01-14",
        "first_rebalance_status": "not_reached_in_opening_only_stage; see conditional_early_replay.py",
        "future_rebalance_cash_policy": "Proposed: at T freeze target quantities from T equity and T raw closes, on T+1 mark old lots for entire session. T+1 buy orders may use only cash already available at T; same-close sale proceeds are excluded unless an explicit netting/settlement contract is separately established. Reject whole orders on missing price, blocked trade, insufficient pre-funded cash, or >100 unique positions; never size quantities with T+1 close. This policy has not been executed because daily ledger blocked first.",
        "limits": ["Old L1 seed/targets are uncorrected and not PIT certified",
                   "No complete security-level corporate action certificate",
                   "Raw close verifies a price, not closing-auction fill or volume",
                   "No board-lot claim; fractional shares are a paper arithmetic contract",
                   "No futures, margin, hedge, financing, or formal performance"],
        "input_sha256": files,
        "code_sha256": sha(Path(__file__)),
    }
    with (OUT / "opening_orders.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(orders[0]))
        writer.writeheader()
        writer.writerows(orders)
    (OUT / "result.json").write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({k: result[k] for k in ("status", "signal_date", "conditional_execution_date",
                   "next_mark_date", "conditional_booked_orders", "cash_after", "paper_fee_total",
                   "conservation_residual", "next_status", "next_missing_price_symbols")}, ensure_ascii=False))


if __name__ == "__main__":
    main()
