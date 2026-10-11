"""Independent, fail-closed T-close/T+1-close account challenge.

Uses frozen old members as an adversarial fixture, not as certified members.
No portfolio NAV or strategy performance is produced.
"""

from __future__ import annotations

import csv
import hashlib
import json
from decimal import Decimal, ROUND_FLOOR, ROUND_HALF_UP
from pathlib import Path


HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
SIGNAL = "2010-01-14"
EXECUTION = "2010-01-15"
SEED = ROOT / "research_reports/20260926_l1_repair/corporate_actions/initial_2010_01_04_executed_seed.csv"
TARGET = ROOT / "outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_members.csv"
BARS = ROOT / "research_reports/20260926_l2_repair/clock/baostock_raw_open_windows.csv"
RAW = ROOT / ".microcap_index_cache/prices_raw"


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8-sig") as file:
        return list(csv.DictReader(file))


def code(value: str) -> str:
    return value.zfill(6)


def price(path: Path, date: str) -> Decimal | None:
    matches = [Decimal(row["close_raw"]) for row in rows(path) if row["date"] == date]
    if len(matches) > 1:
        raise ValueError(f"duplicate raw close: {path.name} {date}")
    return matches[0] if matches else None


def integer_lots(budget: Decimal, known_price: Decimal, lot: int = 100) -> int:
    return int((budget / (known_price * lot)).to_integral_value(rounding=ROUND_FLOOR)) * lot


def run() -> dict[str, object]:
    seed_rows = rows(SEED)
    target_rows = [r for r in rows(TARGET) if r["rebalance_date"] == SIGNAL]
    seed = {code(r["symbol"]) for r in seed_rows}
    target = {code(r["symbol"]) for r in target_rows}
    assert len(seed_rows) == len(seed) == len(target_rows) == len(target) == 100

    union = seed | target
    path_hashes: dict[str, str] = {}
    quotes: dict[str, dict[str, Decimal | None]] = {}
    for symbol in sorted(union):
        path = RAW / f"{symbol}.csv"
        if not path.exists():
            quotes[symbol] = {SIGNAL: None, EXECUTION: None}
            continue
        path_hashes[symbol] = digest(path)
        quotes[symbol] = {date: price(path, date) for date in (SIGNAL, EXECUTION)}

    bar_day = {code(r["symbol"]): r for r in rows(BARS) if r["date"] == EXECUTION}
    proposed_sells = sorted(seed - target)
    proposed_buys = sorted(target - seed)
    assert len(proposed_sells) == len(proposed_buys) == 10

    missing_old_close = [s for s in sorted(seed) if quotes[s][EXECUTION] is None]
    missing_changed = [s for s in proposed_sells + proposed_buys if quotes[s][EXECUTION] is None]
    status_conflict = [s for s in sorted(union) if s in bar_day and
                       ((bar_day[s]["tradestatus"] == "0") != (quotes[s][EXECUTION] is None))]
    blocked_sells = [s for s in proposed_sells if quotes[s][EXECUTION] is None or
                     s not in bar_day or bar_day[s]["tradestatus"] != "1"]
    limit_symbol = "600180"
    limit_bar = bar_day[limit_symbol]
    limit_5pct = (Decimal(limit_bar["preclose"]) * Decimal("1.05")).quantize(
        Decimal("0.01"), rounding=ROUND_HALF_UP
    )
    limit_hit_if_st = Decimal(limit_bar["close"]) == limit_5pct
    assert limit_hit_if_st and quotes[limit_symbol][EXECUTION] == limit_5pct

    # At T the executable close price is unknown. These actual closes produce
    # a counterexample to sizing all shares from an assumed T+1 price.
    chosen = "000019"
    assert chosen in proposed_buys and quotes[chosen][SIGNAL] and quotes[chosen][EXECUTION]
    old_price = quotes[chosen][SIGNAL]
    new_price = quotes[chosen][EXECUTION]
    assert old_price is not None and new_price is not None
    # CNY 1,050 buys one 100-share lot at T even with a 0.3% illustrative fee,
    # but cannot buy that lot at the unknown T+1 close.
    frozen_budget = Decimal("1050")
    frozen_qty = integer_lots(frozen_budget, old_price)
    next_close_qty = integer_lots(frozen_budget, new_price)
    shortfall = frozen_qty * new_price - frozen_budget
    assert frozen_qty == 100 and next_close_qty == 0 and shortfall > 0

    # A separate real-price, illustrative 100-share sale/buy ledger. It closes
    # algebraically only after the sell is credited; no auction fill is implied.
    old_symbol = "000710"
    assert old_symbol in proposed_sells
    sell_close = quotes[old_symbol][EXECUTION]
    old_signal_close = quotes[old_symbol][SIGNAL]
    assert old_signal_close is not None
    assert sell_close is not None
    fee_rate = Decimal("0.003")  # illustrative per-side rate, not a formal cost claim
    pretrade_stock_value = 100 * sell_close
    sell_proceeds = pretrade_stock_value * (1 - fee_rate)
    buy_outflow = 100 * new_price * (1 + fee_rate)
    remaining_cash = sell_proceeds - buy_outflow
    posttrade_equity = remaining_cash + 100 * new_price
    total_fees = fee_rate * (pretrade_stock_value + 100 * new_price)
    assert remaining_cash >= 0
    assert pretrade_stock_value - posttrade_equity == total_fees
    assert buy_outflow > 0  # with zero pre-existing cash, buy-before-sell fails

    output: dict[str, object] = {
        "scope": "first old rebalance, diagnostic refusal only",
        "signal_close": SIGNAL,
        "candidate_execution_close": EXECUTION,
        "seed_count": len(seed),
        "target_count": len(target),
        "proposed_sells": proposed_sells,
        "proposed_buys": proposed_buys,
        "old_positions_without_real_T1_close": missing_old_close,
        "changed_symbols_without_real_T1_close": missing_changed,
        "missing_or_nontrading_exits": blocked_sells,
        "max_entries_after_unsold_exit_capacity": len(proposed_sells) - len(blocked_sells),
        "bar_raw_status_conflict": status_conflict,
        "conditional_close_limit_challenge": {
            "symbol": limit_symbol,
            "baostock_isST_diagnostic": limit_bar["isST"],
            "preclose": limit_bar["preclose"],
            "T1_close": limit_bar["close"],
            "if_5pct_ST_limit_up_price": str(limit_5pct),
            "buy_block_if_issuer_ST_status_confirmed": limit_hit_if_st,
        },
        "T1_close_sizing_counterexample": {
            "symbol": chosen,
            "T_raw_close": str(old_price),
            "T1_raw_close": str(new_price),
            "T_frozen_cash_budget": str(frozen_budget),
            "T_frozen_100_share_quantity": frozen_qty,
            "T1_close_recomputed_100_share_quantity": next_close_qty,
            "cash_shortfall_if_frozen_100_share_buy_fills": str(shortfall),
        },
        "overnight_and_session_ownership": {
            "old_symbol": old_symbol,
            "new_symbol": chosen,
            "old_T_to_T1_close_return": str(sell_close / old_signal_close - 1),
            "new_T_to_T1_close_return_not_owned": str(new_price / old_price - 1),
            "T1_full_day_return_owner": old_symbol,
            "new_position_earliest_return_day": "2010-01-18",
        },
        "conditional_stock_cash_check": {
            "sell_symbol": old_symbol,
            "buy_symbol": chosen,
            "shares_each": 100,
            "initial_cash": "0",
            "sell_close": str(sell_close),
            "buy_close": str(new_price),
            "illustrative_one_side_fee_rate": str(fee_rate),
            "sell_proceeds_after_fee": str(sell_proceeds),
            "buy_outflow_with_fee": str(buy_outflow),
            "posttrade_cash_if_sell_precedes_buy": str(remaining_cash),
            "posttrade_stock_plus_cash": str(posttrade_equity),
            "fee_conservation_residual": str(pretrade_stock_value - posttrade_equity - total_fees),
            "buy_pre_funded_from_initial_cash": False,
            "same_auction_sell_then_buy_execution_evidence": False,
        },
        "gate": "REFUSE_FULL_SLICE",
        "gate_reasons": [
            "T1 close price and sell proceeds are unknown when T-close orders are frozen",
            "old holdings include positions without real T1 close/trading evidence",
            "same-close auction sequence and fill evidence are absent",
            "PIT membership, ST, corporate-action and cash event ledgers remain incomplete",
        ],
        "input_sha256": {
            "seed": digest(SEED),
            "target": digest(TARGET),
            "baostock_raw_bars": digest(BARS),
            "raw_close_files": path_hashes,
        },
    }
    assert missing_old_close and blocked_sells
    assert output["gate"] == "REFUSE_FULL_SLICE"
    (HERE / "result.json").write_text(json.dumps(output, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return output


if __name__ == "__main__":
    result = run()
    print(json.dumps({k: v for k, v in result.items() if k != "input_sha256"}, ensure_ascii=False, indent=2))
