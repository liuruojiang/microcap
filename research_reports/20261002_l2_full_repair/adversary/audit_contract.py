"""Independent L2 contract counterexamples; no strategy engine imports.

All monetary values below are paper arithmetic.  The real-date examples are
read from the refreshed old v2.0 stream solely to challenge model assumptions.
"""

from __future__ import annotations

import csv
import hashlib
import json
from decimal import Decimal
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
OLD_BASE = ROOT / "outputs/microcap_top100_mom16_hedge_zz1000_0p8x_biweekly_thursday_16y_v2_0_base_costed_nav.csv"
OLD_V20 = ROOT / "outputs/microcap_top100_mom16_biweekly_live_v2_0_nav.csv"
RAW_000632 = ROOT / ".microcap_index_cache/prices_raw/000632.csv"
HERE = Path(__file__).resolve().parent
D = Decimal


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def table(path: Path) -> dict[str, dict[str, str]]:
    with path.open(encoding="utf-8-sig", newline="") as stream:
        return {row["date"]: row for row in csv.DictReader(stream)}


def close_prices(path: Path) -> dict[str, Decimal]:
    with path.open(encoding="utf-8-sig", newline="") as stream:
        return {row["date"]: D(row["close_raw"]) for row in csv.DictReader(stream)}


def main() -> None:
    base, v20 = table(OLD_BASE), table(OLD_V20)
    h = base["2021-03-01"]
    assert h["holding"] != "cash"
    stock_return = D(h["microcap_ret"])
    hedge_return = D(h["hedge_ret"])
    drag = D("0.00024")
    short_notional = D("0.8")
    # Unit capital, exactly 1x long stock and zero cash at the previous close.
    cash_after_swap = -short_notional * hedge_return - drag
    equity_after = D(1) + stock_return + cash_after_swap
    assert hedge_return > 0 and cash_after_swap < 0 and equity_after > 0
    assert abs(equity_after - (D(1) + stock_return - short_notional * hedge_return - drag)) < D("1e-25")

    # With fixed hedge units, market value of short exposure drifts by the
    # index return.  Restoring -0.8 times ending equity entails a reset.
    short_exposure_before_reset = -short_notional * (D(1) + hedge_return)
    target_short_after_reset = -short_notional * equity_after
    reset_short_notional = target_short_after_reset - short_exposure_before_reset
    assert reset_short_notional != 0

    p = close_prices(RAW_000632)
    assert p["2026-04-28"] == D("4.1") and "2026-04-29" not in p and p["2026-04-30"] == D("3.9")
    limit_down_exact = (D("4.1") * (D(1) - D("0.05"))).quantize(D("0.01"), rounding="ROUND_HALF_UP")
    assert limit_down_exact == D("3.9")

    late = {date: {key: v20[date][key] for key in ("holding", "next_holding", "return_net", "total_cost")}
            for date in ("2026-09-22", "2026-09-23", "2026-09-24")}
    assert late["2026-09-22"]["holding"] == "cash"
    assert late["2026-09-22"]["next_holding"] != "cash"
    assert late["2026-09-23"]["next_holding"] == "cash"
    assert late["2026-09-24"]["next_holding"] != "cash"

    # Independent arithmetic tests, deliberately with only two/three stocks.
    # A T-close frozen share cannot be recomputed at a later close.
    frozen_qty = D("100") / D("100")
    future_retro_qty = D("100") / D("120")
    assert frozen_qty != future_retro_qty
    old_close_t, old_close_next, new_close_t, new_close_next = map(D, (100, 110, 100, 120))
    old_day_gain = old_close_next / old_close_t - 1
    new_day_gain = new_close_next / new_close_t - 1
    assert old_day_gain == D("0.1") and new_day_gain == D("0.2")
    # Selling old A and buying new B at T+1 close cannot credit the 20% B gain
    # that occurred before the buy; the pretrade equity is 110, not 120.
    equity_before_t1_close_trade = D(1) * old_close_next
    assert equity_before_t1_close_trade == D(110)

    # Split/dividend counterexamples for a raw-price, fixed-share ledger.
    split_raw_loss = D(1) * D(50) - D(1) * D(100)
    split_true_delta = D(2) * D(50) - D(1) * D(100)
    dividend_raw_loss = D(99) - D(100)
    dividend_economic_delta = D(99) + D(1) - D(100)
    assert split_raw_loss == -50 and split_true_delta == 0
    assert dividend_raw_loss == -1 and dividend_economic_delta == 0

    # Funding/capacity: rejected sell must retain stock and a seat.  A new
    # purchase must not be booked with cash at zero unless an explicit credit
    # line or same-close theoretical netting rule has been invoked.
    held = {"A": D(1), "B": D(1)}
    blocked_sell = "A"
    available_seats = 2 - len(held)
    buy_c_permitted = available_seats > 0
    assert blocked_sell in held and not buy_c_permitted
    fee = D("0.003")
    buy_cost = D(1) * D(120) * (D(1) + fee)
    cash_before = D(0)
    assert buy_cost > cash_before

    out = {
        "status": "contract_counterexamples_pass",
        "input_sha256": {str(path.relative_to(ROOT)): sha(path) for path in (OLD_BASE, OLD_V20, RAW_000632)},
        "real_recent_hedge_example": {
            "date": "2021-03-01",
            "old_base_stock_return": str(stock_return),
            "old_base_hedge_index_return": str(hedge_return),
            "assumed_initial_stock": "1",
            "assumed_initial_cash": "0",
            "swap_short_notional": "0.8",
            "daily_drag": str(drag),
            "cash_after_pnl_and_drag": str(cash_after_swap),
            "equity_after": str(equity_after),
            "required_reset_short_notional_as_share_initial_equity": str(reset_short_notional),
        },
        "real_recent_limit_example": {
            "symbol": "000632", "last_traded_close_date": "2026-04-28",
            "last_traded_close": str(p["2026-04-28"]), "missing_raw_date": "2026-04-29",
            "next_close_date": "2026-04-30", "next_close": str(p["2026-04-30"]),
            "conditional_ST_5pct_limit_down_exact": str(limit_down_exact),
        },
        "real_late_pending_orders": late,
        "synthetic": {
            "frozen_quantity_T": str(frozen_qty), "invalid_future_resized_quantity": str(future_retro_qty),
            "old_held_stock_return_to_T1_close": str(old_day_gain),
            "new_unheld_stock_return_to_T1_close": str(new_day_gain),
            "equity_before_T1_close_trade": str(equity_before_t1_close_trade),
            "split_raw_delta": str(split_raw_loss), "split_economic_delta": str(split_true_delta),
            "dividend_raw_delta": str(dividend_raw_loss), "dividend_economic_delta": str(dividend_economic_delta),
            "blocked_sell_retains_member_capacity": not buy_c_permitted,
            "buy_cost_with_fee": str(buy_cost), "cash_before_buy": str(cash_before),
        },
    }
    HERE.mkdir(parents=True, exist_ok=True)
    (HERE / "contract_counterexamples.json").write_text(json.dumps(out, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"status": out["status"], "cash_after_pnl_and_drag": str(cash_after_swap), "reset_short_notional": str(reset_short_notional)}, ensure_ascii=False))


if __name__ == "__main__":
    main()
