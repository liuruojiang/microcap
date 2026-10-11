"""Independent real-window accounting challenge for the old 0.8x index hedge.

Uses the old paper microcap proxy as a *price unit* solely to isolate hedge
funding. This does not validate the proxy's daily equal-weight stock basket.
"""

from __future__ import annotations

import csv
import hashlib
import json
from decimal import Decimal, getcontext
from pathlib import Path


getcontext().prec = 40
D = Decimal
ROOT = Path(__file__).resolve().parents[3]
HERE = Path(__file__).resolve().parent
SRC = ROOT / "outputs/microcap_top100_mom16_hedge_zz1000_0p8x_biweekly_thursday_16y_v2_0_base_costed_nav.csv"
START, END = "2026-07-13", "2026-09-15"
BETA = D("0.8")
DRAG_ON_HEDGE_NOTIONAL = D("0.0003")


def rows() -> list[dict[str, str]]:
    with SRC.open(encoding="utf-8-sig", newline="") as stream:
        return [r for r in csv.DictReader(stream) if START <= r["date"] <= END]


def main() -> None:
    day = rows()
    assert len(day) == 47
    assert all(r["holding"] == "long_microcap_short_zz1000" for r in day)
    # Variant A: daily 1x stock proxy reset and daily 0.8x short reset.
    # The cash sweep into/out of the stock sleeve closes cash each day, but is
    # a *daily stock transaction* absent from the old fortnightly fee ledger.
    daily_equity = D(1)
    daily_stock_sweep_abs = D(0)
    daily_hedge_reset_abs = D(0)
    daily_path = []
    prior_short_abs = None
    # Variant B: no stock sweep; short notional still resets to 0.8x equity.
    floating_stock, floating_cash = D(1), D(0)
    floating_first_cash_deficit = None
    floating_max_cash_deficit = D(0)
    # Variant C: fixed stock unit and fixed 0.8x initial short notional.
    fixed_stock, fixed_cash = D(1), D(0)
    fixed_first_cash_deficit = None
    fixed_max_cash_deficit = D(0)
    fixed_path = []
    max_old_formula_diff = D(0)
    for r in day:
        rs, rh = D(r["microcap_ret"]), D(r["hedge_ret"])
        prior_equity = daily_equity
        short_abs = BETA * prior_equity
        swap_pnl = -short_abs * rh
        drag = short_abs * DRAG_ON_HEDGE_NOTIONAL
        cash_before_sweep = swap_pnl - drag
        stock_before_sweep = prior_equity * (D(1) + rs)
        daily_equity = stock_before_sweep + cash_before_sweep
        assert daily_equity > 0
        # Cash sweep equals trade value; negative means stock sale for debt.
        daily_stock_sweep_abs += abs(cash_before_sweep)
        if prior_short_abs is not None:
            matured_short_abs = prior_short_abs * (D(1) + rh)
            daily_hedge_reset_abs += abs(BETA * daily_equity - matured_short_abs)
        prior_short_abs = BETA * daily_equity
        observed = D(r["return_raw"])
        reconstructed = rs - BETA * rh - BETA * DRAG_ON_HEDGE_NOTIONAL
        max_old_formula_diff = max(max_old_formula_diff, abs(observed - reconstructed))
        assert abs(observed - reconstructed) < D("1e-14")
        daily_path.append({"date": r["date"], "stock_return": str(rs), "index_return": str(rh),
                           "starting_equity": str(prior_equity), "swap_notional": str(short_abs),
                           "swap_pnl": str(swap_pnl), "drag": str(drag),
                           "cash_before_stock_sweep": str(cash_before_sweep),
                           "stock_sweep_trade": str(cash_before_sweep), "ending_equity": str(daily_equity)})

        # Floating stock quantity: synthetic proxy fixed units, no daily stock
        # trade. Negative cash is explicit interest-free borrowing, not zero.
        floating_start_equity = floating_stock + floating_cash
        floating_short_abs = BETA * floating_start_equity
        floating_stock *= D(1) + rs
        floating_cash += -floating_short_abs * rh - floating_short_abs * DRAG_ON_HEDGE_NOTIONAL
        assert abs(floating_stock + floating_cash - (
            floating_start_equity + (floating_stock / (D(1) + rs)) * rs
            - floating_short_abs * rh - floating_short_abs * DRAG_ON_HEDGE_NOTIONAL)) < D("1e-30")
        if floating_cash < 0:
            floating_first_cash_deficit = floating_first_cash_deficit or r["date"]
            floating_max_cash_deficit = max(floating_max_cash_deficit, -floating_cash)

        fixed_stock *= D(1) + rs
        fixed_cash += -BETA * rh - BETA * DRAG_ON_HEDGE_NOTIONAL
        if fixed_cash < 0:
            fixed_first_cash_deficit = fixed_first_cash_deficit or r["date"]
            fixed_max_cash_deficit = max(fixed_max_cash_deficit, -fixed_cash)
        fixed_path.append({"date": r["date"], "floating_equity": str(floating_stock + floating_cash),
                           "floating_cash": str(floating_cash), "fixed_hedge_equity": str(fixed_stock + fixed_cash),
                           "fixed_hedge_cash": str(fixed_cash)})

    result = {
        "status": "hedge_arithmetic_pass_funding_unproven",
        "source": str(SRC.relative_to(ROOT)), "sha256": hashlib.sha256(SRC.read_bytes()).hexdigest(),
        "window": {"start": START, "end": END, "active_rows": len(day)},
        "daily_reset_unit_proxy": {
            "last_equity": str(daily_equity), "max_old_single_day_formula_diff": str(max_old_formula_diff),
            "cumulative_stock_sweep_abs_per_initial_equity": str(daily_stock_sweep_abs),
            "cumulative_hedge_notional_reset_abs_per_initial_equity": str(daily_hedge_reset_abs),
            "first_day_cash_before_sweep": daily_path[0]["cash_before_stock_sweep"],
            "first_negative_cash_before_sweep_date": next((x["date"] for x in daily_path if D(x["cash_before_stock_sweep"]) < 0), None),
        },
        "daily_hedge_reset_fixed_stock_units": {
            "last_equity": str(floating_stock + floating_cash), "last_cash": str(floating_cash),
            "first_negative_cash_date": floating_first_cash_deficit, "max_cash_debt": str(floating_max_cash_deficit),
        },
        "fixed_hedge_notional_fixed_stock_units": {
            "last_equity": str(fixed_stock + fixed_cash), "last_cash": str(fixed_cash),
            "first_negative_cash_date": fixed_first_cash_deficit, "max_cash_debt": str(fixed_max_cash_deficit),
        },
        "assumptions": [
            "stock sleeve is old arithmetic proxy unit, not verified fixed-share basket",
            "zero-initial-value synthetic short index swap, no real IM contract or collateral schedule",
            "0.0003 daily drag on short notional; no additional reset fee or borrowing interest",
            "negative cash recorded as debt; ability and cost of credit unproven",
        ],
    }
    HERE.mkdir(parents=True, exist_ok=True)
    (HERE / "hedge_ledger_summary.json").write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    with (HERE / "hedge_daily_reset.csv").open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(daily_path[0]))
        writer.writeheader(); writer.writerows(daily_path)
    with (HERE / "hedge_fixed_comparison.csv").open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(fixed_path[0]))
        writer.writeheader(); writer.writerows(fixed_path)
    print(json.dumps({"status": result["status"], "rows": len(day),
                      "daily_last": str(daily_equity), "fixed_last": str(fixed_stock + fixed_cash),
                      "first_debt": floating_first_cash_deficit}, ensure_ascii=False))


if __name__ == "__main__":
    main()
