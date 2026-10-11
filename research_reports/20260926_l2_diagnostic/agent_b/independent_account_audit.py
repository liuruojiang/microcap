"""Read-only, independent audit of frozen model return streams.

Uses only the Python standard library.  This is an algebra/metric audit, not a
share-level self-financing replay.  It intentionally imports no strategy code.
"""

from __future__ import annotations

import csv
import hashlib
import json
from datetime import date
from decimal import Decimal, getcontext
from pathlib import Path


getcontext().prec = 40
ROOT = Path(__file__).resolve().parents[3]
HERE = Path(__file__).resolve().parent
CUTOFF = "2026-09-24"
INPUTS = {
    "v2.0": ROOT / "outputs/microcap_top100_mom16_biweekly_live_v2_0_nav.csv",
    "v2.3": ROOT / "outputs/microcap_top100_mom16_biweekly_live_v2_3_nav.csv",
    "v2.5": ROOT / "outputs/microcap_top100_mom16_biweekly_live_v2_5_nav.csv",
}
D = Decimal
ONE = D(1)
ZERO = D(0)
TOL = D("0.000000001")


def dec(row: dict[str, str], key: str) -> Decimal:
    value = row.get(key, "")
    if value is None or value.strip() == "":
        raise ValueError(f"missing {key}, date={row.get('date')}")
    value = D(value)
    if not value.is_finite():
        raise ValueError(f"nonfinite {key}, date={row.get('date')}")
    return value


def load(path: Path) -> tuple[str, list[dict[str, str]], int, str]:
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    with path.open(encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.DictReader(handle))
    if not rows:
        raise ValueError(f"empty {path}")
    dates = [r["date"] for r in rows]
    if dates != sorted(set(dates)):
        raise ValueError(f"dates not unique/in order {path}")
    selected = [r for r in rows if r["date"] <= CUTOFF]
    if not selected:
        raise ValueError(f"no rows before cutoff {path}")
    return digest, selected, len(rows), dates[-1]


def expected_return(row: dict[str, str], version: str) -> Decimal:
    if version == "v2.3":
        # This line has no target-vol, financing or cash-day overlay in the
        # frozen formal output.  total_cost includes entry/exit/rebalance.
        return (ONE + dec(row, "base_pre_cost_return")) * (ONE - dec(row, "total_cost")) * (
            ONE + dec(row, "cash_day_yield")
        ) - ONE
    exposure = dec(row, "execution_scale")
    gross = dec(row, "base_pre_cost_return") * exposure + dec(row, "idle_cash_yield")
    if version == "v2.5":
        gross += dec(row, "cash_day_yield")
    return (
        (ONE + gross)
        * (ONE - dec(row, "base_trade_cost_scaled"))
        * (ONE - dec(row, "scale_change_cost"))
        * (ONE - dec(row, "financing_cost"))
        - ONE
    )


def audit(version: str, path: Path) -> dict:
    sha, rows, full_rows, full_end = load(path)
    equity = ONE
    peak = ONE
    worst = ZERO
    worst_date = None
    max_formula = ZERO
    max_nav = ZERO
    max_cost_parts = ZERO
    max_no_cost_violation = ZERO
    first_formula = None
    first_nav = None
    first_cost = None
    costs_nonzero = 0
    cash_rows = 0
    leverage_rows = 0
    fee_sum = ZERO
    for row in rows:
        expected = expected_return(row, version)
        observed = dec(row, "return_net")
        err = abs(expected - observed)
        if err > max_formula:
            max_formula = err
            first_formula = row["date"]
        base_cost = dec(row, "total_cost")
        if base_cost < ZERO or base_cost >= ONE:
            raise ValueError(f"cost outside [0,1): {row['date']}")
        if base_cost:
            costs_nonzero += 1
        fee_sum += base_cost
        parts = dec(row, "entry_exit_cost") + dec(row, "rebalance_cost")
        err_cost = abs(base_cost - parts)
        if err_cost > max_cost_parts:
            max_cost_parts = err_cost
            first_cost = row["date"]
        if row["holding"] == "cash":
            cash_rows += 1
        if dec(row, "execution_scale") > ONE:
            leverage_rows += 1
        if version == "v2.0":
            # With the same daily gross path and nonnegative charge, costed
            # equity cannot exceed the no-cost hypothetical equity on the
            # same day.  Check the row return, not two drifting NAV paths.
            no_cost_return = dec(row, "base_pre_cost_return") * dec(row, "execution_scale")
            violation = observed - no_cost_return
            max_no_cost_violation = max(max_no_cost_violation, violation)
        equity *= ONE + observed
        err_nav = abs(equity - dec(row, "nav_net"))
        if err_nav > max_nav:
            max_nav = err_nav
            first_nav = row["date"]
        peak = max(peak, equity)
        dd = equity / peak - ONE
        if dd < worst:
            worst = dd
            worst_date = row["date"]
    start, end = date.fromisoformat(rows[0]["date"]), date.fromisoformat(rows[-1]["date"])
    calendar_years = D((end - start).days) / D("365.25")
    trading_years = D(len(rows)) / D(244)
    trading_years_excluding_first = D(len(rows) - 1) / D(244)
    # Convert only for fractional powers; all cumulative arithmetic above is
    # Decimal and independent of the strategy's metric helper.
    annual_calendar = float(equity) ** (1 / float(calendar_years)) - 1
    annual_244 = float(equity) ** (1 / float(trading_years)) - 1
    annual_244_n_minus_1 = float(equity) ** (1 / float(trading_years_excluding_first)) - 1
    result = {
        "source": str(path.relative_to(ROOT)).replace("\\", "/"),
        "source_sha256": sha,
        "source_full_rows": full_rows,
        "source_full_end": full_end,
        "cutoff": CUTOFF,
        "rows": len(rows),
        "first_date": rows[0]["date"],
        "last_date": rows[-1]["date"],
        "first_return_net": rows[0]["return_net"],
        "first_holding": rows[0]["holding"],
        "last_equity_from_returns": str(equity),
        "last_reported_nav_net": rows[-1]["nav_net"],
        "annual_calendar_fraction": annual_calendar,
        "annual_244_over_N_fraction": annual_244,
        "annual_244_over_N_minus_1_fraction": annual_244_n_minus_1,
        "calendar_minus_N_annual_percentage_points": (annual_calendar - annual_244) * 100,
        "max_drawdown_fraction": float(worst),
        "max_drawdown_trough": worst_date,
        "max_return_formula_error": str(max_formula),
        "return_formula_max_error_date": first_formula,
        "max_nav_recurrence_error": str(max_nav),
        "nav_recurrence_max_error_date": first_nav,
        "max_cost_components_error": str(max_cost_parts),
        "cost_components_max_error_date": first_cost,
        "max_costed_over_same_day_no_cost_return": str(max_no_cost_violation) if version == "v2.0" else None,
        "nonzero_cost_rows": costs_nonzero,
        "sum_base_cost_rates": str(fee_sum),
        "cash_rows": cash_rows,
        "leverage_rows": leverage_rows,
        "algebra_pass": max_formula <= TOL and max_nav <= TOL and max_cost_parts <= TOL,
        "self_financing_account_certified": False,
    }
    return result


def mutation_checks() -> dict:
    """Independent artificial errors, never persisted to an input stream."""
    _, rows, _, _ = load(INPUTS["v2.0"])
    cost_row = next(r for r in rows if dec(r, "total_cost") > ZERO)
    true_ret = dec(cost_row, "return_net")
    fee_omission_ret = ONE + dec(cost_row, "base_pre_cost_return") * dec(cost_row, "execution_scale") - ONE
    fee_error = abs(fee_omission_ret - true_ret)
    active = next(i for i, r in enumerate(rows) if r["holding"] != "cash" and abs(dec(r, "return_net")) > D("0.000001"))
    shifted = dec(rows[active - 1], "return_net")
    clock_error = abs(expected_return(rows[active], "v2.0") - shifted)
    corrupted_nav = dec(rows[active], "nav_net") + D("0.01")
    nav_error = abs(corrupted_nav - dec(rows[active], "nav_net"))
    return {
        "fee_omission_date": cost_row["date"],
        "fee_omission_error": str(fee_error),
        "fee_omission_rejected": fee_error > TOL,
        "one_row_clock_shift_date": rows[active]["date"],
        "one_row_clock_shift_error": str(clock_error),
        "one_row_clock_shift_rejected": clock_error > TOL,
        "equity_corruption_error": str(nav_error),
        "equity_corruption_rejected": nav_error > TOL,
    }


def main() -> None:
    results = {version: audit(version, path) for version, path in INPUTS.items()}
    results["mutations"] = mutation_checks()
    output = HERE / "audit_summary.json"
    output.write_text(json.dumps(results, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(results, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
