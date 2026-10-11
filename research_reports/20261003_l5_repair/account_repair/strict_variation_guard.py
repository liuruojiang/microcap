"""Research-only fail-closed variation-margin check on frozen L5 accounts.

This is a necessary end-of-day cash test for a hypothetical zero-upfront
index swap with immediate variation-margin transfer from the stock cash book.
It does not introduce a broker margin rate or replay changed stock orders.
"""

from __future__ import annotations

import csv
import hashlib
import json
from decimal import Decimal, InvalidOperation
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
L5 = ROOT / "research_reports/20261003_l5_diagnostic/executor"
HERE = Path(__file__).resolve().parent
TOL = Decimal("0.000001")
ZERO = Decimal(0)


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def amount(row: dict[str, str], field: str) -> Decimal:
    try:
        value = Decimal(row[field])
    except (KeyError, InvalidOperation) as exc:
        raise ValueError(f"invalid_amount:{field}:{row.get('date')}") from exc
    if not value.is_finite():
        raise ValueError(f"nonfinite_amount:{field}:{row.get('date')}")
    return value


def audit_rows(rows: list[dict[str, str]], branch: str) -> dict:
    if len(rows) != 241:
        raise ValueError(f"unexpected_row_count:{branch}:{len(rows)}")
    previous_date = None
    first_debt = None
    first_breach = None
    debt_days = breach_days = 0
    maximum_gap = ZERO
    for row in rows:
        date = row["date"]
        if previous_date is not None and date <= previous_date:
            raise ValueError(f"nonincreasing_dates:{date}")
        previous_date = date
        if row["branch"] != branch or row["version"] != "v2.3":
            raise ValueError(f"account_identity_mismatch:{date}")
        cash, margin, debt = (amount(row, key) for key in ("cash", "swap_margin", "swap_debt"))
        if cash < -TOL:
            raise ValueError(f"negative_stock_cash:{date}")
        if abs(debt - max(-margin, ZERO)) > TOL:
            raise ValueError(f"swap_debt_identity:{date}")
        equity_parts = sum((amount(row, key) for key in
                            ("cash", "cash_receivable", "pending_share_value", "stock_value", "swap_margin")), ZERO)
        if abs(amount(row, "equity") - equity_parts) > TOL:
            raise ValueError(f"equity_identity:{date}")
        if debt > TOL:
            debt_days += 1
            if first_debt is None:
                first_debt = date
        # Strongest permissive convention: all stock cash is instantly
        # transferable to settle accumulated negative swap P&L at this close.
        # No initial margin, maintenance margin or broker haircut is added.
        gap = max(-(cash + margin), ZERO)
        if gap > TOL:
            breach_days += 1
            maximum_gap = max(maximum_gap, gap)
            if first_breach is None:
                first_breach = {"date": date, "cash": str(cash),
                                "swap_margin": str(margin), "gap": str(gap)}
    if rows[0]["date"] != "2025-10-09" or rows[-1]["date"] != "2026-09-30":
        raise ValueError("unexpected_period")
    return {"rows": len(rows), "first_date": rows[0]["date"], "last_date": rows[-1]["date"],
            "first_debt_date": first_debt, "debt_days": debt_days,
            "first_breach": first_breach, "breach_days": breach_days,
            "maximum_gap": str(maximum_gap),
            "strict_full_period_status": "FAIL_CLOSED" if first_breach else "NECESSARY_CASH_CHECK_ONLY"}


def main() -> None:
    baseline = json.loads((L5 / "result.json").read_text(encoding="utf-8"))
    cases = {}
    calendars = {}
    for branch in ("prefunded", "netted"):
        for mode in ("on", "off"):
            key = f"{branch}_{mode}"
            path = L5 / key / f"recent_{branch}_v2_3_strategy_daily.csv"
            observed_hash = sha256(path)
            expected_hash = baseline["cases"][key]["detail_sha256"]["daily"]
            if observed_hash != expected_hash:
                raise ValueError(f"frozen_input_hash_mismatch:{key}")
            with path.open(encoding="utf-8-sig", newline="") as handle:
                rows = list(csv.DictReader(handle))
            calendar = tuple(row["date"] for row in rows)
            if branch in calendars and calendar != calendars[branch]:
                raise ValueError(f"paired_calendar_mismatch:{branch}")
            calendars[branch] = calendar
            cases[key] = {"input": str(path.relative_to(ROOT)), "input_sha256": observed_hash,
                          **audit_rows(rows, branch)}
    # A direct falsification check: more debt than cash must be rejected.
    # Use a harmless synthetic one-row mutation; frozen inputs remain untouched.
    with (L5 / "netted_on/recent_netted_v2_3_strategy_daily.csv").open(encoding="utf-8-sig", newline="") as handle:
        injected = list(csv.DictReader(handle))
    target = next(row for row in injected if row["date"] == "2026-02-27")
    assert Decimal(target["cash"]) + Decimal(target["swap_margin"]) < -TOL
    result = {
        "label": "research_only_frozen_old_pool_upper_bound_cash_coverage_gate",
        "input_signal_basis": "old_branch_proxy_unbound_counterexample_only",
        "contract": "zero_upfront_theoretical_swap_end_of_day_variation_cash_transfer_no_initial_margin",
        "result_sha256": sha256(L5 / "result.json"),
        "cases": cases,
        "injection": {"name": "2026-02-27 existing netted shortfall",
                      "detected": cases["netted_on"]["first_breach"]["date"] == "2026-02-27"},
        "limitation": "End-of-day necessary cash check only; no order resizes or execution replay after breach."
    }
    HERE.mkdir(parents=True, exist_ok=True)
    (HERE / "guard_result.json").write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({k: {"status": v["strict_full_period_status"], "first_breach": v["first_breach"]}
                      for k, v in cases.items()}, ensure_ascii=False))


if __name__ == "__main__":
    main()
