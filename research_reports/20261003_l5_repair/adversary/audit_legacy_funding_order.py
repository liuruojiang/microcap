"""Independent L5 funding-order challenge on sealed old diagnostic CSVs.

The pre-trade payoff check is a hypothetical stricter contract, not a claim
that a broker used it. All monetary arithmetic uses Decimal.
"""

from __future__ import annotations

import csv
import json
from collections import defaultdict
from decimal import Decimal as D
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
SOURCE = ROOT / "research_reports/20261003_l5_diagnostic/executor"
OUT = Path(__file__).resolve().parent


def load(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def n(row: dict[str, str], key: str) -> D:
    return D(row[key] or "0")


def audit(branch: str, state: str) -> dict:
    stem = f"recent_{branch}_v2_3_strategy_"
    folder = SOURCE / f"{branch}_{state}"
    daily = load(folder / f"{stem}daily.csv")
    orders = load(folder / f"{stem}orders.csv")
    rights = load(folder / f"{stem}rights.csv")
    o_by: dict[str, list[dict[str, str]]] = defaultdict(list)
    paid_by: dict[str, D] = defaultdict(lambda: D(0))
    for order in orders:
        o_by[order["execution_date"]].append(order)
    for event in rights:
        if event["phase"] == "cash_paid":
            paid_by[event["date"]] += n(event, "cash_due")

    old_cash = D(1000000)
    funding_breaches = []
    pretrade_rejections = []
    target_cash_residual = []
    timing_breaches = []
    date_index = {row["date"]: i for i, row in enumerate(daily)}
    for day_number, row in enumerate(daily):
        date = row["date"]
        events = o_by[date]
        booked = [x for x in events if x["booked"].lower() == "true"]
        buys = sum((n(x, "gross") + n(x, "fee") for x in booked if x["side"] == "buy"), D(0))
        sells = sum((n(x, "gross") - n(x, "fee") for x in booked if x["side"] == "sell"), D(0))
        if abs(old_cash + paid_by[date] + sells - buys - n(row, "cash")) > D("1e-18"):
            funding_breaches.append(date + ":stock_cash_roll")
        margin_due = max(-n(row, "swap_margin"), D(0))
        if n(row, "cash") + n(row, "swap_margin") < -D("1e-18"):
            funding_breaches.append(date + ":post_cash_plus_margin")
        # If accumulated variation debt must be paid before current trades,
        # sale proceeds are ineligible for the prefunded buy budget.
        pre_buy_budget = old_cash + paid_by[date] + (sells if branch == "netted" else D(0)) - margin_due
        if buys > 0 and buys > pre_buy_budget + D("1e-18"):
            pretrade_rejections.append({
                "date": date,
                "booked_buys": str(buys),
                "pretrade_budget_after_margin": str(pre_buy_budget),
                "post_cash_plus_margin": str(n(row, "cash") + n(row, "swap_margin")),
                "same_close_net_sales": str(sells),
            })
        for event in events:
            i = date_index[date]
            if i == 0 or event["signal_date"] != daily[i - 1]["date"]:
                timing_breaches.append(date + ":" + event["symbol"])
        prior_scale = daily[day_number - 1]["formal_next_scale"] if day_number else ""
        if prior_scale and D(prior_scale) == 0 and int(row["held_count"]) > 0:
            target_cash_residual.append({
                "signal_date": daily[day_number - 1]["date"],
                "execution_date": date,
                "held_count_after_sell_attempt": int(row["held_count"]),
            })
        old_cash = n(row, "cash")
    return {
        "branch": branch,
        "state": state,
        "days": len(daily),
        "first_date": daily[0]["date"],
        "last_date": daily[-1]["date"],
        "funding_breaches": funding_breaches,
        "pretrade_margin_payoff_would_reject_booked_buys": pretrade_rejections,
        "first_pretrade_rejection": pretrade_rejections[0] if pretrade_rejections else None,
        "target_cash_with_actual_stock": target_cash_residual,
        "order_timing_breaches": timing_breaches,
    }


def synthetic_order_counterexample() -> dict:
    start_cash, margin_due, same_close_sales, frozen_buy = map(D, ("100", "40", "50", "90"))
    old_budget = start_cash
    stock_cash_end = start_cash + same_close_sales - frozen_buy
    return {
        "contract": "prefunded, variation margin payable before frozen buys",
        "start_stock_cash": str(start_cash),
        "pretrade_variation_due": str(margin_due),
        "same_close_net_sales_not_eligible_for_buy_budget": str(same_close_sales),
        "frozen_buy_cost": str(frozen_buy),
        "old_buy_budget": str(old_budget),
        "stock_cash_after_old_bookings": str(stock_cash_end),
        "post_cash_plus_margin_guard": str(stock_cash_end - margin_due),
        "correct_pretrade_buy_budget": str(start_cash - margin_due),
        "old_booked_but_correct_contract_rejects": frozen_buy <= old_budget and stock_cash_end >= margin_due and frozen_buy > start_cash - margin_due,
    }


def main() -> None:
    cases = [audit(branch, state) for branch in ("prefunded", "netted") for state in ("on", "off")]
    result = {"source": str(SOURCE), "cases": cases, "synthetic_counterexample": synthetic_order_counterexample()}
    (OUT / "legacy_funding_order_audit.json").write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"cases": [{"branch": c["branch"], "state": c["state"], "funding_breaches": len(c["funding_breaches"]), "pretrade_rejections": len(c["pretrade_margin_payoff_would_reject_booked_buys"]), "target_cash_residual": len(c["target_cash_with_actual_stock"]), "timing_breaches": len(c["order_timing_breaches"])} for c in cases], "counterexample": result["synthetic_counterexample"]["old_booked_but_correct_contract_rejects"]}, ensure_ascii=False))


if __name__ == "__main__":
    main()
