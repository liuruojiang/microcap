"""Replay the frozen L5 path only until first common-cash order divergence.

The hypothetical swap's daily variation is transferred to common stock cash
before that close's stock orders. A cash shortfall or a different cash rejection
stops the path; saved future orders cannot be used after a divergence.
"""

from __future__ import annotations

import csv
import json
from collections import defaultdict
from decimal import Decimal
from pathlib import Path

from strict_variation_guard import HERE, L5, TOL, amount, sha256

ZERO = Decimal(0)
FEE = Decimal("0.003")


def read(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def replay_until_divergence(daily: list[dict[str, str]], orders: list[dict[str, str]],
                            rights: list[dict[str, str]], branch: str,
                            initial_cash: Decimal = Decimal("1000000")) -> dict:
    by_order: dict[str, list[dict[str, str]]] = defaultdict(list)
    by_right: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in orders:
        by_order[row["execution_date"]].append(row)
    for row in rights:
        if row["phase"] == "cash_paid":
            by_right[row["date"]].append(row)
    common_cash = initial_cash
    checked_orders = 0
    last_verified_date = None
    for day in daily:
        date = day["date"]
        cash_paid = sum((amount(row, "cash_due") for row in by_right[date]), ZERO)
        variation = amount(day, "swap_pnl") - amount(day, "swap_drag")
        # Rights are paid during settlement before T+1 orders. The variation
        # payment is made before buy-budget allocation at the same close.
        common_cash += cash_paid + variation
        if common_cash < -TOL:
            return {"status": "FAIL_CLOSED_VARIATION_SHORTFALL", "date": date,
                    "common_cash_before_orders": str(common_cash), "checked_orders": checked_orders,
                    "last_verified_date": last_verified_date}
        buy_budget = common_cash
        day_orders = by_order[date]
        if [x["side"] for x in day_orders] != sorted([x["side"] for x in day_orders], key=lambda x: 0 if x == "sell" else 1):
            raise ValueError(f"order_side_sequence:{date}")
        for idx, order in enumerate(day_orders):
            side = order["side"]
            gross = amount(order, "frozen_shares") * amount(order, "execution_price")
            fee = gross * FEE
            booked = order["booked"].lower() == "true"
            reason = order["reason"]
            if side == "sell":
                if booked:
                    common_cash += gross - fee
                    if branch == "netted":
                        buy_budget = common_cash
            elif side == "buy":
                affordable = gross + fee <= buy_budget + TOL
                if booked and not affordable:
                    return {"status": "FAIL_CLOSED_BOOKED_BUY_UNFUNDED", "date": date,
                            "order_index": idx, "symbol": order["symbol"],
                            "cash_budget": str(buy_budget), "order_cost": str(gross + fee),
                            "checked_orders": checked_orders,
                            "last_verified_date": last_verified_date}
                if not booked and reason.startswith("insufficient_") and affordable:
                    return {"status": "FAIL_CLOSED_REJECTED_BUY_NOW_AFFORDABLE", "date": date,
                            "order_index": idx, "symbol": order["symbol"],
                            "cash_budget": str(buy_budget), "order_cost": str(gross + fee),
                            "checked_orders": checked_orders,
                            "last_verified_date": last_verified_date}
                if booked:
                    common_cash -= gross + fee
                    buy_budget -= gross + fee
            else:
                raise ValueError(f"invalid_side:{date}:{side}")
            checked_orders += 1
            if common_cash < -TOL or buy_budget < -TOL:
                raise AssertionError(f"negative_cash_after_order:{date}:{idx}")
        expected_common = amount(day, "cash") + amount(day, "swap_margin")
        if abs(common_cash - expected_common) > TOL:
            raise AssertionError(f"common_cash_roll_mismatch:{date}:{common_cash}:{expected_common}")
        last_verified_date = date
    return {"status": "SAVED_PATH_CASH_REPLAYED_TO_END_NECESSARY_ONLY",
            "last_verified_date": daily[-1]["date"], "checked_orders": checked_orders,
            "last_common_cash": str(common_cash)}


def main() -> None:
    baseline = json.loads((L5 / "result.json").read_text(encoding="utf-8"))
    result = {"label": "research_only_old_pool_common_cash_variation_until_first_order_divergence",
              "input_signal_basis": "old_branch_proxy_unbound_counterexample_only",
              "contract": "swap_variation_settled_to_common_cash_before_T_plus_1_close_stock_orders_no_credit_no_upfront_margin",
              "result_sha256": sha256(L5 / "result.json"), "cases": {}}
    for branch in ("prefunded", "netted"):
        for mode in ("on", "off"):
            key = f"{branch}_{mode}"
            directory = L5 / key
            files = {kind: directory / f"recent_{branch}_v2_3_strategy_{kind}.csv"
                     for kind in ("daily", "orders", "rights")}
            for kind, path in files.items():
                expected = baseline["cases"][key]["detail_sha256"][kind]
                if sha256(path) != expected:
                    raise ValueError(f"frozen_input_hash_mismatch:{key}:{kind}")
            case = replay_until_divergence(read(files["daily"]), read(files["orders"]),
                                           read(files["rights"]), branch)
            result["cases"][key] = {"input_sha256": {kind: sha256(path) for kind, path in files.items()}, **case}
    # A one-day counterexample: end-of-day cash covers margin, yet a prefunded
    # buy accepted by the old split books is unaffordable after cash transfer.
    synthetic_day = [{"date": "2025-10-09", "cash": "59.58", "swap_margin": "-40",
                      "swap_pnl": "-40", "swap_drag": "0"}]
    synthetic_orders = [
        {"execution_date": "2025-10-09", "side": "sell", "symbol": "A",
         "frozen_shares": "1", "execution_price": "50", "booked": "True", "reason": ""},
        {"execution_date": "2025-10-09", "side": "buy", "symbol": "B",
         "frozen_shares": "1", "execution_price": "90", "booked": "True", "reason": ""},
    ]
    result["synthetic_prefunded_counterexample"] = {
        "old_end_cash_plus_margin": "19.58",
        "result": replay_until_divergence(synthetic_day, synthetic_orders, [],
                                          "prefunded", Decimal("100"))}
    HERE.mkdir(parents=True, exist_ok=True)
    (HERE / "settlement_result.json").write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({key: {k: value.get(k) for k in ("status", "date", "last_verified_date", "checked_orders")}
                      for key, value in result["cases"].items()}, ensure_ascii=False))


if __name__ == "__main__":
    main()
