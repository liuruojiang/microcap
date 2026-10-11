"""Independent event replay of the L5 proposed common-cash funding contract.

Only verifies the unchanged saved order path until its first divergence. It
does not import the account repair implementation or turn a diagnostic account
into an executable futures backtest.
"""

from __future__ import annotations

import csv
import hashlib
import json
from collections import defaultdict
from decimal import Decimal as D
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
SOURCE = ROOT / "research_reports/20261003_l5_diagnostic/executor"
BOUND = ROOT / "research_reports/20261003_l5_repair/signal_audit"
OFFICIAL = ROOT / "outputs/microcap_top100_mom16_lb25_hl2p5_r2off_eb0p08_vol10_oh26_recovery20_exec0p8_v2_3_costed_nav.csv"
OUT = Path(__file__).resolve().parent
FEE = D("0.003")
TOL = D("0.000001")


def read(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def n(row: dict[str, str], key: str) -> D:
    return D(row[key] or "0")


def bind_signal() -> dict:
    official = {r["date"]: r for r in read(OFFICIAL)}
    reports = []
    paths = [BOUND / branch / f"recent_{branch}_rebuilt_signals.csv" for branch in ("prefunded", "netted")]
    if sha(paths[0]) != sha(paths[1]):
        raise AssertionError("bound_branch_signals_not_byte_identical")
    for path in paths:
        rows = read(path)
        errors = []
        for row in rows:
            date = row["date"]
            if date not in official:
                errors.append(f"missing_official:{date}")
                continue
            orig = official[date]
            if row["v2_3_source_next_holding"] != orig["next_holding"]:
                errors.append(f"state:{date}")
            if abs(n(row, "v2_3_next_session_actionable_scale") - n(orig, "next_session_actionable_scale")) > TOL:
                errors.append(f"scale:{date}")
            if abs(n(row, "hedge_close") - n(orig, "hedge_close")) > TOL:
                errors.append(f"hedge:{date}")
        reports.append({"path": str(path.relative_to(ROOT)), "sha256": sha(path), "rows": len(rows),
                        "first_date": rows[0]["date"], "last_date": rows[-1]["date"], "errors": errors})
    return {"official_path": str(OFFICIAL.relative_to(ROOT)), "official_sha256": sha(OFFICIAL),
            "branches": reports, "byte_identical_branches": True}


def run_case(branch: str, mode: str) -> dict:
    folder = SOURCE / f"{branch}_{mode}"
    prefix = f"recent_{branch}_v2_3_strategy_"
    daily = read(folder / f"{prefix}daily.csv")
    orders = read(folder / f"{prefix}orders.csv")
    rights = read(folder / f"{prefix}rights.csv")
    order_by: dict[str, list[dict[str, str]]] = defaultdict(list)
    paid_by: dict[str, D] = defaultdict(lambda: D(0))
    for event in orders:
        order_by[event["execution_date"]].append(event)
    for event in rights:
        if event["phase"] == "cash_paid":
            paid_by[event["date"]] += n(event, "cash_due")
    dates = [r["date"] for r in daily]
    assert dates == sorted(set(dates))
    day_index = {date: i for i, date in enumerate(dates)}
    for event in orders:
        idx = day_index[event["execution_date"]]
        if idx == 0 or event["signal_date"] != dates[idx - 1]:
            raise AssertionError(f"early_or_late_order:{branch}:{mode}:{event['execution_date']}:{event['symbol']}")
    common_cash = D(1000000)
    checked_orders = 0
    last_verified = None
    first_divergence = None
    for day in daily:
        date = day["date"]
        # Variation is settled now, before stock orders. This isolates the
        # precise order-budget effect of combining the two old paper ledgers.
        common_cash += paid_by[date] + n(day, "swap_pnl") - n(day, "swap_drag")
        if common_cash < -TOL:
            first_divergence = {"date": date, "kind": "unfunded_variation_before_orders",
                                "cash_before_orders": str(common_cash)}
            break
        buy_budget = common_cash
        seen_buy = False
        for idx, order in enumerate(order_by[date]):
            side = order["side"]
            if side == "buy":
                seen_buy = True
            elif side != "sell" or seen_buy:
                raise AssertionError(f"unexpected_order_sequence:{date}:{idx}")
            gross = n(order, "frozen_shares") * n(order, "execution_price")
            fee = gross * FEE
            booked = order["booked"].lower() == "true"
            if booked:
                if abs(n(order, "gross") - gross) > TOL or abs(n(order, "fee") - fee) > TOL:
                    raise AssertionError(f"booked_amount_mismatch:{date}:{idx}")
            elif n(order, "gross") != 0 or n(order, "fee") != 0:
                raise AssertionError(f"rejected_order_booked_amount:{date}:{idx}")
            if side == "sell" and booked:
                common_cash += gross - fee
                if branch == "netted":
                    buy_budget = common_cash
            elif side == "buy":
                affordable = gross + fee <= buy_budget + TOL
                if booked and not affordable:
                    first_divergence = {"date": date, "kind": "old_booked_buy_unaffordable",
                                        "order_index": idx, "symbol": order["symbol"],
                                        "cost": str(gross + fee), "budget": str(buy_budget)}
                    break
                if not booked and order["reason"].startswith("insufficient_") and affordable:
                    first_divergence = {"date": date, "kind": "old_cash_rejection_now_affordable",
                                        "order_index": idx, "symbol": order["symbol"],
                                        "cost": str(gross + fee), "budget": str(buy_budget)}
                    break
                if booked:
                    common_cash -= gross + fee
                    buy_budget -= gross + fee
            checked_orders += 1
            if common_cash < -TOL or buy_budget < -TOL:
                raise AssertionError(f"negative_cash_inside_order_loop:{date}:{idx}")
        if first_divergence:
            break
        if abs(common_cash - n(day, "cash") - n(day, "swap_margin")) > TOL:
            raise AssertionError(f"cash_double_count_or_future_transfer:{date}")
        last_verified = date
    return {"branch": branch, "mode": mode, "input_sha256": {kind: sha(folder / f"{prefix}{kind}.csv")
            for kind in ("daily", "orders", "rights")}, "checked_orders": checked_orders,
            "last_verified_date": last_verified, "first_divergence": first_divergence}


def synthetic_counterexamples() -> dict:
    # An end-of-day coverage check passes 100 + 50 - 90 - 40 = 20. A
    # prefunded buy cannot use same-close sale cash and variation has already
    # consumed 40, leaving a valid pre-buy budget of only 60.
    start, variation_due, sale_net, buy_cost = map(D, ("100", "40", "50", "90"))
    post_guard_pass = start + sale_net - buy_cost - variation_due >= 0
    should_reject_buy = buy_cost > start - variation_due
    return {"end_of_day_guard_passes_but_prefunded_buy_rejected": post_guard_pass and should_reject_buy,
            "post_coverage": str(start + sale_net - buy_cost - variation_due),
            "buy_budget_after_pretrade_variation": str(start - variation_due),
            "negative_variation_150_on_cash_100_fail_closed": start - D(150) < 0}


def main() -> None:
    result = {"label": "independent_adversarial_repair_audit_diagnostic",
              "signal_binding": bind_signal(),
              "cases": [run_case(branch, mode) for branch in ("prefunded", "netted") for mode in ("on", "off")],
              "synthetic_counterexamples": synthetic_counterexamples()}
    (OUT / "independent_repair_audit.json").write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"signal_errors": sum(len(x["errors"]) for x in result["signal_binding"]["branches"]),
                      "cases": [{"branch": c["branch"], "mode": c["mode"], "last_verified": c["last_verified_date"],
                                 "first_divergence": c["first_divergence"]} for c in result["cases"]]}, ensure_ascii=False))


if __name__ == "__main__":
    main()
