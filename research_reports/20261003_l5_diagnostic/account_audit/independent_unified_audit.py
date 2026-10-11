"""Independent event-only audit of the L2 recent fixed-share accounts.

Does not import the strategy replay or its audit helpers. The six CSV families
are frozen conditional diagnostics, not certified historical backtests.
"""

from __future__ import annotations

import csv
import hashlib
import json
from collections import defaultdict
from copy import deepcopy
from decimal import Decimal, getcontext
from pathlib import Path

getcontext().prec = 65
D = Decimal
ZERO = D(0)
TOL = D("1e-18")
ROOT = Path(__file__).resolve().parents[2] / "20261002_l2_full_repair" / "version_integration"
OUT = Path(__file__).resolve().parent
ARM_ROOT = OUT.parent / "executor"


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8-sig", newline="") as stream:
        return list(csv.DictReader(stream))


def n(row: dict[str, str], field: str) -> D:
    return D(row[field] or "0")


def same(a: D, b: D) -> bool:
    return abs(a - b) <= TOL


def audit(daily: list[dict[str, str]], holdings: list[dict[str, str]],
          orders: list[dict[str, str]], rights: list[dict[str, str]],
          branch: str, version: str) -> dict:
    problems = []
    h_by = defaultdict(dict)
    o_by = defaultdict(list)
    r_by = defaultdict(list)
    for row in holdings:
        key = (row["date"], row["symbol"])
        if row["symbol"] in h_by[row["date"]]:
            problems.append(f"duplicate_holding:{key}")
        h_by[row["date"]][row["symbol"]] = row
    for row in orders:
        o_by[row["execution_date"]].append(row)
    for row in rights:
        r_by[row["date"]].append(row)

    dates = [r["date"] for r in daily]
    if dates != sorted(set(dates)):
        problems.append("bad_daily_dates")
    ratio = D("0.8") if version in ("v2.0", "v2.3") else ZERO
    prev = None
    prev_h = {}
    max_resid = defaultdict(lambda: ZERO)
    debt_days = []
    cash_debt_gap = []
    fees_total = ZERO
    swap_drag_total = ZERO
    orders_booked = 0
    orders_rejected = 0
    reject_reasons = defaultdict(int)
    rights_paid = ZERO
    max_hedge_to_equity = (None, ZERO)
    max_debt_to_equity = (None, ZERO)
    shadow_financing_total = ZERO

    def check(label: str, date: str, observed: D, expected: D) -> None:
        delta = abs(observed - expected)
        max_resid[label] = max(max_resid[label], delta)
        if delta > TOL and len(problems) < 100:
            problems.append(f"{label}:{date}:actual={observed}:expected={expected}")

    for idx, row in enumerate(daily):
        date = row["date"]
        stocks = h_by[date]
        listed_stock = sum((n(h, "market_value") for h in stocks.values()), ZERO)
        stock_from_shares = sum((n(h, "shares") * n(h, "valuation_price") for h in stocks.values()), ZERO)
        check("stock_market_value", date, n(row, "stock_value"), listed_stock)
        check("stock_share_valuation", date, listed_stock, stock_from_shares)
        check("held_count", date, D(row["held_count"]), D(len(stocks)))
        expected_equity = sum((n(row, x) for x in (
            "cash", "cash_receivable", "pending_share_value", "stock_value", "swap_margin")), ZERO)
        check("equity_components", date, n(row, "equity"), expected_equity)
        check("debt_definition", date, n(row, "swap_debt"), max(-n(row, "swap_margin"), ZERO))
        check("hedge_reset", date, n(row, "short_notional_after_close"),
              ratio * (n(row, "stock_value") + n(row, "pending_share_value")))
        if n(row, "cash") < 0:
            problems.append(f"negative_stock_cash:{date}")
        if n(row, "swap_debt") > 0:
            debt_days.append(date)
        if n(row, "swap_debt") > n(row, "cash"):
            cash_debt_gap.append((date, n(row, "swap_debt") - n(row, "cash")))
        if n(row, "equity") > 0:
            hedge_ratio = n(row, "short_notional_after_close") / n(row, "equity")
            debt_ratio = n(row, "swap_debt") / n(row, "equity")
            if hedge_ratio > max_hedge_to_equity[1]:
                max_hedge_to_equity = (date, hedge_ratio)
            if debt_ratio > max_debt_to_equity[1]:
                max_debt_to_equity = (date, debt_ratio)
        shadow_financing_total += n(row, "shadow_financing_3pct")

        today_orders = o_by[date]
        booked = [x for x in today_orders if x["booked"].lower() == "true"]
        rejected = [x for x in today_orders if x["booked"].lower() != "true"]
        check("executed_order_count", date, D(row["executed_orders"]), D(len(today_orders)))
        for x in rejected:
            orders_rejected += 1
            reject_reasons[x["reason"]] += 1
            if n(x, "gross") or n(x, "fee") or not x["reason"]:
                problems.append(f"rejected_order_booked_or_no_reason:{date}:{x['symbol']}")
        for x in today_orders:
            if idx == 0 or x["signal_date"] != dates[idx - 1]:
                problems.append(f"order_not_next_session:{date}:{x['symbol']}:{x['signal_date']}")
        for x in booked:
            check("order_gross", date, n(x, "gross"), n(x, "frozen_shares") * n(x, "execution_price"))
            check("order_fee", date, n(x, "fee"), n(x, "gross") * D("0.003"))
        fee = sum((n(x, "fee") for x in booked), ZERO)
        check("daily_stock_fee", date, n(row, "stock_trade_fees"), fee)
        fees_total += fee
        orders_booked += len(booked)

        today_rights = r_by[date]
        cash_paid = sum((n(x, "cash_due") for x in today_rights if x["phase"] == "cash_paid"), ZERO)
        rights_paid += cash_paid
        rights_ex = sum((n(x, "cash_due") for x in today_rights if x["phase"] == "ex"), ZERO)
        credit_by_symbol = defaultdict(lambda: ZERO)
        for x in today_rights:
            if x["phase"] == "stock_credited":
                credit_by_symbol[x["symbol"]] += n(x, "bonus_due")
        buys = sum((n(x, "gross") + n(x, "fee") for x in booked if x["side"] == "buy"), ZERO)
        sells = sum((n(x, "gross") - n(x, "fee") for x in booked if x["side"] == "sell"), ZERO)
        prior_cash = n(prev, "cash") if prev else D("1000000")
        check("cash_event_flow", date, n(row, "cash"), prior_cash + cash_paid + sells - buys)
        prior_rec = n(prev, "cash_receivable") if prev else ZERO
        check("receivable_event_flow", date, n(row, "cash_receivable"), prior_rec + rights_ex - cash_paid)
        if branch == "prefunded" and buys > prior_cash + cash_paid + TOL:
            problems.append(f"prefunded_buy_uses_same_day_sales:{date}")
        if branch == "netted" and buys > prior_cash + cash_paid + sells + TOL:
            problems.append(f"netted_buy_overdraw:{date}")

        symbols = set(prev_h) | set(stocks) | set(credit_by_symbol)
        delta_by_symbol = defaultdict(lambda: ZERO)
        for x in booked:
            delta_by_symbol[x["symbol"]] += n(x, "frozen_shares") * (D(1) if x["side"] == "buy" else D(-1))
        for symbol in symbols:
            old_q = n(prev_h[symbol], "shares") if symbol in prev_h else ZERO
            new_q = n(stocks[symbol], "shares") if symbol in stocks else ZERO
            check("share_event_flow", date, new_q, old_q + delta_by_symbol[symbol] + credit_by_symbol[symbol])

        prior_margin = n(prev, "swap_margin") if prev else ZERO
        prior_notional = n(prev, "short_notional_after_close") if prev else ZERO
        check("swap_pnl", date, n(row, "swap_pnl"), -prior_notional * n(row, "hedge_return"))
        check("swap_drag", date, n(row, "swap_drag"), prior_notional * D("0.0003"))
        check("swap_margin_roll", date, n(row, "swap_margin"),
              prior_margin + n(row, "swap_pnl") - n(row, "swap_drag"))
        swap_drag_total += n(row, "swap_drag")
        if version == "v2.5" and (n(row, "swap_pnl") or n(row, "swap_drag") or n(row, "swap_margin")):
            problems.append(f"hedge_leakage_v25:{date}")
        prev, prev_h = row, stocks

    for idx, row in enumerate(daily[:-1]):
        expected = len(o_by[dates[idx + 1]])
        check("pending_order_count", row["date"], D(row["pending_orders"]), D(expected))
    if daily and n(daily[-1], "pending_orders"):
        # Last-close signal cannot be observed in a following row.
        pass
    return {
        "branch": branch, "version": version, "days": len(daily),
        "first_date": dates[0] if dates else None, "last_date": dates[-1] if dates else None,
        "problems": problems, "max_residuals": {k: str(v) for k, v in max_resid.items()},
        "booked_orders": orders_booked, "fees_total": str(fees_total),
        "rejected_orders": orders_rejected, "reject_reasons": dict(sorted(reject_reasons.items())),
        "swap_drag_total": str(swap_drag_total), "rights_cash_paid": str(rights_paid),
        "shadow_financing_total_unapplied": str(shadow_financing_total),
        "max_hedge_to_equity": [max_hedge_to_equity[0], str(max_hedge_to_equity[1])],
        "max_debt_to_equity": [max_debt_to_equity[0], str(max_debt_to_equity[1])],
        "debt_days": len(debt_days), "first_debt": debt_days[0] if debt_days else None,
        "unfunded_even_if_stock_cash_transferable_days": len(cash_debt_gap),
        "first_unfunded": cash_debt_gap[0][0] if cash_debt_gap else None,
        "max_cash_debt_gap": str(max((x[1] for x in cash_debt_gap), default=ZERO)),
        "max_cash_debt_gap_date": max(cash_debt_gap, key=lambda x: x[1])[0] if cash_debt_gap else None,
        "cash_debt_gaps": [(a, str(b)) for a, b in cash_debt_gap],
    }


def run() -> None:
    summaries = []
    input_hashes = {}
    for branch in ("prefunded", "netted"):
        for version in ("v2.0", "v2.3", "v2.5"):
            stem = f"recent_{branch}_{version.replace('.', '_')}_strategy"
            paths = {kind: ROOT / f"{stem}_{kind}.csv" for kind in ("daily", "holdings", "orders", "rights")}
            inputs = {kind: read_csv(path) for kind, path in paths.items()}
            input_hashes[stem] = {kind: hashlib.sha256(path.read_bytes()).hexdigest() for kind, path in paths.items()}
            summary = audit(inputs["daily"], inputs["holdings"], inputs["orders"], inputs["rights"], branch, version)
            summaries.append(summary)
            if branch == "netted" and version == "v2.3":
                mutants = {}
                def verify_mutant(label, mutated):
                    result = audit(mutated["daily"], mutated["holdings"], mutated["orders"], mutated["rights"], branch, version)
                    mutants[label] = sorted({x.split(":")[0] for x in result["problems"]})
                for label, path, change in (
                    ("omit_order_fee", ("orders", next(i for i,x in enumerate(inputs["orders"]) if x["booked"].lower()=="true"), "fee"), "zero"),
                    ("double_order_fee", ("orders", next(i for i,x in enumerate(inputs["orders"]) if x["booked"].lower()=="true"), "fee"), "double"),
                    ("change_share", ("holdings", 0, "shares"), "double"),
                    ("omit_right_payment", ("rights", next(i for i,x in enumerate(inputs["rights"]) if x["phase"]=="cash_paid"), "cash_due"), "zero"),
                    ("omit_hedge_pnl", ("daily", next(i for i,x in enumerate(inputs["daily"]) if n(x,"swap_pnl")!=0), "swap_pnl"), "zero"),
                    ("omit_hedge_drag", ("daily", next(i for i,x in enumerate(inputs["daily"]) if n(x,"swap_drag")!=0), "swap_drag"), "zero"),
                    ("erase_hedge_reset", ("daily", next(i for i,x in enumerate(inputs["daily"]) if n(x,"short_notional_after_close")!=0), "short_notional_after_close"), "zero"),
                    ("offset_cash_with_margin", ("daily", next(i for i,x in enumerate(inputs["daily"]) if n(x,"swap_margin")!=0), "cash"), "minus100"),
                ):
                    mutated = {k: deepcopy(v) for k,v in inputs.items()}
                    kind, ix, col = path
                    old = n(mutated[kind][ix], col)
                    mutated[kind][ix][col] = str(ZERO if change=="zero" else (old*2 if change=="double" else old-D(100)))
                    verify_mutant(label, mutated)
                if not all(mutants.values()):
                    raise AssertionError(mutants)
    arms = []
    arm_hashes = {}
    for branch in ("prefunded", "netted"):
        for state in ("on", "off"):
            directory = ARM_ROOT / f"{branch}_{state}"
            stem = f"recent_{branch}_v2_3_strategy"
            paths = {kind: directory / f"{stem}_{kind}.csv" for kind in ("daily", "holdings", "orders", "rights")}
            inputs = {kind: read_csv(path) for kind, path in paths.items()}
            arm_hashes[f"{branch}_{state}"] = {kind: hashlib.sha256(path.read_bytes()).hexdigest() for kind, path in paths.items()}
            summary = audit(inputs["daily"], inputs["holdings"], inputs["orders"], inputs["rights"], branch, "v2.3")
            equities = [n(r, "equity") for r in inputs["daily"]]
            peak = equities[0]
            peak_date = inputs["daily"][0]["date"]
            worst = ZERO
            worst_peak = peak_date
            worst_trough = peak_date
            for r, equity in zip(inputs["daily"], equities):
                if equity > peak:
                    peak = equity
                    peak_date = r["date"]
                drawdown = equity / peak - D(1)
                if drawdown < worst:
                    worst = drawdown
                    worst_peak, worst_trough = peak_date, r["date"]
            summary["recent_period_return"] = str(equities[-1] / equities[0] - D(1))
            summary["recent_max_drawdown"] = str(worst)
            summary["recent_mdd_peak_trough"] = [worst_peak, worst_trough]
            summary["recent_terminal_equity"] = str(equities[-1])
            summary["state"] = state
            arms.append(summary)
    arm_comparison = {}
    for branch in ("prefunded", "netted"):
        directory_on = ARM_ROOT / f"{branch}_on"
        directory_off = ARM_ROOT / f"{branch}_off"
        stem = f"recent_{branch}_v2_3_strategy"
        on = read_csv(directory_on / f"{stem}_daily.csv")
        off = read_csv(directory_off / f"{stem}_daily.csv")
        if [r["date"] for r in on] != [r["date"] for r in off]:
            raise AssertionError(f"different_arm_calendar:{branch}")
        difference = [r["date"] for r,s in zip(on,off) if any(r[x] != s[x] for x in ("formal_next_scale","cash","stock_value","swap_margin","equity"))]
        equity_difference = [(r["date"], n(r,"equity")-n(s,"equity")) for r,s in zip(on,off) if not same(n(r,"equity"),n(s,"equity"))]
        arm_comparison[branch] = {
            "first_daily_state_difference": difference[0] if difference else None,
            "state_difference_days": len(difference),
            "first_equity_difference": equity_difference[0][0] if equity_difference else None,
            "equity_difference_days": len(equity_difference),
            "final_equity_on_minus_off": str(n(on[-1],"equity")-n(off[-1],"equity")),
            "on_original_l2_byte_parity": all(
                arm_hashes[f"{branch}_on"][kind] == input_hashes[f"recent_{branch}_v2_3_strategy"][kind]
                for kind in ("daily","holdings","orders","rights")
            ),
        }
    payload = {"input_hashes": input_hashes, "accounts": summaries, "mutants": mutants,
               "arm_hashes": arm_hashes, "arms": arms, "arm_comparison": arm_comparison}
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / "independent_unified_audit.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    show = ("branch","version","state","days","problems","debt_days","unfunded_even_if_stock_cash_transferable_days","first_unfunded","max_cash_debt_gap","max_hedge_to_equity")
    print(json.dumps({"accounts": [{k:v for k,v in x.items() if k in show} for x in summaries],
                      "arms": [{k:v for k,v in x.items() if k in show} for x in arms],
                      "arm_comparison": arm_comparison, "mutants": mutants}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    run()
