"""Independent audit of saved L5 common-cash partial account artifacts."""

from __future__ import annotations

import csv
import hashlib
import json
from collections import defaultdict
from decimal import Decimal as D
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
HERE = ROOT / "research_reports/20261003_l5_repair"
ACCT = HERE / "account_repair"
SIGNAL = HERE / "signal_audit"
EVENTS = ROOT / "research_reports/20261002_l2_full_repair/data/event_candidates_2010_20260930.csv"
OUT = Path(__file__).resolve().parent
TOL = D("0.000001")
FEE = D("0.003")
DRAG = D("0.0003")


def read(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def n(row: dict[str, str], field: str) -> D:
    return D(row[field] or "0")


def close(a: D, b: D, where: str) -> None:
    if abs(a - b) > TOL:
        raise AssertionError(f"{where}: observed={a} expected={b}")


def audit_case(key: str, manifest: dict, binding: dict) -> dict:
    branch, mode = key.split("_")
    folder = ACCT / key
    prefix = f"recent_{branch}_v2_3_strategy_"
    files = {kind: folder / f"{prefix}{kind}.csv" for kind in ("daily", "orders", "holdings", "rights")}
    files["signal"] = folder / f"recent_{branch}_rebuilt_signals.csv"
    for kind, path in files.items():
        if kind != "signal":
            expected = manifest["detail_sha256"][kind]
        else:
            expected = (binding["branches"][branch]["output_sha256"] if mode == "on"
                        else binding["branches"][branch]["off_output_sha256"])
        if sha(path) != expected:
            raise AssertionError(f"manifest_or_binding_sha_mismatch:{key}:{kind}")
    if manifest["source_signal_sha256"] != sha(files["signal"]) or manifest["input_signal_sha256"] != sha(files["signal"]):
        raise AssertionError(f"source_signal_identity_mismatch:{key}")
    daily = read(files["daily"])
    orders = read(files["orders"])
    holdings = read(files["holdings"])
    rights = read(files["rights"])
    signal = read(files["signal"])
    signal_by = {r["date"]: r for r in signal}
    signal_dates = [r["date"] for r in signal]
    if signal_dates != sorted(set(signal_dates)) or len(signal_dates) != 241:
        raise AssertionError(f"signal_calendar:{key}")
    dates = [r["date"] for r in daily]
    if dates != signal_dates[:len(dates)] or len(dates) != manifest["settled_days"] or dates[-1] != manifest["last_settled"]:
        raise AssertionError(f"daily_calendar_or_manifest:{key}")
    order_by: dict[str, list[dict[str, str]]] = defaultdict(list)
    hold_by: dict[str, dict[str, dict[str, str]]] = defaultdict(dict)
    rights_by: dict[str, list[dict[str, str]]] = defaultdict(list)
    for item in orders:
        order_by[item["execution_date"]].append(item)
    for item in holdings:
        if item["symbol"] in hold_by[item["date"]]:
            raise AssertionError(f"duplicate_holding:{key}:{item['date']}:{item['symbol']}")
        hold_by[item["date"]][item["symbol"]] = item
    for item in rights:
        rights_by[item["date"]].append(item)

    old_cash = D(1000000)
    old_receivable = D(0)
    old_notional = D(0)
    old_hedge = None
    old_holdings: dict[str, D] = {}
    checked_orders = 0
    cash_target_residuals = []
    rejected_orders = 0
    for day_number, row in enumerate(daily):
        date = row["date"]
        hed = n(signal_by[date], "hedge_close")
        hedge_ret = D(0) if old_hedge is None else hed / old_hedge - D(1)
        variation_pnl = -old_notional * hedge_ret
        variation_drag = old_notional * DRAG
        close(n(row, "hedge_return"), hedge_ret, f"hedge_return:{key}:{date}")
        close(n(row, "swap_pnl"), variation_pnl, f"swap_pnl:{key}:{date}")
        close(n(row, "swap_drag"), variation_drag, f"swap_drag:{key}:{date}")
        if n(row, "swap_margin") != 0 or n(row, "swap_debt") != 0:
            raise AssertionError(f"margin_duplicated_after_cash_settlement:{key}:{date}")
        events = rights_by[date]
        cash_paid = sum((n(x, "cash_due") for x in events if x["phase"] == "cash_paid"), D(0))
        cash_ex = sum((n(x, "cash_due") for x in events if x["phase"] == "ex"), D(0))
        credited: dict[str, D] = defaultdict(lambda: D(0))
        for event in events:
            if event["phase"] == "stock_credited":
                credited[event["symbol"]] += n(event, "bonus_due")
        preorders_cash = old_cash + cash_paid + variation_pnl - variation_drag
        if preorders_cash < -TOL:
            raise AssertionError(f"unfunded_day_not_blocked:{key}:{date}")
        common_cash = preorders_cash
        buy_budget = preorders_cash
        booked_shares: dict[str, D] = defaultdict(lambda: D(0))
        fees = D(0)
        buys_started = False
        attempted = order_by[date]
        for idx, order in enumerate(attempted):
            side = order["side"]
            if side == "buy":
                buys_started = True
            elif side != "sell" or buys_started:
                raise AssertionError(f"order_side_sequence:{key}:{date}:{idx}")
            if day_number == 0 or order["signal_date"] != dates[day_number - 1]:
                raise AssertionError(f"order_not_T_plus_1:{key}:{date}:{idx}")
            gross = n(order, "frozen_shares") * n(order, "execution_price")
            fee = gross * FEE
            booked = order["booked"].lower() == "true"
            if booked:
                close(n(order, "gross"), gross, f"gross:{key}:{date}:{idx}")
                close(n(order, "fee"), fee, f"fee:{key}:{date}:{idx}")
                if side == "sell":
                    common_cash += gross - fee
                    booked_shares[order["symbol"]] -= n(order, "frozen_shares")
                    if branch == "netted":
                        buy_budget = common_cash
                else:
                    if gross + fee > buy_budget + TOL:
                        raise AssertionError(f"booked_buy_over_budget:{key}:{date}:{idx}")
                    common_cash -= gross + fee
                    buy_budget -= gross + fee
                    booked_shares[order["symbol"]] += n(order, "frozen_shares")
                fees += fee
            else:
                rejected_orders += 1
                if n(order, "gross") != 0 or n(order, "fee") != 0 or not order["reason"]:
                    raise AssertionError(f"rejected_order_was_booked:{key}:{date}:{idx}")
                if side == "buy" and order["reason"].startswith("insufficient_") and gross + fee <= buy_budget + TOL:
                    raise AssertionError(f"affordable_order_rejected_for_cash:{key}:{date}:{idx}")
            checked_orders += 1
        close(n(row, "cash"), common_cash, f"cash_flow:{key}:{date}")
        close(n(row, "cash_receivable"), old_receivable + cash_ex - cash_paid, f"receivable:{key}:{date}")
        close(n(row, "stock_trade_fees"), fees, f"fees:{key}:{date}")
        current_holdings = hold_by[date]
        close(n(row, "stock_value"), sum((n(h, "market_value") for h in current_holdings.values()), D(0)), f"stock_value:{key}:{date}")
        if int(row["held_count"]) != len(current_holdings):
            raise AssertionError(f"held_count:{key}:{date}")
        for symbol in set(old_holdings) | set(current_holdings) | set(booked_shares) | set(credited):
            actual = n(current_holdings[symbol], "shares") if symbol in current_holdings else D(0)
            expected = old_holdings.get(symbol, D(0)) + booked_shares[symbol] + credited[symbol]
            close(actual, expected, f"share_flow:{key}:{date}:{symbol}")
        for h in current_holdings.values():
            close(n(h, "market_value"), n(h, "shares") * n(h, "valuation_price"), f"holding_mark:{key}:{date}:{h['symbol']}")
        close(n(row, "short_notional_after_close"), D("0.8") * (n(row, "stock_value") + n(row, "pending_share_value")), f"hedge_reset:{key}:{date}")
        close(n(row, "equity"), n(row, "cash") + n(row, "cash_receivable") + n(row, "pending_share_value") + n(row, "stock_value"), f"equity:{key}:{date}")
        if day_number and daily[day_number - 1]["formal_next_scale"] and D(daily[day_number - 1]["formal_next_scale"]) == 0 and current_holdings:
            cash_target_residuals.append({"signal_date": dates[day_number - 1], "execution_date": date, "held_count": len(current_holdings)})
        old_cash = n(row, "cash")
        old_receivable = n(row, "cash_receivable")
        old_notional = n(row, "short_notional_after_close")
        old_hedge = hed
        old_holdings = {s: n(h, "shares") for s, h in current_holdings.items()}
    block = manifest["first_block"]
    if not block or block["date"] != signal_dates[len(daily)]:
        raise AssertionError(f"first_block_missing_or_not_next_date:{key}")
    blocked_date = block["date"]
    blocked_hedge = n(signal_by[blocked_date], "hedge_close")
    blocked_variation = -old_notional * (blocked_hedge / old_hedge - D(1)) - old_notional * DRAG
    event_rows = read(EVENTS)
    paid_event_keys = {(e["symbol"], e["record_date"]) for e in event_rows
                       if e["cash_pay_date"] == blocked_date and e["cash_per_10_gross"]}
    blocked_cash_paid = sum((n(r, "cash_due") for r in rights if r["phase"] == "record"
                            and (r["symbol"], r["record_date"]) in paid_event_keys), D(0))
    blocked_cash = old_cash + blocked_cash_paid + blocked_variation
    if "unfunded_variation_before_stock_orders" not in block["reason"] or blocked_cash >= -TOL:
        raise AssertionError(f"first_block_not_a_real_variation_shortfall:{key}")
    amount_text = block["reason"].split("cash=")[-1]
    close(D(amount_text), blocked_cash, f"blocked_cash:{key}:{blocked_date}")
    return {"key": key, "signal_sha256": sha(files["signal"]), "days": len(daily),
            "last_verified_date": dates[-1], "first_block_date": blocked_date,
            "blocked_cash_before_orders": str(blocked_cash),
            "cash_dividend_paid_on_block_date": str(blocked_cash_paid),
            "checked_orders": checked_orders, "rejected_orders": rejected_orders,
            "cash_target_residuals": cash_target_residuals,
            "daily_sha256": sha(files["daily"])}


def main() -> None:
    all_cases = json.loads((ACCT / "common_cash_result.json").read_text(encoding="utf-8"))["cases"]
    binding = json.loads((SIGNAL / "binding_result.json").read_text(encoding="utf-8"))
    results = [audit_case(key, all_cases[key], binding) for key in ("prefunded_on", "prefunded_off", "netted_on", "netted_off")]
    output = {"label": "independent_partial_common_cash_account_audit",
              "source_manifest_sha256": sha(ACCT / "common_cash_result.json"),
              "signal_binding_sha256": sha(SIGNAL / "binding_result.json"),
              "cases": results,
              "limits": "No authenticated PIT universe, actual futures initial/maintenance margin, financing or real close fills; blocked cases have no post-block NAV."}
    (OUT / "new_common_cash_audit.json").write_text(json.dumps(output, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"cases": [{k: x[k] for k in ("key", "days", "last_verified_date", "first_block_date", "blocked_cash_before_orders", "checked_orders")} for x in results]}, ensure_ascii=False))


if __name__ == "__main__":
    main()
