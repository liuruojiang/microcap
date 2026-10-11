"""Independent readback of fixed-share account files; imports no replay functions."""

from __future__ import annotations

import csv
import hashlib
import json
from collections import defaultdict
from decimal import Decimal as D, getcontext
from pathlib import Path


getcontext().prec = 50
ROOT = Path(__file__).resolve().parents[3]
ENGINE = ROOT / "research_reports/20261002_l2_full_repair/engine"
OUT = Path(__file__).resolve().parent
TOL = D("1e-20")


def records(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        return list(csv.DictReader(handle))


def h(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def v(item: dict[str, str], key: str) -> D:
    return D(item[key] or "0")


def near(a: D, b: D, label: str) -> None:
    if abs(a - b) > TOL:
        raise AssertionError(f"{label}: observed={a}, expected={b}, residual={a-b}")


def check() -> dict:
    manifest_path = ENGINE / "full_history_result.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    code = ENGINE / "full_history.py"
    hashes = {"full_history.py": h(code)}
    for name, wanted in manifest["detail_sha256"].items():
        actual = h(ENGINE / name)
        hashes[name] = actual
        if actual != wanted:
            raise AssertionError(f"stale_output_hash:{name}:{actual}!={wanted}")
    if hashes["full_history.py"] != manifest["code_sha256"]:
        raise AssertionError("stale_code_hash")
    for path, wanted in manifest["input_sha256"].items():
        actual = h(ROOT / path)
        if actual != wanted:
            raise AssertionError(f"input_hash_mismatch:{path}")

    daily = records(ENGINE / "full_history_daily_account.csv")
    positions = records(ENGINE / "full_history_daily_positions.csv")
    rights = records(ENGINE / "full_history_rights_events.csv")
    orders = records(ENGINE / "full_history_settled_orders.csv")
    days: dict[tuple[str, str], dict[str, str]] = {}
    pos_by_day: dict[tuple[str, str], dict[str, dict[str, D]]] = defaultdict(
        lambda: defaultdict(dict)
    )
    rights_by_day: dict[tuple[str, str], list[dict[str, str]]] = defaultdict(list)
    orders_by_day: dict[tuple[str, str], list[dict[str, str]]] = defaultdict(list)
    for item in daily:
        key = (item["branch"], item["date"])
        if key in days:
            raise AssertionError(f"duplicate_day:{key}")
        days[key] = item
    for item in positions:
        key = (item["branch"], item["date"])
        state = item["state"]
        symbol = item["symbol"]
        if symbol in pos_by_day[key][state]:
            raise AssertionError(f"duplicate_position:{key}:{state}:{symbol}")
        qty, price, amount = (v(item, x) for x in ("shares", "valuation_price", "market_value"))
        near(amount, qty * price, f"position_value:{key}:{state}:{symbol}")
        pos_by_day[key][state][symbol] = qty
    for item in rights:
        rights_by_day[(item["branch"], item["date"])].append(item)
    for item in orders:
        orders_by_day[(item["branch"], item["execution_date"])].append(item)
    mv_by_day: dict[tuple[str, str], dict[str, D]] = defaultdict(
        lambda: defaultdict(lambda: D(0))
    )
    for item in positions:
        mv_by_day[(item["branch"], item["date"])][item["state"]] += v(item, "market_value")

    branch_days: dict[str, list[str]] = defaultdict(list)
    for branch, date in days:
        branch_days[branch].append(date)
    for branch, dates in branch_days.items():
        dates.sort()
        prev_date = None
        receivable = D(0)
        pending: dict[str, D] = defaultdict(lambda: D(0))
        eligibility: dict[str, list[dict[str, D | str]]] = defaultdict(list)
        for date in dates:
            key = (branch, date)
            row = days[key]
            stock = mv_by_day[key]["settled_shares"]
            pending_value = mv_by_day[key]["pending_bonus_shares"]
            near(v(row, "stock_value"), stock, f"stock_sum:{key}")
            near(v(row, "pending_share_value"), pending_value, f"pending_sum:{key}")
            near(
                v(row, "equity"),
                v(row, "cash") + v(row, "cash_receivable") + stock + pending_value,
                f"equity_sum:{key}",
            )
            ex_cash = paid_cash = D(0)
            bonus_ex: dict[str, D] = defaultdict(lambda: D(0))
            bonus_credit: dict[str, D] = defaultdict(lambda: D(0))
            for ev in rights_by_day[key]:
                symbol = ev["symbol"]
                if ev["phase"] == "record":
                    near(v(ev, "record_shares"), pos_by_day[key]["settled_shares"].get(symbol, D(0)),
                         f"record_close_qualification:{key}:{symbol}")
                    eligibility[symbol].append({"date": date, "cash": v(ev, "cash_due"),
                                                "bonus": v(ev, "bonus_due"), "used": "no"})
                elif ev["phase"] == "ex":
                    matches = [x for x in eligibility[symbol] if x["date"] < date and x["used"] == "no"
                               and x["cash"] == v(ev, "cash_due") and x["bonus"] == v(ev, "bonus_due")]
                    if len(matches) != 1:
                        raise AssertionError(f"ex_without_unique_prior_record:{key}:{symbol}")
                    matches[0]["used"] = "yes"
                    ex_cash += v(ev, "cash_due")
                    bonus_ex[symbol] += v(ev, "bonus_due")
                elif ev["phase"] == "cash_paid":
                    paid_cash += v(ev, "amount")
                elif ev["phase"] == "stock_credited":
                    bonus_credit[symbol] += v(ev, "shares")
                else:
                    raise AssertionError(f"unknown_right_phase:{key}:{ev['phase']}")

            if prev_date is not None:
                prev_key = (branch, prev_date)
                for order in orders_by_day[key]:
                    if order["booked"] != "True":
                        continue
                    near(v(order, "gross"), v(order, "frozen_shares") * v(order, "execution_raw_price"),
                         f"order_gross:{key}:{order['symbol']}")
                    near(v(order, "fee"), v(order, "gross") * D("0.003"),
                         f"order_fee:{key}:{order['symbol']}")
                booked = [x for x in orders_by_day[key] if x["booked"] == "True"]
                sales = sum((v(x, "gross") - v(x, "fee") for x in booked if x["side"] == "sell"), D(0))
                buys = sum((v(x, "gross") + v(x, "fee") for x in booked if x["side"] == "buy"), D(0))
                near(v(row, "cash"), v(days[prev_key], "cash") + sales - buys + paid_cash,
                     f"cash_roll:{key}")
                near(v(row, "cash_receivable"), v(days[prev_key], "cash_receivable") + ex_cash - paid_cash,
                     f"receivable_roll:{key}")
                near(v(row, "fees"), sum((v(x, "fee") for x in booked), D(0)), f"fee_roll:{key}")
                previous = pos_by_day[prev_key]["settled_shares"]
                current = pos_by_day[key]["settled_shares"]
                for symbol in previous.keys() | current.keys() | bonus_credit.keys():
                    bought = sum((v(x, "frozen_shares") for x in booked
                                  if x["symbol"] == symbol and x["side"] == "buy"), D(0))
                    sold = sum((v(x, "frozen_shares") for x in booked
                                if x["symbol"] == symbol and x["side"] == "sell"), D(0))
                    near(current.get(symbol, D(0)), previous.get(symbol, D(0)) + bought - sold + bonus_credit[symbol],
                         f"share_roll:{key}:{symbol}")
            receivable += ex_cash - paid_cash
            near(v(row, "cash_receivable"), receivable, f"receivable_state:{key}")
            for symbol in bonus_ex.keys() | bonus_credit.keys():
                pending[symbol] += bonus_ex[symbol] - bonus_credit[symbol]
                if pending[symbol] < 0:
                    raise AssertionError(f"negative_pending:{key}:{symbol}")
            for symbol in pending.keys() | pos_by_day[key]["pending_bonus_shares"].keys():
                near(pos_by_day[key]["pending_bonus_shares"].get(symbol, D(0)), pending[symbol],
                     f"pending_roll:{key}:{symbol}")
            prev_date = date

    # Inject two local counterexamples in arithmetic only; no output file is rewritten.
    sample = next(x for x in daily if x["branch"] == "netted")
    claim = v(sample, "equity")
    components = v(sample, "cash") + v(sample, "cash_receivable") + v(sample, "stock_value") + v(sample, "pending_share_value")
    near(claim, components, "baseline_injection_control")
    injections = {}
    for name, wrong in {"double_count_bonus": components + D("1"),
                        "omit_cash_right": components - D("0.20")}.items():
        try:
            near(claim, wrong, name)
        except AssertionError:
            injections[name] = "caught"
        else:
            raise AssertionError(f"injection_not_caught:{name}")

    return {
        "manifest_sha256": h(manifest_path),
        "observed_sha256": hashes,
        "branches": {name: {"days": len(dates), "first": dates[0], "last": dates[-1]} for name, dates in branch_days.items()},
        "positions_rows": len(positions),
        "rights_rows": len(rights),
        "orders_rows": len(orders),
        "injections": injections,
        "status": "PASS_readback_arithmetic_only",
    }


if __name__ == "__main__":
    result = check()
    path = OUT / "full_ex_independent_audit.json"
    path.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False, indent=2))
