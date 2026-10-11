"""Independent recomputation of the candidate's first conditional rebalance.

No import from the candidate engine. The old L1 member inputs are diagnostic.
"""

from __future__ import annotations

import csv
import json
from collections import Counter
from decimal import Decimal as D, ROUND_HALF_UP, getcontext
from pathlib import Path


getcontext().prec = 40
HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
ENGINE = HERE.parent / "fixed_share_engine"
OPENING = ENGINE / "opening_orders.csv"
OPEN_RESULT = ENGINE / "result.json"
TARGET = ROOT / "outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_members.csv"
CALENDAR = ROOT / "outputs/wind_microcap_top_100_biweekly_thursday_16y_cached.csv"
RIGHTS = ROOT / "research_reports/20260926_l1_repair/corporate_actions/rights_event_ledger_2010_2014.csv"
HALTS = ENGINE / "missing_raw_status_20100106_15.csv"
BARS = ENGINE.parent / "clock/baostock_raw_open_windows.csv"
RAW = ROOT / ".microcap_index_cache/prices_raw"
FEE = D("0.003")


def read(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8-sig", newline="") as file:
        return list(csv.DictReader(file))


def near(a: D, b: D) -> bool:
    return abs(a - b) < D("1e-25")


def verify_saved_event(event: tuple, row: dict[str, object]) -> None:
    assert (event[0], event[1], event[3], event[4]) == (
        row["symbol"], row["side"], row["booked"], row["reason"])
    assert near(event[2], D(str(row["frozen_shares"])))
    assert near(event[5], D(str(row["gross"])))
    assert near(event[6], D(str(row["fee"])))


def run() -> dict[str, object]:
    opening = read(OPENING)
    saved_open = json.loads(OPEN_RESULT.read_text(encoding="utf-8"))
    saved = json.loads((ENGINE / "early_replay.json").read_text(encoding="utf-8"))
    positions = {r["symbol"].zfill(6): D(r["frozen_fractional_shares"])
                 for r in opening if r["conditional_booked"] == "true"}
    cash = D(saved_open["cash_after"])
    assert len(positions) == 98
    traded_close = {r["symbol"].zfill(6): D(r["execution_raw_close"])
                    for r in opening if r["conditional_booked"] == "true"}
    dates = [r["date"] for r in read(CALENDAR) if "2010-01-05" <= r["date"] <= "2010-01-15"]
    assert dates == saved["conditional_mark_dates"]
    halts = {(r["symbol"].zfill(6), r["date"]): r for r in read(HALTS)}
    bars = {(r["symbol"].zfill(6), r["date"]): r for r in read(BARS)}
    rights = {(r["symbol"].zfill(6), r["ex_date"]) for r in read(RIGHTS)}
    raw = {s: {r["date"]: D(r["close_raw"]) for r in read(RAW / f"{s}.csv")}
           for s in positions}
    mark_by_day = {dates[0]: dict(traded_close)}
    carried = []
    for date in dates[1:]:
        mark = {}
        for s in positions:
            px = raw[s].get(date)
            if px is None:
                h = halts[(s, date)]
                assert h["tradestatus"] == "0" and D(h["volume"]) == 0
                assert D(h["close"]) == D(h["preclose"]) == traded_close[s]
                px = traded_close[s]
                carried.append((date, s, str(px)))
            else:
                traded_close[s] = px
            mark[s] = px
        assert not any((s, date) in rights for s in positions), date
        mark_by_day[date] = mark
    assert len(carried) == 9
    assert {(r["date"], r["symbol"], r["mark"]) for r in saved["suspension_rows"]} == set(carried)
    equity_t = cash + sum((q * mark_by_day["2010-01-14"][s] for s, q in positions.items()), D(0))
    old_equity_next = cash + sum((q * mark_by_day["2010-01-15"][s] for s, q in positions.items()), D(0))

    target = [r["symbol"].zfill(6) for r in sorted(
        (r for r in read(TARGET) if r["rebalance_date"] == "2010-01-14"),
        key=lambda r: int(r["rank"]))]
    assert len(target) == len(set(target)) == 100
    t_price = dict(mark_by_day["2010-01-14"])
    n_price = dict(mark_by_day["2010-01-15"])
    for s in target:
        if s not in t_price:
            history = {r["date"]: D(r["close_raw"]) for r in read(RAW / f"{s}.csv")}
            t_price[s] = history["2010-01-14"]
            n_price[s] = history["2010-01-15"]
    per_target = equity_t / D(100)
    desired = {s: per_target / t_price[s] for s in target}
    orders = [(s, "sell", q - desired.get(s, D(0)))
              for s, q in sorted(positions.items()) if q > desired.get(s, D(0))]
    orders += [(s, "buy", desired[s] - positions.get(s, D(0)))
               for s in target if desired[s] > positions.get(s, D(0))]
    assert len(orders) == 109

    blocked = {}
    for s, side, q in orders:
        assert q > 0
        bar = bars[(s, "2010-01-15")]
        if bar["tradestatus"] != "1" or D(bar["volume"]) <= 0:
            blocked[s] = "suspended_or_zero_volume"
            continue
        assert n_price[s] == D(bar["close"])
        ratio = D("0.05") if bar["isST"] == "1" else D("0.10")
        direction = D(1) if side == "buy" else D(-1)
        limit = (D(bar["preclose"]) * (1 + direction * ratio)).quantize(D("0.01"), rounding=ROUND_HALF_UP)
        if n_price[s] == limit:
            blocked[s] = "at_side_close_limit_diagnostic_ST"
    assert blocked == saved["blocked_order_reasons"]

    scenarios = {}
    for funding in ("prefunded", "netted"):
        held = dict(positions)
        available = cash
        buy_budget = cash
        events = []
        total_fee = D(0)
        for s, side, q in orders:
            gross = q * n_price[s]
            fee = gross * FEE
            reason = ""
            if s in blocked:
                reason = "blocked_trade"
            elif side == "sell" and held.get(s, D(0)) < q:
                reason = "shares_unavailable"
            elif side == "buy" and s not in held and len(held) >= 100:
                reason = "member_capacity"
            elif side == "buy" and gross + fee > buy_budget:
                reason = f"insufficient_{funding}_cash"
            if not reason:
                if side == "sell":
                    held[s] -= q
                    if held[s] == 0:
                        del held[s]
                    available += gross - fee
                    if funding == "netted":
                        buy_budget = available
                else:
                    available -= gross + fee
                    buy_budget -= gross + fee
                    held[s] = held.get(s, D(0)) + q
                total_fee += fee
            assert available >= 0 and buy_budget >= 0 and len(held) <= 100
            events.append((s, side, q, not bool(reason), reason,
                           gross if not reason else D(0), fee if not reason else D(0)))
        original = saved["scenarios"][funding]
        assert len(events) == len(original["events"])
        for event, row in zip(events, original["events"]):
            verify_saved_event(event, row)
        stock_after = sum((q * n_price[s] for s, q in held.items()), D(0))
        assert near(available, D(original["cash_after"]))
        assert near(stock_after, D(original["stock_after"]))
        assert near(total_fee, D(original["fees"]))
        assert near(available + stock_after + total_fee, old_equity_next)
        assert len(held) == original["member_count_after"]
        assert set(held) == set(original["positions_after"])
        assert all(near(q, D(original["positions_after"][s])) for s, q in held.items())
        # A suspension mark is a valuation input, never a trade print.
        suspended_today = {s for date, s, _ in carried if date == "2010-01-15"}
        assert all(not e[3] for e in events if e[0] in suspended_today)
        newly_opened = sorted(set(held) - set(positions))
        fully_closed = sorted(set(positions) - set(held))
        assert len(held) == len(positions) + len(newly_opened) - len(fully_closed)
        scenarios[funding] = {
            "old_equity_after_full_T1_old_hold": str(old_equity_next),
            "booked_sell": sum(e[1] == "sell" and e[3] for e in events),
            "booked_buy": sum(e[1] == "buy" and e[3] for e in events),
            "members_after": len(held),
            "cash_after": str(available),
            "fee": str(total_fee),
            "new_members": len(newly_opened),
            "fully_exited_members": len(fully_closed),
            "not_owned_target_symbols": sorted(set(target) - set(held)),
            "retained_outside_target_symbols": sorted(set(held) - set(target)),
            "event_reasons": {f"{side}:{state}:{reason}": n for (side, state, reason), n in
                              Counter((e[1], e[3], e[4]) for e in events).items()},
        }
        if funding == "netted":
            # In-memory fault injections: neither future-sized quantity nor a
            # suppressed fee is allowed to pass the independent row checker.
            idx = next(i for i, e in enumerate(events) if e[1] == "buy" and e[3])
            future_sized = dict(original["events"][idx])
            future_sized["frozen_shares"] = str(D(future_sized["frozen_shares"]) + D(1))
            try:
                verify_saved_event(events[idx], future_sized)
            except AssertionError:
                pass
            else:
                raise AssertionError("future-sized injected shares escaped checker")
            missing_fee = dict(original["events"][idx])
            missing_fee["fee"] = "0"
            try:
                verify_saved_event(events[idx], missing_fee)
            except AssertionError:
                pass
            else:
                raise AssertionError("omitted fee escaped checker")
    output = {
        "audit": "INDEPENDENT_ARITHMETIC_PARITY_ONLY",
        "halt_carried_mark_count": len(carried),
        "halt_carried_rows": carried,
        "frozen_order_count": len(orders),
        "blocked_order_reasons": blocked,
        "equity_T_2010_01_14": str(equity_t),
        "old_equity_T1_2010_01_15_before_trade": str(old_equity_next),
        "scenarios": scenarios,
        "fault_injections_rejected": ["future_sized_buy_quantity", "omitted_booked_buy_fee"],
        "limitations": ["Old L1 targets and rights incomplete", "BaoStock ST diagnostic only",
                        "No closing auction fill", "netted same-close proceeds theoretical"],
    }
    (HERE / "first_rebalance_audit.json").write_text(json.dumps(output, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return output


if __name__ == "__main__":
    print(json.dumps(run(), ensure_ascii=False, indent=2))
