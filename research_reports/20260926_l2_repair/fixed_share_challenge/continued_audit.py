"""Independent stock/cash continuation through the second and third rebalance."""

from __future__ import annotations

import json
from collections import Counter
from decimal import Decimal as D, ROUND_HALF_UP, getcontext
from pathlib import Path

from first_rebalance_audit import HERE, ROOT, ENGINE, RAW, TARGET, CALENDAR, RIGHTS, BARS, FEE, read, near


getcontext().prec = 40
EXTRA = ENGINE / "extra_bars_20100212.csv"
SIGNAL_TO_EXEC = {"2010-01-28": "2010-01-29", "2010-02-11": "2010-02-12"}


def run() -> dict[str, object]:
    early = json.loads((ENGINE / "early_replay.json").read_text(encoding="utf-8"))
    saved = json.loads((ENGINE / "continued_replay.json").read_text(encoding="utf-8"))
    first_check = json.loads((HERE / "first_rebalance_audit.json").read_text(encoding="utf-8"))
    bars = {(r["symbol"].zfill(6), r["date"]): r for r in read(BARS) + read(EXTRA)}
    rights = {(r["symbol"].zfill(6), r["ex_date"]) for r in read(RIGHTS)}
    targets = read(TARGET)
    dates = [r["date"] for r in read(CALENDAR) if "2010-01-18" <= r["date"] <= "2010-02-12"]
    history: dict[str, dict[str, D]] = {}

    def price_history(symbol: str) -> dict[str, D]:
        if symbol not in history:
            history[symbol] = {r["date"]: D(r["close_raw"]) for r in read(RAW / f"{symbol}.csv")}
        return history[symbol]

    def valuation(symbol: str, date: str, previous: D | None) -> tuple[D, bool]:
        raw = price_history(symbol)
        if date in raw:
            return raw[date], False
        if previous is None:
            prior_dates = [d for d in raw if d < date]
            previous = raw[max(prior_dates)] if prior_dates else None
        bar = bars.get((symbol, date))
        assert previous is not None and bar is not None, (symbol, date)
        assert bar["tradestatus"] == "0" and D(bar["volume"]) == 0
        assert D(bar["close"]) == D(bar["preclose"]) == previous
        return previous, True

    by_branch = {}
    for funding in ("prefunded", "netted"):
        old = early["scenarios"][funding]
        positions = {s: D(q) for s, q in old["positions_after"].items()}
        assert len(positions) == first_check["scenarios"][funding]["members_after"]
        cash = D(old["cash_after"])
        last = {s: valuation(s, "2010-01-15", None)[0] for s in positions}
        expected_events = saved["branches"][funding]["events"]
        event_index = 0
        pending = None
        pending_t_price = None
        each_day = []
        event_results = []
        for date in dates:
            before = dict(positions)
            mark = {}
            carried = []
            for s in before:
                px, halt = valuation(s, date, last.get(s))
                mark[s] = px
                last[s] = px
                if halt:
                    carried.append(s)
            assert not any((s, date) in rights for s in before), (date, before)
            stock = sum((q * mark[s] for s, q in before.items()), D(0))
            old_equity = cash + stock
            each_day.append({"date": date, "old_equity_before_any_trade": str(old_equity),
                             "old_members": len(before), "carried_halt_marks": sorted(carried)})
            if date in SIGNAL_TO_EXEC:
                target = [r["symbol"].zfill(6) for r in sorted(
                    (r for r in targets if r["rebalance_date"] == date),
                    key=lambda r: int(r["rank"]))]
                assert len(target) == len(set(target)) == 100
                t_px = dict(mark)
                for s in target:
                    if s not in t_px:
                        t_px[s], _ = valuation(s, date, None)
                budget = old_equity / D(100)
                desired = {s: budget / t_px[s] for s in target}
                pending = [(s, "sell", q - desired.get(s, D(0)))
                           for s, q in sorted(before.items()) if q > desired.get(s, D(0))]
                pending += [(s, "buy", desired[s] - before.get(s, D(0)))
                            for s in target if desired[s] > before.get(s, D(0))]
                pending_t_price = t_px
            elif date in SIGNAL_TO_EXEC.values():
                assert pending is not None and pending_t_price is not None
                original = expected_events[event_index]
                event_index += 1
                assert len(pending) == original["frozen_orders"]
                next_price = dict(mark)
                blocked = {}
                for s, side, q in pending:
                    assert q > 0
                    if s not in next_price:
                        next_price[s], _ = valuation(s, date, None)
                    bar = bars[(s, date)]
                    if bar["tradestatus"] != "1" or D(bar["volume"]) <= 0:
                        blocked[s] = "suspended_or_zero_volume"
                        continue
                    assert next_price[s] == D(bar["close"])
                    ratio = D("0.05") if bar["isST"] == "1" else D("0.10")
                    direction = D(1) if side == "buy" else D(-1)
                    limit = (D(bar["preclose"]) * (1 + direction * ratio)).quantize(D("0.01"), rounding=ROUND_HALF_UP)
                    if next_price[s] == limit:
                        blocked[s] = "at_side_close_limit_diagnostic_ST"
                assert blocked == original["blocked_order_reasons"]
                available = cash
                buy_budget = cash
                fees = D(0)
                booked_sell = booked_buy = 0
                other_rejections = []
                for s, side, q in pending:
                    gross = q * next_price[s]
                    fee = gross * FEE
                    reason = ""
                    if s in blocked:
                        reason = "blocked_trade"
                    elif side == "sell" and positions.get(s, D(0)) < q:
                        reason = "shares_unavailable"
                    elif side == "buy" and s not in positions and len(positions) >= 100:
                        reason = "member_capacity"
                    elif side == "buy" and gross + fee > buy_budget:
                        reason = f"insufficient_{funding}_cash"
                    if reason and reason != "blocked_trade":
                        other_rejections.append({"symbol": s, "side": side, "reason": reason})
                    if reason:
                        continue
                    fees += fee
                    if side == "sell":
                        positions[s] -= q
                        if positions[s] == 0:
                            del positions[s]
                        available += gross - fee
                        if funding == "netted":
                            buy_budget = available
                        booked_sell += 1
                    else:
                        available -= gross + fee
                        buy_budget -= gross + fee
                        positions[s] = positions.get(s, D(0)) + q
                        booked_buy += 1
                    assert available >= 0 and buy_budget >= 0 and len(positions) <= 100
                final_stock = sum((q * next_price[s] for s, q in positions.items()), D(0))
                assert near(old_equity - fees, available + final_stock)
                assert (booked_sell, booked_buy, len(positions)) == (
                    original["booked_sells"], original["booked_buys"], original["member_count_after"])
                assert other_rejections == original["other_rejections"]
                assert near(available, D(original["cash_after"]))
                assert near(fees, D(original["fees"]))
                assert near(available + final_stock, D(original["equity_after"]))
                assert set(positions) == set(original["positions_after"])
                assert all(near(q, D(original["positions_after"][s])) for s, q in positions.items())
                assert all(s not in positions or s in before for s in carried)
                event_results.append({"signal": original["signal_date"], "execution": date,
                                      "frozen_orders": len(pending), "blocked": blocked,
                                      "booked_sell": booked_sell, "booked_buy": booked_buy,
                                      "members_after": len(positions), "fee": str(fees),
                                      "cash_after": str(available),
                                      "old_equity_entire_T1_before_trade": str(old_equity),
                                      "conservation_residual": str(old_equity - fees - available - final_stock)})
                cash = available
                last = {s: next_price[s] for s in positions}
                pending = pending_t_price = None
        assert event_index == 2 and len(each_day) == saved["branches"][funding]["conditional_daily_dates"]
        saved_old_daily = [r for r in saved["branches"][funding]["daily"]
                           if r["stage"] == "old_lots_before_rebalance"]
        assert len(saved_old_daily) == len(each_day)
        for computed, reported in zip(each_day, saved_old_daily):
            assert computed["date"] == reported["date"]
            assert near(D(computed["old_equity_before_any_trade"]), D(reported["equity"]))
            assert computed["old_members"] == reported["held_count"]
            assert computed["carried_halt_marks"] == sorted(filter(None, reported["suspended_marked"].split(";")))
            assert D(reported["fees_today"]) == 0
        by_branch[funding] = {"events": event_results,
                              "last_conditional_day": each_day[-1]["date"],
                              "mark_days_recomputed": len(each_day),
                              "old_lot_daily_equity_rows_matched": len(each_day)}
    out = {"audit": "INDEPENDENT_TWO_MORE_REBALANCES_ARITHMETIC_PARITY_ONLY",
           "branches": by_branch,
           "bounds": ["old L1 members", "incomplete rights event ledger",
                      "diagnostic ST and trade status", "no auction fill", "netted theoretical"]}
    (HERE / "continued_audit.json").write_text(json.dumps(out, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return out


if __name__ == "__main__":
    print(json.dumps(run(), ensure_ascii=False, indent=2))
