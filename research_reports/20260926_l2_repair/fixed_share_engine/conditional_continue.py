"""Continue two isolated paper funding branches through available Jan/Feb bars."""

import csv
import json
from decimal import Decimal as D, ROUND_HALF_UP
from pathlib import Path

from candidate import ROOT, OUT, CALENDAR, PRICE_DIR, RIGHTS, TARGETS, rows, sha
from contract import freeze_orders, settle_next_close


BARS = ROOT / "research_reports/20260926_l2_repair/clock/baostock_raw_open_windows.csv"
EXTRA_BARS = OUT / "extra_bars_20100212.csv"
FIRST = OUT / "early_replay.json"
EVENTS = {"2010-01-28": "2010-01-29", "2010-02-11": "2010-02-12"}


def main():
    first = json.loads(FIRST.read_text(encoding="utf-8"))
    if first["first_strict_block"] is not None:
        raise RuntimeError("First slice stopped before initial rebalance")
    bars = {(r["symbol"], r["date"]): r for r in rows(BARS) + rows(EXTRA_BARS)}
    rights = rows(RIGHTS)
    targets = rows(TARGETS)
    all_dates = [r["date"] for r in rows(CALENDAR) if "2010-01-18" <= r["date"] <= "2010-02-12"]
    price_cache = {}

    def raw(symbol):
        if symbol not in price_cache:
            price_cache[symbol] = {r["date"]: D(r["close_raw"])
                                   for r in rows(PRICE_DIR / f"{symbol}.csv")}
        return price_cache[symbol]

    def marked(symbol, date, old=None):
        true = raw(symbol).get(date)
        if true is not None:
            return true, "true_raw_close"
        bar = bars.get((symbol, date))
        prior = old
        if prior is None:
            earlier = [d for d in raw(symbol) if d < date]
            if earlier:
                prior = raw(symbol)[max(earlier)]
        if (bar and prior is not None and bar["tradestatus"] == "0"
                and D(bar["volume"]) == 0 and D(bar["close"]) == prior
                and D(bar["preclose"]) == prior):
            return prior, "verified_suspension_last_true_raw_close"
        raise ValueError(f"{date} {symbol}: no true raw close or verified same-price suspension")

    branches = {}
    for funding in ("prefunded", "netted"):
        first_branch = first["scenarios"][funding]
        positions = {s: D(q) for s, q in first_branch["positions_after"].items()}
        cash = D(first_branch["cash_after"])
        last = {s: marked(s, "2010-01-15")[0] for s in positions}
        daily, event_summaries, block = [], [], None
        frozen = None
        for date in all_dates:
            day = {}
            stale = []
            try:
                for symbol in positions:
                    px, basis = marked(symbol, date, last.get(symbol))
                    day[symbol] = px
                    last[symbol] = px
                    if basis.startswith("verified_suspension"):
                        stale.append(symbol)
            except ValueError as exc:
                block = {"date": date, "reason": str(exc)}
                break
            listed = [r for r in rights if r.get("ex_date") == date and r.get("symbol", "").zfill(6) in positions]
            if listed:
                block = {"date": date, "reason": "listed_corporate_action_not_applied",
                         "symbols": [r["symbol"] for r in listed]}
                break
            stock_value = sum((q * day[s] for s, q in positions.items()), D(0))
            daily.append({"branch": funding, "date": date, "stage": "old_lots_before_rebalance",
                          "cash": str(cash), "stock_value": str(stock_value),
                          "equity": str(cash + stock_value), "fees_today": "0",
                          "held_count": len(positions), "suspended_marked": ";".join(sorted(stale))})
            if date in EVENTS:
                target = [r["symbol"].zfill(6) for r in sorted(
                    (r for r in targets if r["rebalance_date"] == date), key=lambda r: int(r["rank"]))]
                if len(target) != 100 or len(set(target)) != 100:
                    block = {"date": date, "reason": "target_list_invalid"}
                    break
                price_t = dict(day)
                try:
                    for symbol in target:
                        if symbol not in price_t:
                            price_t[symbol] = marked(symbol, date)[0]
                    frozen = freeze_orders(positions, cash, price_t, target)
                except ValueError as exc:
                    block = {"date": date, "reason": str(exc)}
                    break
            elif date in EVENTS.values():
                if frozen is None:
                    raise AssertionError("Execution without frozen T orders")
                price_next = dict(day)
                blocked = set()
                reasons = {}
                try:
                    for order in frozen:
                        symbol = order["symbol"]
                        if symbol not in price_next:
                            price_next[symbol] = marked(symbol, date)[0]
                        bar = bars.get((symbol, date))
                        if not bar:
                            raise ValueError(f"{date} {symbol}: no trade-status bar")
                        if bar["tradestatus"] != "1" or D(bar["volume"]) <= 0:
                            blocked.add(symbol)
                            reasons[symbol] = "suspended_or_zero_volume"
                        elif price_next[symbol] != D(bar["close"]):
                            raise ValueError(f"{date} {symbol}: raw/vendor mismatch")
                        else:
                            pct = D("0.05") if bar["isST"] == "1" else D("0.10")
                            sign = D(1) if order["side"] == "buy" else D(-1)
                            limit = (D(bar["preclose"]) * (D(1) + sign * pct)).quantize(D("0.01"), rounding=ROUND_HALF_UP)
                            if price_next[symbol] == limit:
                                blocked.add(symbol)
                                reasons[symbol] = "at_side_close_limit_diagnostic_ST"
                except ValueError as exc:
                    block = {"date": date, "reason": str(exc)}
                    break
                result = settle_next_close(positions, cash, frozen, price_next, blocked, funding)
                positions, cash = result["positions_after"], result["cash_after"]
                last = {s: price_next[s] for s in positions}
                event_summaries.append({"signal_date": next(k for k, v in EVENTS.items() if v == date),
                                        "execution_date": date, "frozen_orders": len(frozen),
                                        "blocked_order_reasons": reasons,
                                        "booked_sells": sum(e["booked"] and e["side"] == "sell" for e in result["events"]),
                                        "booked_buys": sum(e["booked"] and e["side"] == "buy" for e in result["events"]),
                                        "other_rejections": [{"symbol": e["symbol"], "side": e["side"], "reason": e["reason"]}
                                                             for e in result["events"] if e["reason"] and e["reason"] != "blocked_trade"],
                                        "cash_after": str(cash), "fees": str(result["fees"]),
                                        "equity_after": str(result["equity_after"]),
                                        "member_count_after": len(positions),
                                        "positions_after": {s: str(q) for s, q in positions.items()}})
                daily.append({"branch": funding, "date": date, "stage": "post_rebalance",
                              "cash": str(cash), "stock_value": str(result["stock_after"]),
                              "equity": str(result["equity_after"]),
                              "fees_today": str(result["fees"]),
                              "held_count": len(positions), "suspended_marked": ""})
                frozen = None
        branches[funding] = {"conditional_daily_dates": len({r["date"] for r in daily}),
                             "last_conditional_date": daily[-1]["date"] if daily else None,
                             "first_strict_block": block,
                             "events": event_summaries, "daily": daily}
    output = {"status": "conditional_old_L1_stock_only_not_certified",
              "scope": "T close frozen fractional quantities; T+1 old lots all day; BaoStock suspended last true price mark only; diagnostic isST limits",
              "source_window_end": "2010-02-12", "next_unchecked_trading_date": "2010-02-22",
              "first_strict_block_in_checked_window": None if all(b["first_strict_block"] is None for b in branches.values()) else "see branch blocks",
              "branches": branches,
              "input_sha256": {str(p.relative_to(ROOT)).replace("\\", "/"): sha(p)
                               for p in (FIRST, BARS, EXTRA_BARS, CALENDAR, RIGHTS, TARGETS)},
              "code_sha256": sha(Path(__file__))}
    with (OUT / "continued_daily_account.csv").open("w", encoding="utf-8", newline="") as handle:
        all_daily = [r for b in branches.values() for r in b["daily"]]
        writer = csv.DictWriter(handle, fieldnames=list(all_daily[0]))
        writer.writeheader()
        writer.writerows(all_daily)
    (OUT / "continued_replay.json").write_text(json.dumps(output, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({k: {"last_date": v["last_conditional_date"], "events": len(v["events"]),
                          "block": v["first_strict_block"]} for k, v in branches.items()}, ensure_ascii=False))


if __name__ == "__main__":
    main()
