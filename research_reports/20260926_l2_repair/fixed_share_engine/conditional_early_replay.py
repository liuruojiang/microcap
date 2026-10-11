"""Early frozen-old-target paper account with verified suspension marks.

Runs arithmetic branches only. L1 universe/actions and closing fills unverified.
"""

import csv
import hashlib
import json
from decimal import Decimal as D, ROUND_HALF_UP

from candidate import ROOT, OUT, CALENDAR, PRICE_DIR, RIGHTS, TARGETS, rows, sha
from contract import freeze_orders, settle_next_close


def main():
    opening = rows(OUT / "opening_orders.csv")
    positions = {r["symbol"]: D(r["frozen_fractional_shares"])
                 for r in opening if r["conditional_booked"] == "true"}
    opening_result = json.loads((OUT / "result.json").read_text(encoding="utf-8"))
    cash = D(opening_result["cash_after"])
    status_path = OUT / "missing_raw_status_20100106_15.csv"
    status = {(r["symbol"], r["date"]): r for r in rows(status_path)}
    bars_path = ROOT / "research_reports/20260926_l2_repair/clock/baostock_raw_open_windows.csv"
    bars = {(r["symbol"], r["date"]): r for r in rows(bars_path)}
    calendar = [r["date"] for r in rows(CALENDAR) if "2010-01-05" <= r["date"] <= "2010-01-15"]
    rights = rows(RIGHTS)
    prices = {s: {r["date"]: D(r["close_raw"]) for r in rows(PRICE_DIR / f"{s}.csv")}
              for s in positions}
    last = {r["symbol"]: D(r["execution_raw_close"]) for r in opening
            if r["conditional_booked"] == "true"}
    marks = []
    close_marks = {"2010-01-05": dict(last)}
    blocked_at = None
    for date in calendar[1:]:
        for symbol in positions:
            raw = prices[symbol].get(date)
            if raw is not None:
                last[symbol] = raw
                marks.append({"date": date, "symbol": symbol, "mark": str(raw),
                              "mark_basis": "true_raw_close", "trade_eligible": "unknown"})
                continue
            probe = status.get((symbol, date))
            if not probe or probe["tradestatus"] != "0" or D(probe["volume"]) != 0 or D(probe["close"]) != last[symbol] or D(probe["preclose"]) != last[symbol]:
                blocked_at = {"date": date, "symbol": symbol,
                              "reason": "missing_true_close_without_verified_suspension_same_mark"}
                break
            marks.append({"date": date, "symbol": symbol, "mark": str(last[symbol]),
                          "mark_basis": "verified_suspension_last_true_raw_close", "trade_eligible": "false"})
        if blocked_at:
            break
        listed = [r for r in rights if r.get("ex_date") == date and r.get("symbol", "").zfill(6) in positions]
        if listed:
            blocked_at = {"date": date, "reason": "listed_corporate_action_not_applied",
                          "symbols": [r["symbol"] for r in listed]}
            break
        close_marks[date] = dict(last)

    summary = {"status": "conditional_old_L1_stock_account_not_certified",
               "suspension_marks": sum(r["mark_basis"].startswith("verified_suspension") for r in marks),
               "suspension_rows": [r for r in marks if r["mark_basis"].startswith("verified_suspension")],
               "conditional_mark_dates": list(close_marks), "first_strict_block": blocked_at,
               "rights_scope": "Partial recovered rights table; absence of rows is not absence proof",
               "input_sha256": {str(p.relative_to(ROOT)).replace("\\", "/"): sha(p)
                                for p in (OUT / "opening_orders.csv", OUT / "result.json", status_path,
                                          bars_path, CALENDAR, RIGHTS, TARGETS)},
               "code_sha256": sha(__import__("pathlib").Path(__file__))}
    daily_accounts = []
    previous_equity = None
    for date, day_marks in close_marks.items():
        stock_value = sum((q * day_marks[s] for s, q in positions.items()), D(0))
        equity = cash + stock_value
        daily_accounts.append({"date": date, "stage": "old_lots_before_rebalance",
                               "cash": str(cash), "stock_value": str(stock_value),
                               "equity": str(equity),
                               "equity_change_from_prior_mark": "" if previous_equity is None else str(equity - previous_equity),
                               "fees_today": opening_result["paper_fee_total"] if date == "2010-01-05" else "0",
                               "suspended_held_count": sum(r["date"] == date and r["mark_basis"].startswith("verified_suspension") for r in marks),
                               "held_symbols": len(positions)})
        previous_equity = equity
    if not blocked_at and "2010-01-15" in close_marks:
        target = [r["symbol"].zfill(6) for r in sorted(
            (r for r in rows(TARGETS) if r["rebalance_date"] == "2010-01-14"),
            key=lambda r: int(r["rank"]))]
        if len(target) != 100 or len(set(target)) != 100:
            raise ValueError("Bad first target")
        price_t = dict(close_marks["2010-01-14"])
        price_next = dict(close_marks["2010-01-15"])
        # New target-only symbols need true raw close on both dates.
        for symbol in target:
            if symbol not in price_t:
                raw = {r["date"]: D(r["close_raw"]) for r in rows(PRICE_DIR / f"{symbol}.csv")}
                if "2010-01-14" not in raw or "2010-01-15" not in raw:
                    blocked_at = {"date": "2010-01-15", "symbol": symbol,
                                  "reason": "target_price_missing_for_order"}
                    break
                price_t[symbol], price_next[symbol] = raw["2010-01-14"], raw["2010-01-15"]
        if not blocked_at:
            frozen = freeze_orders(positions, cash, price_t, target)
            block = set()
            block_reasons = {}
            for order in frozen:
                symbol = order["symbol"]
                bar = bars.get((symbol, "2010-01-15"))
                if not bar:
                    blocked_at = {"date": "2010-01-15", "symbol": symbol,
                                  "reason": "missing_trade_status_for_order"}
                    break
                if bar["tradestatus"] != "1" or D(bar["volume"]) <= 0:
                    block.add(symbol)
                    block_reasons[symbol] = "suspended_or_zero_volume"
                elif price_next[symbol] != D(bar["close"]):
                    blocked_at = {"date": "2010-01-15", "symbol": symbol,
                                  "reason": "raw_close_vendor_mismatch"}
                    break
                else:
                    pct = D("0.05") if bar["isST"] == "1" else D("0.10")
                    direction = D(1) if order["side"] == "buy" else D(-1)
                    limit = (D(bar["preclose"]) * (D(1) + direction * pct)).quantize(D("0.01"), rounding=ROUND_HALF_UP)
                    if price_next[symbol] == limit:
                        block.add(symbol)
                        block_reasons[symbol] = "at_side_close_limit_diagnostic_ST"
            if not blocked_at:
                scenarios = {}
                for funding in ("prefunded", "netted"):
                    out = settle_next_close(positions, cash, frozen, price_next, block, funding)
                    scenarios[funding] = {
                        "old_equity_at_2010_01_15_close": str(out["old_equity_at_next_close"]),
                        "cash_after": str(out["cash_after"]), "stock_after": str(out["stock_after"]),
                        "fees": str(out["fees"]), "equity_after": str(out["equity_after"]),
                        "member_count_after": len(out["positions_after"]),
                        "positions_after": {s: str(q) for s, q in out["positions_after"].items()},
                        "booked_sell_count": sum(e["booked"] and e["side"] == "sell" for e in out["events"]),
                        "booked_buy_count": sum(e["booked"] and e["side"] == "buy" for e in out["events"]),
                        "events": [{**e, "frozen_shares": str(e["frozen_shares"])} for e in out["events"]],
                    }
                    daily_accounts.append({"date": "2010-01-15", "stage": f"post_rebalance_{funding}",
                                           "cash": str(out["cash_after"]),
                                           "stock_value": str(out["stock_after"]),
                                           "equity": str(out["equity_after"]),
                                           "equity_change_from_prior_mark": str(-out["fees"]),
                                           "fees_today": str(out["fees"]),
                                           "suspended_held_count": "",
                                           "held_symbols": len(out["positions_after"])})
                summary.update({"first_rebalance_signal": "2010-01-14",
                                "conditional_execution_close": "2010-01-15",
                                "frozen_orders": len(frozen), "blocked_order_reasons": block_reasons,
                                "scenario_contracts": {"prefunded": "buys funded only from T cash",
                                                       "netted": "theoretical same-close sells before buys, not an executable sequence"},
                                "scenarios": scenarios})
    summary["first_strict_block"] = blocked_at
    with (OUT / "daily_marks_20100106_15.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(marks[0]))
        writer.writeheader()
        writer.writerows(marks)
    with (OUT / "daily_account_20100105_15.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(daily_accounts[0]))
        writer.writeheader()
        writer.writerows(daily_accounts)
    (OUT / "early_replay.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({k: summary.get(k) for k in ("conditional_mark_dates", "suspension_marks", "first_strict_block", "frozen_orders")}, ensure_ascii=False))


if __name__ == "__main__":
    main()
