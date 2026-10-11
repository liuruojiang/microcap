"""Independent event-sourced cross-check of the recent fixed-share paper ledger.

Does not import the execution engine or its ledger contract.  It consumes
written orders, holdings, rights and daily totals, plus raw source prices.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from collections import defaultdict
from decimal import Decimal, getcontext
from pathlib import Path


getcontext().prec = 40
D = Decimal
ROOT = Path(__file__).resolve().parents[3]
HERE = Path(__file__).resolve().parent
ENGINE = ROOT / "research_reports/20261002_l2_full_repair/engine"
RESULT = ENGINE / "recent_result.json"
CODE = ENGINE / "recent_slice.py"
CONTRACT = ENGINE / "ledger_contract.py"
TARGET = ROOT / "outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_members.csv"
SEED = ROOT / "research_reports/20260926_l1_repair/full_replay/members_by_rebalance.csv"
RAW_DIR = ROOT / ".microcap_index_cache/prices_raw"
BAR_CACHE = ENGINE / "queried_baostock_bars.csv"
SUPPLEMENTS = ENGINE / "raw_close_supplements.csv"
EVENTS = ROOT / "research_reports/20261002_l2_full_repair/data/event_candidates_2010_20260930.csv"
CREDIT_SUPPLEMENT = ROOT / "research_reports/20261002_l2_full_repair/data/issuer_credit_date_supplements.csv"
CALENDAR = ROOT / "outputs/wind_microcap_top_100_biweekly_thursday_16y_cached.csv"
TOL = D("1e-18")


def rows(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8-sig", newline="") as stream:
        return list(csv.DictReader(stream))


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def amt(value: str | None) -> Decimal:
    return D(value or "0")


def within(a: Decimal, b: Decimal, label: str) -> None:
    if abs(a - b) > TOL:
        raise AssertionError(f"{label}: {a} != {b}; residual={a-b}")


class Raw:
    def __init__(self, supplements: dict[tuple[str, str], dict[str, str]]) -> None:
        self.base_cache: dict[str, dict[str, Decimal]] = {}
        self.cache: dict[str, dict[str, Decimal]] = {}
        self.supplements = supplements

    def base_prices(self, symbol: str) -> dict[str, Decimal]:
        if symbol not in self.base_cache:
            self.base_cache[symbol] = {r["date"]: D(r["close_raw"])
                                       for r in rows(RAW_DIR / f"{symbol}.csv")}
        return self.base_cache[symbol]

    def prices(self, symbol: str) -> dict[str, Decimal]:
        if symbol not in self.cache:
            self.cache[symbol] = dict(self.base_prices(symbol))
            for (s, date), row in self.supplements.items():
                if s == symbol:
                    assert date not in self.cache[symbol]
                    self.cache[symbol][date] = D(row["close_raw"])
        return self.cache[symbol]

    def mark(self, symbol: str, date: str) -> Decimal:
        prices = self.prices(symbol)
        past = [d for d in prices if d <= date]
        if not past:
            raise AssertionError(f"no_true_raw_history:{symbol}:{date}")
        return prices[max(past)]


def grouped_by(records: list[dict[str, str]], key: str) -> dict[str, list[dict[str, str]]]:
    result: dict[str, list[dict[str, str]]] = defaultdict(list)
    for r in records:
        result[r[key]].append(r)
    return result


def audit_branch(branch: str, summary: dict, capital: Decimal, fee_rate: Decimal,
                 targets: dict[str, list[str]], seed: list[str], raw: Raw,
                 bars: dict[tuple[str, str], dict[str, str]],
                 candidates: dict[tuple[str, str], dict[str, str]],
                 credit_dates: dict[tuple[str, str], str]) -> dict:
    paths = {kind: ENGINE / f"recent_{branch}_{kind}.csv"
             for kind in ("daily", "holdings", "orders", "rights")}
    for kind, path in paths.items():
        assert path.is_file(), f"missing_detail:{kind}"
        assert sha(path) == summary["detail_sha256"][path.name], f"detail_sha_mismatch:{path.name}"
    daily = rows(paths["daily"])
    holdings = grouped_by(rows(paths["holdings"]), "date")
    orders = grouped_by(rows(paths["orders"]), "execution_date")
    orders_signal = grouped_by(rows(paths["orders"]), "signal_date")
    rights = grouped_by(rows(paths["rights"]), "date")
    actual_record_keys = {(r["record_date"], r["symbol"])
                          for group in rights.values() for r in group if r["phase"] == "record"}
    held_record_keys = {(r["date"], r["symbol"])
                        for group in holdings.values() for r in group
                        if r["state"] == "settled_shares" and D(r["shares"]) > 0}
    expected_record_keys = set(candidates) & held_record_keys
    assert actual_record_keys == expected_record_keys, (
        f"record_source_completeness:{branch}:missing={sorted(expected_record_keys-actual_record_keys)[:3]}:"
        f"extra={sorted(actual_record_keys-expected_record_keys)[:3]}")
    for group in rights.values():
        for event in group:
            key = (event["record_date"], event["symbol"])
            assert key in candidates, f"unknown_right_source:{branch}:{key}"
            source = candidates[key]
            assert event["source_status"] == source["source_status"]
            assert event["kind"] == source["event_type"] == "dividend_or_bonus"
            expected_date = {
                "record": source["record_date"], "ex": source["ex_date"],
                "cash_paid": source["cash_pay_date"],
            }.get(event["phase"])
            if event["phase"] == "stock_credited":
                expected_date = source["share_credit_date"] or credit_dates[(event["symbol"], source["ex_date"])]
            assert event["date"] == expected_date
            within(D(event["cash_due"]), D(event["record_shares"]) *
                   D(source["cash_per_10_gross"] or "0") / D(10),
                   f"source_cash_due:{branch}:{key}")
            within(D(event["bonus_due"]), D(event["record_shares"]) *
                   D(source["bonus_per_10"] or "0") / D(10),
                   f"source_bonus_due:{branch}:{key}")
    assert len(daily) == summary["settled_days"] and daily[-1]["date"] == summary["last_settled_date"]
    assert len({r["date"] for r in daily}) == len(daily)
    assert daily[0]["date"] > "2025-09-30"
    all_trading_dates = [r["date"] for r in rows(CALENDAR)]
    assert all_trading_dates == sorted(set(all_trading_dates))
    for signal_date, group in orders_signal.items():
        i = all_trading_dates.index(signal_date)
        assert i + 1 < len(all_trading_dates)
        assert all(o["execution_date"] == all_trading_dates[i + 1] for o in group), signal_date
    opening = orders_signal.get("2025-09-30", [])
    assert len(opening) == 100 and {o["symbol"] for o in opening} == set(seed)
    for o in opening:
        assert o["side"] == "buy"
        p = raw.mark(o["symbol"], "2025-09-30")
        within(D(o["signal_price"]), p, f"opening_price:{branch}:{o['symbol']}")
        within(D(o["frozen_shares"]), capital/D(100)/p/(D(1)+fee_rate),
               f"opening_qty:{branch}:{o['symbol']}")
    positions: dict[str, Decimal] = {}
    pending: dict[str, Decimal] = {}
    cash, receivable = capital, D(0)
    max_daily_residual = D(0)
    trade_count = 0
    record_count = 0
    missed_raw_marks = 0
    cash_deficit = None
    for day in daily:
        date = day["date"]
        payouts = D(0)
        for ev in rights.get(date, []):
            symbol, phase = ev["symbol"], ev["phase"]
            if phase == "ex":
                receivable += amt(ev["cash_due"])
                if amt(ev["bonus_due"]):
                    pending[symbol] = pending.get(symbol, D(0)) + amt(ev["bonus_due"])
            elif phase == "cash_paid":
                paid = amt(ev["cash_due"])
                cash += paid
                receivable -= paid
                payouts += paid
            elif phase == "stock_credited":
                q = amt(ev["bonus_due"])
                positions[symbol] = positions.get(symbol, D(0)) + q
                pending[symbol] -= q
                if pending[symbol] == 0:
                    del pending[symbol]
            elif phase == "record":
                record_count += 1
                # Registration occurs after any T+1-close booked trade.
            else:
                raise AssertionError(f"unknown_right_phase:{phase}")
        buy_budget = cash
        fee_day = D(0)
        assert all(o["side"] == "sell" for o in orders.get(date, [])[:sum(o["side"] == "sell" for o in orders.get(date, []))])
        for o in orders.get(date, []):
            trade_count += 1
            symbol, side, q = o["symbol"], o["side"], D(o["frozen_shares"])
            assert q > 0 and o["signal_date"] < date
            execution = D(o["execution_price"])
            actual = raw.prices(symbol).get(date)
            if actual is None:
                assert o["booked"] == "False" and o["reason"]
            else:
                within(execution, actual, f"raw_fill_price:{branch}:{date}:{symbol}")
            if o["booked"] == "True":
                assert not o["reason"]
                gross, fee = amt(o["gross"]), amt(o["fee"])
                within(gross, q * execution, f"gross:{branch}:{date}:{symbol}")
                within(fee, gross * fee_rate, f"fee:{branch}:{date}:{symbol}")
                fee_day += fee
                if side == "sell":
                    assert positions.get(symbol, D(0)) >= q
                    positions[symbol] -= q
                    if positions[symbol] == 0:
                        del positions[symbol]
                    cash += gross - fee
                    if branch == "netted":
                        buy_budget = cash
                elif side == "buy":
                    assert gross + fee <= buy_budget + TOL
                    if symbol not in positions:
                        assert len(positions) < 100
                    cash -= gross + fee
                    buy_budget -= gross + fee
                    positions[symbol] = positions.get(symbol, D(0)) + q
                else:
                    raise AssertionError(f"bad_order_side:{side}")
            else:
                assert o["reason"] and amt(o["gross"]) == amt(o["fee"]) == 0
        if cash < -TOL:
            cash_deficit = cash_deficit or date
        within(cash, amt(day["cash"]), f"cash:{branch}:{date}")
        within(receivable, amt(day["cash_receivable"]), f"receivable:{branch}:{date}")
        within(fee_day, amt(day["fees"]), f"day_fees:{branch}:{date}")
        rows_today = holdings.get(date, [])
        actual_positions = {r["symbol"]: D(r["shares"]) for r in rows_today if r["state"] == "settled_shares"}
        actual_pending = {r["symbol"]: D(r["shares"]) for r in rows_today if r["state"] == "pending_bonus_shares"}
        assert set(actual_positions) == set(positions), f"position_keys:{branch}:{date}"
        assert set(actual_pending) == set(pending), f"pending_keys:{branch}:{date}"
        for s, q in positions.items():
            within(q, actual_positions[s], f"shares:{branch}:{date}:{s}")
        for s, q in pending.items():
            within(q, actual_pending[s], f"pending_shares:{branch}:{date}:{s}")
        settled_value = D(0)
        pending_value = D(0)
        for r in rows_today:
            symbol, price, q = r["symbol"], D(r["valuation_price"]), D(r["shares"])
            within(amt(r["market_value"]), price*q, f"market_value:{branch}:{date}:{symbol}")
            expected_mark = raw.prices(symbol).get(date)
            if expected_mark is None:
                missed_raw_marks += 1
                assert r["price_basis"].startswith("verified_suspension")
                within(price, raw.mark(symbol, date), f"suspended_mark:{branch}:{date}:{symbol}")
                bar = bars.get((symbol, date))
                assert bar and bar["tradestatus"] == "0" and amt(bar["volume"]) == 0
            else:
                within(price, expected_mark, f"true_raw_mark:{branch}:{date}:{symbol}")
            if r["state"] == "settled_shares":
                settled_value += q*price
            else:
                pending_value += q*price
        within(settled_value, amt(day["stock_value"]), f"stock_value:{branch}:{date}")
        within(pending_value, amt(day["pending_share_value"]), f"pending_value:{branch}:{date}")
        components = cash + receivable + settled_value + pending_value
        residual = abs(components - amt(day["equity"]))
        max_daily_residual = max(max_daily_residual, residual)
        assert residual <= TOL, f"equity:{branch}:{date}:{residual}"
        assert len(positions) == int(day["held_count"])
        for ev in rights.get(date, []):
            if ev["phase"] == "record":
                symbol = ev["symbol"]
                assert symbol in positions
                within(D(ev["record_shares"]), positions[symbol], f"record_qualification:{branch}:{date}:{symbol}")

        # Independently derive T-close order quantities from current shares,
        # equity and raw/verified last marks.  The opening seed is special.
        scheduled = orders_signal.get(date, [])
        if scheduled:
            if date == "2025-09-30":
                raise AssertionError("opening date not in daily rows")
            target = targets.get(date)
            assert target and len(target) == 100 and len(set(target)) == 100
            per_name = components / D(100)
            expected = {}
            for symbol in set(positions) | set(target):
                price_t = raw.mark(symbol, date)
                desired = per_name / price_t if symbol in target else D(0)
                delta = desired - positions.get(symbol, D(0))
                if abs(delta) > TOL:
                    expected[(symbol, "buy" if delta > 0 else "sell")] = abs(delta)
            actual_keys = {(o["symbol"], o["side"]) for o in scheduled}
            assert actual_keys == set(expected) and len(scheduled) == len(expected), f"frozen_order_keys:{branch}:{date}"
            for o in scheduled:
                symbol = o["symbol"]
                within(D(o["frozen_shares"]), expected[(symbol, o["side"])],
                       f"frozen_qty:{branch}:{date}:{symbol}")
                within(D(o["signal_price"]), raw.mark(symbol, date),
                       f"frozen_price:{branch}:{date}:{symbol}")

    assert cash_deficit is None
    within(cash, D(summary["last_cash"]), f"last_cash:{branch}")
    within(receivable, D(summary["last_receivable"]), f"last_receivable:{branch}")
    assert set(positions) == set(summary["last_positions"])
    for symbol, q in summary["last_positions"].items():
        within(positions[symbol], D(q), f"last_position:{branch}:{symbol}")
    return {"days": len(daily), "last_date": daily[-1]["date"], "orders_audited": trade_count,
            "record_events": record_count, "verified_suspension_marks": missed_raw_marks,
            "max_equity_residual": str(max_daily_residual), "first_cash_deficit": cash_deficit,
            "first_block": summary["first_block"]}


def main() -> None:
    cli = argparse.ArgumentParser()
    cli.add_argument("--allow-stale-code", action="store_true", help="Interim output audit only; final certification still needs matching code hash")
    args = cli.parse_args()
    result = json.loads(RESULT.read_text(encoding="utf-8"))
    hashes = {"recent_slice.py": sha(CODE), "ledger_contract.py": sha(CONTRACT)}
    code_match = hashes["recent_slice.py"] == result["code_sha256"]
    contract_match = hashes["ledger_contract.py"] == result["contract_sha256"]
    if not args.allow_stale_code:
        assert code_match and contract_match
    for name, expected in result["input_sha256"].items():
        path = ROOT / name
        assert sha(path) == expected, f"input_sha_mismatch:{name}"
    assert sha(BAR_CACHE) == result["source_cache_sha256"]
    seed_grouped = grouped_by(rows(SEED), "rebalance_date")
    prior_date = result["old_effective_members_as_of"]
    seed = [r["symbol"].zfill(6) for r in seed_grouped[prior_date]]
    assert len(seed) == 100 and len(set(seed)) == 100
    targets_raw = grouped_by(rows(TARGET), "rebalance_date")
    targets = {date: [r["symbol"].zfill(6) for r in sorted(group, key=lambda x: int(x["rank"]))]
               for date, group in targets_raw.items()}
    bars = {(r["symbol"].zfill(6), r["date"]): r for r in rows(BAR_CACHE)}
    candidate_rows = [r for r in rows(EVENTS) if r["event_type"] == "dividend_or_bonus"
                      and r["source_status"] in ("single_source_only", "two_source_match")]
    candidates = {(r["record_date"], r["symbol"].zfill(6)): r for r in candidate_rows}
    assert len(candidates) == len(candidate_rows)
    credit_rows = [r for r in rows(CREDIT_SUPPLEMENT) if r["auto_apply_to_conditional_paper"] == "true"]
    credit_dates = {(r["symbol"],r["ex_date"]):r["share_list_date"] for r in credit_rows}
    supplements = {(r["symbol"].zfill(6), r["date"]): r for r in rows(SUPPLEMENTS)}
    assert len(supplements) == result["real_trade_raw_supplement_count"]
    assert sha(SUPPLEMENTS) == result["raw_supplement_sha256"]
    raw = Raw(supplements)
    for (symbol, date), row in supplements.items():
        assert date not in raw.base_prices(symbol)
        bar = bars.get((symbol, date))
        assert bar and bar["tradestatus"] == "1" and amt(bar["volume"]) > 0
        within(D(row["close_raw"]), D(bar["close"]), f"supplement_bar_close:{date}:{symbol}")
        within(D(row["preclose"]), D(bar["preclose"]), f"supplement_bar_previous:{date}:{symbol}")
        assert amt(row["volume"]) == amt(bar["volume"])
        previous = [d for d in raw.prices(symbol) if d < date]
        if previous:
            within(D(row["preclose"]), raw.prices(symbol)[max(previous)],
                   f"supplement_previous_true_close:{date}:{symbol}")
    branches = {name: audit_branch(name, branch, D(result["capital_start"]), D(result["fee_each_side"]),
                                   targets, seed, raw, bars, candidates, credit_dates) for name, branch in result["branches"].items()}
    out = {"status": "recent_stock_paper_account_independent_event_replay_pass" if code_match and contract_match else "recent_account_event_replay_pass_but_source_code_stale",
           "result_sha256": sha(RESULT), "code_sha256": hashes,
           "code_hash_match": code_match, "contract_hash_match": contract_match,
           "input_count": len(result["input_sha256"]), "supplement_count": len(supplements),
           "supplement_sha256": sha(SUPPLEMENTS), "event_source_sha256": sha(EVENTS),
           "credit_supplement_sha256": sha(CREDIT_SUPPLEMENT), "branches": branches,
           "scope": "2025-09-30 seed is old effective membership, not continuous 2010 account. No hedge, formal v2.0/v2.3/v2.5 signal regeneration or real fills."}
    HERE.mkdir(parents=True, exist_ok=True)
    (HERE / "recent_account_audit.json").write_text(json.dumps(out, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"status": out["status"], "branches": branches}, ensure_ascii=False))


if __name__ == "__main__":
    main()
