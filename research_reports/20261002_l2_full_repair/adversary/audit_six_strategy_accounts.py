"""Independent event replay of six recent signal-gated stock+swap paper accounts.

Reads written CSVs and raw close sources only; no strategy or engine imports.
Hashes are strict so an in-progress producer run cannot receive a PASS.
"""

from __future__ import annotations

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
VERSION = ROOT / "research_reports/20261002_l2_full_repair/version_integration"
ENGINE = ROOT / "research_reports/20261002_l2_full_repair/engine"
RAW = ROOT / ".microcap_index_cache/prices_raw"
BAR_CACHE = ENGINE / "queried_baostock_bars.csv"
SUPPLEMENT = ENGINE / "raw_close_supplements.csv"
EVENTS = ROOT / "research_reports/20261002_l2_full_repair/data/event_candidates_2010_20260930.csv"
CREDIT_SUPPLEMENT = ROOT / "research_reports/20261002_l2_full_repair/data/issuer_credit_date_supplements.csv"
TARGET = ROOT / "outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_members.csv"
TOL = D("1e-18")


def rows(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8-sig", newline="") as stream:
        return list(csv.DictReader(stream))


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def close(a: D, b: D, name: str) -> None:
    assert abs(a-b) <= TOL, f"{name}:{a}!={b}:{a-b}"


def grouped(items: list[dict[str, str]], key: str) -> dict[str, list[dict[str, str]]]:
    out: dict[str, list[dict[str, str]]] = defaultdict(list)
    for item in items:
        out[item[key]].append(item)
    return out


class Prices:
    def __init__(self, bars: dict[tuple[str, str], dict[str, str]], supplement: dict[tuple[str, str], dict[str, str]]):
        self.bars = bars
        self.supplement = supplement
        self.cache: dict[str, dict[str, D]] = {}
        self.source_paths: set[str] = set()
        self.queried_true_trade_marks: set[tuple[str, str]] = set()

    def all(self, symbol: str) -> dict[str, D]:
        if symbol not in self.cache:
            path = RAW / f"{symbol}.csv"
            self.source_paths.add(str(path.relative_to(ROOT)))
            data = {r["date"]: D(r["close_raw"]) for r in rows(path)}
            for (s, date), item in self.supplement.items():
                if s == symbol:
                    assert date not in data
                    data[date] = D(item["close_raw"])
            self.cache[symbol] = data
        return self.cache[symbol]

    def mark(self, symbol: str, date: str) -> tuple[D, bool]:
        data = self.all(symbol)
        if date in data:
            return data[date], True
        bar = self.bars.get((symbol, date))
        if bar and bar["tradestatus"] == "1" and D(bar["volume"] or "0") > 0:
            price = D(bar["close"])
            assert price > 0
            self.queried_true_trade_marks.add((symbol,date))
            return price, True
        prior = [d for d in data if d < date]
        assert prior, f"missing_all_raw_price:{symbol}:{date}"
        assert bar and bar["tradestatus"] == "0" and D(bar["volume"] or "0") == 0, f"missing_not_verified_suspension:{symbol}:{date}:{bar}"
        px = data[max(prior)]
        close(D(bar["close"]), px, f"suspension_bar_mark:{symbol}:{date}")
        return px, False


def account(branch: str, version: str, prices: Prices,
            source_events: dict[tuple[str, str], dict[str, str]],
            targets: dict[str, list[str]], credit_dates: dict[tuple[str, str], str]) -> dict:
    stem = f"recent_{branch}_{version.replace('.', '_')}_strategy"
    manifest_path = VERSION / f"{stem}_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    input_paths = {"baostock_cache": BAR_CACHE, "event_candidates": EVENTS, "target_members": TARGET}
    for name, path in input_paths.items():
        assert sha(path) == manifest["input_sha256"][name], f"source_input_hash_changed:{branch}:{version}:{name}"
    code_paths = {"engine_extend_ledger":ENGINE/"extend_ledger.py",
                  "engine_recent_slice":ENGINE/"recent_slice.py",
                  "strategy_replay":VERSION/"strategy_replay.py"}
    for name,path in code_paths.items():
        assert sha(path) == manifest["source_code_sha256"][name], f"source_code_hash_changed:{branch}:{version}:{name}"
    for data in manifest.get("source_response_files",{}).values():
        path = Path(data["path"])
        assert sha(path) == data["sha256"] and len(rows(path)) == data["rows"], f"source_response_changed:{branch}:{version}:{path.name}"
    paths = {name: VERSION / f"{stem}_{name}.csv" for name in ("daily", "holdings", "orders", "rights")}
    for name, path in paths.items():
        assert sha(path) == manifest["detail_sha256"][name], f"detail_hash_changed:{path.name}"
    signal_path = VERSION / f"recent_{branch}_rebuilt_signals.csv"
    assert sha(signal_path) == manifest["source_signal_sha256"]
    signals = rows(signal_path)
    signal_by_date = {r["date"]: r for r in signals}
    daily = rows(paths["daily"])
    holdings = grouped(rows(paths["holdings"]), "date")
    orders_all = rows(paths["orders"])
    orders = grouped(orders_all, "execution_date")
    orders_signal = grouped(orders_all, "signal_date")
    rights_all = rows(paths["rights"])
    rights = grouped(rights_all, "date")
    assert len(daily) == manifest["settled_days"] == len(signals) == 241
    assert daily[-1]["date"] == manifest["last_settled"] == "2026-09-30"
    assert [r["date"] for r in daily] == [r["date"] for r in signals]
    assert len({r["date"] for r in daily}) == len(daily)
    record_set = {(r["record_date"], r["symbol"]) for r in rights_all if r["phase"] == "record"}
    held_set = {(r["date"], r["symbol"]) for group in holdings.values() for r in group if D(r["shares"]) > 0}
    expected_records = set(source_events) & held_set
    assert record_set == expected_records, f"record_event_omission_or_extra:{branch}:{version}:{len(expected_records-record_set)}:{len(record_set-expected_records)}"
    for event in rights_all:
        key = (event["record_date"], event["symbol"])
        assert key in source_events
        source = source_events[key]
        assert event["source_status"] == source["source_status"]
        expected_date = {"record": source["record_date"], "ex": source["ex_date"],
                         "cash_paid": source["cash_pay_date"]}.get(event["phase"])
        if event["phase"] == "stock_credited":
            expected_date = source["share_credit_date"] or credit_dates[(event["symbol"],source["ex_date"])]
        assert event["date"] == expected_date
        close(D(event["cash_due"]), D(event["record_shares"])*D(source["cash_per_10_gross"] or 0)/10,
              f"source_cash:{branch}:{version}:{key}")
        close(D(event["bonus_due"]), D(event["record_shares"])*D(source["bonus_per_10"] or 0)/10,
              f"source_bonus:{branch}:{version}:{key}")
    positions: dict[str, D] = {}
    pending: dict[str, D] = {}
    cash, receivable, margin = D(1000000), D(0), D(0)
    previous_hedge: D | None = None
    previous_notional = D(0)
    fee_total = D(0)
    drag_total = D(0)
    max_debt = D(0)
    booked_orders = 0
    first_600543_after_st = None
    blocked_orders = 0
    marks_suspended = 0
    debt_first = None
    mdd = D(0)
    peak = D(1)
    prev_equity = D(1000000)
    for i, day in enumerate(daily):
        date = day["date"]
        signal = signal_by_date[date]
        key = version.replace(".", "_")
        assert day["signal_at_close"].lower() == signal[f"{key}_signal_at_close"].lower()
        hedge_close = D(signal["hedge_close"])
        hedge_ret = D(0) if previous_hedge is None else hedge_close/previous_hedge-1
        close(D(day["hedge_return"]), hedge_ret, f"hedge_return:{branch}:{version}:{date}")
        swap_pnl = -previous_notional*hedge_ret
        swap_drag = previous_notional*D("0.0003")
        drag_total += swap_drag
        close(D(day["swap_pnl"]), swap_pnl, f"swap_pnl:{branch}:{version}:{date}")
        close(D(day["swap_drag"]), swap_drag, f"swap_drag:{branch}:{version}:{date}")
        close(D(day["shadow_financing_3pct"]), max(-margin,D(0))*D("0.03")/D(244),
              f"shadow_financing:{branch}:{version}:{date}")
        margin += swap_pnl - swap_drag
        close(D(day["swap_margin"]), margin, f"margin:{branch}:{version}:{date}")
        close(D(day["swap_debt"]), max(-margin,D(0)), f"debt:{branch}:{version}:{date}")
        if margin < 0 and debt_first is None:
            debt_first = date
        max_debt = max(max_debt,max(-margin,D(0)))
        for ev in rights.get(date, []):
            s, phase = ev["symbol"], ev["phase"]
            if phase == "ex":
                receivable += D(ev["cash_due"])
                q = D(ev["bonus_due"])
                if q:
                    pending[s] = pending.get(s,D(0))+q
            elif phase == "cash_paid":
                cash += D(ev["cash_due"])
                receivable -= D(ev["cash_due"])
            elif phase == "stock_credited":
                q = D(ev["bonus_due"])
                pending[s] -= q
                if pending[s] == 0:
                    del pending[s]
                positions[s] = positions.get(s,D(0))+q
            else:
                assert phase == "record"
        pre_stock = sum((q*prices.mark(s,date)[0] for s,q in positions.items()),D(0))
        pre_pending = sum((q*prices.mark(s,date)[0] for s,q in pending.items()),D(0))
        before_trade = cash+receivable+margin+pre_stock+pre_pending
        budget = cash
        fee_today = D(0)
        day_orders = orders.get(date, [])
        assert all(x["side"] == "sell" for x in day_orders[:sum(x["side"] == "sell" for x in day_orders)])
        for order in day_orders:
            assert i > 0 and order["signal_date"] == daily[i-1]["date"], f"T_plus_1_clock:{branch}:{version}:{date}"
            s, side, q = order["symbol"],order["side"],D(order["frozen_shares"])
            px, true = prices.mark(s,date)
            close(D(order["execution_price"]), px, f"execution_price:{branch}:{version}:{date}:{s}")
            if order["booked"] == "True":
                booked_orders += 1
                assert true and not order["reason"]
                gross, fee = D(order["gross"]),D(order["fee"])
                close(gross,q*px,f"gross:{branch}:{version}:{date}:{s}")
                close(fee,gross*D("0.003"),f"fee:{branch}:{version}:{date}:{s}")
                fee_today += fee
                if side == "sell":
                    assert positions.get(s,D(0)) >= q
                    positions[s] -= q
                    if positions[s] == 0:
                        del positions[s]
                    cash += gross-fee
                    if branch == "netted":
                        budget = cash
                else:
                    assert side == "buy"
                    assert gross+fee <= budget+TOL, f"buy_funding:{branch}:{version}:{date}:{s}"
                    assert s in positions or len(positions) < 100
                    cash -= gross+fee
                    budget -= gross+fee
                    positions[s] = positions.get(s,D(0))+q
            else:
                blocked_orders += 1
                assert order["reason"] and D(order["gross"]) == D(order["fee"]) == 0
        assert cash >= -TOL and budget >= -TOL
        close(cash,D(day["cash"]),f"cash:{branch}:{version}:{date}")
        close(receivable,D(day["cash_receivable"]),f"receivable:{branch}:{version}:{date}")
        close(fee_today,D(day["stock_trade_fees"]),f"fees:{branch}:{version}:{date}")
        fee_total += fee_today
        actual = {r["symbol"]: D(r["shares"]) for r in holdings.get(date, [])}
        assert set(actual) == set(positions), f"position_keys:{branch}:{version}:{date}"
        for s,q in positions.items():
            close(actual[s],q,f"shares:{branch}:{version}:{date}:{s}")
        stock_value = D(0)
        for hold in holdings.get(date, []):
            s,q = hold["symbol"],D(hold["shares"])
            px,true = prices.mark(s,date)
            close(D(hold["valuation_price"]),px,f"mark:{branch}:{version}:{date}:{s}")
            close(D(hold["market_value"]),q*px,f"value:{branch}:{version}:{date}:{s}")
            if not true:
                marks_suspended += 1
                assert hold["price_basis"].startswith("verified_suspension")
            stock_value += q*px
        pending_value = sum((q*prices.mark(s,date)[0] for s,q in pending.items()),D(0))
        close(stock_value,D(day["stock_value"]),f"stock_value:{branch}:{version}:{date}")
        close(pending_value,D(day["pending_share_value"]),f"pending_value:{branch}:{version}:{date}")
        equity = cash+receivable+pending_value+stock_value+margin
        close(equity,D(day["equity"]),f"equity:{branch}:{version}:{date}")
        close(before_trade-fee_today,equity,f"trade_conservation:{branch}:{version}:{date}")
        assert len(positions) == int(day["held_count"])
        if date >= "2026-04-27" and "600543" in positions and first_600543_after_st is None:
            first_600543_after_st = date
        assert len(day_orders) == int(day["executed_orders"])
        for ev in rights.get(date, []):
            if ev["phase"] == "record":
                assert ev["symbol"] in positions
                close(D(ev["record_shares"]),positions[ev["symbol"]],f"record_qty:{branch}:{version}:{date}")
        previous_notional = (D("0.8") if version != "v2.5" else D(0))*(stock_value+pending_value)
        close(D(day["short_notional_after_close"]),previous_notional,f"short_notional:{branch}:{version}:{date}")
        previous_hedge = hedge_close
        peak = max(peak,equity/D(1000000))
        mdd = min(mdd,equity/D(1000000)/peak-1)
        prev_equity = equity
        for order in orders_signal.get(date, []):
            s,side,q = order["symbol"],order["side"],D(order["frozen_shares"])
            price_t, _ = prices.mark(s,date)
            close(D(order["signal_price"]),price_t,f"signal_mark:{branch}:{version}:{date}:{s}")
            target_day = max(t for t in targets if t <= date)
            if signal[f"{key}_signal_at_close"] == "True":
                desired = equity/D(100)/price_t if s in targets[target_day] else D(0)
            else:
                desired = D(0)
            delta = desired-positions.get(s,D(0))
            assert (delta > 0 and side == "buy") or (delta < 0 and side == "sell")
            close(q,abs(delta),f"frozen_qty:{branch}:{version}:{date}:{s}")
        if i < len(daily)-1:
            assert len(orders_signal.get(date, [])) == int(day["pending_orders"]), (
                f"pending_order_count:{branch}:{version}:{date}:"
                f"{len(orders_signal.get(date, []))}!={day['pending_orders']}")
    assert debt_first == manifest["first_swap_margin_debt"]
    if version == "v2.5":
        assert all(D(r["swap_margin"]) == D(r["swap_drag"]) == D(r["short_notional_after_close"]) == 0 for r in daily)
    annualized = (prev_equity/D(1000000)) ** (D(244)/D(len(daily))) - 1
    return {"days":len(daily),"last_date":daily[-1]["date"],"orders":len(orders_all),
            "booked_orders":booked_orders,
            "blocked_orders":blocked_orders,"record_events":len(record_set),
            "verified_suspension_marks":marks_suspended,"first_swap_debt":debt_first,
            "max_swap_debt":str(max_debt),"swap_drag_total":str(drag_total),
            "first_600543_held_after_actual_st":first_600543_after_st,
            "fees_total":str(fee_total),"equity_last":str(prev_equity),
            "annualized_244d":str(annualized),"max_drawdown_including_initial_1":str(mdd),
            "open_frozen_order_count_at_final_close":int(daily[-1]["pending_orders"]),
            "manifest_sha256":sha(manifest_path),"detail_sha256":{p.name:sha(p) for p in paths.values()}}


def main() -> None:
    bars = {(r["symbol"],r["date"]):r for r in rows(BAR_CACHE)}
    supplement = {(r["symbol"],r["date"]):r for r in rows(SUPPLEMENT)}
    prices = Prices(bars,supplement)
    source_rows = [r for r in rows(EVENTS) if r["event_type"] == "dividend_or_bonus"
                   and r["source_status"] in ("single_source_only","two_source_match")]
    source_events = {(r["record_date"],r["symbol"]):r for r in source_rows}
    assert len(source_events) == len(source_rows)
    credit_rows = [r for r in rows(CREDIT_SUPPLEMENT) if r["auto_apply_to_conditional_paper"] == "true"]
    credit_dates = {(r["symbol"],r["ex_date"]):r["share_list_date"] for r in credit_rows}
    targets_by_date = grouped(rows(TARGET),"rebalance_date")
    targets = {d:[r["symbol"].zfill(6) for r in sorted(group,key=lambda r:int(r["rank"]))]
               for d,group in targets_by_date.items()}
    result = {}
    for branch in ("prefunded","netted"):
        for version in ("v2.0","v2.3","v2.5"):
            result[f"{branch}_{version}"] = account(branch,version,prices,source_events,targets,credit_dates)
    producer_summary_path = VERSION/"strategy_accounts_audit.json"
    producer_summary = json.loads(producer_summary_path.read_text(encoding="utf-8"))
    for item in producer_summary["accounts"]:
        key = f"{item['branch']}_{item['version']}"
        ours = result[key]
        assert item["sessions"] == ours["days"] and item["booked_orders"] == ours["booked_orders"]
        assert item["rejected_orders"] == ours["blocked_orders"]
        assert item["first_actual_600543_held_after_ST"] == ours["first_600543_held_after_actual_st"]
        assert item["manifest_sha256"] == ours["manifest_sha256"]
        for field,own in (("end_equity","equity_last"),("stock_fees","fees_total"),
                          ("swap_drag","swap_drag_total"),("max_swap_debt","max_swap_debt")):
            close(D(item[field]),D(ours[own]),f"producer_summary:{key}:{field}")
    out = {"status":"six_recent_conditional_paper_accounts_independent_replay_pass",
           "source_sha256":{p.name:sha(p) for p in (BAR_CACHE,SUPPLEMENT,EVENTS,CREDIT_SUPPLEMENT,TARGET)},
           "price_file_sha256":{name:sha(ROOT/name) for name in sorted(prices.source_paths)},
           "producer_summary_sha256":sha(producer_summary_path),
           "queried_true_trade_mark_count":len(prices.queried_true_trade_marks),
           "accounts":result,
           "scope":"Conditional old L1 target pool; no formal full-history or actual futures/margin financing. Annualization uses 244 sessions and initial equity 1 in drawdown."}
    (HERE/"six_strategy_accounts_audit.json").write_text(json.dumps(out,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
    print(json.dumps({"status":out["status"],"accounts":{k:{x:v[x] for x in ("equity_last","blocked_orders","first_swap_debt")} for k,v in result.items()}},ensure_ascii=False))


if __name__ == "__main__":
    main()
