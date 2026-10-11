"""Extend the frozen 2010 paper account until the first unproved settlement.

This is a diagnostic continuation of the older isolated candidate.  It does
not consume a NAV or infer cash from proxy returns.  Order quantities remain
frozen at the prior close; execution prices only accept or reject them.
"""

from __future__ import annotations

import csv
import hashlib
import json
import sys
from datetime import date as Date, timedelta
from decimal import Decimal as D, ROUND_HALF_UP, getcontext
from pathlib import Path

import baostock as bs


getcontext().prec = 40
ROOT = Path(__file__).resolve().parents[3]
HERE = Path(__file__).resolve().parent
OLD = ROOT / "research_reports/20260926_l2_repair/fixed_share_engine"
sys.path.insert(0, str(OLD))
from ledger_contract import freeze_orders, settle_next_close  # noqa: E402

PRICES = ROOT / ".microcap_index_cache/prices_raw"
MEMBERS = ROOT / "outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_members.csv"
CALENDAR = ROOT / "outputs/wind_microcap_top_100_biweekly_thursday_16y_cached.csv"
RIGHTS = ROOT / "research_reports/20260926_l1_repair/corporate_actions/cninfo_distribution_events.csv"
LEDGER = ROOT / "research_reports/20260926_l1_repair/corporate_actions/rights_event_ledger_2010_2014.csv"
RESTRUCTURING_EVIDENCE = ROOT / "research_reports/20260926_l1_repair/st_timing/600633_full_chain/600633_EVENT_CHAIN.md"
FIRST = OLD / "continued_replay.json"
EXISTING_BARS = [
    ROOT / "research_reports/20260926_l2_repair/clock/baostock_raw_open_windows.csv",
    OLD / "extra_bars_20100212.csv",
]
CACHED_BARS = HERE / "queried_baostock_bars.csv"
COVERAGE = HERE / "queried_baostock_coverage.json"
FIELDS = "date,code,close,preclose,volume,tradestatus,isST"


def rows(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def pos(v: str | None) -> bool:
    if not v:
        return False
    try:
        return D(v) > 0
    except Exception:
        return False


def require_ex_raw(source, item: dict, date: str) -> None:
    """A stale pre-ex raw mark plus a new right would double-count value."""
    if date in source.price_map(item["symbol"]):
        return
    if hasattr(source, "mark"):
        _, basis = source.mark(item["symbol"],date)
        if basis in {"true_raw_close", "BaoStock_unadjusted_real_trade_supplement"}:
            return
    if date not in source.price_map(item["symbol"]):
        raise ValueError(f"unpriced_ex_action_no_raw_close:{date}:{item['symbol']}")


def side_limit_pct(source, symbol: str, date: str, is_st: str) -> D | None:
    """Only the dated exchange regimes we can identify from local symbol/date.

    None means the STAR first-five-session no-limit regime.  Unknown boards
    fail closed.  `isST` itself remains the frozen vendor diagnostic field.
    Official rules: SZSE 2020-08-24 ChiNext 20%; SSE STAR first 5 sessions
    unlimited then 20%; SSE/SZSE 2026-07-06 main-board ST 10%.
    """
    if is_st not in {"0", "1"}:
        raise ValueError(f"unknown_ST_status:{date}:{symbol}:{is_st}")
    if symbol == "600633" and date == "2011-09-29":
        # Exchange-announced first session after re-listing, N 浙传媒.
        return None
    pmap = source.price_map(symbol)
    first_raw = min(pmap)
    if first_raw >= "2019-01-01":
        listing = source.list_date(symbol)
        ordinal = sum(d <= date for d in pmap)
        if ordinal <= 5 and first_raw != listing:
            raise ValueError(f"first_five_listing_date_mismatch:{date}:{symbol}:{first_raw}:{listing}")
        if ordinal <= 5 and (symbol.startswith(("688","689"))
                or (symbol.startswith(("300","301")) and first_raw >= "2020-08-24")
                or (symbol.startswith(("0","6")) and first_raw >= "2023-04-10")):
            return None
    if symbol.startswith(("688", "689")):
        return D("0.20")
    if symbol.startswith(("300", "301")) and date >= "2020-08-24":
        return D("0.20")
    if symbol.startswith(("0", "3", "6")):
        if is_st == "1":
            return D("0.10") if date >= "2026-07-06" and not symbol.startswith(("300", "301")) else D("0.05")
        return D("0.10")
    raise ValueError(f"unsupported_limit_board:{date}:{symbol}")


def check_paper_trade(source, symbol: str, date: str, side: str, price: D) -> str | None:
    """Return a rejection reason; query public status only on a material edge.

    A local raw-close row is evidence of a traded daily close.  For a close
    that might be on the blocked side limit, confirm status, ST and preclose
    from BaoStock.  Missing raw requires an explicit suspension bar.
    """
    pmap = source.price_map(symbol)
    if symbol == "600633" and "2010-04-30" <= date < "2011-09-29":
        return "issuer_documented_suspension"
    if date not in pmap:
        bar = source.bar(symbol, date)
        if bar and bar["tradestatus"] == "0" and bar["volume"] in {"", "0", "0.0"}:
            return "suspended_or_zero_volume"
        raise ValueError(f"missing_raw_on_trade_date:{date}:{symbol}")
    earlier = [d for d in pmap if d < date]
    if not earlier:
        raise ValueError(f"missing_previous_true_close_for_limit:{date}:{symbol}")
    prev_date = max(earlier)
    prev = pmap[prev_date]
    sign = D(1) if side == "buy" else D(-1)
    possible = [side_limit_pct(source, symbol, date, st) for st in ("0", "1")]
    prior_market = source.previous_market_day.get(date)
    must_confirm_reference = (prev_date != prior_market or (symbol,date) in source.action_exdates)
    at_raw_estimated_limit = any(pct is not None and price == (prev * (1 + sign*pct)).quantize(D("0.01"), rounding=ROUND_HALF_UP)
               for pct in possible)
    if not must_confirm_reference and not at_raw_estimated_limit:
        return None
    bar = source.bar(symbol, date)
    if not bar:
        raise ValueError(f"missing_limit_status:{date}:{symbol}")
    if bar["tradestatus"] != "1" or not bar["volume"] or D(bar["volume"]) <= 0:
        return "suspended_or_zero_volume"
    if price != D(bar["close"]):
        raise ValueError(f"raw_baostock_close_mismatch:{date}:{symbol}")
    pct = side_limit_pct(source, symbol, date, bar["isST"])
    if pct is None:
        return None
    if not bar["preclose"]:
        raise ValueError(f"missing_exchange_limit_reference:{date}:{symbol}")
    limit = (D(bar["preclose"]) * (1 + sign*pct)).quantize(D("0.01"), rounding=ROUND_HALF_UP)
    return "at_side_close_limit_diagnostic_ST_and_board" if price == limit else None


class Source:
    def __init__(self, namespace: str = "recent"):
        if namespace not in {"recent", "full"}:
            raise ValueError(f"invalid_source_namespace:{namespace}")
        self.bars_path = CACHED_BARS if namespace == "recent" else HERE / "full_queried_baostock_bars.csv"
        self.coverage_path = COVERAGE if namespace == "recent" else HERE / "full_queried_baostock_coverage.json"
        self.supplement_path = HERE / ("raw_close_supplements.csv" if namespace == "recent" else "full_raw_close_supplements.csv")
        self.raw: dict[str, dict[str, D]] = {}
        self.raw_paths: set[Path] = set()
        self.raw_supplements: dict[tuple[str,str], dict[str,str]] = {}
        self.bars: dict[tuple[str, str], dict[str, str]] = {}
        self.coverage: dict[str, list[str]] = {}
        self.queries: list[dict[str, str | int]] = []
        days = [r["date"] for r in rows(CALENDAR)]
        self.previous_market_day = {days[i]: days[i-1] for i in range(1,len(days))}
        event_path = ROOT / "research_reports/20261002_l2_full_repair/data/event_candidates_2010_20260930.csv"
        self.action_exdates = {(r["symbol"].zfill(6),r["ex_date"]) for r in rows(event_path) if r["ex_date"]}
        for path in EXISTING_BARS + ([self.bars_path] if self.bars_path.exists() else []):
            for r in rows(path):
                self.bars[(r["symbol"].zfill(6), r["date"])] = r
        if self.bars_path.exists():
            if self.coverage_path.exists():
                loaded = json.loads(self.coverage_path.read_text(encoding="utf-8"))
                self.coverage = {s: (v if isinstance(v,list) else ["2010-02-22",v]) for s,v in loaded.items()}
            else:
                # Pre-coverage cache was fetched over this exact interval.
                self.coverage = {r["symbol"].zfill(6): ["2010-02-22","2010-04-30"] for r in rows(self.bars_path)}

    def price_map(self, symbol: str) -> dict[str, D]:
        if symbol not in self.raw:
            path = PRICES / f"{symbol}.csv"
            if not path.is_file():
                raise ValueError(f"missing_raw_file:{symbol}")
            data = {}
            for r in rows(path):
                v = D(r["close_raw"])
                if v <= 0 or not v.is_finite() or r["date"] in data:
                    raise ValueError(f"bad_raw_file:{symbol}:{r['date']}")
                data[r["date"]] = v
            self.raw[symbol] = data
            self.raw_paths.add(path)
        return self.raw[symbol]

    def list_date(self, symbol: str) -> str:
        path = ROOT / ".microcap_index_cache/security_meta" / f"{symbol}.json"
        if not path.exists():
            raise ValueError(f"missing_listing_metadata:{symbol}")
        listed = json.loads(path.read_text(encoding="utf-8")).get("list_date")
        if not listed:
            raise ValueError(f"empty_listing_date:{symbol}")
        self.raw_paths.add(path)
        return listed

    def ensure_bars(self, symbol: str, date: str):
        interval = self.coverage.get(symbol)
        if (symbol, date) in self.bars or (interval and interval[0] <= date <= interval[1]):
            return
        code = ("sh." if symbol.startswith("6") else "sz.") + symbol
        if interval and date < interval[0]:
            start, end = Date.fromisoformat(date), Date.fromisoformat(interval[0])-timedelta(days=1)
        else:
            start = (Date.fromisoformat(interval[1]) + timedelta(days=1)) if interval else Date.fromisoformat(date)
            end = Date(2026,9,30)
        # The recent slice has at most one year left.  The early full-history
        # branch uses bounded 90-day windows so a single 2010 suspension does
        # not request sixteen years of bars before the next rights decision.
        if self.bars_path.name.startswith("full_") and not (interval and date < interval[0]):
            end = min(Date.fromisoformat(date)+timedelta(days=90),Date(2026,9,30))
        query = bs.query_history_k_data_plus(code, FIELDS,
            start_date=start.isoformat(), end_date=end.isoformat(), frequency="d", adjustflag="3")
        if query.error_code == "10001001":
            reauth = bs.login()
            if reauth.error_code != "0":
                raise ValueError(f"baostock_relogin_failed:{symbol}:{reauth.error_code}:{reauth.error_msg}")
            query = bs.query_history_k_data_plus(code, FIELDS,
                start_date=start.isoformat(), end_date=end.isoformat(), frequency="d", adjustflag="3")
        if query.error_code != "0":
            raise ValueError(f"baostock_query_failed:{symbol}:{query.error_code}:{query.error_msg}")
        count = 0
        while query.next():
            r = dict(zip(FIELDS.split(","), query.get_row_data()))
            r["symbol"] = symbol
            self.bars[(symbol, r["date"])] = r
            count += 1
        self.coverage[symbol] = [min(start.isoformat(),interval[0]) if interval else start.isoformat(),
            max(end.isoformat(),interval[1]) if interval else end.isoformat()]
        self.queries.append({"symbol": symbol, "rows": count, "code": code,
            "start_date": start.isoformat(), "end_date": end.isoformat()})

    def bar(self, symbol: str, date: str) -> dict[str, str] | None:
        self.ensure_bars(symbol, date)
        return self.bars.get((symbol, date))

    def mark(self, symbol: str, date: str, last: D | None = None) -> tuple[D, str]:
        pmap = self.price_map(symbol)
        actual = pmap.get(date)
        if actual is not None:
            return actual, "true_raw_close"
        if last is None:
            earlier = [d for d in pmap if d < date]
            if earlier:
                last = pmap[max(earlier)]
        if symbol == "600633" and "2010-04-30" <= date < "2011-09-29":
            if last != D("7.28") or pmap.get("2010-04-29") != D("7.28"):
                raise ValueError(f"issuer_suspension_mark_anchor_mismatch:{date}:600633")
            return last, "issuer_verified_suspension_last_true_close"
        bar = self.bar(symbol, date)
        if (bar and bar["tradestatus"] == "1" and bar["volume"]
                and D(bar["volume"]) > 0 and bar["close"]):
            value = D(bar["close"])
            if value <= 0 or (last is not None and (symbol,date) not in self.action_exdates
                    and bar["preclose"] and D(bar["preclose"]) != last):
                raise ValueError(f"public_raw_supplement_previous_close_mismatch:{date}:{symbol}")
            pmap[date] = value
            self.raw_supplements[(symbol,date)] = {"date":date,"symbol":symbol,
                "close_raw":str(value),"preclose":bar["preclose"],
                "volume":bar["volume"],"tradestatus":bar["tradestatus"],
                "basis":"BaoStock_unadjusted_real_trade_supplement"}
            return value, "BaoStock_unadjusted_real_trade_supplement"
        if (bar and last is not None and bar["tradestatus"] == "0"
                and bar["volume"] in {"", "0", "0.0"} and D(bar["close"]) == last
                and D(bar["preclose"]) == last):
            return last, "verified_suspension_last_true_close"
        raise ValueError(f"unverified_missing_raw_close:{date}:{symbol}")

    def save(self):
        # The deterministic cache is a raw public-source observation, not a
        # strategy cache.  A query error leaves previously fetched rows intact.
        source_rows = [r for (s, d), r in sorted(self.bars.items()) if d >= "2010-02-22"]
        with self.bars_path.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=FIELDS.split(",") + ["symbol"])
            writer.writeheader()
            writer.writerows(source_rows)
        if self.raw_supplements:
            with self.supplement_path.open("w",encoding="utf-8",newline="") as f:
                w=csv.DictWriter(f,fieldnames=list(next(iter(self.raw_supplements.values()))))
                w.writeheader()
                w.writerows(r for _,r in sorted(self.raw_supplements.items()))
        self.coverage_path.write_text(json.dumps(self.coverage, ensure_ascii=False, sort_keys=True, indent=2)+"\n", encoding="utf-8")


def main():
    HERE.mkdir(exist_ok=True)
    prior = json.loads(FIRST.read_text(encoding="utf-8"))
    calendar = [r["date"] for r in rows(CALENDAR)]
    if calendar != sorted(set(calendar)):
        raise ValueError("calendar_not_ordered_unique")
    dates = [d for d in calendar if d > "2010-02-12"]
    by_signal: dict[str, list[str]] = {}
    for r in rows(MEMBERS):
        by_signal.setdefault(r["rebalance_date"], []).append(r["symbol"].zfill(6))
    target_rows = rows(MEMBERS)
    by_signal = {}
    for r in target_rows:
        by_signal.setdefault(r["rebalance_date"], []).append(r)
    for day, members in by_signal.items():
        by_signal[day] = [r["symbol"].zfill(6) for r in sorted(members, key=lambda x: int(x["rank"]))]
    actions: dict[tuple[str, str], list[dict[str, str]]] = {}
    for r in rows(RIGHTS):
        if not any(pos(r.get(k)) for k in ("送股比例", "转增比例", "派息比例")):
            continue
        symbol = r["symbol"].zfill(6)
        for key, kind in (("股权登记日", "record"), ("除权日", "ex"), ("派息日", "cash_paid"), ("股份到账日", "stock_credited")):
            d = r.get(key, "")
            if d and "2010-02-12" < d <= dates[-1]:
                actions.setdefault((d, kind), []).append(r)
    reviewed = {(r["symbol"].zfill(6), r["record_date"], r["ex_date"]): r for r in rows(LEDGER)}
    source = Source(namespace="full")
    login = bs.login()
    if login.error_code != "0":
        raise RuntimeError(f"baostock_login_failed:{login.error_code}:{login.error_msg}")
    branches = {}
    all_positions = []
    all_orders = []
    all_rights = []
    try:
        for funding in ("prefunded", "netted"):
            previous = prior["branches"][funding]
            if previous["first_strict_block"] or previous["last_conditional_date"] != "2010-02-12":
                raise ValueError(f"invalid_prior_branch:{funding}")
            end = previous["events"][-1]
            positions = {s: D(q) for s, q in end["positions_after"].items()}
            cash = D(end["cash_after"])
            last = {s: source.mark(s, "2010-02-12")[0] for s in positions}
            entitlements = []
            cash_receivable = D(0)
            pending_shares = {}
            frozen = None
            daily = []
            trades = []
            first_block = None
            suspensions = 0
            for i, date in enumerate(dates):
                snapshot = (dict(positions), cash, dict(last), [dict(x) for x in entitlements],
                    cash_receivable, dict(pending_shares), frozen, len(trades), len(all_positions),
                    len(all_orders), len(all_rights), len(daily))
                try:
                    entitlement_events = []
                    settled_orders = []
                    # Rights are fixed from the already-booked record-close
                    # shares.  Ex date creates economic receivables; payout
                    # and credit transfer those to spendable cash/shares.
                    for item in entitlements:
                        if item["ex"] == date:
                            require_ex_raw(source, item, date)
                            if item["active"]:
                                raise AssertionError("duplicate_ex_event")
                            item["active"] = True
                            cash_receivable += item["cash_due"]
                            if item["bonus_due"]:
                                pending_shares[item["symbol"]] = pending_shares.get(item["symbol"], D(0)) + item["bonus_due"]
                            entitlement_events.append({"phase": "ex", "symbol": item["symbol"],
                                "cash_due": str(item["cash_due"]), "bonus_due": str(item["bonus_due"])})
                        if item["active"] and item["cash_paid"] == date and item["cash_due"]:
                            cash += item["cash_due"]
                            cash_receivable -= item["cash_due"]
                            entitlement_events.append({"phase": "cash_paid", "symbol": item["symbol"], "amount": str(item["cash_due"])})
                        if item["active"] and item["stock_credited"] == date and item["bonus_due"]:
                            positions[item["symbol"]] = positions.get(item["symbol"], D(0)) + item["bonus_due"]
                            pending_shares[item["symbol"]] -= item["bonus_due"]
                            if pending_shares[item["symbol"]] == 0:
                                del pending_shares[item["symbol"]]
                            entitlement_events.append({"phase": "stock_credited", "symbol": item["symbol"], "shares": str(item["bonus_due"])})
                    # A corporate-action row for a currently held stock with
                    # no record-close entitlement is harmless if it was bought
                    # after record.  A pending entitlement with impossible
                    # event dates is not harmless and was checked at record.
                    prices = {}
                    basis_by_symbol = {}
                    suspended = []
                    for symbol in positions:
                        price, basis = source.mark(symbol, date, last.get(symbol))
                        prices[symbol] = price
                        basis_by_symbol[symbol] = basis
                        last[symbol] = price
                        if "suspension" in basis:
                            suspended.append(symbol)
                    suspensions += len(suspended)
                    pending_value = D(0)
                    for s, q in pending_shares.items():
                        pending_price = prices[s] if s in prices else source.mark(s, date)[0]
                        pending_value += q * pending_price
                    equity_before = cash + cash_receivable + pending_value + sum((q * prices[s] for s, q in positions.items()), D(0))
                    fees = D(0)
                    if frozen is not None:
                        blocked = set()
                        why = {}
                        for order in frozen:
                            symbol = order["symbol"]
                            if symbol not in prices:
                                prices[symbol], basis_by_symbol[symbol] = source.mark(symbol, date)
                            rejection = check_paper_trade(source, symbol, date, order["side"], prices[symbol])
                            if rejection:
                                blocked.add(symbol)
                                why[symbol] = rejection
                        result = settle_next_close(positions, cash, frozen, prices, blocked, funding)
                        settled_orders = [{"branch": funding, "signal_date": e["signal_date"],
                            "execution_date": date, "symbol": e["symbol"], "side": e["side"],
                            "frozen_shares": str(e["frozen_shares"]), "signal_raw_price": e["signal_price"],
                            "execution_raw_price": e["price_next"], "booked": e["booked"],
                            "reason": e["reason"], "gross": e["gross"], "fee": e["fee"]}
                            for e in result["events"]]
                        positions, cash = result["positions_after"], result["cash_after"]
                        fees = result["fees"]
                        last = {s: prices[s] for s in positions}
                        trades.append({"execution_date": date, "signal_date": dates[i-1],
                            "frozen_orders": len(frozen), "booked_sells": sum(e["booked"] and e["side"] == "sell" for e in result["events"]),
                            "booked_buys": sum(e["booked"] and e["side"] == "buy" for e in result["events"]),
                            "blocked": why, "fees": str(fees), "member_count": len(positions)})
                        frozen = None
                    equity_after = cash + cash_receivable + pending_value + sum((q * last[s] for s, q in positions.items()), D(0))
                    if abs(equity_before - fees - equity_after) > D("1e-20") or cash < 0:
                        raise AssertionError(f"account_conservation:{date}")
                    # A record-date close can create next-day cash/shares due;
                    # stop before claiming the close state is settled.
                    hit = [r for r in actions.get((date, "record"), []) if r["symbol"].zfill(6) in positions]
                    if hit:
                        for r in hit:
                            symbol, ex = r["symbol"].zfill(6), r["除权日"]
                            if any(x["symbol"] == symbol and x["record"] == date and x["ex"] == ex for x in entitlements):
                                raise ValueError(f"duplicate_record_entitlement:{date}:{symbol}")
                            proof = reviewed.get((symbol, date, ex))
                            if (not proof or proof["status"] != "candidate_complete"
                                    or proof["secondary_values_match"] != "True"
                                    or proof["secondary_fetch_status"] != "ok"
                                    or not ex or ex <= date):
                                raise ValueError(f"unverified_record_entitlement:{date}:{symbol}:ex={ex}")
                            bonus = D(proof["bonus_per_10"]) / D(10) * positions[symbol]
                            payout = D(proof["cash_per_10_gross"]) / D(10) * positions[symbol]
                            paid = proof["cash_paid_date"]
                            credited = proof["stock_credited_date"]
                            if (payout and (not paid or paid < ex)) or (bonus and (not credited or credited < ex)):
                                raise ValueError(f"missing_or_invalid_rights_payment_date:{date}:{symbol}")
                            entitlements.append({"symbol": symbol, "record": date, "ex": ex,
                                "cash_paid": paid, "stock_credited": credited,
                                "cash_due": payout, "bonus_due": bonus, "active": False})
                            entitlement_events.append({"phase": "record", "symbol": symbol,
                                "record_shares": str(positions[symbol]), "cash_due": str(payout), "bonus_due": str(bonus)})
                    if date in by_signal:
                        target = by_signal[date]
                        if len(target) != 100 or len(set(target)) != 100:
                            raise ValueError(f"target_invalid:{date}")
                        price_t = dict(last)
                        for symbol in target:
                            if symbol not in price_t:
                                price_t[symbol] = source.mark(symbol, date)[0]
                        if pending_shares:
                            raise ValueError(f"pending_bonus_shares_at_signal:{date}:{','.join(sorted(pending_shares))}")
                        # Receivable is economic equity but cannot fund orders
                        # until its payment date.  Settlement still uses cash.
                        frozen = freeze_orders(positions, cash + cash_receivable, price_t, target)
                        for order in frozen:
                            order["signal_date"] = date
                            order["signal_price"] = str(price_t[order["symbol"]])
                    daily.append({"branch": funding, "date": date, "cash": str(cash),
                        "stock_value": str(equity_after-cash-cash_receivable-pending_value),
                        "cash_receivable": str(cash_receivable), "pending_share_value": str(pending_value),
                        "equity": str(equity_after),
                        "fees": str(fees), "held_count": len(positions),
                        "suspended_held_count": len(suspended), "frozen_orders_for_next": len(frozen or [])})
                    for symbol, q in sorted(positions.items()):
                        all_positions.append({"branch": funding, "date": date,
                            "state": "settled_shares", "symbol": symbol, "shares": str(q),
                            "valuation_price": str(last[symbol]),
                            "market_value": str(q * last[symbol]),
                            "price_basis": basis_by_symbol.get(symbol, "true_raw_close")})
                    for symbol, q in sorted(pending_shares.items()):
                        px, basis = source.mark(symbol, date)
                        all_positions.append({"branch": funding, "date": date,
                            "state": "pending_bonus_shares", "symbol": symbol, "shares": str(q),
                            "valuation_price": str(px), "market_value": str(q * px),
                            "price_basis": basis})
                    all_orders.extend(settled_orders)
                    if entitlement_events:
                        trades.append({"date": date, "corporate_events": entitlement_events})
                        for event in entitlement_events:
                            all_rights.append({"branch": funding, "date": date,
                                "phase": event["phase"], "symbol": event["symbol"],
                                "record_shares": event.get("record_shares", ""),
                                "cash_due": event.get("cash_due", ""),
                                "bonus_due": event.get("bonus_due", ""),
                                "amount": event.get("amount", ""), "shares": event.get("shares", "")})
                except (ValueError, AssertionError) as exc:
                    (positions, cash, last, entitlements, cash_receivable,
                        pending_shares, frozen, old_trade_count, old_position_count,
                        old_order_count, old_rights_count, old_daily_count) = snapshot
                    trades = trades[:old_trade_count]
                    del all_positions[old_position_count:]
                    del all_orders[old_order_count:]
                    del all_rights[old_rights_count:]
                    del daily[old_daily_count:]
                    first_block = {"date": date, "reason": str(exc)}
                    break
            branches[funding] = {"last_settled_date": daily[-1]["date"] if daily else "2010-02-12",
                "settled_days_after_prior": len(daily), "first_block": first_block,
                "suspension_marks": suspensions, "executed_rebalances": trades,
                "last_cash": str(cash), "last_positions": {s: str(q) for s,q in positions.items()},
                "last_cash_receivable": str(cash_receivable),
                "last_pending_shares": {s: str(q) for s,q in pending_shares.items()},
                "daily": daily}
    finally:
        bs.logout()
        source.save()
    all_daily = [r for b in branches.values() for r in b["daily"]]
    if all_daily:
        with (HERE / "daily_account.csv").open("w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(all_daily[0]))
            w.writeheader()
            w.writerows(all_daily)
    for filename, data in (("daily_positions.csv", all_positions),
                           ("settled_orders.csv", all_orders),
                           ("rights_events.csv", all_rights)):
        if data:
            with (HERE / filename).open("w", encoding="utf-8", newline="") as f:
                w = csv.DictWriter(f, fieldnames=list(data[0]))
                w.writeheader()
                w.writerows(data)
    manifest = {"status": "diagnostic_old_L1_inputs_no_formal_NAV", "capital_start": "1000000",
        "funding_contracts": ["prefunded", "netted_same_close_theoretical"],
        "fractional_shares": True, "adjustment": "raw_unadjusted", "fee_each_side": "0.003",
        "corporate_action_policy": "at record close freeze actual held shares; only candidate_complete CNInfo/Sina matched rows; at ex create gross cash and bonus receivable, later transfer to cash/shares at documented dates; otherwise fail closed",
        "source": "BaoStock anonymous query_history_k_data_plus adjustflag=3; local raw cache; CNInfo historical implementation table",
        "queries": source.queries, "queried_bars_sha256": digest(source.bars_path),
        "detail_sha256": {p.name: digest(p) for p in (HERE / "daily_account.csv", HERE / "daily_positions.csv",
            HERE / "settled_orders.csv", HERE / "rights_events.csv") if p.exists()},
        "input_sha256": {str(p.relative_to(ROOT)).replace('\\','/'): digest(p) for p in
            [FIRST, MEMBERS, CALENDAR, RIGHTS, LEDGER, RESTRUCTURING_EVIDENCE, *EXISTING_BARS, *sorted(source.raw_paths)]},
        "code_sha256": digest(Path(__file__)), "contract_sha256": digest(HERE / "ledger_contract.py"),
        "branches": branches}
    (HERE / "result.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2)+"\n", encoding="utf-8")
    print(json.dumps({k: {"last": v["last_settled_date"], "days": v["settled_days_after_prior"],
        "block": v["first_block"], "rebalances": len(v["executed_rebalances"])}
        for k,v in branches.items()}, ensure_ascii=False))


if __name__ == "__main__":
    main()
