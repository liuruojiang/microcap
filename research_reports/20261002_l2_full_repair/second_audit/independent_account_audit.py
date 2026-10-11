"""Read-only ledger audit of the six isolated L2 strategy accounts.

This intentionally does not import or call either replay or prior audit code.
"""

from __future__ import annotations

import csv
import hashlib
import json
import bisect
from collections import defaultdict
from decimal import Decimal as D, getcontext
from pathlib import Path

getcontext().prec = 60
HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
INTEGRATION = HERE.parent / "version_integration"
ENGINE = HERE.parent / "engine"
FEE = D("0.003")
DRAG = D("0.0003")
TOL = D("1e-16")


def rows(path):
    with path.open(encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def close(a, b, label, date):
    if abs(a - b) > TOL:
        raise ValueError(f"{label}:{date}:expected={a}:reported={b}:delta={a-b}")


def relpath(name):
    return ROOT / name


def validate_order_row(order, previous_session):
    assert order["signal_date"] == previous_session, (
        "not_frozen_previous_close", order["signal_date"], previous_session)
    quantity = D(order["frozen_shares"])
    execution_price = D(order["execution_price"])
    assert quantity > 0 and execution_price > 0
    if order["booked"].lower() == "true":
        close(quantity * execution_price, D(order["gross"]), "order_gross", order["execution_date"])
        close(quantity * execution_price * FEE, D(order["fee"]), "order_fee", order["execution_date"])


def audit_one(branch, version):
    stem = f"recent_{branch}_{version.replace('.', '_')}_strategy"
    files = {kind: INTEGRATION / f"{stem}_{kind}.csv"
             for kind in ("daily", "holdings", "orders", "rights", "queried_bars")}
    manifest = json.loads((INTEGRATION / f"{stem}_manifest.json").read_text(encoding="utf-8"))
    signals_file = INTEGRATION / f"recent_{branch}_rebuilt_signals.csv"
    assert manifest["source_signal_sha256"] == sha(signals_file)
    for kind in ("daily", "holdings", "orders", "rights"):
        assert manifest["detail_sha256"][kind] == sha(files[kind]), (stem, kind)
    query = manifest["source_response_files"].get("queried_bars")
    if query:
        assert Path(query["path"]).resolve() == files["queried_bars"].resolve()
        assert query["sha256"] == sha(files["queried_bars"])
        assert query["rows"] == len(rows(files["queried_bars"]))
    input_paths = {
        "baostock_cache": ENGINE / "queried_baostock_bars.csv",
        "event_candidates": HERE.parent / "data" / "event_candidates_2010_20260930.csv",
        "target_members": ROOT / "outputs" / "microcap_top100_mom16_biweekly_live_v2_0_base_proxy_members.csv",
    }
    for key, path in input_paths.items():
        assert manifest["input_sha256"][key] == sha(path), (stem, key)
    code_paths = {
        "engine_extend_ledger": ENGINE / "extend_ledger.py",
        "engine_recent_slice": ENGINE / "recent_slice.py",
        "strategy_replay": INTEGRATION / "strategy_replay.py",
    }
    for key, path in code_paths.items():
        assert manifest["source_code_sha256"][key] == sha(path), (stem, key)

    daily, holdings, orders, rights = [rows(files[x]) for x in ("daily", "holdings", "orders", "rights")]
    signals = rows(signals_file)
    dates = [x["date"] for x in daily]
    assert len(dates) == 241 and dates == sorted(set(dates))
    assert dates == [x["date"] for x in signals]
    assert manifest["settled_days"] == 241 and manifest["last_settled"] == dates[-1]
    assert manifest["first_block"] is None and manifest["first_old_pool_ST_contamination"] == "2026-04-27:600543"
    assert manifest["model"] == "theoretical_zero_upfront_daily_reset_index_swap_with_explicit_margin_debt"
    assert manifest["financing_rate_applied"] == 0
    assert manifest["target_scale_values"] in (["0.0", "1.0"], ["1.0", "0.0"])
    prefix = version.replace(".", "_")
    scale_key = prefix + "_next_session_actionable_scale"
    sig = {x["date"]: x for x in signals}
    daily_by_date = {x["date"]: x for x in daily}
    holdings_by_date_symbol = {(h["date"], h["symbol"]): h for h in holdings}
    target_groups = defaultdict(list)
    for member in rows(input_paths["target_members"]):
        target_groups[member["rebalance_date"]].append(member)
    target_dates = sorted(target_groups)
    targets = {date: {m["symbol"].zfill(6) for m in group}
               for date, group in target_groups.items()}
    assert all(len(group) == 100 for group in targets.values())
    for s in signals:
        if s[scale_key]:
            assert D(s[scale_key]) in {D(0), D(1)}

    hold_by_day = defaultdict(list)
    order_by_day = defaultdict(list)
    right_by_day = defaultdict(list)
    for r in holdings: hold_by_day[r["date"]].append(r)
    for r in orders: order_by_day[r["execution_date"]].append(r)
    for r in rights: right_by_day[r["date"]].append(r)
    cash = D(1000000)
    receivable = D(0)
    margin = D(0)
    positions = defaultdict(D)
    pending = defaultdict(D)
    prior_short = D(0)
    prev_hedge = None
    max_account_residual = D(0)
    fee_total = D(0)
    booked_count = 0
    all_signal_dates = {date: ix for ix, date in enumerate(dates)}
    eq = [D(1000000)]
    for d in daily:
        date = d["date"]
        day_fee = D(0)
        for e in right_by_day[date]:
            q, bonus, due = D(e["record_shares"]), D(e["bonus_due"]), D(e["cash_due"])
            assert q >= 0 and bonus >= 0 and due >= 0
            if e["phase"] == "ex":
                receivable += due
                pending[e["symbol"]] += bonus
            elif e["phase"] == "cash_paid":
                receivable -= due
                cash += due
            elif e["phase"] == "stock_credited":
                pending[e["symbol"]] -= bonus
                positions[e["symbol"]] += bonus
            elif e["phase"] != "record":
                raise ValueError(f"unknown_right_phase:{date}:{e['phase']}")
        hedge = D(sig[date]["hedge_close"])
        hedge_ret = D(0) if prev_hedge is None else hedge / prev_hedge - D(1)
        pnl, drag = -prior_short * hedge_ret, prior_short * DRAG
        margin += pnl - drag
        close(pnl, D(d["swap_pnl"]), "swap_pnl", date)
        close(drag, D(d["swap_drag"]), "swap_drag", date)
        close(hedge_ret, D(d["hedge_return"]), "hedge_return", date)
        close(margin, D(d["swap_margin"]), "swap_margin", date)
        close(max(-margin, D(0)), D(d["swap_debt"]), "swap_debt", date)
        prev_hedge = hedge

        day_orders = order_by_day[date]
        buy_budget = cash
        buying_started = False
        # The signal was frozen at the previous *listed trading close*.
        for o in day_orders:
            prior_ix = all_signal_dates[date] - 1
            assert prior_ix >= 0
            validate_order_row(o, dates[prior_ix])
            assert sig[o["signal_date"]][scale_key] in ("0.0", "1.0", "0", "1")
            assert D(o["frozen_shares"]) > 0 and D(o["signal_price"]) > 0
            assert D(o["execution_price"]) > 0
            # Independently derive the frozen number of shares from T-close
            # equity, raw signal price, current shares, and PIT target list.
            target_ix = bisect.bisect_right(target_dates, o["signal_date"]) - 1
            assert target_ix >= 0
            target = targets[target_dates[target_ix]]
            scale = D(sig[o["signal_date"]][scale_key])
            prior_holding = holdings_by_date_symbol.get((o["signal_date"], o["symbol"]))
            current = D(prior_holding["shares"]) if prior_holding else D(0)
            desired = (D(daily_by_date[o["signal_date"]]["equity"]) / D(100)
                       / D(o["signal_price"]) if scale and o["symbol"] in target else D(0))
            frozen_expected = max(desired-current, D(0)) if o["side"] == "buy" else max(current-desired, D(0))
            close(frozen_expected, D(o["frozen_shares"]), f"frozen_qty_{o['symbol']}", date)
            if o["booked"].lower() == "true":
                gross = D(o["frozen_shares"]) * D(o["execution_price"])
                fee = gross * FEE
                day_fee += fee
                fee_total += fee
                booked_count += 1
                if o["side"] == "buy":
                    buying_started = True
                    assert buy_budget + TOL >= gross + fee, (stem, date, "buy_unfunded")
                    buy_budget -= gross + fee
                    cash -= gross + fee
                    positions[o["symbol"]] += D(o["frozen_shares"])
                elif o["side"] == "sell":
                    assert not buying_started, (stem, date, "sell_after_buy")
                    assert positions[o["symbol"]] >= D(o["frozen_shares"]) - TOL
                    cash += gross - fee
                    if branch == "netted":
                        buy_budget = cash
                    positions[o["symbol"]] -= D(o["frozen_shares"])
                else:
                    raise ValueError(f"bad_side:{date}")
            else:
                assert D(o["gross"]) == D(o["fee"]) == 0
        assert cash >= -TOL, (stem, date, cash)
        close(cash, D(d["cash"]), "stock_cash", date)
        close(receivable, D(d["cash_receivable"]), "cash_receivable", date)
        close(day_fee, D(d["stock_trade_fees"]), "daily_fees", date)
        hv = D(0)
        listed = set()
        for h in hold_by_day[date]:
            s = h["symbol"]
            assert s not in listed
            listed.add(s)
            q = D(h["shares"])
            close(positions[s], q, f"stock_qty_{s}", date)
            value = q * D(h["valuation_price"])
            close(value, D(h["market_value"]), f"stock_value_{s}", date)
            hv += value
        assert {s for s, q in positions.items() if q > TOL} == listed, (stem, date)
        assert int(d["held_count"]) == len(listed)
        # The source field counts attempted execution records, including rejected orders.
        assert int(d["executed_orders"]) == len(day_orders)
        if date != dates[-1]:
            assert int(d["pending_orders"]) == sum(o["signal_date"] == date for o in orders)
        close(hv, D(d["stock_value"]), "stock_value", date)
        pending_value = D(d["pending_share_value"])
        assert pending_value >= 0
        ratio = D("0.8") if version != "v2.5" else D(0)
        next_short = ratio * (hv + pending_value)
        close(next_short, D(d["short_notional_after_close"]), "short_notional", date)
        prior_short = next_short
        expected_eq = cash + receivable + pending_value + hv + margin
        close(expected_eq, D(d["equity"]), "equity_identity", date)
        max_account_residual = max(max_account_residual, abs(expected_eq - D(d["equity"])))
        eq.append(expected_eq)
    assert abs(cash - D(daily[-1]["cash"])) <= TOL
    # Initial 1 is part of drawdown peaks. There are 241 actual returns.
    final_ratio = eq[-1] / eq[0]
    annual = final_ratio ** (D(244) / D(241)) - D(1)
    peak = eq[0]
    mdd = D(0)
    for value in eq[1:]:
        peak = max(peak, value)
        mdd = min(mdd, value / peak - D(1))
    return {"branch": branch, "version": version, "days": len(daily),
            "first_date": dates[0], "last_date": dates[-1],
            "booked_orders": booked_count, "fees": str(fee_total),
            "ending_equity": str(eq[-1]), "conditional_annualized": str(annual),
            "conditional_max_drawdown": str(mdd),
            "max_identity_residual": str(max_account_residual),
            "sha_valid": True, "target_scale_0_1": True}


def mutation_checks():
    """Mutate an actual booked order; independent guards must reject it."""
    path = INTEGRATION / "recent_netted_v2_0_strategy_orders.csv"
    original = next(r for r in rows(path) if r["booked"].lower() == "true" and r["side"] == "buy")
    prior = original["signal_date"]
    validate_order_row(original, prior)
    early_holding = dict(original, signal_date=original["execution_date"])
    fee_omitted = dict(original, fee="0")
    caught = {}
    for name, bad in (("same_day_holding_shift", early_holding), ("fee_omission", fee_omitted)):
        try:
            validate_order_row(bad, prior)
        except (AssertionError, ValueError) as exc:
            caught[name] = str(exc)
        else:
            raise AssertionError(f"mutation_not_detected:{name}")
    return {"source_order": {k: original[k] for k in
                              ("signal_date", "execution_date", "symbol", "side", "frozen_shares", "execution_price", "fee")},
            "rejected_mutations": caught}


def main():
    results = [audit_one(branch, version)
               for branch in ("prefunded", "netted")
               for version in ("v2.0", "v2.3", "v2.5")]
    report = {"label": "independent_read_only_conditional_account_audit",
              "results": results, "mutation_checks": mutation_checks(),
              "certification_blocker": "2026-04-27 600543 old target ST contamination; seeded slice, theoretical swap, not continuous formal NAV"}
    (HERE / "independent_account_audit.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
