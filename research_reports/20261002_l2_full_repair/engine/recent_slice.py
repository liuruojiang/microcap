"""Independent 2025-09-30 seeded Top100 stock paper-account slice.

The seed is the frozen old effective 100, not the continuous 2010 account.
T close freezes quantities; T+1 close accepts/rejects fixed orders.  This
script never reads the old return or NAV to generate cash or holdings.
"""

from __future__ import annotations

import csv
import json
from decimal import Decimal as D, getcontext
from pathlib import Path

import baostock as bs

from extend_ledger import (ROOT, HERE, MEMBERS, CALENDAR, Source, check_paper_trade,
                           digest, require_ex_raw, rows)
from ledger_contract import freeze_orders, settle_next_close


getcontext().prec = 40
SEED = ROOT / "research_reports/20260926_l1_repair/full_replay/members_by_rebalance.csv"
EVENTS = ROOT / "research_reports/20261002_l2_full_repair/data/event_candidates_2010_20260930.csv"
START = "2025-09-30"
END = "2026-09-30"
CAPITAL = D("1000000")
FEE = D("0.003")
SHARE_CREDIT_OVERRIDE = {("603709", "2026-07-23"): ("2026-07-23",
    "https://money.finance.sina.com.cn/corp/view/vCB_AllBulletinDetail.php?id=12449850&stockid=603709"),
    ("688657", "2026-05-14"): ("2026-05-14", "issuer_notice_verified_by_data_agent"),
    ("688501", "2026-08-06"): ("2026-08-06", "issuer_notice_verified_by_data_agent")}


def write_csv(path: Path, records: list[dict]) -> None:
    if not records:
        return
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(records[0]))
        writer.writeheader()
        writer.writerows(records)


def load_events() -> dict[str, list[dict]]:
    out: dict[str, list[dict]] = {}
    for r in rows(EVENTS):
        if not (START <= r["record_date"] <= END):
            continue
        out.setdefault(r["record_date"], []).append(r)
    return out


def event_contract(r: dict, record_shares: D) -> dict:
    symbol, date, ex = r["symbol"].zfill(6), r["record_date"], r["ex_date"]
    if r["event_type"] == "rights_issue":
        # Conservative no-subscription policy: no purchase, no cash outlay,
        # no free shares.  The dilution remains in the raw close path.  If a
        # transferable/compensable entitlement is evidenced, stop separately.
        if not r["rights_per_10"] or not r["subscription_price"]:
            raise ValueError(f"incomplete_rights_offer:{date}:{symbol}")
        return {"symbol": symbol, "record": date, "ex": ex, "kind": "declined_rights",
            "record_shares": record_shares, "cash_due": D(0), "bonus_due": D(0),
            "cash_paid": "", "stock_credited": "", "active": False,
            "source_status": r["source_status"]}
    if (r["event_type"] != "dividend_or_bonus" or r["source"] != "cninfo"
            or r["source_status"] not in {"two_source_match", "single_source_only"}
            or not ex or ex <= date or not r["announcement_date"]
            or r["announcement_date"] > date):
        raise ValueError(f"unverified_dividend_event:{date}:{symbol}:{r['source_status']}")
    bonus_per = D(r["bonus_per_10"] or "0")
    cash_per = D(r["cash_per_10_gross"] or "0")
    if bonus_per < 0 or cash_per < 0 or bonus_per + cash_per <= 0:
        raise ValueError(f"bad_dividend_amount:{date}:{symbol}")
    paid = r["cash_pay_date"]
    credit = r["share_credit_date"]
    if not credit and bonus_per:
        credit = SHARE_CREDIT_OVERRIDE.get((symbol, ex), ("", ""))[0]
    if (cash_per and (not paid or paid < ex)) or (bonus_per and (not credit or credit < ex)):
        raise ValueError(f"missing_or_invalid_payment_date:{date}:{symbol}")
    return {"symbol": symbol, "record": date, "ex": ex, "kind": "dividend_or_bonus",
        "record_shares": record_shares, "cash_due": record_shares*cash_per/D(10),
        "bonus_due": record_shares*bonus_per/D(10), "cash_paid": paid,
        "stock_credited": credit, "active": False, "source_status": r["source_status"]}


def run_branch(source: Source, funding: str, dates: list[str], seed: list[str],
               by_target: dict[str, list[str]], events_by_record: dict[str, list[dict]]) -> dict:
    positions: dict[str, D] = {}
    cash = CAPITAL
    receivable = D(0)
    pending: dict[str,D] = {}
    entitlements: list[dict] = []
    last: dict[str,D] = {}
    opening_price = {s: source.mark(s, START)[0] for s in seed}
    if len(opening_price) != 100 or any(START not in source.price_map(s) for s in seed):
        raise ValueError("seed_not_100_real_raw_closes")
    frozen = [{"symbol": s, "side": "buy", "frozen_shares": CAPITAL/D(100)/opening_price[s]/(1+FEE),
               "signal_date": START, "signal_price": str(opening_price[s])} for s in seed]
    daily, holdings, orders, rights, trade_summaries = [], [], [], [], []
    block = None
    for i, date in enumerate(dates):
        snapshot = (dict(positions), cash, receivable, dict(pending),
                    [dict(x) for x in entitlements], dict(last), frozen,
                    len(daily), len(holdings), len(orders), len(rights), len(trade_summaries))
        try:
            day_rights = []
            for e in entitlements:
                if e["ex"] == date:
                    require_ex_raw(source, e, date)
                    e["active"] = True
                    if e["kind"] == "dividend_or_bonus":
                        receivable += e["cash_due"]
                        if e["bonus_due"]:
                            pending[e["symbol"]] = pending.get(e["symbol"], D(0)) + e["bonus_due"]
                    day_rights.append(("ex", e))
                if e["active"] and e["kind"] == "dividend_or_bonus":
                    if e["cash_due"] and e["cash_paid"] == date:
                        cash += e["cash_due"]
                        receivable -= e["cash_due"]
                        day_rights.append(("cash_paid", e))
                    if e["bonus_due"] and e["stock_credited"] == date:
                        positions[e["symbol"]] = positions.get(e["symbol"], D(0)) + e["bonus_due"]
                        pending[e["symbol"]] -= e["bonus_due"]
                        if pending[e["symbol"]] == 0:
                            del pending[e["symbol"]]
                        day_rights.append(("stock_credited", e))
            marks, basis = {}, {}
            for s in positions:
                marks[s], basis[s] = source.mark(s, date, last.get(s))
                last[s] = marks[s]
            pending_value = D(0)
            for s,q in pending.items():
                px = marks[s] if s in marks else source.mark(s,date)[0]
                pending_value += q*px
            equity_pre = cash + receivable + pending_value + sum((q*marks[s] for s,q in positions.items()),D(0))
            fees = D(0)
            settled = []
            if frozen:
                blocked, why = set(), {}
                for o in frozen:
                    s = o["symbol"]
                    if s not in marks:
                        marks[s], basis[s] = source.mark(s,date)
                    rejection = check_paper_trade(source,s,date,o["side"],marks[s])
                    if rejection:
                        blocked.add(s)
                        why[s] = rejection
                result = settle_next_close(positions,cash,frozen,marks,blocked,funding)
                positions,cash,fees = result["positions_after"],result["cash_after"],result["fees"]
                last = {s:marks[s] for s in positions}
                settled = result["events"]
                trade_summaries.append({"signal_date": frozen[0]["signal_date"],
                    "execution_date":date,"frozen":len(frozen),
                    "sells":sum(e["booked"] and e["side"]=="sell" for e in settled),
                    "buys":sum(e["booked"] and e["side"]=="buy" for e in settled),
                    "fee":str(fees),"blocked":why,"held_after":len(positions)})
                frozen = None
            equity = cash + receivable + pending_value + sum((q*last[s] for s,q in positions.items()),D(0))
            if cash < 0 or abs(equity_pre-fees-equity)>D("1e-20"):
                raise AssertionError(f"conservation:{date}")
            for r in events_by_record.get(date, []):
                s = r["symbol"].zfill(6)
                if s not in positions:
                    continue
                if any(e["symbol"]==s and e["record"]==date and e["ex"]==r["ex_date"] for e in entitlements):
                    raise ValueError(f"duplicate_event_key:{date}:{s}")
                e = event_contract(r, positions[s])
                entitlements.append(e)
                day_rights.append(("record",e))
            if date in by_target:
                target=by_target[date]
                if len(target)!=100 or len(set(target))!=100 or pending:
                    raise ValueError(f"invalid_target_or_pending_bonus:{date}")
                prices_t = dict(last)
                for s in target:
                    if s not in prices_t:
                        prices_t[s]=source.mark(s,date)[0]
                frozen=freeze_orders(positions,cash+receivable,prices_t,target)
                for o in frozen:
                    o["signal_date"]=date
                    o["signal_price"]=str(prices_t[o["symbol"]])
            prior_equity = CAPITAL if not daily else D(daily[-1]["equity"])
            daily.append({"branch":funding,"date":date,"cash":str(cash),
                "cash_receivable":str(receivable),"pending_share_value":str(pending_value),
                "stock_value":str(equity-cash-receivable-pending_value),"equity":str(equity),
                "normalized_proxy_index":str(equity/CAPITAL*D(1000)),
                "return_net":str(equity/prior_equity-D(1)),
                "return_before_current_day_fee_same_path":str((equity+fees)/prior_equity-D(1)),
                "fees":str(fees),"held_count":len(positions),"pending_symbol_count":len(pending),
                "frozen_next_orders":len(frozen or [])})
            for s,q in sorted(positions.items()):
                holdings.append({"branch":funding,"date":date,"state":"settled_shares",
                    "symbol":s,"shares":str(q),"valuation_price":str(last[s]),
                    "market_value":str(q*last[s]),"price_basis":basis.get(s,"true_raw_close")})
            for s,q in sorted(pending.items()):
                px,b=source.mark(s,date)
                holdings.append({"branch":funding,"date":date,"state":"pending_bonus_shares",
                    "symbol":s,"shares":str(q),"valuation_price":str(px),
                    "market_value":str(q*px),"price_basis":b})
            for e in settled:
                orders.append({"branch":funding,"signal_date":e["signal_date"],
                    "execution_date":date,"symbol":e["symbol"],"side":e["side"],
                    "frozen_shares":str(e["frozen_shares"]),"signal_price":e["signal_price"],
                    "execution_price":e["price_next"],"booked":e["booked"],
                    "reason":e["reason"],"gross":e["gross"],"fee":e["fee"]})
            for phase,e in day_rights:
                rights.append({"branch":funding,"date":date,"phase":phase,"symbol":e["symbol"],
                    "record_date":e["record"],"record_shares":str(e["record_shares"]),
                    "cash_due":str(e["cash_due"]),"bonus_due":str(e["bonus_due"]),
                    "source_status":e["source_status"],"kind":e["kind"]})
        except (ValueError,AssertionError) as exc:
            positions,cash,receivable,pending,entitlements,last,frozen,nd,nh,no,nr,nt=snapshot
            del daily[nd:];del holdings[nh:];del orders[no:];del rights[nr:];del trade_summaries[nt:]
            block={"date":date,"reason":str(exc)}
            break
    write_csv(HERE / f"recent_{funding}_daily.csv",daily)
    write_csv(HERE / f"recent_{funding}_holdings.csv",holdings)
    write_csv(HERE / f"recent_{funding}_orders.csv",orders)
    write_csv(HERE / f"recent_{funding}_rights.csv",rights)
    return {"branch":funding,"last_settled_date":daily[-1]["date"] if daily else START,
        "settled_days":len(daily),"first_block":block,"executions":trade_summaries,
        "last_cash":str(cash),"last_receivable":str(receivable),
        "last_positions":{s:str(q) for s,q in positions.items()},
        "detail_sha256":{p.name:digest(p) for p in (HERE/f"recent_{funding}_{kind}.csv"
            for kind in ("daily","holdings","orders","rights")) if p.exists()}}


def main():
    seed_rows=rows(SEED)
    prior_date=max(r["rebalance_date"] for r in seed_rows if r["rebalance_date"]<=START)
    seed=[r["symbol"].zfill(6) for r in seed_rows if r["rebalance_date"]==prior_date]
    if len(seed)!=100 or len(set(seed))!=100:
        raise ValueError("bad_recent_seed")
    calendar=[r["date"] for r in rows(CALENDAR)]
    dates=[d for d in calendar if START<d<=END]
    if not dates or dates[-1]!=END:
        raise ValueError("recent_calendar_not_fresh_to_end")
    targets={}
    for r in rows(MEMBERS):
        if START<r["rebalance_date"]<=END:
            targets.setdefault(r["rebalance_date"],[]).append(r)
    targets={d:[r["symbol"].zfill(6) for r in sorted(rr,key=lambda x:int(x["rank"]))] for d,rr in targets.items()}
    events_by_record=load_events()
    source=Source()
    login=bs.login()
    if login.error_code!="0":
        raise RuntimeError(f"baostock_login_failed:{login.error_code}:{login.error_msg}")
    branches={}
    try:
        for funding in ("prefunded","netted"):
            branches[funding]=run_branch(source,funding,dates,seed,targets,events_by_record)
    finally:
        bs.logout()
        source.save()
    result={"status":"independent_recent_old_L1_seed_conditional_stock_paper_only",
        "seed_date":START,"old_effective_members_as_of":prior_date,"seed_count":len(seed),
        "end_requested":END,"capital_start":str(CAPITAL),"fractional_shares":True,
        "price_adjustment":"raw_unadjusted","fee_each_side":str(FEE),
        "pre_fee_return_note":"same fee-affected prior holdings and capital, only current-day fee added back; not a separate zero-fee counterfactual",
        "cash_contracts":["prefunded","netted_theoretical_same_close"],
        "source_attempts":source.queries,"source_cache_sha256":digest(HERE/"queried_baostock_bars.csv"),
        "real_trade_raw_supplement_count":len(source.raw_supplements),
        "first_real_trade_raw_supplement":min((d,s) for s,d in source.raw_supplements) if source.raw_supplements else None,
        "raw_supplement_sha256":digest(HERE/"raw_close_supplements.csv") if (HERE/"raw_close_supplements.csv").exists() else None,
        "input_sha256":{str(p.relative_to(ROOT)).replace('\\','/'):digest(p) for p in
            (SEED,MEMBERS,CALENDAR,EVENTS,*sorted(source.raw_paths))},
        "code_sha256":digest(Path(__file__)),"source_module_sha256":digest(HERE/"extend_ledger.py"),
        "contract_sha256":digest(HERE/"ledger_contract.py"),
        "branches":branches}
    (HERE/"recent_result.json").write_text(json.dumps(result,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
    print(json.dumps({k:{"last":v["last_settled_date"],"days":v["settled_days"],
        "block":v["first_block"],"executions":len(v["executions"])} for k,v in branches.items()},ensure_ascii=False))


if __name__=="__main__":
    main()
