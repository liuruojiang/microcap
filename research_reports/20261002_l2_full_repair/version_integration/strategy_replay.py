"""Isolated recent strategy account: stock lots plus a theoretical index swap.

This consumes newly rebuilt signals, never the old official next_holding.
The swap is a zero-upfront, daily-reset paper contract.  Negative variation
margin is shown as debt; its financing rate is zero to match the disabled
formal financing switch, with a 3% shadow charge separately disclosed.
"""

from __future__ import annotations

import argparse
import bisect
import csv
import json
import sys
from decimal import Decimal as D, getcontext
from pathlib import Path

import baostock as bs
import pandas as pd

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
ENGINE = ROOT / "research_reports/20261002_l2_full_repair/engine"
sys.path.insert(0, str(ENGINE))

from extend_ledger import CACHED_BARS, MEMBERS, Source, check_paper_trade, digest, require_ex_raw, rows  # noqa: E402
from recent_slice import CAPITAL, EVENTS, FEE, event_contract, load_events  # noqa: E402

getcontext().prec = 40
ACTIVE_VERSIONS = {"v2.0": D("0.8"), "v2.3": D("0.8"), "v2.5": D(0)}
FINANCING_SHADOW_ANNUAL = D("0.03")
TRADING_DAYS = D(244)
SWAP_DRAG_PER_NOTIONAL_DAY = D("0.0003")


def write_csv(path: Path, records: list[dict]) -> None:
    if not records:
        return
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(records[0]))
        writer.writeheader()
        writer.writerows(records)


def mark_checked(source: Source, symbol: str, date: str, last: D | None = None):
    try:
        return source.mark(symbol,date,last)
    except ValueError as exc:
        if "unverified_missing_raw_close" not in str(exc):
            raise
        bar=source.bars.get((symbol,date))
        previous=source.price_map(symbol).get(source.previous_market_day.get(date,""))
        raise ValueError(f"{exc}:last={last}:previous_market_raw={previous}:bar={bar}") from exc


def target_map() -> tuple[list[str], dict[str, list[str]]]:
    by_date: dict[str, list[dict[str, str]]] = {}
    for row in rows(MEMBERS):
        by_date.setdefault(row["rebalance_date"], []).append(row)
    targets = {date: [r["symbol"].zfill(6) for r in sorted(group, key=lambda x: int(x["rank"]))]
               for date, group in by_date.items()}
    if any(len(v) != 100 or len(set(v)) != 100 for v in targets.values()):
        raise ValueError("bad_target_members")
    return sorted(targets), targets


def orders_to_target(positions: dict[str, D], marks: dict[str, D],
                     target: list[str], total_equity: D, scale: D, date: str) -> list[dict]:
    if total_equity <= 0:
        raise ValueError(f"nonpositive_equity_at_signal:{date}")
    required = set(positions) | set(target)
    if any(s not in marks or marks[s] <= 0 for s in required):
        raise ValueError(f"missing_signal_raw_mark:{date}")
    if scale<=0 or scale>D("1.5"):
        raise ValueError(f"invalid_target_scale:{date}:{scale}")
    desired = {s: total_equity*scale/D(100)/marks[s] for s in target}
    orders = []
    for s, q in sorted(positions.items()):
        if q > desired.get(s, D(0)):
            orders.append({"symbol": s, "side": "sell", "frozen_shares": q-desired.get(s,D(0)),
                           "signal_date": date, "signal_price": str(marks[s])})
    for s in target:
        if desired[s] > positions.get(s, D(0)):
            orders.append({"symbol": s, "side": "buy", "frozen_shares": desired[s]-positions.get(s,D(0)),
                           "signal_date": date, "signal_price": str(marks[s])})
    return orders


def liquidate_orders(positions: dict[str, D], marks: dict[str, D], date: str) -> list[dict]:
    return [{"symbol":s,"side":"sell","frozen_shares":q,"signal_date":date,
             "signal_price":str(marks[s])} for s,q in sorted(positions.items()) if q>0]


def settle_orders(positions: dict[str,D], cash: D, orders: list[dict],
                  source: Source, date: str, marks: dict[str,D],
                  funding: str) -> tuple[dict[str,D],D,D,list[dict]]:
    """Book frozen lots; prefunded buys cannot use same-close sale proceeds."""
    if funding not in {"prefunded", "netted"} or cash < 0:
        raise ValueError(f"bad_stock_cash_or_funding:{date}")
    held = dict(positions)
    fee_sum = D(0)
    events = []
    buy_budget = cash
    before = cash + sum((q*marks[s] for s,q in held.items()),D(0))
    for side in ("sell", "buy"):
        for order in (x for x in orders if x["side"] == side):
            s, q = order["symbol"], order["frozen_shares"]
            px = marks[s]
            reason = check_paper_trade(source,s,date,side,px)
            if not reason and side == "sell" and held.get(s,D(0)) < q:
                reason = "shares_unavailable"
            if not reason and side == "buy" and s not in held and len(held)>=100:
                reason = "member_capacity"
            gross, fee = q*px, q*px*FEE
            if not reason and side == "buy" and gross+fee > buy_budget:
                reason = ("insufficient_prefunded_cash" if funding == "prefunded"
                          else "insufficient_netted_cash")
            if not reason:
                if side == "sell":
                    held[s] -= q
                    if held[s] == 0:
                        del held[s]
                    cash += gross-fee
                    if funding == "netted":
                        buy_budget = cash
                else:
                    held[s] = held.get(s,D(0))+q
                    cash -= gross+fee
                    buy_budget -= gross+fee
                fee_sum += fee
            if cash < 0 or buy_budget < 0:
                raise AssertionError(f"stock_cash_negative:{date}")
            events.append({"signal_date":order["signal_date"],"execution_date":date,
                           "symbol":s,"side":side,"frozen_shares":str(q),
                           "signal_price":order["signal_price"],"execution_price":str(px),
                           "booked":not bool(reason),"reason":reason or "", 
                           "gross":str(gross if not reason else D(0)),
                           "fee":str(fee if not reason else D(0))})
    after = cash + sum((q*marks[s] for s,q in held.items()),D(0))
    if abs(before-fee_sum-after)>D("1e-20"):
        raise AssertionError(f"stock_order_conservation:{date}")
    return held,cash,fee_sum,events


def run_account(branch: str, version: str, source: Source) -> dict:
    signal_file = HERE / f"recent_{branch}_rebuilt_signals.csv"
    signal = pd.read_csv(signal_file,dtype={"date":str})
    if signal.date.duplicated().any() or signal.date.tolist() != sorted(signal.date.tolist()):
        raise ValueError("bad_signal_calendar")
    scale_col=version.replace('.','_')+"_next_session_actionable_scale"
    scales={r.date:(None if pd.isna(getattr(r,scale_col)) else D(str(getattr(r,scale_col))))
            for r in signal.itertuples(index=False)}
    if any(x is not None and x not in {D(0),D(1)} for x in scales.values()):
        raise ValueError(f"unexpected_formal_scale_values:{version}")
    if any((r.date in scales and scales[r.date] is not None and
            (scales[r.date]>0)!=(str(getattr(r,version.replace('.','_')+"_signal_at_close")).lower()=="true"))
           for r in signal.itertuples(index=False)):
        raise ValueError(f"signal_scale_mismatch:{version}")
    hedge_close = {r.date:D(str(r.hedge_close)) for r in signal.itertuples(index=False)}
    dates = signal.date.tolist()
    target_dates, targets = target_map()
    events_by_record = load_events()
    ratio = ACTIVE_VERSIONS[version]
    positions: dict[str,D] = {}
    pending_shares: dict[str,D] = {}
    entitlements: list[dict] = []
    last_mark: dict[str,D] = {}
    cash, cash_receivable, swap_margin = CAPITAL, D(0), D(0)
    short_notional = D(0)
    frozen: list[dict] | None = None
    previous_hedge: D | None = None
    previous_scale = D(0)
    daily,holdings,orders,rights = [],[],[],[]
    first_block = None
    for date in dates:
        try:
            day_rights=[]
            for e in entitlements:
                if e["ex"]==date:
                    require_ex_raw(source,e,date)
                    e["active"]=True
                    if e["kind"]=="dividend_or_bonus":
                        cash_receivable += e["cash_due"]
                        if e["bonus_due"]:
                            s=e["symbol"]
                            pending_shares[s]=pending_shares.get(s,D(0))+e["bonus_due"]
                    day_rights.append(("ex",e))
                if e["active"] and e["kind"]=="dividend_or_bonus":
                    if e["cash_due"] and e["cash_paid"]==date:
                        cash+=e["cash_due"]
                        cash_receivable-=e["cash_due"]
                        day_rights.append(("cash_paid",e))
                    if e["bonus_due"] and e["stock_credited"]==date:
                        s=e["symbol"]
                        positions[s]=positions.get(s,D(0))+e["bonus_due"]
                        pending_shares[s]-=e["bonus_due"]
                        if pending_shares[s]==0:
                            del pending_shares[s]
                        day_rights.append(("stock_credited",e))
            marks,basis={},{}
            for s in positions:
                marks[s],basis[s]=mark_checked(source,s,date,last_mark.get(s))
                last_mark[s]=marks[s]
            for s in pending_shares:
                if s not in marks:
                    marks[s],basis[s]=mark_checked(source,s,date,last_mark.get(s))
                    last_mark[s]=marks[s]
            pending_value=sum((q*marks[s] for s,q in pending_shares.items()),D(0))
            stock_before=sum((q*marks[s] for s,q in positions.items()),D(0))
            if previous_hedge is None:
                hedge_ret=D(0)
            else:
                hedge_ret=hedge_close[date]/previous_hedge-D(1)
            swap_pnl=-short_notional*hedge_ret
            swap_drag=short_notional*SWAP_DRAG_PER_NOTIONAL_DAY
            shadow_financing=max(-swap_margin,D(0))*FINANCING_SHADOW_ANNUAL/TRADING_DAYS
            swap_margin+=swap_pnl-swap_drag
            before_trade_equity=cash+cash_receivable+pending_value+stock_before+swap_margin
            fees=D(0)
            executed=[]
            if frozen:
                for o in frozen:
                    s=o["symbol"]
                    if s not in marks:
                        marks[s],basis[s]=mark_checked(source,s,date)
                positions,cash,fees,executed=settle_orders(positions,cash,frozen,source,date,marks,branch)
                for s in positions:
                    last_mark[s]=marks[s]
                frozen=None
            stock_after=sum((q*marks[s] for s,q in positions.items()),D(0))
            equity=cash+cash_receivable+pending_value+stock_after+swap_margin
            if abs(before_trade_equity-fees-equity)>D("1e-20") or equity<=0:
                raise AssertionError(f"portfolio_conservation_or_bankruptcy:{date}")
            for r in events_by_record.get(date,[]):
                s=r["symbol"].zfill(6)
                if s not in positions:
                    continue
                if any(e["symbol"]==s and e["record"]==date and e["ex"]==r["ex_date"] for e in entitlements):
                    raise ValueError(f"duplicate_event_key:{date}:{s}")
                e=event_contract(r,positions[s])
                entitlements.append(e)
                day_rights.append(("record",e))
            # The swap is reset at this close on actually held stock value.
            # Residual blocked shares remain hedged until they can be sold.
            short_notional=ratio*(stock_after+pending_value)
            previous_hedge=hedge_close[date]
            target_scale=scales[date]
            if target_scale is not None:
                decision=target_scale>0
                target_day=target_dates[bisect.bisect_right(target_dates,date)-1]
                desired=targets[target_day]
                if decision:
                    if not positions or target_scale!=previous_scale or date in targets:
                        if pending_shares:
                            raise ValueError(f"pending_bonus_at_rebalance:{date}")
                        for s in desired:
                            if s not in marks:
                                marks[s],basis[s]=mark_checked(source,s,date)
                        frozen=orders_to_target(positions,marks,desired,equity,target_scale,date)
                elif positions:
                    frozen=liquidate_orders(positions,marks,date)
                previous_scale=target_scale
            daily.append({"branch":branch,"version":version,"date":date,
                          "signal_at_close":"" if target_scale is None else target_scale>0,
                          "formal_next_scale":"" if target_scale is None else str(target_scale),
                          "cash":str(cash),"cash_receivable":str(cash_receivable),
                          "pending_share_value":str(pending_value),"stock_value":str(stock_after),
                          "swap_margin":str(swap_margin),"swap_debt":str(max(-swap_margin,D(0))),
                          "swap_pnl":str(swap_pnl),"swap_drag":str(swap_drag),
                          "shadow_financing_3pct":str(shadow_financing),
                          "short_notional_after_close":str(short_notional),
                          "hedge_return":str(hedge_ret),"stock_trade_fees":str(fees),
                          "equity":str(equity),"held_count":len(positions),
                          "pending_orders":len(frozen or []),"executed_orders":len(executed)})
            for s,q in sorted(positions.items()):
                holdings.append({"branch":branch,"version":version,"date":date,
                                 "symbol":s,"shares":str(q),"valuation_price":str(marks[s]),
                                 "market_value":str(q*marks[s]),"price_basis":basis.get(s,"true_raw_close")})
            for e in executed:
                orders.append({"branch":branch,"version":version,**e})
            for phase,e in day_rights:
                rights.append({"branch":branch,"version":version,"date":date,"phase":phase,
                               "symbol":e["symbol"],"record_date":e["record"],
                               "record_shares":str(e["record_shares"]),
                               "cash_due":str(e["cash_due"]),"bonus_due":str(e["bonus_due"]),
                               "source_status":e["source_status"],"kind":e["kind"]})
        except (ValueError,AssertionError) as exc:
            first_block={"date":date,"reason":str(exc)}
            break
    prefix=f"recent_{branch}_{version.replace('.','_')}_strategy"
    paths={"daily":HERE/f"{prefix}_daily.csv","holdings":HERE/f"{prefix}_holdings.csv",
           "orders":HERE/f"{prefix}_orders.csv","rights":HERE/f"{prefix}_rights.csv"}
    for kind,path in paths.items():
        write_csv(path,{"daily":daily,"holdings":holdings,"orders":orders,"rights":rights}[kind])
    return {"branch":branch,"version":version,"last_settled":daily[-1]["date"] if daily else None,
            "settled_days":len(daily),"first_block":first_block,
            "model":"theoretical_zero_upfront_daily_reset_index_swap_with_explicit_margin_debt",
            "target_scale_source":"new_proxy_formal_version_next_session_actionable_scale_T_close",
            "target_scale_values":sorted({r["formal_next_scale"] for r in daily if r["formal_next_scale"]!=""}),
            "strategy_stock_funding_contract":branch,
            "first_negative_stock_cash":next((r["date"] for r in daily if D(r["cash"])<0),None),
            "min_stock_cash":str(min((D(r["cash"]) for r in daily),default=CAPITAL)),
            "first_swap_margin_debt":next((r["date"] for r in daily if D(r["swap_debt"])>0),None),
            "max_swap_margin_debt":str(max((D(r["swap_debt"]) for r in daily),default=D(0))),
            "cumulative_shadow_financing_3pct":str(sum((D(r["shadow_financing_3pct"]) for r in daily),D(0))),
            "first_old_pool_ST_contamination":"2026-04-27:600543",
            "first_actual_600543_held_after_ST":next((r["date"] for r in holdings
                if r["symbol"]=="600543" and r["date"]>="2026-04-27"),None),
            "result_label":"old_L1_pool_conditional_diagnostic_not_certified",
            "hedge_notional_policy":"0.8_x_actual_stock_plus_pending_rights_value_at_each_close",
            "financing_rate_applied":0,"shadow_financing_annual":0.03,
            "stock_fee_each_booked_side":str(FEE),
            "source_signal_sha256":digest(signal_file),
            "detail_sha256":{kind:digest(path) for kind,path in paths.items() if path.exists()}}


def main() -> None:
    parser=argparse.ArgumentParser()
    parser.add_argument("--branch",choices=["prefunded","netted"],required=True)
    parser.add_argument("--version",choices=list(ACTIVE_VERSIONS),required=True)
    args=parser.parse_args()
    source=Source()
    login=bs.login()
    if login.error_code!="0":
        raise RuntimeError(f"baostock_login_failed:{login.error_code}:{login.error_msg}")
    try:
        result=run_account(args.branch,args.version,source)
    finally:
        bs.logout()
    result["source_queries"]=source.queries
    stem=f"recent_{args.branch}_{args.version.replace('.','_')}_strategy"
    queried_ranges={(x["symbol"],x["start_date"],x["end_date"]) for x in source.queries}
    queried_bars=[r for (symbol,date),r in sorted(source.bars.items())
                  if any(s==symbol and start<=date<=end for s,start,end in queried_ranges)]
    response_files={}
    if queried_bars:
        p=HERE/f"{stem}_queried_bars.csv"
        write_csv(p,queried_bars)
        response_files["queried_bars"]={"path":str(p),"sha256":digest(p),"rows":len(queried_bars)}
    if source.raw_supplements:
        p=HERE/f"{stem}_raw_close_supplements.csv"
        write_csv(p,[r for _,r in sorted(source.raw_supplements.items())])
        response_files["raw_close_supplements"]={"path":str(p),"sha256":digest(p),
                                                  "rows":len(source.raw_supplements)}
    result["source_response_files"]=response_files
    result["input_sha256"]={"baostock_cache":digest(CACHED_BARS),
                             "event_candidates":digest(EVENTS),"target_members":digest(MEMBERS)}
    result["source_code_sha256"]={"engine_extend_ledger":digest(ENGINE/"extend_ledger.py"),
                                   "engine_recent_slice":digest(ENGINE/"recent_slice.py"),
                                   "strategy_replay":digest(Path(__file__))}
    output=HERE/f"recent_{args.branch}_{args.version.replace('.','_')}_strategy_manifest.json"
    output.write_text(json.dumps(result,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
    print(json.dumps({k:v for k,v in result.items() if k not in ("source_queries","detail_sha256")},ensure_ascii=False))


if __name__=="__main__":
    main()
