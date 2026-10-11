"""Independent CSV-level identity, fill-clock, and lineage audit of six ledgers."""

from __future__ import annotations

import csv
import json
from decimal import Decimal as D, getcontext
from pathlib import Path

from rebuild_recent_signals import HERE, sha256

ROOT = HERE.parents[2]
ENGINE = ROOT / "research_reports/20261002_l2_full_repair/engine"
EVENTS = ROOT / "research_reports/20261002_l2_full_repair/data/event_candidates_2010_20260930.csv"
MEMBERS = ROOT / "outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_members.csv"
BARS = ENGINE / "queried_baostock_bars.csv"
getcontext().prec = 60


def rows(path: Path) -> list[dict[str,str]]:
    with path.open(encoding="utf-8-sig",newline="") as f:
        return list(csv.DictReader(f))


def audit_one(branch: str, version: str) -> dict:
    stem=f"recent_{branch}_{version.replace('.','_')}_strategy"
    paths={kind:HERE/f"{stem}_{kind}.csv" for kind in ("daily","holdings","orders","rights")}
    manifest_path=HERE/f"{stem}_manifest.json"
    manifest=json.loads(manifest_path.read_text(encoding="utf-8"))
    daily=rows(paths["daily"])
    holdings=rows(paths["holdings"])
    orders=rows(paths["orders"])
    rights=rows(paths["rights"]) if paths["rights"].exists() else []
    if len(daily)!=241 or daily[-1]["date"]!="2026-09-30":
        raise AssertionError(f"incomplete_ledger:{branch}:{version}")
    idx={r["date"]:i for i,r in enumerate(daily)}
    hold_by_date:dict[str,list[dict[str,str]]]={}
    for r in holdings:
        hold_by_date.setdefault(r["date"],[]).append(r)
    orders_by_date:dict[str,list[dict[str,str]]]={}
    for r in orders:
        orders_by_date.setdefault(r["execution_date"],[]).append(r)
    rights_paid:dict[str,D]={}
    for r in rights:
        if r["phase"]=="cash_paid":
            rights_paid[r["date"]]=rights_paid.get(r["date"],D(0))+D(r["cash_due"])
    first_stock_ST=None
    for i,r in enumerate(daily):
        date=r["date"]
        cash=D(r["cash"]); margin=D(r["swap_margin"])
        stock=D(r["stock_value"]); pending=D(r["pending_share_value"])
        receivable=D(r["cash_receivable"]); equity=D(r["equity"])
        if cash<0 or D(r["swap_debt"])!=max(-margin,D(0)):
            raise AssertionError(f"cash_or_debt:{branch}:{version}:{date}")
        if abs(cash+margin+stock+pending+receivable-equity)>D("1e-20"):
            raise AssertionError(f"equity_decomposition:{branch}:{version}:{date}")
        lots=hold_by_date.get(date,[])
        if len(lots)!=int(r["held_count"]) or len({x["symbol"] for x in lots})!=len(lots):
            raise AssertionError(f"holdings_count:{branch}:{version}:{date}")
        if abs(sum((D(x["market_value"]) for x in lots),D(0))-stock)>D("1e-20"):
            raise AssertionError(f"stock_value:{branch}:{version}:{date}")
        if not first_stock_ST and date>="2026-04-27" and any(x["symbol"]=="600543" for x in lots):
            first_stock_ST=date
        booked=orders_by_date.get(date,[])
        fee=sum((D(x["fee"]) for x in booked),D(0))
        if abs(fee-D(r["stock_trade_fees"]))>D("1e-20"):
            raise AssertionError(f"fees:{branch}:{version}:{date}")
        trade_cash=D(0)
        for x in booked:
            sign=D(1) if x["side"]=="sell" else D(-1)
            if x["booked"].lower()=="true":
                if abs(D(x["gross"])-D(x["frozen_shares"])*D(x["execution_price"]))>D("1e-20"):
                    raise AssertionError(f"frozen_fill_arithmetic:{branch}:{version}:{date}")
                if abs(D(x["fee"])-D(x["gross"])*D("0.003"))>D("1e-20"):
                    raise AssertionError(f"side_fee:{branch}:{version}:{date}")
                trade_cash+=sign*D(x["gross"])-D(x["fee"])
            elif D(x["gross"]) or D(x["fee"]):
                raise AssertionError(f"rejected_order_cash:{branch}:{version}:{date}")
            signal_pos=idx.get(x["signal_date"])
            if signal_pos is None or signal_pos+1!=i:
                raise AssertionError(f"not_next_close:{branch}:{version}:{date}:{x['signal_date']}")
        previous_cash=D(daily[i-1]["cash"]) if i else D(1_000_000)
        if abs(cash-previous_cash-trade_cash-rights_paid.get(date,D(0)))>D("1e-20"):
            raise AssertionError(f"stock_cash_flow:{branch}:{version}:{date}")
        previous_margin=D(daily[i-1]["swap_margin"]) if i else D(0)
        if abs(margin-previous_margin-D(r["swap_pnl"])+D(r["swap_drag"]))>D("1e-20"):
            raise AssertionError(f"swap_margin_flow:{branch}:{version}:{date}")
        previous_short=D(daily[i-1]["short_notional_after_close"]) if i else D(0)
        if abs(D(r["swap_pnl"])+previous_short*D(r["hedge_return"]))>D("1e-20"):
            raise AssertionError(f"swap_pnl:{branch}:{version}:{date}")
        if abs(D(r["swap_drag"])-previous_short*D("0.0003"))>D("1e-20"):
            raise AssertionError(f"swap_drag:{branch}:{version}:{date}")
        expected_short=(D("0.8") if version!="v2.5" else D(0))*(stock+pending)
        if abs(D(r["short_notional_after_close"])-expected_short)>D("1e-20"):
            raise AssertionError(f"short_reset:{branch}:{version}:{date}")
    source_signal=HERE/f"recent_{branch}_rebuilt_signals.csv"
    if manifest["source_signal_sha256"]!=sha256(source_signal):
        raise AssertionError(f"signal_sha:{branch}:{version}")
    current_inputs={"baostock_cache":sha256(BARS),"event_candidates":sha256(EVENTS),
                    "target_members":sha256(MEMBERS)}
    if manifest["input_sha256"]!=current_inputs:
        raise AssertionError(f"input_sha:{branch}:{version}:{manifest['input_sha256']}:{current_inputs}")
    for kind,path in paths.items():
        if manifest["detail_sha256"].get(kind)!=sha256(path):
            raise AssertionError(f"detail_sha:{branch}:{version}:{kind}")
    code_current={"engine_extend_ledger":sha256(ENGINE/"extend_ledger.py"),
                  "engine_recent_slice":sha256(ENGINE/"recent_slice.py"),
                  "strategy_replay":sha256(HERE/"strategy_replay.py")}
    if manifest["source_code_sha256"]!=code_current:
        raise AssertionError(f"code_sha:{branch}:{version}")
    for kind,item in manifest.get("source_response_files",{}).items():
        if sha256(Path(item["path"]))!=item["sha256"]:
            raise AssertionError(f"source_response_sha:{branch}:{version}:{kind}")
    return {"branch":branch,"version":version,"sessions":len(daily),
            "first_actual_600543_held_after_ST":first_stock_ST,
            "max_swap_debt":str(max((D(r["swap_debt"]) for r in daily),default=D(0))),
            "booked_orders":sum(x["booked"].lower()=="true" for x in orders),
            "rejected_orders":sum(x["booked"].lower()!="true" for x in orders),
            "stock_fees":str(sum((D(r["stock_trade_fees"]) for r in daily),D(0))),
            "swap_drag":str(sum((D(r["swap_drag"]) for r in daily),D(0))),
            "end_equity":daily[-1]["equity"],"input_sha256":current_inputs,
            "manifest_sha256":sha256(manifest_path)}


def main() -> None:
    result={"accounts":[audit_one(branch,version) for branch in ("prefunded","netted")
                        for version in ("v2.0","v2.3","v2.5")],
            "label":"old_L1_pool_conditional_diagnostic_not_certified"}
    output=HERE/"strategy_accounts_audit.json"
    output.write_text(json.dumps(result,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
    print(json.dumps(result,ensure_ascii=False,indent=2))


if __name__=="__main__":
    main()
