"""Independently recompute daily totals and the 003017 cash entitlement."""

import csv
import hashlib
import json
from collections import defaultdict
from decimal import Decimal as D, getcontext
from pathlib import Path


HERE = Path(__file__).resolve().parent
CAPITAL = D("1000000")
getcontext().prec = 40


def rows(name):
    with (HERE/name).open(encoding="utf-8-sig",newline="") as f:
        return list(csv.DictReader(f))


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    run=json.loads((HERE/"recent_result.json").read_text(encoding="utf-8"))
    assert run["code_sha256"]==sha(HERE/"recent_slice.py")
    assert run["source_module_sha256"]==sha(HERE/"extend_ledger.py")
    assert run["contract_sha256"]==sha(HERE/"ledger_contract.py")
    assert run["raw_supplement_sha256"]==sha(HERE/"raw_close_supplements.csv")
    results={}
    for funding in ("prefunded","netted"):
        b=run["branches"][funding]
        assert b["last_settled_date"]=="2026-09-30" and b["first_block"] is None
        for name,digest in b["detail_sha256"].items():
            assert digest==sha(HERE/name),name
        daily=rows(f"recent_{funding}_daily.csv")
        holdings=rows(f"recent_{funding}_holdings.csv")
        orders=rows(f"recent_{funding}_orders.csv")
        rights=rows(f"recent_{funding}_rights.csv")
        by_day=defaultdict(list)
        fees_by_day=defaultdict(lambda:D(0))
        for h in holdings:
            by_day[h["date"]].append(h)
            assert D(h["shares"])>0
            assert D(h["shares"])*D(h["valuation_price"])==D(h["market_value"])
        for o in orders:
            assert o["signal_date"]<o["execution_date"]
            assert D(o["frozen_shares"])>0 and D(o["signal_price"])>0
            if o["booked"]=="True":
                assert not o["reason"]
                assert D(o["gross"])==D(o["frozen_shares"])*D(o["execution_price"])
                assert D(o["fee"])==D(o["gross"])*D("0.003")
                fees_by_day[o["execution_date"]]+=D(o["fee"])
            else:
                assert o["reason"] and D(o["gross"])==0 and D(o["fee"])==0
        prior=CAPITAL
        worst=D(0)
        for d in daily:
            date=d["date"]
            stock=sum((D(h["market_value"]) for h in by_day[date] if h["state"]=="settled_shares"),D(0))
            pending=sum((D(h["market_value"]) for h in by_day[date] if h["state"]=="pending_bonus_shares"),D(0))
            residual=D(d["equity"])-D(d["cash"])-D(d["cash_receivable"])-stock-pending
            worst=max(worst,abs(residual))
            assert abs(residual)<D("1e-20"),(funding,date,residual)
            assert abs(D(d["fees"])-fees_by_day[date])<D("1e-20")
            assert abs(D(d["return_net"])-(D(d["equity"])/prior-1))<D("1e-20")
            assert abs(D(d["return_before_current_day_fee_same_path"])-((D(d["equity"])+D(d["fees"]))/prior-1))<D("1e-20")
            prior=D(d["equity"])
        rec=[r for r in rights if r["symbol"]=="003017" and r["date"]=="2026-09-29" and r["phase"]=="record"]
        entitlement=D(0)
        if rec:
            assert len(rec)==1
            entitlement=D(rec[0]["record_shares"])*D("0.30")
            assert abs(entitlement-D(rec[0]["cash_due"]))<D("1e-20")
            ex=[r for r in rights if r["symbol"]=="003017" and r["date"]=="2026-09-30"]
            assert {r["phase"] for r in ex}=={"ex","cash_paid"}
        results[funding]={"days":len(daily),"holding_rows":len(holdings),"order_rows":len(orders),
            "rights_rows":len(rights),"max_account_residual":str(worst),
            "003017_record_shares":rec[0]["record_shares"] if rec else "0",
            "003017_gross_cash":str(entitlement),"end_equity":daily[-1]["equity"]}
    result={"status":"PASS_conditional_paper_account_arithmetic_only",
        "source_result_sha256":sha(HERE/"recent_result.json"),"audit_code_sha256":sha(Path(__file__)),
        "branches":results}
    (HERE/"recent_audit.json").write_text(json.dumps(result,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
    print(json.dumps(result,ensure_ascii=False))


if __name__=="__main__":
    main()
