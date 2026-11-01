"""Real 002072 progress reports must not release an active risk warning."""
import json
from pathlib import Path
import pandas as pd
from scripts.st_notice_evidence import load_verified_events
import microcap_top100_mom16_biweekly_live_v2_0 as v
import analyze_top100_rebalance_frequency as standalone
ROOT=Path(__file__).resolve().parents[1]
def test_real_progress_is_no_exit_for_both_formal_entrypoints():
    event=next(e for e in json.loads((ROOT/"data/st_notice_evidence/registry.json").read_text(encoding="utf-8"))["events"] if e["announcement_id"]=="AN201203010004601977")
    assert event["risk_actions"]=={}
    for module in (v.freq_mod,standalone):
        assert module.classify_st_notice_risk_actions(event["title"])=={}
        rows=pd.DataFrame([dict(notice_date="2010-04-28",title="关于公司股票交易实行退市风险警示的提示性公告"),dict(notice_date="2010-05-05",title=event["title"]),dict(notice_date="2011-06-02",title="关于股票交易撤销退市风险警示并实行其他特别处理的提示性公告"),dict(notice_date="2013-05-22",title="关于撤销股票交易其他风险警示的公告")])
        withdrawal=next(e for e in json.loads((ROOT/"data/st_notice_evidence/registry.json").read_text(encoding="utf-8"))["events"] if e["announcement_id"]=="AN201203020004841452")
        for index, item in [(1,event),(2,withdrawal)]:
            for key in ("announcement_id","notice_date","title","source_url","source_provider"):
                rows.loc[index,key]=item[key]
            rows.loc[index,"issuer_symbol"]="002072"
        verified=load_verified_events(ROOT,"002072","2010-01-01","2013-05-23",rows)
        intervals=module.build_st_intervals_from_notices(pd.Timestamp("2010-01-01"),pd.Timestamp("2013-05-23"),rows,verified_events=verified)
        assert [(x["start"],x["end"]) for x in intervals]==[("2010-04-28","2013-05-22")]
def test_actual_withdrawal_not_suppressed():
    for module in (v.freq_mod,standalone):
        assert module.classify_st_notice_risk_actions("关于公司股票交易撤销退市风险警示的公告")=={"delisting":False}
