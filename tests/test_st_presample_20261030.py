"""Real 000017 issuer notice establishes ST state before its cached sample."""
from pathlib import Path
import json
import pandas as pd
import microcap_top100_mom16_biweekly_live_v2_0 as v
import analyze_top100_rebalance_frequency as standalone
from scripts.st_notice_evidence import load_verified_events
ROOT=Path(__file__).resolve().parents[1]
FIXTURES=ROOT/"tests/fixtures/st_presample_20261030"
def test_real_2007_notice_is_not_discarded_at_2010_price_start():
    e=next(x for x in json.loads((ROOT/"data/st_notice_evidence/registry.json").read_text(encoding="utf-8"))["events"] if x["announcement_id"]=="AN201202270004188446")
    row=dict(source_provider="eastmoney",announcement_id=e["announcement_id"],issuer_symbol="000017",notice_date=e["notice_date"],title=e["title"],source_url=e["source_url"])
    verified=load_verified_events(ROOT,"000017","1990-01-01","2009-12-31",[row])
    assert verified[e["announcement_id"]]["risk_actions"]=={"delisting":False,"other":True}
    for module in (v.freq_mod,standalone):
        intervals=module.build_st_intervals_from_notices(pd.Timestamp("2010-03-18"),pd.Timestamp("2010-04-01"),pd.DataFrame([row]),verified_events=verified)
        assert intervals[0]["start"]=="2010-03-18" and intervals[0]["end"] is None
        assert module.build_st_intervals_from_notices(pd.Timestamp("2010-03-18"),pd.Timestamp("2010-04-01"),pd.DataFrame(columns=["notice_date","title"]))==[]


def test_real_exchange_anchors_do_not_guess_categories_or_leak_future_names():
    import pytest
    from scripts.st_notice_evidence import StNoticeEvidenceError
    names=pd.DataFrame(json.loads((FIXTURES/"name_changes.json").read_text(encoding="utf-8")))
    registry=json.loads((ROOT/"data/st_notice_evidence/registry.json").read_text(encoding="utf-8"))["events"]
    event=next(e for e in registry if e["announcement_id"]=="AN201202270004188446")
    for module in (v.freq_mod,standalone):
        for symbol,day in (("000007","2007-04-23"),("000008","2008-04-03")):
            subset=names.loc[names.symbol==symbol]
            anchor=module.build_st_presample_name_anchor("2010-01-04",subset)
            assert anchor["date"]==day and anchor["category_status"]=="unknown"
            # Real known category-only exit must not silently leave generic ST forever.
            title="关于撤销其他风险警示的公告"
            with pytest.raises(StNoticeEvidenceError,match="category unknown"):
                module.build_st_intervals_from_notices(pd.Timestamp("2010-01-04"),pd.Timestamp("2026-10-29"),pd.DataFrame([dict(notice_date="2013-01-01",title=title)]),initial_name_anchor=anchor)
            truncated=subset.loc[pd.to_datetime(subset.change_date)<pd.Timestamp("2010-01-04")]
            assert module.build_st_presample_name_anchor("2010-01-04",truncated)==anchor
        subset=names.loc[names.symbol=="000017"]
        anchor=module.build_st_presample_name_anchor("2010-03-18",subset,verified_events={event["announcement_id"]:event})
        assert anchor["date"]=="2007-02-09"
        assert anchor["risk_state"]=={"delisting":False,"other":True,"unspecified":False}
        with pytest.raises(StNoticeEvidenceError,match="Same-day"):
            module.build_st_intervals_from_notices(pd.Timestamp("2010-03-18"),pd.Timestamp("2020-01-01"),pd.DataFrame([dict(notice_date="2007-02-09",title="关于股票交易实行退市风险警示的公告")]),initial_name_anchor=anchor)
        rows=pd.DataFrame([dict(notice_date="1999-05-04",title="关于股票实行特别处理的公告"),dict(notice_date="2014-01-01",title="关于撤销其他风险警示的公告")])
        intervals=module.build_st_intervals_from_notices(pd.Timestamp("2010-03-18"),pd.Timestamp("2020-01-01"),rows,initial_name_anchor=anchor)
        assert intervals==[{"start":"2010-03-18","end":"2014-01-01","source":"cninfo_notice"}]
        # A newer pre-sample announcement must still override the older anchor.
        rows.loc[1,"notice_date"]="2009-01-01"
        assert module.build_st_intervals_from_notices(pd.Timestamp("2010-03-18"),pd.Timestamp("2020-01-01"),rows,initial_name_anchor=anchor)==[]


def test_real_000007_and_000017_full_archives_clear_old_generic_residue():
    names=pd.DataFrame(json.loads((FIXTURES/"name_changes.json").read_text(encoding="utf-8")))
    base=FIXTURES
    for symbol,first,expected in (("000007","2010-01-04",["2012-05-03","2022-06-30","2024-07-01"]),("000008","2010-01-20",["2013-04-18"]),("000034","2010-09-03",["2011-03-31"]),("000408","2010-01-04",["2012-11-27","2017-05-22","2021-05-11"]),("000017","2010-03-18",["2014-05-13","2021-06-19"])):
        rows=json.loads((base/(symbol+"_notices.json")).read_text(encoding="utf-8"))
        verified=load_verified_events(ROOT,symbol,"1990-01-01","2026-10-29",rows)
        for module in (v.freq_mod,standalone):
            anchor=module.build_st_presample_name_anchor(first,names.loc[names.symbol==symbol],verified_events=verified)
            intervals=module.build_st_intervals_from_notices(pd.Timestamp(first),pd.Timestamp("2026-10-29"),pd.DataFrame(rows),verified_events=verified,initial_name_anchor=anchor)
            assert [item["end"] for item in intervals]==expected
            assert intervals[0]["start"]==first
