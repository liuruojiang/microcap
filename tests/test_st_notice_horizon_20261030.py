"""A stale price tail must not truncate explicitly requested notice coverage."""
import json
import pandas as pd
import pytest
import microcap_top100_mom16_biweekly_live_v2_0 as v
import analyze_top100_rebalance_frequency as standalone
from scripts.st_notice_evidence import evidence_fingerprint
@pytest.mark.parametrize("module",[v.freq_mod,standalone])
def test_build_queries_requested_day_separate_from_price_tail(module,tmp_path,monkeypatch):
    g=module.build_security_meta.__globals__;p=tmp_path/"price.csv"
    pd.DataFrame({"date":["2010-01-04","2026-10-09"]}).to_csv(p,index=False)
    calls=[]
    monkeypatch.setitem(g,"resolve_cache_path",lambda *args:p)
    monkeypatch.setitem(g,"load_security_master",lambda:pd.DataFrame())
    monkeypatch.setitem(g,"load_current_st_name_map",lambda:{})
    monkeypatch.setitem(g,"resolve_security_meta_path",lambda *args:None)
    monkeypatch.setitem(g,"SECURITY_META_DIR",tmp_path/"meta")
    def fetch(**kw):
        calls.append(kw);return pd.DataFrame(columns=["notice_date","title","announcement_id","issuer_symbol","source_url"])
    monkeypatch.setitem(g,"fetch_cninfo_st_notices",fetch)
    result=module.build_security_meta("600999",as_of="2026-10-29")
    assert calls[0]["end_date"]=="20261029" and calls[0]["start_date"]=="19900101"
    assert result["last_trade_date"]=="2026-10-09" and result["notice_query_end_date"]=="2026-10-29"
@pytest.mark.parametrize("module",[v.freq_mod,standalone])
def test_cached_notice_horizon_is_mandatory(module):
    g=module.load_security_meta.__globals__;covers=g["_notice_horizon_covers"]
    assert not covers({"last_trade_date":"2026-10-29"},pd.Timestamp("2026-10-29"))
    assert not covers({"notice_query_end_date":"2026-10-09"},pd.Timestamp("2026-10-29"))
    assert covers({"notice_source_complete":True,"notice_query_start_date":"1990-01-01","notice_query_end_date":"2026-10-29"},pd.Timestamp("2026-10-29"))
    assert covers({"notice_source_complete":True,"notice_query_start_date":"1990-01-01","notice_query_end_date":"2020-01-02","delist_date":"2020-01-02"},pd.Timestamp("2026-10-29"))
