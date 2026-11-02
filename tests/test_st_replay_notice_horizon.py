import json
import pandas as pd
import pytest
import microcap_top100_mom16_biweekly_live_v2_0 as v
import analyze_top100_rebalance_frequency as standalone
from scripts import st_notice_archive, st_notice_evidence

@pytest.mark.parametrize('module',[v.freq_mod,standalone])
def test_verified_notice_horizon_survives_new_price_rows(module,monkeypatch,tmp_path):
    g=module.load_security_meta.__globals__
    p=dict(meta_version=g['SECURITY_META_VERSION'],symbol='000010',st_notice_policy_version=g['ST_NOTICE_POLICY_VERSION'],st_notice_parser_revision=g['ST_NOTICE_PARSER_REVISION'],st_notice_evidence_sha256='verified',current_st_snapshot_name='ST示例',current_st_history_resolved=True,last_trade_date='2026-10-30',notice_source_complete=True,notice_query_start_date='1990-01-01',notice_query_end_date='2026-11-02',st_intervals=[{'start':'2026-01-01','end':None}])
    monkeypatch.setitem(g,'resolve_security_meta_path',lambda s:tmp_path/'meta.json')
    monkeypatch.setitem(g,'_read_security_meta_payload',lambda *a:p)
    monkeypatch.setitem(g,'load_current_st_name_map',lambda:{'000010':'ST示例'})
    monkeypatch.setitem(g,'latest_symbol_price_date',lambda s:pd.Timestamp('2026-11-02'))
    monkeypatch.setitem(g,'_security_meta_source_failures',lambda p:False)
    monkeypatch.setattr(st_notice_evidence,'evidence_fingerprint',lambda root:'verified')
    monkeypatch.setattr(st_notice_archive,'notice_archive_cache_matches',lambda *a:True)
    (tmp_path/'meta.json').write_text(json.dumps(p),encoding='utf-8')
    def reject(*a,**kw):raise st_notice_evidence.StNoticeEvidenceError('rebuild requested')
    monkeypatch.setitem(g,'build_security_meta',reject)
    assert module.load_security_meta('000010',as_of='2026-10-29') == p
    assert module.load_security_meta('000010',as_of='2026-11-02') == p
    p['notice_query_end_date']='2026-10-30'
    (tmp_path/'meta.json').write_text(json.dumps(p),encoding='utf-8')
    with pytest.raises(Exception,match='rebuild requested'):module.load_security_meta('000010',as_of='2026-11-02')
