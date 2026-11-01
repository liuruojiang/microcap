"""Controlled source-boundary counterexamples, not market research observations."""
import pandas as pd
import pytest
import analyze_top100_rebalance_frequency as standalone
import microcap_top100_mom16_biweekly_live_v2_0 as v20
from scripts import st_notice_archive, st_notice_evidence

@pytest.fixture(params=['standalone','embedded'])
def builder(request,tmp_path,monkeypatch):
 fn=standalone.build_security_meta if request.param=='standalone' else v20.freq_mod.build_security_meta
 ns=fn.__globals__;price=tmp_path/'price.csv';price.write_text('date\n2026-10-28\n2026-10-29\n')
 monkeypatch.setitem(ns,'ROOT',tmp_path);monkeypatch.setitem(ns,'SECURITY_META_DIR',tmp_path/'meta')
 monkeypatch.setitem(ns,'SHARED_SECURITY_META_DIR',None)
 monkeypatch.setitem(ns,'resolve_cache_path',lambda *_:price)
 monkeypatch.setitem(ns,'resolve_security_meta_path',lambda *_:None)
 monkeypatch.setitem(ns,'load_security_master',lambda:pd.DataFrame([dict(symbol='600001',name='ST未来',list_date='2020-01-01')]))
 monkeypatch.setitem(ns,'load_current_st_name_map',lambda:{'600001':'ST未来'})
 monkeypatch.setitem(ns,'build_st_interval_from_current_name_snapshot',lambda *_:pytest.fail('Snapshot helper must never be called by history builder'))
 state={'rows':[],'verified':{},'names':[dict(change_date='2020-01-01',old_name='',new_name='普通名称')]}
 def archive(*args):
  return dict(rows=state['rows'],name_changes=state['names'],manifest_sha256='a'*64,source_providers=['controlled_fixture'])
 monkeypatch.setattr(st_notice_archive,'load_notice_archive',archive)
 monkeypatch.setattr(st_notice_evidence,'load_verified_events',lambda *_:state['verified'])
 monkeypatch.setattr(st_notice_evidence,'evidence_fingerprint',lambda *_:'b'*64)
 return fn,ns,state,tmp_path

def test_future_current_st_does_not_open_interval_on_old_raw_last(builder):
 fn,ns,state,tmp=builder
 state['rows']=[dict(notice_date='2026-10-30',title='真实源边界受控测试',announcement_id='fixture',issuer_symbol='600001',source_url='fixture://notice',source_provider='controlled_fixture')]
 state['verified']={'fixture':dict(notice_date='2026-10-30',risk_actions={'other':True})}
 meta=fn('600001',as_of='2026-10-30')
 assert meta['last_trade_date']=='2026-10-29'
 assert meta['notice_query_end_date']=='2026-10-30'
 assert meta['current_st_history_resolved'] is True
 assert meta['st_intervals']==[{'start':'2026-10-30','end':None,'source':'archived_mixed_provider_notice'}]
 flags=ns['build_st_status_series'](meta,pd.to_datetime(['2026-10-29','2026-10-30']))
 assert flags.tolist()==[False,True]

def test_missing_history_current_st_fails_without_writing(builder):
 fn,ns,state,tmp=builder
 with pytest.raises((RuntimeError,st_notice_evidence.StNoticeEvidenceError),match='historical interval'):
  fn('600001',as_of='2026-10-30')
 assert not (tmp/'meta/600001.json').exists()

def test_master_current_name_alone_never_seeds_history(builder,monkeypatch):
 fn,ns,state,tmp=builder;monkeypatch.setitem(ns,'load_current_st_name_map',lambda:{})
 assert fn('600001',as_of='2026-10-30')['st_intervals']==[]


@pytest.mark.parametrize('exited',[False,True])
def test_raw_pre2010_preserves_trade_origin_and_real_presample_state(builder,monkeypatch,exited):
 fn,ns,state,tmp=builder
 (tmp/'price.csv').write_text('date\n1991-04-03\n2010-01-04\n2026-10-29\n')
 monkeypatch.setitem(ns,'load_security_master',lambda:pd.DataFrame([dict(symbol='600001',name='普通名称',list_date='1991-04-03')]))
 monkeypatch.setitem(ns,'load_current_st_name_map',lambda:{})
 state['names']=[dict(change_date='2005-05-06',old_name='普通名称',new_name='ST历史')]
 state['rows']=[dict(notice_date='2005-05-05',title='受控实际入口',announcement_id='entry',issuer_symbol='600001',source_url='fixture://entry',source_provider='controlled_fixture')]
 state['verified']={'entry':dict(notice_date='2005-05-05',actual_effective_date='2005-05-06',risk_actions={'delisting':False,'other':True})}
 if exited:
  state['names'].append(dict(change_date='2008-05-06',old_name='ST历史',new_name='普通名称'))
  state['rows'].append(dict(notice_date='2008-05-05',title='受控实际退出',announcement_id='exit',issuer_symbol='600001',source_url='fixture://exit',source_provider='controlled_fixture'))
  state['verified']['exit']=dict(notice_date='2008-05-05',actual_effective_date='2008-05-06',risk_actions={'delisting':False,'other':False})
 meta=fn('600001',as_of='2026-10-30')
 assert meta['first_trade_date']=='1991-04-03'
 assert meta['st_history_sample_start']=='2010-01-04'
 assert meta['notice_query_start_date']=='1991-04-03'
 flags=ns['build_st_status_series'](meta,pd.to_datetime(['2010-01-04','2026-10-29']))
 assert flags.tolist()==[not exited,not exited]
