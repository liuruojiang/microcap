"""Transaction tests with synthetic metadata; never production history approval."""
import copy,json
from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace
import pytest
from scripts import st_metadata_refresh as m
from scripts.st_notice_evidence import StNoticeEvidenceError

@pytest.fixture
def batch(tmp_path,monkeypatch):
    root=tmp_path;cache=root/'.microcap_index_cache';meta=cache/'security_meta';meta.mkdir(parents=True)
    previous=['old.json','a'*64];current=['new.json','b'*64]
    (root/'old.json').write_text(json.dumps({'daily_increments':[{'date':'2026-10-29'}]}))
    proof=root/m.FRESHNESS;proof.parent.mkdir();proof.write_text(json.dumps({'previous_authority':previous}))
    monkeypatch.setattr(m,'resolve_archive_reference',lambda root:tuple(current))
    monkeypatch.setattr(m,'frozen_notice_sources',lambda root:nullcontext())
    old=dict(st_notice_archive_sha256=previous[1],st_notice_evidence_sha256='registry',notice_source_complete=True,notice_query_end_date='2026-10-29',st_intervals=[],list_date='2000-01-01',delist_date=None)
    for s in ('600000','600001'):(meta/(s+'.json')).write_text(json.dumps(dict(old,symbol=s)))
    mod=SimpleNamespace(SECURITY_META_DIR=meta,list_backtest_universe_symbols=lambda:['600000','600001'],_notice_horizon_covers=lambda p,t:True)
    def build(symbol,as_of):
        data=dict(old,symbol=symbol,st_notice_archive_sha256=current[1],st_intervals=[{'start':'2026-10-30','end':None}])
        (mod.SECURITY_META_DIR/(symbol+'.json')).write_text(json.dumps(data));return data
    mod.build_security_meta=build
    return root,mod,meta

def run(batch,changed=True):
    root,mod,meta=batch
    return m.refresh_metadata_after_notice(root=root,freq_mod=mod,target='2026-10-30',notice_result={'authority_changed':changed},consumed_through='2026-10-29')

def test_append_installs_all_metadata_after_stage(batch):
    assert run(batch)['symbols']==2
    assert all(json.loads(p.read_bytes())['st_notice_archive_sha256']=='b'*64 for p in batch[2].glob('*.json'))
    assert run(batch,False)['requires_full_rebuild']

@pytest.mark.parametrize('day',['2026-10-28','2026-10-29'])
def test_inclusive_history_change_does_not_install(batch,day):
    root,mod,meta=batch;before={p.name:p.read_bytes() for p in meta.glob('*.json')};original=mod.build_security_meta
    def changed(*a,**kw):
        obj=original(*a,**kw);obj['st_intervals'][0]['start']=day
        (mod.SECURITY_META_DIR/(a[0]+'.json')).write_text(json.dumps(obj));return obj
    mod.build_security_meta=changed
    with pytest.raises(StNoticeEvidenceError,match='Historical ST/active'):run(batch)
    assert {p.name:p.read_bytes() for p in meta.glob('*.json')}==before
    assert json.loads((root/m.MARKER).read_bytes())['status']=='incomplete'
    mod.build_security_meta=original
    assert run(batch,False)['rebuilt']

def test_partial_install_is_explicit_and_resumable(batch,monkeypatch):
    original=m._atomic;meta=batch[2];calls=[]
    def fail(path,raw):
        if path.parent==meta:
            calls.append(path)
            if len(calls)==2:raise OSError('fixture interrupted install')
        original(path,raw)
    monkeypatch.setattr(m,'_atomic',fail)
    with pytest.raises(OSError):run(batch)
    assert json.loads((batch[0]/m.MARKER).read_bytes())['status']=='installing'
    monkeypatch.setattr(m,'_atomic',original)
    assert run(batch,False)['symbols']==2

def test_source_failure_after_build_preserves_old_metadata(batch,monkeypatch):
    from contextlib import contextmanager
    @contextmanager
    def invalid(root):
        yield
        raise StNoticeEvidenceError('source changed')
    monkeypatch.setattr(m,'frozen_notice_sources',invalid)
    with pytest.raises(StNoticeEvidenceError,match='source changed'):run(batch)
    assert all(json.loads(p.read_bytes())['st_notice_archive_sha256']=='a'*64 for p in batch[2].glob('*.json'))

def test_active_history_change_is_rejected():
    assert not m._history_equal({'st_intervals':[],'list_date':'2000-01-01'},{'st_intervals':[],'list_date':'2000-01-02'},'2026-10-29')

def test_actual_embedded_builder_writes_only_stage_then_installs(batch,monkeypatch):
    import pandas as pd
    import microcap_top100_mom16_biweekly_live_v2_0 as v
    from scripts import st_notice_archive as archive, st_notice_evidence as evidence
    root,fake,meta=batch
    actual=v.base_mod.freq_mod
    ns=actual.build_security_meta.__globals__
    price=root/'fixture_prices.csv';price.write_text('date\n2000-01-01\n2026-10-29\n')
    master=pd.DataFrame([dict(symbol=s,list_date=pd.Timestamp('2000-01-01'),delist_date=pd.NaT,name='普通样本',exchange='SH',board='main') for s in fake.list_backtest_universe_symbols()])
    for name,value in {'ROOT':root,'SECURITY_META_DIR':meta,'SHARED_SECURITY_META_DIR':None,'resolve_cache_path':lambda *a:price,'load_security_master':lambda:master,'load_current_st_name_map':lambda:{}}.items():
        monkeypatch.setitem(ns,name,value)
    monkeypatch.setattr(actual,'ROOT',root)
    monkeypatch.setattr(actual,'SECURITY_META_DIR',meta)
    monkeypatch.setattr(actual,'list_backtest_universe_symbols',fake.list_backtest_universe_symbols)
    monkeypatch.setattr(archive,'load_notice_archive',lambda *a,**kw:dict(rows=[],name_changes=[],manifest_sha256='b'*64,manifest_path='new.json',source_boundary='fixture',source_providers=['eastmoney'],notice_query_start_date='2000-01-01',notice_query_end_date='2026-10-30'))
    monkeypatch.setattr(evidence,'load_verified_events',lambda *a,**kw:[])
    monkeypatch.setattr(evidence,'evidence_fingerprint',lambda *a:'registry')
    result=m.refresh_metadata_after_notice(root=root,freq_mod=actual,target='2026-10-30',notice_result={'authority_changed':True},consumed_through='2026-10-29')
    assert result['symbols']==2
    assert ns['SECURITY_META_DIR']==meta
    assert json.loads((meta/'600000.json').read_bytes())['notice_query_end_date']=='2026-10-30'

def test_consumed_history_gap_blocks_even_if_append_is_later(batch):
    root,mod,meta=batch
    for path in meta.glob('*.json'):
        payload=json.loads(path.read_bytes());payload['notice_query_end_date']='2026-10-28';path.write_text(json.dumps(payload))
    with pytest.raises(StNoticeEvidenceError,match='unverified gap inside consumed proxy history'):
        run(batch)
    assert all(json.loads(p.read_bytes())['st_notice_archive_sha256']=='a'*64 for p in meta.glob('*.json'))

def test_same_authority_missing_horizon_still_rebuilds(batch):
    root,mod,meta=batch
    for path in meta.glob('*.json'):
        payload=json.loads(path.read_bytes());payload['st_notice_archive_sha256']='b'*64;path.write_text(json.dumps(payload))
    (root/m.FRESHNESS).write_text(json.dumps({'previous_authority':['old.json','b'*64]}))
    (root/'old.json').write_text(json.dumps({'daily_increments':[{'date':'2026-10-30'}]}))
    mod._notice_horizon_covers=lambda p,t:p['notice_query_end_date']>=str(t.date())
    original=mod.build_security_meta
    def build(*a,**kw):
        obj=original(*a,**kw);obj['notice_query_end_date']='2026-10-30'
        (mod.SECURITY_META_DIR/(a[0]+'.json')).write_text(json.dumps(obj));return obj
    mod.build_security_meta=build
    assert run(batch,False)['rebuilt']

def test_same_authority_missing_metadata_cannot_skip_batch(batch):
    root,mod,meta=batch
    (meta/'600001.json').unlink()
    payload=json.loads((meta/'600000.json').read_bytes());payload['st_notice_archive_sha256']='b'*64
    (meta/'600000.json').write_text(json.dumps(payload))
    (root/m.FRESHNESS).write_text(json.dumps({'previous_authority':['old.json','b'*64]}))
    with pytest.raises(FileNotFoundError):run(batch,False)

@pytest.mark.parametrize('wrong_field',['target','authority'])
def test_pending_transaction_blocks_hook_before_any_mutation(tmp_path,monkeypatch,wrong_field):
    import sys
    from scripts import realtime_state_bundle as bundle, st_notice_refresh, st_history_scope
    cache=tmp_path/'.microcap_index_cache';cache.mkdir()
    (cache/'st_notice_archive_pointer.json').write_text('{}')
    pending={'status':'installing','target':'2026-10-30','authority':['new.json','b'*64]}
    pending[wrong_field]='2026-10-29' if wrong_field=='target' else ['old.json','a'*64]
    (tmp_path/m.MARKER).write_text(json.dumps(pending))
    monkeypatch.setattr(st_history_scope,'resolve_archive_reference',lambda root:('new.json','b'*64))
    calls=[]
    monkeypatch.setattr(st_notice_refresh,'refresh_notice_archive',lambda **kw:calls.append('MUTATED'))
    fake=SimpleNamespace(_sync_embedded_base_config=lambda:None,_build_base_args=lambda **kw:SimpleNamespace(),base_mod=SimpleNamespace(DEFAULT_OUTPUT_PREFIX='fixture',build_output_paths=lambda p:{},build_refreshed_panel_shadow=lambda *a:(tmp_path/'panel.csv','2026-10-30')))
    monkeypatch.setitem(sys.modules,'microcap_top100_mom16_biweekly_live_v2_0',fake)
    monkeypatch.setitem(sys.modules,'run_top100_v1_6_v1_8_realtime_signals',SimpleNamespace(ensure_static_realtime_inputs=lambda **kw:None))
    with pytest.raises(RuntimeError,match='current ST notice refresh did not complete'):
        bundle.refresh_state(tmp_path)
    assert calls==[]
