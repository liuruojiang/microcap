"""Bounded semantic metadata continuation: real ST rows, synthetic 2-member returns."""
import base64
import copy
import hashlib
import json
from contextlib import nullcontext
from pathlib import Path
import socket
import pandas as pd
import pytest
from scripts import st_metadata_refresh as m
from test_frozen_tail_chain_20260917 import chain as semantic_price_chain

@pytest.fixture
def semantic_tail(semantic_price_chain, tmp_path, monkeypatch):
    c=semantic_price_chain; b=c.base;ns=b.ensure_strategy_files.__globals__
    def no_network(*a,**k):raise AssertionError('offline only')
    monkeypatch.setattr(socket.socket,'connect',no_network)
    from scripts import exchange_calendar
    from datetime import date
    monkeypatch.setattr(exchange_calendar,'latest_completed_session',lambda:date(2026,11,5))
    monkeypatch.setattr(exchange_calendar,'sessions_for_day',lambda d:(date(2026,11,4),date(2026,11,5)))
    # Shift only synthetic price/portfolio fixtures; actual 300237 evidence stays dated.
    olddates=pd.bdate_range('2026-09-17','2026-10-02');newdates=pd.bdate_range('2026-11-04',periods=len(olddates))
    for p in tmp_path.glob('*.csv'):
        raw=p.read_text('utf8')
        for a,z in zip(olddates,newdates):raw=raw.replace(str(a.date()),str(z.date()))
        p.write_text(raw,encoding='utf8')
    pd.DataFrame({'date':newdates[:2]}).to_csv(c.paths['panel_shadow'],index=False)
    gross=pd.read_csv(c.args.costed_nav_csv,index_col='date');gross.index=pd.to_datetime(gross.index)
    tail=gross.copy();tail.index=pd.DatetimeIndex(['2026-11-05'],name='date');tail['microcap_close']=101.0
    gross=pd.concat([gross,tail]);monkeypatch.setitem(ns,'load_close_df',lambda *a,**k:gross)
    monkeypatch.setitem(ns,'build_biweekly_rebalance_dates',lambda d:pd.DatetimeIndex([]))
    monkeypatch.setitem(ns,'frozen_tail_authority_matches_seed',lambda *a:False)
    monkeypatch.setitem(ns,'frozen_tail_extension_matches_authority',lambda *a:False)
    def no_rebuild(*a,**k):raise AssertionError('historical rebuild forbidden')
    monkeypatch.setitem(ns,'build_local_proxy_bundle',no_rebuild)
    cache=tmp_path/'.microcap_index_cache';(cache/'security_meta').mkdir(parents=True);(cache/'txn/backup').mkdir(parents=True)
    container=json.loads((Path(__file__).parent/'fixtures/st_300237_metadata_D4_20261105.json').read_bytes())
    real=base64.b64decode(container['raw_utf8_base64']);assert hashlib.sha256(real).hexdigest()==container['sha256'];old=json.loads(real)
    oldref=['old_archive.json',m._sha(b'old source')];ref=['new_archive.json',m._sha(b'new source')]
    (tmp_path/oldref[0]).write_bytes(b'old source');(tmp_path/ref[0]).write_bytes(b'new source')
    old.update(st_notice_archive_sha256=oldref[1],st_notice_archive_manifest_path=oldref[0])
    new=copy.deepcopy(old);new.update(st_notice_archive_sha256=ref[1],st_notice_archive_manifest_path=ref[0],notice_query_end_date='2026-11-05',notice_count=4)
    from scripts.st_history_scope import _daily_notice_row
    new['st_intervals'][0]['end']='2026-11-05'
    marker=dict(status='complete',target='2026-11-05',boundary='2026-11-04',symbols=['300237'],authority=ref,previous_authority=oldref,work='.microcap_index_cache/txn',old_hashes={},new_hashes={})
    a=m._bytes(old);z=m._bytes(new);(cache/'txn/backup/300237.json').write_bytes(a);(cache/'security_meta/300237.json').write_bytes(z)
    marker['old_hashes']['300237']=m._sha(a);marker['new_hashes']['300237']=m._sha(z);(tmp_path/m.MARKER).write_bytes(m._bytes(marker))
    def fp(raw):return dict(present_count=1,missing_count=0,sha256=hashlib.sha256(b'300237\0'+raw+b'\0').hexdigest())
    monkeypatch.setattr(b.freq_mod,'list_backtest_universe_symbols',lambda:['300237'])
    monkeypatch.setitem(ns,'security_meta_cache_fingerprint',lambda:fp((cache/'security_meta/300237.json').read_bytes()))
    (cache/'security_master.csv').write_bytes(b'symbol\n300237\n')
    stock={'file_count':1,'sha256':hashlib.sha256(b'.microcap_index_cache/security_master.csv\0'+(cache/'security_master.csv').read_bytes()+b'\0').hexdigest(),'code':{}}
    from scripts.stock_input_contract import SOURCE_FILES
    stock['code']={name:'b'*64 for name in SOURCE_FILES}
    monkeypatch.setitem(ns,'stock_input_evidence_fingerprint',lambda:stock)
    meta=json.loads(c.paths['proxy_meta'].read_bytes());meta['core_params'].update(security_meta_cache_fingerprint=fp(a),stock_input_evidence_fingerprint=stock);meta['return_basis_consumption']['full_rebuild_anchor']='2026-11-04'
    c.paths['proxy_meta'].write_bytes(m._bytes(meta))
    monkeypatch.setitem(ns,'proxy_meta_matches_execution_model',lambda p:p['core_params']['security_meta_cache_fingerprint']==fp((cache/'security_meta/300237.json').read_bytes()))
    monkeypatch.setattr(m,'resolve_archive_reference',lambda r:tuple(ref));monkeypatch.setattr(m,'frozen_notice_sources',lambda r:nullcontext())
    for name in ('_proxy_meta_core_matches_execution_model','stock_input_evidence_fingerprint','security_meta_cache_fingerprint','proxy_meta_matches_execution_model','FROZEN_TAIL_AUTHORITY_PATH','build_biweekly_rebalance_dates','TOP_N'):
        monkeypatch.setattr(b,name,ns[name])
    kwargs=dict(root=tmp_path,base=b,args=c.args,paths=c.paths,panel_path=c.paths['panel_shadow'],target='2026-11-05')
    return c,kwargs,marker,old,new

def test_semantic_tail_native_proxy_nav_preserves_prefix(semantic_tail):
    c,kw,marker,old,new=semantic_tail
    before={p:p.read_bytes() for p in [c.args.index_csv,c.args.costed_nav_csv,c.paths['proxy_members'],c.paths['proxy_turnover'],c.paths['proxy_effective_members']]}
    assert m._history_equal(old,new,'2026-11-05')
    assert m.try_cached_history_rebuild(**kw)
    for p,raw in before.items():assert p.read_bytes().startswith(raw)
    for p in [c.args.index_csv,c.args.costed_nav_csv]:assert c.base.read_csv_last_date(p)==pd.Timestamp('2026-11-05')
    meta=json.loads(c.paths['proxy_meta'].read_bytes());assert c.base.proxy_meta_matches_execution_model(meta)
    assert meta['core_params']['security_meta_cache_fingerprint']==c.base.security_meta_cache_fingerprint()
    assert meta['return_basis_consumption']['tail_extensions'][-1]['end']=='2026-11-05'

@pytest.mark.parametrize('case',['tamper','old_fingerprint','unknown','historical','future_interval','future_target','archive','missing','rebalance','nav_failure','concurrent','current_st_changed','extra_metadata'])
def test_semantic_tail_fails_closed_without_output_install(semantic_tail,monkeypatch,case):
    c,kw,marker,old,new=semantic_tail;root=kw['root'];p=root/'.microcap_index_cache/security_meta/300237.json';mp=root/m.MARKER
    if case=='tamper':p.write_bytes(p.read_bytes()+b' ')
    elif case in ('unknown','historical','future_interval'):
        if case=='unknown':new['board']='unreviewed'
        elif case=='historical':new['st_intervals'][0]['start']='2025-09-28'
        else:new['st_intervals'][0]['end']='2026-11-06'
        p.write_bytes(m._bytes(new));marker['new_hashes']['300237']=m._sha(p.read_bytes());mp.write_bytes(m._bytes(marker))
    elif case=='old_fingerprint':
        meta=json.loads(c.paths['proxy_meta'].read_bytes());meta['core_params']['security_meta_cache_fingerprint']['sha256']='f'*64;c.paths['proxy_meta'].write_bytes(m._bytes(meta))
    elif case=='future_target':kw['target']='2099-01-01';marker['target']=kw['target'];mp.write_bytes(m._bytes(marker))
    elif case=='archive':(root/'new_archive.json').write_bytes(b'changed')
    elif case=='missing':p.unlink()
    elif case=='rebalance':monkeypatch.setattr(c.base,'build_biweekly_rebalance_dates',lambda d:pd.DatetimeIndex(['2026-11-05']))
    elif case=='nav_failure':monkeypatch.setattr(c.base,'try_extend_costed_nav_without_turnover',lambda *a:False)
    else:
        original=c.base.try_extend_costed_nav_without_turnover
        def drift(*a):
            result=original(*a)
            if case=='current_st_changed':c.base.freq_mod.CURRENT_ST.write_bytes(b'changed snapshot')
            elif case=='extra_metadata':(root/'.microcap_index_cache/security_meta/999999.json').write_bytes(b'{}')
            else:mp.write_bytes(mp.read_bytes()+b' ')
            return result
        monkeypatch.setattr(c.base,'try_extend_costed_nav_without_turnover',drift)
    before={p:p.read_bytes() for p in [c.args.index_csv,c.args.costed_nav_csv,c.paths['proxy_meta']]}
    with pytest.raises((ValueError,FileNotFoundError)):m.try_cached_history_rebuild(**kw)
    assert all(p.read_bytes()==raw for p,raw in before.items())

@pytest.mark.parametrize('case',['replace_then_error','receipt_failure','rollback_failure','owner_changed','receipt_owner_changed'])
def test_semantic_tail_transaction_failure_preserves_ownership(semantic_tail, monkeypatch, case):
    c,kw,marker,old,new=semantic_tail
    root=kw['root'];lock=root/'.microcap_index_cache/st_metadata_refresh.lock'
    originals={p:p.read_bytes() for p in (c.args.index_csv,c.args.costed_nav_csv,c.paths['proxy_meta'])}
    atomic=m._atomic; fired=[]
    def broken(path,raw):
        if case=='owner_changed' and path==c.args.index_csv:
            lock.write_bytes(b'other writer')
        if path==c.args.costed_nav_csv and case in ('replace_then_error','rollback_failure') and not fired:
            fired.append(True);atomic(path,raw);raise OSError('after replace')
        if case=='rollback_failure' and fired and path==c.args.costed_nav_csv and raw==originals[path]:
            raise OSError('rollback denied')
        if case=='receipt_failure' and path.name=='receipt.json':raise OSError('receipt failed')
        result=atomic(path,raw)
        if case=='receipt_owner_changed' and path.name=='receipt.json':lock.write_bytes(b'other writer')
        return result
    monkeypatch.setattr(m,'_atomic',broken)
    with pytest.raises((OSError,ValueError)):m.try_cached_history_rebuild(**kw)
    if case in ('replace_then_error','receipt_failure'):
        assert all(p.read_bytes()==raw for p,raw in originals.items())
        assert not lock.exists()
    else:
        assert lock.exists()
        if case in ('owner_changed','receipt_owner_changed'):assert lock.read_bytes()==b'other writer'


def test_semantic_tail_native_pack_metadata_fingerprint_boundary(semantic_tail, monkeypatch):
    from scripts import realtime_state_bundle as state, stock_input_contract
    c,kw,marker,old,new=semantic_tail
    assert m.try_cached_history_rebuild(**kw)
    root=kw['root'];outputs=root/'outputs';outputs.mkdir()
    canonical=outputs/'microcap_top100_mom16_biweekly_live_v2_0_base_proxy_meta.json'
    canonical.write_bytes(c.paths['proxy_meta'].read_bytes())
    monkeypatch.setattr(state,'_st_source_transport_files',lambda root:[])
    monkeypatch.setattr(stock_input_contract,'source_fingerprint',lambda root:c.base.stock_input_evidence_fingerprint()['code'])
    records=state.validate_security_metadata(root)
    assert len(records)==1 and records[0]['path'].endswith('/300237.json')
    metadata=root/'.microcap_index_cache/security_meta/300237.json'
    metadata.write_bytes(metadata.read_bytes()+b' ')
    with pytest.raises(ValueError,match='fingerprint mismatch'):state.validate_security_metadata(root)
