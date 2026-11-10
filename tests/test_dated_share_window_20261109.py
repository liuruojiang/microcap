"""Frozen real Nov9 observations; full forward windows and rejection probes."""
import base64
import copy
from datetime import datetime
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import zlib

import pytest

from scripts import share_change_source as source
from scripts import share_unchanged_tail as legacy
from frozen_exchange_calendar import frozen_exchange_sessions

ROOT=Path(__file__).resolve().parents[1]


@pytest.fixture(autouse=True)
def calendar(frozen_exchange_sessions):return frozen_exchange_sessions


@pytest.fixture
def fixture():
    value=json.loads((ROOT/'tests/fixtures/dated_share_window_20261109.json').read_bytes())
    bodies={p:zlib.decompress(base64.b64decode(b)) for p,b in value['observations_zlib_b64'].items()}
    prefix={p:zlib.decompress(base64.b64decode(b)) for p,b in value['prefix_objects_zlib_b64'].items()}
    return value,bodies,prefix


def verify(fixture,index):
    data,bodies,_=fixture;case=data['cases'][index]
    def bound(path,digest):
        raw=bodies[path]
        if source._sha(raw)!=digest:raise ValueError('Source object digest differs')
        return raw
    return source.verify_dated_window(case['manifest'],symbol=case['symbol'],previous_day=case['previous_day'],end=case['end'],
        previous_candidate=base64.b64decode(case['previous_candidate_b64']),bound=bound,root=ROOT,
        now=datetime.fromisoformat(data['observed_at']))


@pytest.mark.parametrize('index',range(6))
def test_real_unchanged_and_five_changed_windows_preserve_exact_prefix(fixture,index):
    data,_,_=fixture;case=data['cases'][index]
    actual,facts=verify(fixture,index)
    assert actual==base64.b64decode(case['candidate_b64'])
    assert actual.startswith(base64.b64decode(case['previous_candidate_b64']))
    assert facts==case['verified_facts']
    assert facts['session_count']>=2
    assert facts['legal_effective_date_not_asserted'] is True
    assert bool(facts['changes'])==(index!=0)


def rewrite(fixture,role,mutate,receipt=False,index=1):
    data,bodies,_=fixture;entry=data['cases'][index]['manifest']['files'][role]
    if isinstance(entry,list):entry=entry[0]
    name='receipt_path' if receipt else 'raw_path'
    value=json.loads(bodies[entry[name]]);mutate(value)
    raw=json.dumps(value).encode();bodies[entry[name]]=raw
    entry['receipt_sha256' if receipt else 'raw_sha256']=source._sha(raw)
    if not receipt:
        saved=json.loads(bodies[entry['receipt_path']]);saved.update(raw_sha256=source._sha(raw),bytes=len(raw))
        body=json.dumps(saved).encode();bodies[entry['receipt_path']]=body;entry['receipt_sha256']=source._sha(body)


@pytest.mark.parametrize('kind',['missing','duplicate','boundary','middle_total','units','issuer','exchange','non_daily_clock','count_bool'])
def test_every_dated_session_and_exact_share_unit_is_required(fixture,kind):
    def mutate(value):
        result=value['result'];rows=result['data']
        if kind=='missing':rows.pop(2);result['count']-=1
        if kind=='duplicate':rows[2]['TRADE_DATE']=rows[1]['TRADE_DATE']
        if kind in ('boundary','middle_total','units'):
            row=rows[-1] if kind=='boundary' else rows[2]
            row['TOTAL_SHARES']=row['TOTAL_SHARES']+1 if kind!='units' else row['TOTAL_SHARES']/10000
            row['TOTAL_MARKET_CAP']=float(Decimal(str(row['TOTAL_SHARES']))*Decimal(str(row['CLOSE_PRICE'])))
        if kind=='issuer':rows[1]['SECURITY_CODE']='000001'
        if kind=='exchange':rows[1]['SECUCODE']='301229.SH'
        if kind=='non_daily_clock':rows[1]['TRADE_DATE']='2026-11-05 15:00:00'
        if kind=='count_bool':result['count']=True
    rewrite(fixture,'dated_window',mutate)
    with pytest.raises(ValueError):verify(fixture,1)


@pytest.mark.parametrize('role',['unfiltered','bounded','sina','quote','dated_window'])
def test_transport_tls_and_original_receipt_binding_remain_hard_gates(fixture,role):
    rewrite(fixture,role,lambda r:r.update(tls_verification=False),receipt=True)
    with pytest.raises(ValueError):verify(fixture,1)


@pytest.mark.parametrize('kind',['total','future_clock','other_observation_day'])
def test_quote_cannot_contradict_terminal_date_or_observation_clock(fixture,kind):
    def mutate(v):
        if kind=='total':v['data']['f84']+=1
        else:v['data']['f86']=int(datetime.fromisoformat('2026-11-10T07:00:00+00:00' if kind=='future_clock'
                                                       else '2026-11-07T07:00:00+00:00').timestamp())
    rewrite(fixture,'quote',mutate)
    with pytest.raises(ValueError):verify(fixture,1)


def test_late_old_period_report_must_match_the_protected_old_curve(fixture):
    def mutate(v):
        row=next(r for r in v['result']['data'] if r['END_DATE'].startswith('2026-09-30'))
        row['TOTAL_SHARES']+=1
    for role in ('unfiltered','bounded'):rewrite(fixture,role,mutate)
    with pytest.raises(ValueError,match='late historical report conflicts'):verify(fixture,1)


def test_unreviewed_future_event_cannot_be_hidden_by_asof_projection(fixture):
    def full(v):
        v['result']['data'][0]['END_DATE']='2026-11-10 00:00:00'
        v['result']['data'][0]['LISTING_DATE']='2026-11-10 00:00:00'
    def bounded(v):v['result']['data'].pop(0);v['result']['count']-=1
    rewrite(fixture,'unfiltered',full);rewrite(fixture,'bounded',bounded)
    with pytest.raises(ValueError):verify(fixture,1)


def prepare_prefix(tmp_path,fixture,index=1):
    tmp_path=source._extended(tmp_path)
    data,bodies,prefix=fixture;case=data['cases'][index]
    src=tmp_path/'.microcap_index_cache/share_sources';src.mkdir(parents=True)
    for name,raw in {**prefix,**bodies}.items():
        p=src/name;p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(raw)
    p=tmp_path/'.microcap_index_cache/share_change'/(case['symbol']+'.csv');p.parent.mkdir(parents=True)
    p.write_bytes(base64.b64decode(case['previous_candidate_b64']))
    manifest=src/case['reference']['path'];manifest.parent.mkdir(parents=True,exist_ok=True)
    raw=json.dumps(case['manifest'],ensure_ascii=False,indent=2).encode();manifest.write_bytes(raw)
    ref=dict(path=case['reference']['path'],sha256=source._sha(raw))
    return case,p,src,ref


def test_real_prefix_and_window_roundtrip_through_native_public_refresh(tmp_path,fixture,monkeypatch):
    case,cache,src,ref=prepare_prefix(tmp_path,fixture)
    old=cache.read_bytes();old_state=(src/(case['symbol']+'.json')).read_bytes()
    monkeypatch.setenv('MICROCAP_SHARE_DATED_WINDOW','1')
    (src/'RATE_LIMIT_STOP.json').write_text('{"fixture":"existing provider stop; do not query CNInfo"}')
    monkeypatch.setattr(source,'collect_dated_window',lambda **kwargs:(base64.b64decode(case['candidate_b64']),ref))
    source.refresh_share_change(root=tmp_path,symbol=case['symbol'],cache_path=cache,as_of=case['end'],allow_network=True)
    result=source.verify_share_cache(root=tmp_path,symbol=case['symbol'],as_of=case['end'])
    assert cache.read_bytes().startswith(old) and result['candidate_sha256']==source._sha(cache.read_bytes())
    state=json.loads((src/(case['symbol']+'.json')).read_bytes())
    assert state['kind']==source.DATED_WINDOW_KIND
    assert (src/state['previous_state']['path']).read_bytes()==old_state
    assert (src/'RATE_LIMIT_STOP.json').exists()
    cache.write_bytes(cache.read_bytes().replace(b'18291.4165',b'18291.4166'))
    with pytest.raises(ValueError):source.verify_share_cache(root=tmp_path,symbol=case['symbol'],as_of=case['end'])


def test_native_failure_after_publication_rolls_back_both_original_files(tmp_path,fixture,monkeypatch):
    case,cache,src,ref=prepare_prefix(tmp_path,fixture)
    original={cache:cache.read_bytes(),src/(case['symbol']+'.json'):(src/(case['symbol']+'.json')).read_bytes()}
    monkeypatch.setattr(source,'collect_dated_window',lambda **kwargs:(base64.b64decode(case['candidate_b64']),ref))
    actual=source.verify_share_cache;calls=[]
    def inject(**kwargs):
        calls.append(kwargs['as_of'])
        if len(calls)==2:raise RuntimeError('injected post-publication failure')
        return actual(**kwargs)
    monkeypatch.setattr(source,'verify_share_cache',inject)
    with pytest.raises(RuntimeError,match='injected'):
        source._refresh_dated_window(root=tmp_path,symbol=case['symbol'],cache_path=cache,as_of=case['end'])
    assert all(p.read_bytes()==raw for p,raw in original.items())
    actual(root=tmp_path,symbol=case['symbol'],as_of=case['previous_day'])


def test_dated_window_is_opt_in_and_legacy_route_does_not_change(tmp_path,fixture,monkeypatch):
    case,cache,src,_=prepare_prefix(tmp_path,fixture)
    (src/'RATE_LIMIT_STOP.json').write_text('{"fixture":"provider stop"}')
    monkeypatch.delenv('MICROCAP_SHARE_DATED_WINDOW',raising=False)
    calls=[]
    monkeypatch.setattr(source,'_refresh_dated_window',lambda **kw:pytest.fail('new route unexpectedly enabled'))
    monkeypatch.setattr(source,'_refresh_share_change',lambda **kw:calls.append(kw['as_of']))
    source.refresh_share_change(root=tmp_path,symbol=case['symbol'],cache_path=cache,as_of=case['end'],allow_network=True)
    assert calls==['2026-10-30','2026-11-02','2026-11-03','2026-11-04','2026-11-05','2026-11-06']


def test_exact_real_structure_transfer_keeps_all_original_share_bytes(fixture):
    data,_,_=fixture;case=data['cases'][6]
    result,facts=verify(fixture,6)
    assert case['symbol']=='300743'
    assert result==base64.b64decode(case['previous_candidate_b64'])
    assert facts==case['verified_facts'] and facts['changes']==[]
    assert facts['structure_events'][0]['old_total_unchanged'] is True


@pytest.mark.parametrize('field',['INFO_CODE','CHANGE_REASON','TOTAL_SHARES_CHANGE',
                                 'LIMITED_SHARES_CHANGE','UNLIMITED_SHARES_CHANGE'])
def test_other_zero_change_semantics_are_not_covered_by_exact_review(fixture,field):
    def mutate(v):
        row=v['result']['data'][0]
        row[field]=row[field]+1 if isinstance(row[field],int) else 'unreviewed'
    for role in ('unfiltered','bounded'):rewrite(fixture,role,mutate,index=6)
    with pytest.raises(ValueError):verify(fixture,6)


def test_exact_structural_transfer_requires_the_original_document(fixture):
    fixture[0]['cases'][6]['manifest']['files'].pop('structure_documents')
    with pytest.raises(ValueError,match='lacks issuer document'):verify(fixture,6)


def test_exact_document_cannot_be_replaced_even_with_rebound_transport_hashes(fixture):
    entry=fixture[0]['cases'][6]['manifest']['files']['structure_documents'][0]
    raw=fixture[1][entry['raw_path']]+b'changed'
    fixture[1][entry['raw_path']]=raw;entry['raw_sha256']=source._sha(raw)
    receipt=json.loads(fixture[1][entry['receipt_path']]);receipt.update(raw_sha256=source._sha(raw),bytes=len(raw))
    body=json.dumps(receipt).encode();fixture[1][entry['receipt_path']]=body;entry['receipt_sha256']=source._sha(body)
    with pytest.raises(ValueError,match='issuer document differs'):verify(fixture,6)


def test_exact_structure_event_recursively_reads_back_native_chain(tmp_path,fixture,monkeypatch):
    case,cache,src,ref=prepare_prefix(tmp_path,fixture,index=6)
    old=cache.read_bytes()
    monkeypatch.setattr(source,'collect_dated_window',lambda **kwargs:(old,ref))
    source._refresh_dated_window(root=tmp_path,symbol='300743',cache_path=cache,as_of=case['end'])
    assert cache.read_bytes()==old
    assert source.verify_share_cache(root=tmp_path,symbol='300743',as_of=case['end'])


def test_later_observed_future_plan_never_changes_the_target_history(fixture):
    case=fixture[0]['cases'][7]
    result,facts=verify(fixture,7)
    assert case['symbol']=='688092'
    assert result==base64.b64decode(case['previous_candidate_b64'])
    assert facts==case['verified_facts'] and facts['changes']==[]
    future=facts['future_plan_not_consumed']
    assert future['date']=='2026-11-12' and future['observed_after_target'] is True
    assert future['present_in_sina_future_inventory'] is False
    assert b'2026-11-12' not in result


@pytest.mark.parametrize('field',['END_DATE','NOTICE_DATE','INFO_CODE','TOTAL_SHARES','SECURITY_CODE'])
def test_later_future_observation_never_waives_another_row_or_date(fixture,field):
    def mutate(v):
        row=v['result']['data'][0]
        row[field]=row[field]+1 if isinstance(row[field],int) else 'unreviewed'
    rewrite(fixture,'unfiltered',mutate,index=7)
    with pytest.raises(ValueError):verify(fixture,7)


def test_later_future_observation_requires_its_exact_document(fixture):
    fixture[0]['cases'][7]['manifest']['files'].pop('later_future_documents')
    with pytest.raises(ValueError,match='lacks original issuer document'):verify(fixture,7)


def test_future_announced_quantity_cannot_substitute_for_target_quote(fixture):
    rewrite(fixture,'quote',lambda v:v['data'].update(f84=82148940),index=7)
    with pytest.raises(ValueError,match='quote total differs'):verify(fixture,7)


def test_exact_future_observation_recursively_reads_back_without_appending_plan(tmp_path,fixture,monkeypatch):
    case,cache,src,ref=prepare_prefix(tmp_path,fixture,index=7)
    old=cache.read_bytes()
    monkeypatch.setattr(source,'collect_dated_window',lambda **kwargs:(old,ref))
    source._refresh_dated_window(root=tmp_path,symbol='688092',cache_path=cache,as_of=case['end'])
    assert cache.read_bytes()==old
    assert source.verify_share_cache(root=tmp_path,symbol='688092',as_of=case['end'])
