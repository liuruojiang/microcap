"""Real Nov9 observations for the Nov5/6 close, plus rebound negative probes."""
import base64
import copy
from datetime import datetime
import hashlib
import json
import zlib
from pathlib import Path

import pytest

from scripts import share_unchanged_tail as source
from scripts import realtime_state_bundle as state
from scripts import share_change_source as native
from scripts.stock_input_contract import source_fingerprint
from frozen_exchange_calendar import frozen_exchange_sessions

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(autouse=True)
def sessions(frozen_exchange_sessions):
    return frozen_exchange_sessions


@pytest.fixture
def evidence():
    fixture = json.loads((ROOT / 'tests/fixtures/historical_share_close_002316_20261109.json').read_bytes())
    bodies = {p: base64.b64decode(raw) for p, raw in fixture['artifacts_b64'].items()}
    candidate = base64.b64decode(fixture['candidate_b64'])
    assert source.sha(candidate) == fixture['candidate_sha256']
    return fixture, bodies, candidate


def verify(evidence, root=ROOT):
    fixture, bodies, candidate = evidence
    def bound(path, digest):
        raw = bodies[path]
        if source.sha(raw) != digest:
            raise ValueError('source hash mismatch')
        return raw
    return source.verify_manifest(fixture['manifest'], symbol=fixture['manifest']['symbol'], previous_day='2026-11-05',
                                  end='2026-11-06', candidate=candidate, bound=bound, root=root,
                                  now=datetime.fromisoformat(fixture['observed_at']))


def rewrite(evidence, name, mutate, receipt=False):
    fixture, bodies, _ = evidence
    entry = fixture['manifest']['files'][name]
    if isinstance(entry, list):
        entry = entry[0]
    key = 'receipt_path' if receipt else 'raw_path'
    value = json.loads(bodies[entry[key]])
    mutate(value)
    bodies[entry[key]] = json.dumps(value).encode()
    entry['receipt_sha256' if receipt else 'raw_sha256'] = source.sha(bodies[entry[key]])
    if not receipt:
        saved = json.loads(bodies[entry['receipt_path']])
        saved.update(raw_sha256=entry['raw_sha256'], bytes=len(bodies[entry['raw_path']]))
        bodies[entry['receipt_path']] = json.dumps(saved).encode()
        entry['receipt_sha256'] = source.sha(bodies[entry['receipt_path']])


def test_real_dated_total_shares_preserve_old_history_and_distinguish_later_quote(evidence):
    original = copy.deepcopy(evidence)
    result = verify(evidence)
    assert result['total_shares_10k'] == '39312'
    assert result['historical_close_evidence']['sessions'] == ['2026-11-05', '2026-11-06']
    assert result['historical_close_evidence']['total_shares'] == '393120000'
    assert result['quote_is_target_close_frame'] is False
    assert result['frame_time'].startswith('2026-11-09')
    assert result['f84_independent_asof_guarantee_proven'] is False
    assert evidence == original


def test_later_quote_without_dated_evidence_still_fails(evidence):
    evidence[0]['manifest']['files'].pop('historical_close')
    with pytest.raises(ValueError, match='quote frame not target close'):
        verify(evidence)


@pytest.mark.parametrize('mutation', [
    lambda d: d.update(success=False),
    lambda d: d.update(code=True),
    lambda d: d['result'].update(pages=2),
    lambda d: d['result'].update(count=1),
    lambda d: d['result']['data'].pop(),
    lambda d: d['result']['data'].reverse(),
    lambda d: d['result']['data'][0].update(TRADE_DATE='2026-11-09 00:00:00'),
    lambda d: d['result']['data'][1].update(TRADE_DATE='2026-11-06 00:00:00'),
    lambda d: d['result']['data'][0].update(TRADE_DATE='2026-11-06 12:00:00'),
    lambda d: d['result']['data'][0].update(SECURITY_CODE='000001'),
    lambda d: d['result']['data'][0].update(SECUCODE='002316.SH'),
    lambda d: d['result']['data'][0].update(TOTAL_SHARES=39312),
    lambda d: d['result']['data'][1].update(TOTAL_SHARES=393120001),
    lambda d: d['result']['data'][0].update(TOTAL_SHARES=True),
    lambda d: d['result']['data'][0].update(TOTAL_MARKET_CAP=1),
    lambda d: d['result']['data'][0].update(CLOSE_PRICE=0),
])
def test_dated_raw_data_semantics_reject_rebound_conflicts(evidence, mutation):
    rewrite(evidence, 'historical_close', mutation)
    with pytest.raises(ValueError):
        verify(evidence)


@pytest.mark.parametrize('mutation', [
    lambda d: d.update(tls_verification=False),
    lambda d: d.update(http_status=403),
    lambda d: d.update(http_start='2026-11-06T14:59:59+08:00'),
    lambda d: d.update(actual_url=d['actual_url'].replace('eastmoney.com', 'example.invalid')),
    lambda d: d['params'].update(filter='(SECURITY_CODE="000001")'),
    lambda d: d.update(raw_sha256='0'*64),
])
def test_dated_receipt_rejects_rebound_transport_identity(evidence, mutation):
    rewrite(evidence, 'historical_close', mutation, receipt=True)
    with pytest.raises(ValueError):
        verify(evidence)


@pytest.mark.parametrize('name,mutation', [
    ('quote', lambda d: d['data'].update(f86=0)),
    ('quote', lambda d: d['data'].update(f86=1794006000)),
    ('quote', lambda d: d['data'].update(f86=9999999999)),
    ('quote', lambda d: d['data'].update(f124=1794006000)),
    ('quote', lambda d: d['data'].update(f84=1)),
    ('unfiltered', lambda d: d['result']['data'][0].update(NOTICE_DATE='2026-11-06 00:00:00')),
    ('bounded', lambda d: d['result']['data'][0].update(TOTAL_SHARES=1)),
])
def test_historical_evidence_never_waives_original_other_source_guards(evidence, name, mutation):
    rewrite(evidence, name, mutation)
    with pytest.raises(ValueError):
        verify(evidence)


def identities():
    fixture = json.loads((ROOT / 'tests/fixtures/maintenance_d5_source_identity_20261109.json').read_bytes())
    old = fixture['stock_input_evidence_fingerprint']
    current = copy.deepcopy(old)
    current['code'] = source_fingerprint(ROOT)
    return old, current, fixture


def test_exact_rehashed_d5_data_permits_only_actual_reviewed_source():
    old, current, fixture = identities()
    assert json.loads(json.dumps(state.APPROVED_EVENT_REOBSERVATION_D5_SOURCE_REVIEW)) == fixture['review']
    assert hashlib.sha256(json.dumps(old,sort_keys=True,separators=(',', ':')).encode()).hexdigest() == state.HISTORICAL_CLOSE_D5_SOURCE_REVIEW['stock_fingerprint_sha256']
    assert {n for n in old['code'] if old['code'][n] != current['code'][n]} == {'scripts/share_unchanged_tail.py','scripts/share_change_source.py'}
    before = copy.deepcopy((old,current))
    assert state.stock_input_evidence_matches_validated_source(old,current)
    assert (old,current) == before


@pytest.mark.parametrize('side', ['old','current','both'])
@pytest.mark.parametrize('fault', ['data','count','code','extra','missing'])
def test_source_compatibility_rejects_other_data_or_source(evidence, side, fault):
    old,current,_=identities()
    for target in ([old,current] if side=='both' else [old if side=='old' else current]):
        if fault=='data': target['sha256']='0'*64
        elif fault=='count': target['file_count']+=1
        elif fault=='code': target['code']['scripts/share_unchanged_tail.py']='0'*64
        elif fault=='extra': target['code']['other.py']='0'*64
        else: target['code'].pop('scripts/st_notice_refresh.py')
    assert not state.stock_input_evidence_matches_validated_source(old,current)


def test_no_reverse_or_unreviewed_transitive_source_permission():
    old,current,_=identities()
    assert not state.stock_input_evidence_matches_validated_source(current,old)
    future=copy.deepcopy(current);future['code']['scripts/share_unchanged_tail.py']='f'*64
    assert not state.stock_input_evidence_matches_validated_source(old,future)
    assert not state.stock_input_evidence_matches_validated_source(current,future)


@pytest.fixture
def reobserved_evidence():
    fixture=json.loads((ROOT/'tests/fixtures/historical_share_close_301288_20261109.json').read_bytes())
    return fixture,{p:base64.b64decode(raw) for p,raw in fixture['artifacts_b64'].items()},base64.b64decode(fixture['candidate_b64'])


def test_exact_existing_zero_event_reobservation_keeps_original_approval_and_history(reobserved_evidence):
    before=copy.deepcopy(reobserved_evidence)
    result=verify(reobserved_evidence)
    event=result['approved_late_unchanged_event']
    assert result['total_shares_10k']=='10801' and result['old_candidate_sha256']=='abf7572f5abcb80bdaf73c83d6e6851356e2d3bdd7ed1707d68b7e8b0a83a5f4'
    assert event['signal_day']=='2026-11-05' and event['qualification_day']=='2026-11-06'
    assert event['reobserved_existing_approved_event'] is True
    assert event['package_sha256']=='3af53c7fea86b3334321112981747560e9fcea9dba5ee7667c9f011d8a70f4ff'
    assert reobserved_evidence==before
    assert state.HISTORICAL_CLOSE_D5_SOURCE_REVIEW['changes']['scripts/share_unchanged_tail.py'][1]=='f97d5ef6ed1814f36551579580b4075316dbdd188d70b38c4c9a842e18f3aa27'


@pytest.mark.parametrize('field,value',[('NOTICE_DATE','2026-11-07 00:00:00'),('END_DATE','2026-11-06 00:00:00'),
    ('INFO_CODE','AN000000'),('TOTAL_SHARES',108010001),('TOTAL_SHARES_CHANGE',1),('FREELIQCI_SHARES',41680325)])
def test_reobserved_event_requires_the_entire_original_exact_row(reobserved_evidence,field,value):
    for name in ('unfiltered','bounded'):
        rewrite(reobserved_evidence,name,lambda d:d['result']['data'][0].update({field:value}))
    with pytest.raises(ValueError):verify(reobserved_evidence)


def test_reobserved_event_still_requires_original_approval_and_candidate(reobserved_evidence,tmp_path):
    fixture,bodies,candidate=reobserved_evidence
    with pytest.raises(ValueError,match='old history differs'):
        verify((fixture,bodies,candidate+b'\n'))
    registry=json.loads((ROOT/'data/share_source_future_plan_reviews.json').read_bytes())
    registry['unchanged_event_approval']['approval']['decision']='pending'
    (tmp_path/'data').mkdir()
    (tmp_path/'data/share_source_future_plan_reviews.json').write_text(json.dumps(registry),encoding='utf8')
    with pytest.raises(ValueError,match='approval/package differs'):
        verify(reobserved_evidence,root=tmp_path)


@pytest.mark.parametrize('fail_second', [False,True])
def test_real_native_multisession_chain_and_resume_only_failed_session(tmp_path, monkeypatch, fail_second):
    fixture = json.loads((ROOT/'tests/fixtures/share_chain_002316_Nov4_to_Nov6.json').read_bytes())
    directory=tmp_path/'.microcap_index_cache/share_sources'
    original={}
    for name,item in fixture['sources'].items():
        raw=zlib.decompress(base64.b64decode(item['zlib_b64']))
        assert source.sha(raw)==item['sha256']
        p=directory/name;p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(raw)
        original[name]=raw
    pointer=directory/'002316.json'
    pointer.write_bytes(original[fixture['start_state_path']])
    candidate=base64.b64decode(fixture['candidate_b64'])
    cache=tmp_path/'.microcap_index_cache/share_change/002316.csv'
    cache.parent.mkdir(parents=True);cache.write_bytes(candidate)
    latest=json.loads((ROOT/'tests/fixtures/historical_share_close_002316_20261109.json').read_bytes())
    for name,encoded in latest['artifacts_b64'].items():
        p=directory/name;p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(base64.b64decode(encoded))
    manifest_raw=json.dumps(latest['manifest']).encode()
    mp=directory/'providers/real-Nov6-manifest.json';mp.parent.mkdir(parents=True,exist_ok=True);mp.write_bytes(manifest_raw)
    refs={'2026-11-05':fixture['nov5_manifest'],
          '2026-11-06':dict(path=mp.relative_to(directory).as_posix(),sha256=source.sha(manifest_raw))}
    calls=[]
    def collect(**kwargs):
        assert kwargs['candidate']==candidate
        calls.append(kwargs['end'])
        if fail_second and calls==['2026-11-05','2026-11-06']:
            raise ValueError('independent second-session evidence rejected')
        return refs[kwargs['end']]
    monkeypatch.setattr(source,'collect_manifest',collect)
    monkeypatch.setattr(native.requests,'post',lambda *a,**k:pytest.fail('denied primary queried'))
    assert native.verify_share_cache(root=tmp_path,symbol='002316',as_of='2026-11-04')
    if fail_second:
        with pytest.raises(ValueError,match='second-session evidence rejected'):
            native.refresh_share_change(root=tmp_path,symbol='002316',cache_path=cache,as_of='2026-11-06',allow_network=True)
        assert native.verify_share_cache(root=tmp_path,symbol='002316',as_of='2026-11-05')
        assert cache.read_bytes()==candidate
    native.refresh_share_change(root=tmp_path,symbol='002316',cache_path=cache,as_of='2026-11-06',allow_network=True)
    assert native.verify_share_cache(root=tmp_path,symbol='002316',as_of='2026-11-06')
    assert calls==(['2026-11-05','2026-11-06','2026-11-06'] if fail_second else ['2026-11-05','2026-11-06'])
    assert cache.read_bytes()==candidate
    assert all((directory/name).read_bytes()==raw for name,raw in original.items() if name!='002316.json')
    assert not (directory/'locks/002316.lock').exists()
