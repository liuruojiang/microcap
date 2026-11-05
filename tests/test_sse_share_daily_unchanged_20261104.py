"""Real preserved source bodies; mutate facts without mocking native validation."""
import base64
import copy
import hashlib
import json
from pathlib import Path
import shutil
import zlib
import pytest
import requests
from scripts import sse_share_daily_unchanged_20261104 as sse
from scripts import share_unchanged_tail as native
from scripts import share_change_source as chain
from scripts.realtime_state_bundle import _verify_share_source_json_protocol
from frozen_exchange_calendar import frozen_exchange_sessions


@pytest.fixture(autouse=True)
def calendar(frozen_exchange_sessions):
    return frozen_exchange_sessions


@pytest.fixture
def evidence(tmp_path):
    path=Path(__file__).parent/'fixtures/sse_603028_actual_20261104.json.zlib.b64'
    fixture=json.loads(zlib.decompress(base64.b64decode(path.read_bytes())))
    bodies={name:base64.b64decode(raw) for name,raw in fixture['objects_b64'].items()}
    candidate=base64.b64decode(fixture['candidate_b64'])
    root=tmp_path/'root';root.mkdir()
    shutil.copytree(Path(sse.__file__).resolve().parents[1]/sse.PREFIX,root/sse.PREFIX)
    (root/'data/share_source_future_plan_reviews.json').write_bytes((Path(sse.__file__).resolve().parents[1]/'data/share_source_future_plan_reviews.json').read_bytes())
    return root,copy.deepcopy(fixture['manifest']),candidate,bodies,fixture


def verify(evidence, **overrides):
    root,m,candidate,bodies,_=evidence
    def bound(name,digest):
        raw=bodies[name]
        if hashlib.sha256(raw).hexdigest()!=digest:raise ValueError('source digest changed')
        return raw
    kw=dict(symbol='603028',previous_day='2026-11-03',end='2026-11-04',candidate=candidate,bound=bound,root=root)
    kw.update(overrides)
    return sse.verify_manifest(m,**kw)


def rewrite(evidence,key,change,receipt=False):
    _,m,_,bodies,_=evidence;ref=m['files'][key]
    if isinstance(ref,list):ref=ref[0]
    if receipt:
        value=json.loads(bodies[ref['receipt_path']]);change(value);raw=json.dumps(value,ensure_ascii=False).encode();bodies[ref['receipt_path']]=raw;ref['receipt_sha256']=hashlib.sha256(raw).hexdigest();return
    value=json.loads(bodies[ref['raw_path']]);change(value);raw=json.dumps(value,ensure_ascii=False).encode();bodies[ref['raw_path']]=raw;ref['raw_sha256']=hashlib.sha256(raw).hexdigest()
    value=json.loads(bodies[ref['receipt_path']]);value.update(raw_sha256=ref['raw_sha256'],bytes=len(raw));raw=json.dumps(value).encode();bodies[ref['receipt_path']]=raw;ref['receipt_sha256']=hashlib.sha256(raw).hexdigest()


def test_real_approved_snapshot_preserves_history_and_discloses_conflict(evidence):
    r=verify(evidence)
    assert r['total_shares_10k']=='28514.52' and r['conflicting_commercial_total_shares_10k']=='28483.52'
    assert r['source_conflict_disclosed'] and r['legal_cancellation_completion_not_claimed']


@pytest.mark.parametrize('changes',[{'symbol':'603029'},{'previous_day':'2026-11-02'},{'end':'2026-11-05'},{'candidate':b'changed history'}])
def test_other_stock_day_or_history_has_no_authority(evidence,changes):
    with pytest.raises(ValueError):verify(evidence,**changes)


@pytest.mark.parametrize('change',[
    lambda d:d['result'][0].update(TRADE_DATE='20261103'),
    lambda d:d['result'][0].update(TRADE_DATE='20261105'),
    lambda d:d['result'][0].update(TOTAL_DOMESTIC_VOL='28483.52'),
    lambda d:d['result'][0].update(A_LIMIT_VOL='127.00'),
    lambda d:d.update(result=[]),
    lambda d:d['result'].append(dict(d['result'][0])),
    lambda d:d.update(sqlId='other'),
    lambda d:d.update(actionErrors=['provider error']),
])
def test_sse_date_total_inventory_and_error_mutations_fail(evidence,change):
    rewrite(evidence,'sse_capital',change)
    with pytest.raises(ValueError):verify(evidence)


@pytest.mark.parametrize('change',[
    lambda d:d['result'][0].update(COMPANY_CODE='603029'),
    lambda d:d['result'][0].update(FULL_NAME='other issuer'),
    lambda d:d['pageHelp'].update(total=True),
])
def test_sse_issuer_mutations_fail(evidence,change):
    rewrite(evidence,'sse_listing',change)
    with pytest.raises(ValueError):verify(evidence)


@pytest.mark.parametrize('change',[
    lambda d:d.update(tls_verification=False),
    lambda d:d.update(http_status=503),
    lambda d:d.update(http_start='2026-11-04T14:59:59+08:00'),
    lambda d:d.update(http_complete='2099-11-04T21:00:00+08:00'),
    lambda d:d.update(actual_url='https://query.sse.com.cn/commonQuery.do?COMPANY_CODE=603029'),
])
def test_sse_tls_clock_and_actual_query_are_required(evidence,change):
    rewrite(evidence,'sse_capital',change,receipt=True)
    with pytest.raises(ValueError):verify(evidence)


@pytest.mark.parametrize('key,change',[
    ('unfiltered',lambda d:d['result']['data'][0].update(TOTAL_SHARES=284835201)),
    ('bounded',lambda d:d['result']['data'][0].update(LISTING_DATE='2026-11-03 00:00:00')),
    ('quote',lambda d:d['data'].update(f84=285145200)),
    ('quote',lambda d:d['data'].update(f86=0)),
    ('quote',lambda d:d['data'].update(f57='603029')),
])
def test_other_source_conflicts_are_not_generally_waived(evidence,key,change):
    rewrite(evidence,key,change)
    with pytest.raises(ValueError):verify(evidence)


@pytest.mark.parametrize('what',['approval','review','disclosure','completion'])
def test_exact_approval_and_disclosure_cannot_be_removed(evidence,what):
    root,m,_,_,_=evidence
    if what in ('approval','review'):
        p=root/sse.PREFIX/(what+'.json');p.write_bytes(p.read_bytes()+b' ')
    elif what=='disclosure':m.pop('source_selection_disclosure')
    else:m['source_selection_disclosure']['legal_cancellation_completion_not_claimed']=False
    with pytest.raises(ValueError):verify(evidence)


def install_old_chain(evidence):
    root,_,candidate,bodies,fixture=evidence;directory=root/'.microcap_index_cache/share_sources';directory.mkdir(parents=True)
    for name,raw in bodies.items():
        p=directory/name;p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(raw)
    previous=fixture['state']['previous_state'];(directory/'603028.json').write_bytes(bodies[previous['path']])
    csv=root/'.microcap_index_cache/share_change/603028.csv';csv.parent.mkdir();csv.write_bytes(candidate)
    chain.verify_share_cache(root=root,symbol='603028',as_of='2026-11-03')
    return root,directory,csv


def test_full_native_collection_publication_and_transport_use_real_preserved_bodies(evidence,monkeypatch):
    root,directory,csv=install_old_chain(evidence);_,m,candidate,bodies,_=evidence;calls=[]
    def get(url,*,params,headers,timeout,verify):
        assert verify is True and timeout==20
        if url==native.EM_URL:key='bounded' if 'END_DATE<=' in str(params.get('filter','')) else 'unfiltered'
        elif url==native.SINA_URL.format(symbol='603028'):key='sina'
        elif url==native.QUOTE_URL:key='quote'
        elif url==sse.SSE_URL:
            assert headers['Referer']==sse.SSE_REFERER
            key='sse_capital' if params==sse.CAPITAL_PARAMS else 'sse_listing'
        else:raise AssertionError('Unexpected source request '+url)
        ref=m['files'][key];ref=ref[0] if isinstance(ref,list) else ref;raw=bodies[ref['raw_path']]
        response=requests.Response();response.status_code=200;response.url=requests.Request('GET',url,params=params).prepare().url;response._content=raw;response._content_consumed=True;response.headers={'Content-Type':'application/json'};calls.append(key);return response
    monkeypatch.setattr(native.requests,'get',get)
    before=csv.read_bytes();previous=(directory/'603028.json').read_bytes()
    chain.refresh_share_change(root=root,symbol='603028',cache_path=csv,as_of='2026-11-04',allow_network=True)
    proof=chain.verify_share_cache(root=root,symbol='603028',as_of='2026-11-04');_verify_share_source_json_protocol(root,'603028',proof)
    state=json.loads((directory/'603028.json').read_bytes());assert state['kind']==sse.KIND
    assert csv.read_bytes()==before==candidate and (directory/state['previous_state']['path']).read_bytes()==previous
    assert set(calls)=={'unfiltered','bounded','sina','quote','sse_listing','sse_capital'}
    assert not list((directory/'locks').glob('*.lock'))
    disclosure=sse.disclosures(root,'2026-11-04');assert len(disclosure)==1
    assert '28514.52万股' in disclosure[0]['text'] and '28483.52万股' in disclosure[0]['text']
    assert sse.disclosures(root,'2026-11-05')==[]


def test_real_state_and_manifest_provider_cannot_disagree(evidence):
    root,directory,csv=install_old_chain(evidence);fixture=evidence[-1];state=copy.deepcopy(fixture['state']);state['kind']=native.KIND;(directory/'603028.json').write_text(json.dumps(state))
    with pytest.raises(ValueError,match='kind differs'):chain.verify_share_cache(root=root,symbol='603028',as_of='2026-11-04')


def test_whole_transport_closure_includes_policy_and_original_approval_bytes():
    from scripts.top100_cloud_delivery import SOURCE_FILES
    assert set(sse.SOURCE_FILES)<=set(SOURCE_FILES)


# D5 transition: original reviewed source bytes plus explicit synthetic quote
# frames for offline control tests. This is never live quote acceptance.
def transition_package(root):
    import io,zipfile
    registry=json.loads((root/'data/share_source_future_plan_reviews.json').read_bytes())
    with zipfile.ZipFile(io.BytesIO(base64.b64decode(registry['sse_transition_approval']['package_base64']))) as z:
        return {name:z.read(name) for name in z.namelist()}


def transition_http(evidence, monkeypatch, fault=None):
    from datetime import datetime
    root,m,old,bodies,fixture=evidence
    package=transition_package(root)
    def get(url,*,params,headers,timeout,verify):
        assert verify is True
        if url==native.EM_URL:
            raw=package['source/em_bounded.raw']
            if fault=='event':
                data=json.loads(raw);data['result']['data'][0]['TOTAL_SHARES']=284835201;raw=json.dumps(data).encode()
        elif url==native.SINA_URL.format(symbol='603028'):raw=package['source/sina.raw']
        elif url==native.QUOTE_URL:
            # Clearly synthetic offline same-contract quote; real live gate still mandatory.
            raw=bodies[m['files']['quote']['raw_path']];data=json.loads(raw)
            stamp=int(datetime.fromisoformat('2026-11-05T15:00:00+08:00').timestamp())
            data['data'].update(f86=stamp,f124=stamp)
            if fault=='stale':data['data']['f86']=stamp-86400
            if fault=='count':data['data']['f84']=285145200
            if fault=='issuer':data['data']['f57']='603029'
            raw=json.dumps(data).encode()
        else:raise AssertionError(url)
        response=requests.Response();response.status_code=502 if fault=='quote502' and url==native.QUOTE_URL else 200
        response.url=requests.Request('GET',url,params=params).prepare().url
        response._content=raw;response._content_consumed=True;response.headers={}
        return response
    monkeypatch.setattr(native.requests,'get',get)
    monkeypatch.setattr(native.time,'sleep',lambda _:None)
    return package


def install_d4_chain(evidence):
    root,directory,csv=install_old_chain(evidence)
    state=evidence[-1]['state']
    (directory/'603028.json').write_text(json.dumps(state),encoding='utf-8')
    chain.verify_share_cache(root=root,symbol='603028',as_of='2026-11-04')
    return root,directory,csv


def test_d5_native_append_preserves_complete_d4_chain_and_roundtrip(evidence,monkeypatch):
    root,directory,csv=install_d4_chain(evidence)
    package=transition_http(evidence,monkeypatch)
    before=csv.read_bytes();previous=(directory/'603028.json').read_bytes()
    result=chain.refresh_share_change(root=root,symbol='603028',cache_path=csv,as_of='2026-11-05',allow_network=True)
    assert csv.read_bytes()==package['candidate_603028.csv'] and csv.read_bytes().startswith(before)
    state=json.loads((directory/'603028.json').read_bytes())
    assert state['kind']==sse.TRANSITION_KIND and (directory/state['previous_state']['path']).read_bytes()==previous
    proof=chain.verify_share_cache(root=root,symbol='603028',as_of='2026-11-05')
    _verify_share_source_json_protocol(root,'603028',proof)
    assert str(result.iloc[-1]['change_date'].date())=='2026-11-05'
    monkeypatch.setattr(native.requests,'get',lambda *a,**k:pytest.fail('completed day repeated network'))
    chain.refresh_share_change(root=root,symbol='603028',cache_path=csv,as_of='2026-11-05',allow_network=True)
    csv.write_bytes(before)
    with pytest.raises(ValueError):chain.verify_share_cache(root=root,symbol='603028',as_of='2026-11-05')


@pytest.mark.parametrize('fault',['stale','count','issuer','quote502','event'])
def test_transition_does_not_waive_live_quote_or_commercial_source(evidence,monkeypatch,fault):
    root,directory,csv=install_d4_chain(evidence);transition_http(evidence,monkeypatch,fault)
    before=csv.read_bytes();state=(directory/'603028.json').read_bytes()
    with pytest.raises((ValueError,requests.HTTPError)):
        chain.refresh_share_change(root=root,symbol='603028',cache_path=csv,as_of='2026-11-05',allow_network=True)
    assert csv.read_bytes()==before and (directory/'603028.json').read_bytes()==state


@pytest.mark.parametrize('fault',['old_history','approval','package','day','symbol'])
def test_d5_exact_approval_cannot_expand_or_change_history(evidence,fault):
    root,_,old,_,_=evidence
    kwargs=dict(root=root,symbol='603028',previous_day='2026-11-04',end='2026-11-05',previous_candidate=old)
    if fault=='old_history':kwargs['previous_candidate']=old+b'\n'
    elif fault=='day':kwargs['end']='2026-11-06'
    elif fault=='symbol':kwargs['symbol']='603029'
    else:
        path=root/'data/share_source_future_plan_reviews.json';registry=json.loads(path.read_bytes())
        if fault=='approval':registry['sse_transition_approval']['approval']['decision']='pending'
        else:registry['sse_transition_approval']['package_base64']=base64.b64encode(b'corrupt').decode()
        path.write_text(json.dumps(registry),encoding='utf-8')
    with pytest.raises(ValueError):sse.approved_transition(**kwargs)
