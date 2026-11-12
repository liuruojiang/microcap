"""Real provider bodies and dated inventories; explicitly software transport envelopes."""
import base64
from copy import deepcopy
from datetime import datetime, timezone
import gzip
import hashlib
import json
from pathlib import Path
import re
from urllib.parse import urlencode

import pytest
import requests

from scripts import share_unchanged_tail as quote
from scripts import share_change_source as shares
from frozen_exchange_calendar import frozen_exchange_sessions

ROOT = Path(__file__).resolve().parents[1]
DAY = '2026-11-12'
NOW = datetime.fromisoformat('2026-11-12T11:00:00+00:00')


@pytest.fixture
def data(monkeypatch):
    monkeypatch.setattr(quote, '_QUOTE_ENDPOINT_FAILURES', {})
    return json.loads(gzip.decompress((ROOT/'tests/fixtures/share_typed_quote_20261112.json.gz').read_bytes()))


def raw(data, provider):
    return base64.b64decode(data['quotes'][provider]['raw'])


def replace_field(body, prefix, symbol, index, value, separator):
    text = body.decode('gb18030')
    pattern = re.compile('('+prefix+symbol+r'=")([^"]*)(";)')
    def change(match):
        fields = match[2].split(separator);fields[index]=value
        return match[1]+separator.join(fields)+match[3]
    changed, count = pattern.subn(change, text)
    assert count == 1
    return changed.encode('gb18030')


@pytest.mark.parametrize('symbol,total', [('000953','366122195'),('000545','986833096')])
def test_actual_tencent_and_sina_total_fields_are_exact_shares(data,symbol,total):
    facts=quote.parse_typed_quote(raw(data,'tencent'),raw(data,'sina'),symbol=symbol,
        symbols=data['symbols'],end=DAY,receipts=[data['quotes'][p]['receipt'] for p in ['tencent','sina']])
    assert facts['total_shares']==total and facts['source_market_dates']==dict(tencent=DAY,sina=DAY)
    assert facts['tencent_total_field']==73 and facts['sina_total_field']==7
    assert facts['independent_capital_asof_guarantee_proven'] is False


def test_same_issuer_legal_name_update_is_diagnostic_not_a_new_capital_gate(data):
    body=replace_field(raw(data,'tencent'),'v_','sz000953',1,'河化股份','~')
    facts=quote.parse_typed_quote(body,raw(data,'sina'),symbol='000953',symbols=data['symbols'],end=DAY,
        receipts=[data['quotes'][p]['receipt'] for p in ['tencent','sina']])
    assert facts['total_shares']=='366122195' and facts['source_names_agree'] is False
    assert facts['source_names']==dict(tencent='河化股份',sina='中哲精化',sina_information='中哲精化')


def test_next_morning_provider_dates_remain_distinct_without_blocking_previous_target(data):
    body=replace_field(raw(data,'tencent'),'v_','sz000953',30,'20261113090500','~')
    receipts=[dict(http_complete='2026-11-13T01:10:00+00:00') for _ in range(2)]
    facts=quote.parse_typed_quote(body,raw(data,'sina'),symbol='000953',symbols=data['symbols'],end=DAY,receipts=receipts)
    assert facts['total_shares']=='366122195'
    assert facts['source_market_dates']==dict(tencent='2026-11-13',sina=DAY)
    assert facts['source_market_dates_agree'] is False and 'market_date' not in facts


@pytest.mark.parametrize('fault', ['float_field','one_share','unit','issuer','asset','stale','future','duplicate'])
def test_typed_total_rejects_semantic_conflicts(data,fault):
    t,s=raw(data,'tencent'),raw(data,'sina')
    if fault=='float_field':t=replace_field(t,'v_','sz000545',73,'985403096','~')
    elif fault=='one_share':t=replace_field(t,'v_','sz000545',73,'986833097','~')
    elif fault=='unit':s=replace_field(s,'hq_str_','sz000545_i',7,'986833096',',')
    elif fault=='issuer':t=replace_field(t,'v_','sz000545',2,'000953','~')
    elif fault=='asset':s=replace_field(s,'hq_str_','sz000545_i',0,'I',',')
    elif fault=='stale':t=replace_field(t,'v_','sz000545',30,'20261111161415','~')
    elif fault=='future':t=replace_field(t,'v_','sz000545',30,'20261113161415','~')
    else:t+=t
    with pytest.raises(ValueError):
        quote.parse_typed_quote(t,s,symbol='000545',symbols=data['symbols'],end=DAY,
            receipts=[data['quotes'][p]['receipt'] for p in ['tencent','sina']])


def typed_manifest(data,symbol):
    bodies={};files={};urls=quote.typed_quote_urls(data['symbols'])
    def add(name,body):bodies[name]=body;return dict(path=name,sha256=quote.sha(body))
    bad=add('software_502.raw',b'Software transport fixture: 502')
    receipt=dict(url=quote.QUOTE_URL,params=quote.quote_params(symbol),http_status=502,
        http_start='2026-11-12T10:39:00+00:00',http_complete='2026-11-12T10:39:01+00:00',
        tls_verification=True,raw_sha256=bad['sha256'],bytes=len(bodies[bad['path']]))
    failed=add('software_502.receipt.json',json.dumps(receipt).encode())
    failure=add('primary_failure.json',json.dumps(dict(schema='quote_primary_transport_failure_v1',symbol=symbol,
        target=DAY,attempts=[dict(raw=bad,receipt=failed)])).encode())
    for provider,key,url in zip(['tencent','sina'],['tencent_quote','sina_quote'],urls):
        body=raw(data,provider);p=provider+'.raw';rp=provider+'.receipt.json'
        original=data['quotes'][provider]['receipt']
        r=dict(schema='share_unchanged_tail_receipt_v1',url=url,actual_url=url,params={},
            http_status=200,tls_verification=True,raw_sha256=quote.sha(body),bytes=len(body),
            http_start=original['http_start'],http_complete=original['http_complete'])
        add(p,body);add(rp,json.dumps(r).encode())
        files[key]=dict(raw_path=p,raw_sha256=quote.sha(body),receipt_path=rp,receipt_sha256=quote.sha(bodies[rp]))
    return dict(quote_source=quote.TYPED_QUOTE,quote_symbols=data['symbols'],quote_primary_failure=failure,files=files),bodies


@pytest.mark.parametrize('symbol',['000953','000545'])
@pytest.mark.parametrize('next_morning',[False,True])
def test_full_original_dated_inventory_and_prefix_gate_with_typed_quote(data,symbol,next_morning,frozen_exchange_sessions):
    fixture=data['dated'][symbol];candidate=base64.b64decode(fixture['candidate'])
    bodies={name:base64.b64decode(value) for name,value in fixture['bodies'].items()}
    files={}
    for purpose in ['dated_window','unfiltered_1','bounded_1','sina']:
        p=fixture['prefix']+purpose+'.raw';rp=fixture['prefix']+purpose+'.receipt.json'
        ref=dict(raw_path=p,raw_sha256=quote.sha(bodies[p]),receipt_path=rp,receipt_sha256=quote.sha(bodies[rp]))
        if purpose=='unfiltered_1':files['unfiltered']=[ref]
        elif purpose=='bounded_1':files['bounded']=[ref]
        else:files[purpose]=ref
    typed,extra=typed_manifest(data,symbol);files.update(typed.pop('files'));bodies.update(extra)
    now=NOW
    if next_morning:
        bodies['tencent.raw']=replace_field(bodies['tencent.raw'],'v_','sz'+symbol,30,'20261113090500','~')
        for provider in ['tencent','sina']:
            entry=files[provider+'_quote'];p=entry['raw_path'];rp=entry['receipt_path']
            entry['raw_sha256']=quote.sha(bodies[p]);receipt=json.loads(bodies[rp])
            receipt.update(raw_sha256=entry['raw_sha256'],bytes=len(bodies[p]),
                http_start='2026-11-13T01:10:00+00:00',http_complete='2026-11-13T01:10:02+00:00')
            bodies[rp]=json.dumps(receipt).encode();entry['receipt_sha256']=quote.sha(bodies[rp])
        now=datetime.fromisoformat('2026-11-13T01:30:00+00:00')
    manifest=dict(schema=shares.DATED_WINDOW_KIND,symbol=symbol,previous_day='2026-11-11',query_end=DAY,
        previous_candidate_sha256=quote.sha(candidate),future_plan_documents=[],files=files,**typed)
    reads=[]
    def bound(name,digest):
        body=bodies[name];assert quote.sha(body)==digest;reads.append(name);return body
    actual,facts=shares.verify_dated_window(manifest,symbol=symbol,previous_day='2026-11-11',end=DAY,
        previous_candidate=candidate,bound=bound,root=ROOT,now=now)
    assert actual==candidate and facts['old_prefix_preserved'] is True
    assert facts['typed_quote']['total_shares'] in ['366122195','986833096']
    if next_morning:
        assert facts['typed_quote']['source_market_dates']==dict(tencent='2026-11-13',sina=DAY)
    assert all(any(purpose in name for name in reads) for purpose in ['dated_window','unfiltered_1','bounded_1','sina.raw'])
    # Matching two bad current providers cannot revise dated terminal shares.
    for provider,prefix,separator,index,value in [('tencent','v_','~',73,'366122196' if symbol=='000953' else '986833097'),
            ('sina','hq_str_',',',7,'36612.2196' if symbol=='000953' else '98683.3097')]:
        ticker='sz'+symbol+('_i' if provider=='sina' else '')
        p=provider+'.raw';bodies[p]=replace_field(bodies[p],prefix,ticker,index,value,separator)
        entry=files[provider+'_quote'];entry['raw_sha256']=quote.sha(bodies[p])
        rp=entry['receipt_path'];r=json.loads(bodies[rp]);r['raw_sha256']=entry['raw_sha256'];r['bytes']=len(bodies[p])
        bodies[rp]=json.dumps(r).encode();entry['receipt_sha256']=quote.sha(bodies[rp])
    with pytest.raises(ValueError,match='dated terminal total'):
        shares.verify_dated_window(manifest,symbol=symbol,previous_day='2026-11-11',end=DAY,
            previous_candidate=candidate,bound=bound,root=ROOT,now=now)


class Response:
    def __init__(self,url,params,body,status=200):
        self.url=url+('?' + urlencode(params) if params else '');self.content=body;self.status_code=status;self.headers={}
    def raise_for_status(self):
        if self.status_code>=400:raise requests.HTTPError('Software HTTP fixture '+str(self.status_code))
    def close(self):
        pass


def mocked_get(data,calls,primary_status=502):
    def request(url,*,params,**kwargs):
        calls.append(url)
        if url==quote.QUOTE_URL:
            return Response(url,params,b'Software primary response',primary_status)
        provider='tencent' if url.startswith('https://qt.gtimg.cn/') else 'sina'
        tickers=re.findall(r'(?:sz|sh)[0-9]{6}(?:_i)?',url)
        body=raw(data,provider).decode('gb18030')
        prefix='v_' if provider=='tencent' else 'hq_str_'
        selected='\n'.join(re.findall(r'(?:var\s+)?'+prefix+'(?:'+'|'.join(tickers)+r')="[^"]*";',body))
        return Response(url,params,selected.encode('gb18030'))
    return request


def collect(data,tmp_path,monkeypatch,symbol='000953',status=502):
    calls=[];monkeypatch.setattr(quote.requests,'get',mocked_get(data,calls,status))
    directory=tmp_path/symbol;directory.mkdir(exist_ok=True)
    return directory,calls,quote.collect_quote_inputs(directory=directory,source=tmp_path,symbol=symbol,end=DAY)


def test_one_primary_attempt_then_two_real_source_shapes_and_restart_no_http(data,tmp_path,monkeypatch):
    directory,calls,(meta,files)=collect(data,tmp_path,monkeypatch)
    assert calls.count(quote.QUOTE_URL)==1 and len(calls)==3 and meta['quote_source']==quote.TYPED_QUOTE
    original=(tmp_path/meta['quote_primary_failure']['path']).read_bytes()
    again=quote.collect_quote_inputs(directory=directory,source=tmp_path,symbol='000953',end=DAY)
    assert again==(meta,files) and len(calls)==3
    assert (tmp_path/meta['quote_primary_failure']['path']).read_bytes()==original
    def bound(name,digest):
        raw=(tmp_path/name).read_bytes();assert quote.sha(raw)==digest;return raw
    facts=quote.verify_typed_quote(dict(**meta,files=files),symbol='000953',end=DAY,bound=bound,now=datetime.now(timezone.utc))
    assert facts['total_shares']=='366122195'


def test_run_endpoint_outage_short_circuits_second_issuer_without_resetting_own_failure(data,tmp_path,monkeypatch):
    _,calls,_=collect(data,tmp_path,monkeypatch,'000953')
    folder=tmp_path/'000545';folder.mkdir()
    meta,files=quote.collect_quote_inputs(directory=folder,source=tmp_path,symbol='000545',end=DAY)
    assert calls.count(quote.QUOTE_URL)==1 and len(calls)==5
    def bound(name,digest):
        raw=(tmp_path/name).read_bytes();assert quote.sha(raw)==digest;return raw
    assert quote.verify_typed_quote(dict(**meta,files=files),symbol='000545',end=DAY,bound=bound,
        now=datetime.now(timezone.utc))['total_shares']=='986833096'


@pytest.mark.parametrize('status',[200,403])
def test_primary_content_or_http_refusal_never_uses_fallback(data,tmp_path,monkeypatch,status):
    if status==403:
        with pytest.raises(requests.HTTPError):collect(data,tmp_path,monkeypatch,status=status)
    else:
        _,calls,(meta,files)=collect(data,tmp_path,monkeypatch,status=status)
        assert meta=={} and 'quote' in files and calls==[quote.QUOTE_URL]


def test_unknown_primary_budget_cannot_start_another_attempt(data,tmp_path,monkeypatch):
    ledger=tmp_path/'quote_failover/000953'/DAY;ledger.mkdir(parents=True)
    (ledger/'primary_started.json').write_bytes(b'original unknown outcome')
    folder=tmp_path/'source';folder.mkdir()
    monkeypatch.setattr(quote.requests,'get',lambda *a,**k:pytest.fail('Unknown main request must not repeat'))
    with pytest.raises(ValueError,match='outcome unknown'):
        quote.collect_quote_inputs(directory=folder,source=tmp_path,symbol='000953',end=DAY)
