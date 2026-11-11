"""Actual catalog/PDF evidence; missing HTTP502 quotes never become approval."""
import base64
import copy
from datetime import datetime
from decimal import Decimal
from functools import cache
import gzip
import json
from pathlib import Path
from urllib.parse import urlencode

import pytest

from scripts import share_change_source as source
from scripts import share_unchanged_tail as native
from frozen_exchange_calendar import frozen_exchange_sessions

ROOT=Path(__file__).resolve().parents[1]


@pytest.fixture(autouse=True)
def calendar(frozen_exchange_sessions):return frozen_exchange_sessions


@cache
def recorded():
    return json.loads(gzip.decompress((ROOT/'tests/fixtures/share_catalog_and_plans_20261111.json.gz').read_bytes()))


def case(symbol):
    item=copy.deepcopy(next(c for c in recorded()['cases'] if c['symbol']==symbol))
    return item,{p:base64.b64decode(raw) for p,raw in item['objects_b64'].items()}


def bound_for(bodies):
    def bound(path,digest):
        raw=bodies[path]
        if source._sha(raw)!=digest:raise ValueError('Source object digest differs')
        return raw
    return bound


def verify(value):
    item,bodies=value
    return source.verify_dated_window(item['manifest'],symbol=item['symbol'],previous_day=item['previous_day'],
        end=item['end'],previous_candidate=base64.b64decode(item['previous_candidate_b64']),bound=bound_for(bodies),
        root=ROOT,now=datetime.fromisoformat(recorded()['observed_at']))


def rewrite(value,entry,raw):
    bodies=value[1];bodies[entry['raw_path']]=raw;entry['raw_sha256']=source._sha(raw)
    receipt=json.loads(bodies[entry['receipt_path']]);receipt.update(raw_sha256=source._sha(raw),bytes=len(raw))
    body=json.dumps(receipt).encode();bodies[entry['receipt_path']]=body;entry['receipt_sha256']=source._sha(body)


def test_actual_catalog_variant_preserves_total_plan_status_and_old_hash():
    value=case('301429');candidate,facts=verify(value)
    assert candidate==base64.b64decode(value[0]['previous_candidate_b64']) and facts['changes']==[]
    assert facts==value[0]['manifest']['verified_facts']
    assert facts['future_structure_not_consumed']['provider_dates']==['2026-11-12']
    with gzip.open(ROOT/'tests/fixtures/structure_reobservation_301429_20261110.json.gz','rb') as stream:old=json.load(stream)
    entry=old['manifest']['files']['unfiltered'][0]
    previous=next(r for r in json.loads(base64.b64decode(old['objects_b64'][entry['raw_path']]),parse_float=Decimal)['result']['data']
                  if r['END_DATE'][:10]=='2026-11-12')
    entry=value[0]['manifest']['files']['unfiltered'][0]
    current=next(r for r in json.loads(value[1][entry['raw_path']],parse_float=Decimal)['result']['data']
                 if r['END_DATE'][:10]=='2026-11-12')
    assert {k for k in previous.keys()|current.keys() if previous.get(k)!=current.get(k)}=={'FREELIQCI_SHARES'}
    assert (previous['FREELIQCI_SHARES'],current['FREELIQCI_SHARES'])==(35884736,35884739)
    assert source.EXACT_FUTURE_STRUCTURE_301429['row_sha256_by_day']['2026-11-12']=='1eeabcec4ff5f278a00cd2e4b3e702d5858881c1edec4fc2eb11f761bf959843'


@pytest.mark.parametrize('field',['FREELIQCI_SHARES','TOTAL_SHARES','NOTICE_DATE','INFO_CODE'])
def test_catalog_equivalence_accepts_no_other_row_identity(field):
    value=case('301429');entry=value[0]['manifest']['files']['unfiltered'][0];payload=json.loads(value[1][entry['raw_path']])
    row=next(r for r in payload['result']['data'] if r['END_DATE'][:10]=='2026-11-12')
    row[field]=row[field]+1 if isinstance(row[field],int) else 'unreviewed'
    rewrite(value,entry,json.dumps(payload).encode())
    with pytest.raises(ValueError):verify(value)


@pytest.mark.parametrize('symbol',['301429','300535','300644'])
def test_actual_old10_share_source_chain_revalidates_with_new_reader_and_registry(tmp_path,symbol):
    item=case(symbol)[0];root=source._extended(tmp_path);directory=root/'.microcap_index_cache/share_sources'
    for name,raw in item['prefix_objects_b64'].items():
        path=source._extended(directory/name);path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(base64.b64decode(raw))
    old=base64.b64decode(item['previous_candidate_b64']);p=root/('.microcap_index_cache/share_change/'+symbol+'.csv')
    p.parent.mkdir(parents=True);p.write_bytes(old)
    registry=root/'data/share_source_future_plan_reviews.json';registry.parent.mkdir()
    registry.write_bytes((ROOT/'data/share_source_future_plan_reviews.json').read_bytes())
    result=source.verify_share_cache(root=root,symbol=symbol,as_of='2026-11-10')
    assert result['query_end']=='2026-11-10' and result['candidate_sha256']==source._sha(old)
    assert p.read_bytes()==old


def future_plan_readback(value):
    item,bodies=value;files=item['manifest']['files'];bound=bound_for(bodies)
    rows=native._em_inventory(files['unfiltered'],bound,item['symbol'],item['end'],False,datetime.fromisoformat(recorded()['observed_at']))
    future=[r for r in rows if r['END_DATE'][:10]>item['end']]
    review=next(r for r in native.plan_reviews(ROOT) if r['symbol']==item['symbol'])
    return native._reviewed_future_plans(item['manifest'],future,symbol=item['symbol'],end=item['end'],
        boundary=Decimal(review['before_total_shares'])/10000,bound=bound,now=datetime.fromisoformat(recorded()['observed_at']),root=ROOT)


@pytest.mark.parametrize('symbol,after',[('300535','10817.7343'),('300644','11097.3506')])
def test_actual_unexecuted_plan_documents_match_future_inventory_only(symbol,after):
    value=case(symbol);allowed,disclosures=future_plan_readback(value)
    assert allowed==[dict(date='2026-11-12',total_shares_10k=after)] and len(disclosures)==1
    assert value[0]['real_collection_status']=='blocked_missing_quote_after_actual_HTTP502_exhaustion'
    # Actual captures do not contain a successful quote. The full source gate
    # therefore stays incomplete even though the future-only review matches.
    with pytest.raises(KeyError,match='quote'):verify(value)


@pytest.mark.parametrize('fault',['pdf','quantity','date','announcement'])
@pytest.mark.parametrize('symbol',['300535','300644'])
def test_new_future_plan_review_does_not_waive_document_date_or_quantity(symbol,fault):
    value=case(symbol);files=value[0]['manifest']['files']
    if fault=='pdf':
        entry=value[0]['manifest']['future_plan_documents'][0];rewrite(value,entry,value[1][entry['raw_path']]+b'changed')
    else:
        entry=files['unfiltered'][0];payload=json.loads(value[1][entry['raw_path']]);row=payload['result']['data'][0]
        if fault=='quantity':row['TOTAL_SHARES']+=1
        elif fault=='date':row['END_DATE']='2026-11-13 00:00:00'
        else:row['INFO_CODE']='AN00000000'
        rewrite(value,entry,json.dumps(payload).encode())
    with pytest.raises(ValueError):future_plan_readback(value)


def software_quote_only(symbol):
    """Supply a synthetic quote solely to test the future-exclusion branch.

    The real quote is unavailable. This generated object must never be treated
    as market evidence or used to mark the actual collection complete.
    """
    value=case(symbol);item,bodies=value;item['software_quote_only']=True
    template=case('301429');entry=template[0]['manifest']['files']['quote']
    quote=json.loads(template[1][entry['raw_path']]);receipt=json.loads(template[1][entry['receipt_path']])
    plan=next(r for r in native.plan_reviews(ROOT) if r['symbol']==symbol)
    quote['data'].update(f57=symbol,f58='software fixture only',f84=plan['before_total_shares'])
    raw=json.dumps(quote).encode();params=native.quote_params(symbol)
    receipt.update(params=params,actual_url=native.QUOTE_URL+'?'+urlencode(params),raw_sha256=source._sha(raw),bytes=len(raw))
    receipt_raw=json.dumps(receipt).encode();name='software_quote_only/'
    bodies[name+'quote.raw']=raw;bodies[name+'quote.receipt.json']=receipt_raw
    item['manifest']['files']['quote']=dict(raw_path=name+'quote.raw',raw_sha256=source._sha(raw),
        receipt_path=name+'quote.receipt.json',receipt_sha256=source._sha(receipt_raw))
    return value


@pytest.mark.parametrize('symbol',['300535','300644'])
def test_software_only_quote_allows_no_early_total_or_csv_rewrite(symbol):
    value=software_quote_only(symbol);candidate,facts=verify(value)
    assert value[0]['software_quote_only']
    assert candidate==base64.b64decode(value[0]['previous_candidate_b64']) and facts['changes']==[]
    assert b'2026-11-12' not in candidate and facts['legal_effective_date_not_asserted']
