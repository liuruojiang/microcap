"""Real provider-date zero-total change; no legal execution date is inferred."""
import base64
import copy
from datetime import datetime
import gzip
import json
import re
from pathlib import Path
from urllib.parse import urlencode

import pytest

from scripts import share_change_source as source
from frozen_exchange_calendar import frozen_exchange_sessions

ROOT=Path(__file__).resolve().parents[1]


@pytest.fixture(autouse=True)
def calendar(frozen_exchange_sessions):return frozen_exchange_sessions


@pytest.fixture
def case():
    path=ROOT/'tests/fixtures/structure_reobservation_301429_20261110.json.gz'
    value=json.loads(gzip.decompress(path.read_bytes()))
    return value,{p:base64.b64decode(raw) for p,raw in value['objects_b64'].items()}


def verify(case):
    value,bodies=case
    def bound(path,digest):
        raw=bodies[path]
        if source._sha(raw)!=digest:raise ValueError('Source digest differs')
        return raw
    return source.verify_dated_window(value['manifest'],symbol=value['symbol'],previous_day=value['previous_day'],
        end=value['end'],previous_candidate=base64.b64decode(value['previous_candidate_b64']),bound=bound,
        root=ROOT,now=datetime.fromisoformat(value['observed_at']))


def rewrite(case,entry,raw):
    bodies=case[1];bodies[entry['raw_path']]=raw;entry['raw_sha256']=source._sha(raw)
    receipt=json.loads(bodies[entry['receipt_path']]);receipt.update(raw_sha256=source._sha(raw),bytes=len(raw))
    body=json.dumps(receipt).encode();bodies[entry['receipt_path']]=body;entry['receipt_sha256']=source._sha(body)


def test_actual_nov10_structure_reobservation_never_changes_share_curve(case):
    value,_=case;before=copy.deepcopy(case)
    candidate,facts=verify(case)
    assert candidate==base64.b64decode(value['previous_candidate_b64'])
    assert source._sha(candidate)=='3f01ea2c74510132e4481bb1ba34569d0e5f1a29723cec385eaca9fb323630b4'
    assert facts==value['verified_facts'] and facts['changes']==[]
    assert facts['structure_reobservations'][0]['provider_date_reobservation']
    assert facts['structure_reobservations'][0]['execution_or_legal_effect_not_asserted']
    assert facts['future_structure_not_consumed']['provider_dates']==['2026-11-12']
    assert 'structure_events' not in facts and b'2026-11-10' not in candidate and b'2026-11-12' not in candidate
    assert case==before


@pytest.mark.parametrize('field',['END_DATE','LISTING_DATE','NOTICE_DATE','INFO_CODE','TOTAL_SHARES',
    'TOTAL_SHARES_CHANGE','LIMITED_SHARES','UNLIMITED_SHARES','LIMITED_SHARES_CHANGE','UNLIMITED_SHARES_CHANGE','SECURITY_CODE'])
def test_unreviewed_current_row_cannot_claim_total_neutral_equivalence(case,field):
    for role in ('unfiltered','bounded'):
        entry=case[0]['manifest']['files'][role][0];data=json.loads(case[1][entry['raw_path']])
        row=next(r for r in data['result']['data'] if r['END_DATE'][:10]=='2026-11-10')
        row[field]=row[field]+1 if isinstance(row[field],int) else 'unreviewed'
        rewrite(case,entry,json.dumps(data).encode())
    with pytest.raises(ValueError):verify(case)


@pytest.mark.parametrize('fault',['missing_pdf','changed_pdf','duplicate_pdf','dated_total','quote_total','future_row','sina_quantity'])
def test_all_independent_sources_and_future_exclusion_remain_required(case,fault):
    files=case[0]['manifest']['files']
    if fault=='missing_pdf':files.pop('later_structure_documents')
    elif fault=='duplicate_pdf':files['later_structure_documents']*=2
    elif fault=='changed_pdf':
        entry=files['later_structure_documents'][0];rewrite(case,entry,case[1][entry['raw_path']]+b'changed')
    elif fault=='sina_quantity':
        entry=files['sina'];raw=case[1][entry['raw_path']]
        changed=raw.replace(b'11822',b'11823');assert changed!=raw
        rewrite(case,entry,changed)
    else:
        role={'dated_total':'dated_window','quote_total':'quote','future_row':'unfiltered'}[fault]
        entry=files[role][0] if role=='unfiltered' else files[role];value=json.loads(case[1][entry['raw_path']])
        if fault=='dated_total':value['result']['data'][0]['TOTAL_SHARES']+=1
        elif fault=='quote_total':value['data']['f84']+=1
        else:value['result']['data'][0]['END_DATE']='2026-11-13 00:00:00'
        rewrite(case,entry,json.dumps(value).encode())
    with pytest.raises(ValueError):verify(case)


def test_original_complete_collection_resumes_without_any_network_and_native_revalidates(tmp_path,case,monkeypatch):
    value,bodies=case;root=source._extended(tmp_path);directory=root/'.microcap_index_cache/share_sources'
    old=base64.b64decode(value['previous_candidate_b64'])
    cache=root/'.microcap_index_cache/share_change/301429.csv';cache.parent.mkdir(parents=True);cache.write_bytes(old)
    for name,body in {**{p:base64.b64decode(b) for p,b in value['prefix_objects_b64'].items()},**bodies}.items():
        path=source._extended(directory/name);path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(body)
    registry=root/'data/share_source_future_plan_reviews.json';registry.parent.mkdir()
    registry.write_bytes((ROOT/'data/share_source_future_plan_reviews.json').read_bytes())
    original={name:body for name,body in bodies.items()}
    monkeypatch.setenv('MICROCAP_SHARE_DATED_WINDOW','1')
    monkeypatch.setattr(source.requests,'get',lambda *a,**kw:pytest.fail('Complete raw observations must not be recollected'))
    source.refresh_share_change(root=root,symbol='301429',cache_path=cache,as_of='2026-11-10',allow_network=True)
    result=source.verify_share_cache(root=root,symbol='301429',as_of='2026-11-10')
    assert result['query_end']=='2026-11-10' and result['candidate_sha256']==source._sha(old)
    assert cache.read_bytes()==old and all((directory/name).read_bytes()==raw for name,raw in original.items())


def software_later_case(real_case,end):
    """Hypothetical unchanged source observations, never real Nov12/13 data.

    Future clocks and intervening dated rows are explicitly generated only to
    test the reader/collector contract. The original real fixture is untouched.
    """
    from scripts import share_unchanged_tail as native
    from scripts.exchange_calendar import sessions_for_day
    value,bodies=copy.deepcopy(real_case);case=(value,bodies)
    value.update(software_fixture_only=True,end=end,observed_at=end+'T16:00:00+08:00')
    manifest=value['manifest'];manifest['query_end']=end;files=manifest['files']
    entry=files['dated_window'];payload=json.loads(bodies[entry['raw_path']])
    old_rows={row['TRADE_DATE'][:10]:row for row in payload['result']['data']}
    days=[d.isoformat() for d in sessions_for_day(datetime.fromisoformat(end).date()) if value['previous_day']<=d.isoformat()<=end]
    rows=[]
    for day in reversed(days):
        row=copy.deepcopy(old_rows.get(day,old_rows['2026-11-10']));row['TRADE_DATE']=day+' 00:00:00'
        row['FREE_SHARES_A']=65615267 if day>='2026-11-12' else row['FREE_SHARES_A'];rows.append(row)
    payload['result'].update(data=rows,count=len(rows),pages=1)
    rewrite(case,entry,json.dumps(payload).encode())
    # At this hypothetical later close both exact catalog rows would be inside
    # the bounded inventory. Keep their original row bytes/types intact.
    full=bodies[files['unfiltered'][0]['raw_path']]
    rewrite(case,files['bounded'][0],full)
    entry=files['quote'];quote=json.loads(bodies[entry['raw_path']])
    quote['data']['f86']=int(datetime.fromisoformat(end+'T15:31:00+08:00').timestamp())
    rewrite(case,entry,json.dumps(quote).encode())
    params=[(files['dated_window'],native.historical_params(value['symbol'],value['previous_day'],end)),
        (files['sina'],{}),(files['quote'],native.quote_params(value['symbol']))]
    for bounded,role in ((False,'unfiltered'),(True,'bounded')):
        params.extend((item,native.em_params(value['symbol'],end,bounded,i)) for i,item in enumerate(files[role],1))
    params.extend((item,{}) for item in files['later_structure_documents'])
    for entry,query in params:
        receipt=json.loads(bodies[entry['receipt_path']])
        receipt.update(params=query,actual_url=receipt['url']+('?' + urlencode(query) if query else ''),
            http_start=end+'T15:30:00+08:00',http_complete=end+'T15:32:00+08:00')
        raw=json.dumps(receipt).encode();bodies[entry['receipt_path']]=raw;entry['receipt_sha256']=source._sha(raw)
    return case


def test_real_nov10_observations_cannot_be_used_to_certify_nov12(case):
    value,_=case;value['end']='2026-11-12';value['manifest']['query_end']='2026-11-12'
    value['observed_at']='2026-11-12T16:00:00+08:00'
    with pytest.raises(ValueError):verify(case)


@pytest.mark.parametrize('end',['2026-11-12','2026-11-13'])
def test_software_later_close_requires_observations_then_keeps_total_neutral_curve(case,end):
    later=software_later_case(case,end);candidate,facts=verify(later)
    assert later[0]['software_fixture_only']
    assert candidate==base64.b64decode(case[0]['previous_candidate_b64']) and facts['changes']==[]
    assert [e['date'] for e in facts['structure_reobservations']]==['2026-11-10','2026-11-12']
    assert 'future_structure_not_consumed' not in facts and facts['legal_effective_date_not_asserted']


@pytest.mark.parametrize('fault',['old_clock','missing_session','missing_sina','missing_pdf','future_change'])
def test_software_later_close_cannot_skip_real_source_requirements(case,fault):
    later=software_later_case(case,'2026-11-12');files=later[0]['manifest']['files']
    if fault=='old_clock':
        entry=files['dated_window'];receipt=json.loads(later[1][entry['receipt_path']]);receipt['http_start']='2026-11-10T15:30:00+08:00'
        raw=json.dumps(receipt).encode();later[1][entry['receipt_path']]=raw;entry['receipt_sha256']=source._sha(raw)
    elif fault=='missing_pdf':files.pop('later_structure_documents')
    elif fault=='missing_sina':files.pop('sina')
    elif fault=='missing_session':
        entry=files['dated_window'];payload=json.loads(later[1][entry['raw_path']]);payload['result']['data'].pop(1);payload['result']['count']-=1
        rewrite(later,entry,json.dumps(payload).encode())
    else:
        entry=files['unfiltered'][0];payload=json.loads(later[1][entry['raw_path']]);payload['result']['data'][0]['TOTAL_SHARES']+=1
        rewrite(later,entry,json.dumps(payload).encode())
    with pytest.raises((ValueError,KeyError)):verify(later)


def test_software_later_collector_keeps_required_document_after_its_date_is_current(tmp_path,case,monkeypatch):
    later=software_later_case(case,'2026-11-12');value,bodies=later
    assert value['software_fixture_only']
    old=base64.b64decode(value['previous_candidate_b64']);root=source._extended(tmp_path)
    directory=root/'.microcap_index_cache/share_sources'
    identity=source._sha(json.dumps([value['symbol'],value['previous_day'],value['end'],source._sha(old)]).encode())
    old_prefix=value['manifest']['files']['dated_window']['raw_path'].rsplit('/',1)[0]
    new_prefix='providers/'+source.DATED_WINDOW_KIND+'/'+value['symbol']+'/'+identity
    for name,raw in bodies.items():
        path=source._extended(directory/name.replace(old_prefix,new_prefix));path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(raw)
    registry=root/'data/share_source_future_plan_reviews.json';registry.parent.mkdir()
    registry.write_bytes((ROOT/'data/share_source_future_plan_reviews.json').read_bytes())
    real_datetime=datetime
    class SoftwareClock(real_datetime):
        @classmethod
        def now(cls,tz=None):
            value=real_datetime.fromisoformat('2026-11-12T16:00:00+08:00')
            return value.astimezone(tz) if tz else value.replace(tzinfo=None)
    monkeypatch.setattr(source,'datetime',SoftwareClock)
    monkeypatch.setattr(source.requests,'get',lambda *a,**kw:pytest.fail('Cached software sources must not trigger a real request'))
    candidate,ref=source.collect_dated_window(root=root,symbol=value['symbol'],previous_day=value['previous_day'],end=value['end'],candidate=old)
    manifest=json.loads((directory/ref['path']).read_bytes())
    assert manifest['files']['later_structure_documents'][0]['announcement_id']=='AN202611091831558999'
    assert candidate==old and manifest['verified_facts']['changes']==[]


def software_subsequent_capital_change(real_case,missing=None):
    """Pure software counterexample: a hypothetical new Nov13 total-share event.

    No part of this generated event is an observed market fact or an approval.
    It ensures the old neutral rows do not suppress the normal strict pathway.
    """
    later=software_later_case(real_case,'2026-11-13');value,bodies=later;files=value['manifest']['files']
    entry=files['dated_window'];payload=json.loads(bodies[entry['raw_path']]);row=payload['result']['data'][0]
    assert row['TRADE_DATE'][:10]=='2026-11-13'
    row.update(TOTAL_SHARES=119220000,TOTAL_MARKET_CAP=119220000*row['CLOSE_PRICE'],FREE_SHARES_A=66615267)
    rewrite(later,entry,json.dumps(payload).encode())
    entry=files['quote'];payload=json.loads(bodies[entry['raw_path']]);payload['data'].update(f84=119220000,f85=66615267)
    rewrite(later,entry,json.dumps(payload).encode())
    if missing!='em':
        for role in ('unfiltered','bounded'):
            entry=files[role][0];raw=bodies[entry['raw_path']];payload=json.loads(raw)
            event=copy.deepcopy(payload['result']['data'][0])
            event.update(END_DATE='2026-11-13 00:00:00',LISTING_DATE='2026-11-13 00:00:00',
                NOTICE_DATE='2026-11-13 00:00:00',INFO_CODE='SOFTWARE_ONLY_NEW_TOTAL_EVENT_20261113',
                TOTAL_SHARES=119220000,TOTAL_SHARES_CHANGE=1000000,LIMITED_SHARES=52604733,
                UNLIMITED_SHARES=66615267,LIMITED_SHARES_CHANGE=0,UNLIMITED_SHARES_CHANGE=1000000,
                CHANGE_REASON='software test only')
            # Preserve the byte spelling of the two exact old catalog rows.
            match=re.search(rb'"data"\s*:\s*\[',raw);assert match
            raw=raw[:match.end()]+json.dumps(event,ensure_ascii=False).encode()+b','+raw[match.end():]
            count=len(payload['result']['data'])+1
            raw,n=re.subn(rb'"count"\s*:\s*\d+',b'"count":'+str(count).encode(),raw);assert n==1
            rewrite(later,entry,raw)
    if missing!='sina':
        entry=files['sina'];text=bodies[entry['raw_path']].decode('gb18030')
        assert text.count('</graph>')==2
        text=text.replace('</graph>',"<set name='software only' value='11922' hoverText='2026-11-13'/></graph>")
        pattern=r'(<tr>\s*<td><div align="center">2026-11-12</div></td>\s*<td><div align="center">11822万股</div></td>\s*</tr>)'
        addition='<tr><td><div align="center">2026-11-13</div></td><td><div align="center">11922万股</div></td></tr>'
        text,n=re.subn(pattern,lambda m:m[1]+addition,text);assert n==1
        rewrite(later,entry,text.encode('gb18030'))
    return later


def test_software_genuine_later_total_change_keeps_original_strict_event_path(case):
    later=software_subsequent_capital_change(case);candidate,facts=verify(later)
    assert later[0]['software_fixture_only']
    old=base64.b64decode(case[0]['previous_candidate_b64'])
    assert candidate.startswith(old) and b'2026-11-13,11922,' in candidate
    assert facts['changes']==[dict(date='2026-11-13',total_shares_10k='11922',
        announcement_id='SOFTWARE_ONLY_NEW_TOTAL_EVENT_20261113')]
    assert [r['date'] for r in facts['structure_reobservations']]==['2026-11-10','2026-11-12']
    assert facts['old_prefix_preserved'] and facts['legal_effective_date_not_asserted']


@pytest.mark.parametrize('missing',['em','sina'])
def test_software_later_total_change_cannot_borrow_the_old_neutral_review(case,missing):
    with pytest.raises(ValueError):verify(software_subsequent_capital_change(case,missing))
