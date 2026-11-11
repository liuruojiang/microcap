"""Actual frozen 10-Nov representation evidence plus explicit software timelines.

The 11/12/13/26-Nov rows below are synthetic regression cases, not fresh prices.
No production root, calendar cache, data collection, mail or order is touched.
"""
import base64
import copy
import csv
from datetime import date, datetime
import gzip
import io
import json
from pathlib import Path
import socket
import subprocess
import sys

import pandas as pd
import pytest

from scripts import member_hedge_precision_bridge as p
from scripts import member_period_handoff as h
from scripts import member_daily_signal as daily

ROOT = Path(__file__).resolve().parents[1]
FIXTURE = Path(__file__).parent/'fixtures/member_hedge_precision_20261110.json.gz'


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def fail(*args, **kwargs):
        raise AssertionError('This regression must never collect market data')
    monkeypatch.setattr(socket.socket, 'connect', fail)
    monkeypatch.setattr(socket, 'create_connection', fail)


@pytest.fixture
def actual(tmp_path):
    packet = json.loads(gzip.decompress(FIXTURE.read_bytes()))
    for name, obj in packet['objects'].items():
        raw = base64.b64decode(obj['base64'], validate=True)
        assert h.digest(raw) == obj['sha256']
        path = tmp_path/name; path.parent.mkdir(parents=True, exist_ok=True); path.write_bytes(raw)
    return tmp_path


def record(root):
    period = json.loads((root/'formal/period.json').read_bytes())
    return dict(date='2026-11-10',
                earned=dict(provider='eastmoney',instrument='1.000852',
                            hedge_json=p.blob((root/'daily-inputs/hedge.json').read_bytes()),
                            bar_csv=p.blob((root/'daily-inputs/hedge_daily_bar.csv').read_bytes())),
                formal=dict(provider='sina_legacy',instrument='sh000852',close='6579.356',
                            manifest_sha256=period['source_manifest_sha256'],
                            panel_sha256=period['source_files'][p.PANEL],
                            source_log=p.blob((root/'formal-source.log').read_bytes())))


def witness(current, candidate, records):
    return dict(schema=p.WITNESS_SCHEMA,current_checkpoint_sha256=h.digest(current.read_bytes()),
                candidate_checkpoint_sha256=h.digest(candidate.read_bytes()),records=records)


def states(history, turnover):
    frame = pd.read_csv(io.BytesIO(history)).set_index('date'); frame.index = pd.to_datetime(frame.index)
    events = pd.read_csv(io.BytesIO(turnover))
    return {v:daily.clean(daily.calculate_native(v,m,frame,events)[0].iloc[0].to_dict())
            for v,m in daily.native_modules().items()}


def materialize_actual_bridge(root):
    # 10 Nov is not a rebalance. This tests the representation proof itself;
    # the real handoff's original calendar restriction is tested separately.
    old, new = root/'daily/checkpoint.json', root/'formal/checkpoint.json'
    current, candidate = h.verify(old), h.verify(new)
    current_period = json.loads(h.read_bound(current,'period'))
    candidate_period = json.loads(h.read_bound(candidate,'period'))
    old_raw, new_raw = h.read_bound(current,'history'), h.read_bound(candidate,'history')
    derived, proof, added = p.bridge_history(current=current,candidate=candidate,current_raw=old_raw,candidate_raw=new_raw,
        current_period=current_period,candidate_period=candidate_period,current_checkpoint_raw=old.read_bytes(),
        candidate_checkpoint_raw=new.read_bytes(),witness=witness(old,new,[record(root)]))
    current_state = h.read_bound(current,'version_state')
    calculated = states(derived,h.read_bound(candidate,'turnover'))
    formal_state = json.loads(h.read_bound(candidate,'version_state'))['versions']
    for v in h.VERSIONS:
        for field in ('current_holding','next_holding','strategy_revision'):
            assert str(calculated[v][field]) == str(formal_state[v][field])
    candidate_period[p.KEY] = p.finish(proof,history=derived,current_checkpoint_raw=old.read_bytes(),
        candidate_checkpoint_raw=new.read_bytes(),current_history_raw=old_raw,candidate_history_raw=new_raw,
        candidate_period_raw=h.read_bound(candidate,'period'),current_state_raw=current_state,
        candidate_state_raw=h.read_bound(candidate,'version_state'),derived_state_raw=current_state,new_record_dates=added)
    files = {key:(current[key]['path'],h.read_bound(current,key)) for key in ('history','turnover','version_state','prices')}
    files['period']=('period.json',h.encode(candidate_period))
    h._write_package(root/'bridged',files,dict(as_of_date='2026-11-10',next_trade_date='2026-11-11',strategy_sha=current['strategy_sha']))
    return root/'bridged/checkpoint.json'


def replace_part(checkpoint,key,raw):
    obj=json.loads(checkpoint.read_bytes()); path=checkpoint.parent/obj[key]['path'];path.write_bytes(raw)
    obj[key]['sha256']=h.digest(raw); checkpoint.write_bytes(h.encode(obj))


def advance_with_original_477(checkpoint, out, day, next_day):
    obj=h.verify(checkpoint);period=json.loads(h.read_bound(obj,'period'))
    original=subprocess.check_output(['git','show','4774966da38f0b5af18617f73cc67c39c9e41a8e:scripts/member_daily_signal.py'],cwd=ROOT)
    path=out.parent/'reader477.py';path.write_bytes(original)
    import importlib.util
    spec=importlib.util.spec_from_file_location('frozen_reader477_precision_test',path)
    reader=importlib.util.module_from_spec(spec);spec.loader.exec_module(reader)
    symbols=[m['symbol'] for m in period['effective_members']]
    basis={r['symbol']:r['previous_raw_close'] for r in json.loads(h.read_bound(obj,'prices'))['rows']}
    quotes=dict(market_date=day,collected_at=day+'T16:01:00+08:00',close_confirmed=True,
        rows=[dict(symbol=s,trade_date=day,previous_date=obj['as_of_date'],
                   previous_raw_close=basis[s],raw_close=basis[s],previous_close_adj=basis[s],close_adj=basis[s],
                   return_basis_revision=reader.RETURN_BASIS,source='explicit software fixture') for s in symbols])
    hedge=dict(market_date=day,collected_at=quotes['collected_at'],close_confirmed=True,close=6580.00,source='explicit software fixture')
    frame=pd.read_csv(io.BytesIO(h.read_bound(obj,'history')))
    sessions=[date.fromisoformat(d) for d in frame.date]+[date.fromisoformat(day),date.fromisoformat(next_day)]
    result=reader.build(ROOT,checkpoint,quotes,hedge,day,'close_confirmed',out,
        now=datetime.fromisoformat(day+'T16:02:00+08:00'),calendar_sessions=sessions)
    assert result['checkpoint']=='checkpoint.json'
    return h.verify(out/'checkpoint.json')


def test_actual_sources_and_only_one_exact_difference(actual):
    old=h.verify(actual/'daily/checkpoint.json');new=h.verify(actual/'formal/checkpoint.json')
    a,b=h._history_values(h.read_bound(old,'history')),h._history_values(h.read_bound(new,'history'))
    assert len(a)==len(b)==4090
    differences=[(x,y) for x,y in zip(a,b) if x!=y]
    assert len(differences)==1 and differences[0][0]['date']=='2026-11-10'
    assert differences[0][0]['microcap']==differences[0][1]['microcap']
    assert p.checked_record(record(actual))[1:]==(p.Decimal('6579.36'),p.Decimal('6579.356'))
    manifest=(actual/'formal-outputs/top100_delivery_manifest.json').read_bytes()
    panel=(actual/'formal-outputs'/Path(p.PANEL).name).read_bytes()
    assert h.digest(manifest)==record(actual)['formal']['manifest_sha256']
    assert h.digest(panel)==record(actual)['formal']['panel_sha256']
    # The real bootstrap explicitly records the transport's LF-equivalence.
    assert json.loads(manifest)['artifacts'][p.PANEL]==h.digest(panel.replace(b'\r\n',b'\n'))
    cp=materialize_actual_bridge(actual)
    assert h.read_bound(h.verify(cp),'history')==h.read_bound(old,'history')


@pytest.mark.parametrize('case',[
    'tencent','wrong_index','intraday','naive_clock','wrong_day','failed_source','no_source',
    'bar_value','bar_day','unrounded','extra_precision','source_hash','no_sina','sina_not_last',
    'range','wrong_formal','missing_panel','missing_manifest',
])
def test_source_counterexamples(actual,case):
    rec=record(actual)
    earned=rec['earned'];formal=rec['formal']
    quote=json.loads(p._blob(earned['hedge_json']))
    attrs=json.loads(p._blob(formal['source_log']).decode().split('[index-history] ',1)[1].splitlines()[0])
    if case=='tencent': earned['provider']='tencent'
    if case=='wrong_index': earned['instrument']='1.000300'
    if case=='intraday': quote['close_confirmed']=False
    if case=='naive_clock': quote['collected_at']='2026-11-10T16:01:58'
    if case=='wrong_day': quote['market_date']='2026-11-09'
    if case=='failed_source':quote['source_attempts'][-1]['ok']=False
    if case=='no_source':quote['source']=''
    if case=='bar_value':earned['bar_csv']=p.blob(p._blob(earned['bar_csv']).replace(b'6579.36',b'6579.35'))
    if case=='bar_day':earned['bar_csv']=p.blob(p._blob(earned['bar_csv']).replace(b'2026-11-10',b'2026-11-09'))
    if case=='unrounded':formal['close']='6579.354'
    if case=='extra_precision':formal['close']='6579.3561'
    if case=='no_sina':attrs['independent_history_source']='eastmoney'
    if case=='sina_not_last':attrs['independent_history_attempts'].append(dict(source='tencent',ok=True))
    if case=='range':attrs['independent_history_end']='2026-11-09'
    if case=='wrong_formal':formal['provider']='sina_static'
    if case=='missing_panel':formal['panel_sha256']=''
    if case=='missing_manifest':formal['manifest_sha256']=''
    earned['hedge_json']=p.blob(h.encode(quote));formal['source_log']=p.blob(h.encode(attrs))
    if case=='source_hash':earned['hedge_json']['sha256']='a'*64
    with pytest.raises(ValueError):p.checked_record(rec)


@pytest.mark.parametrize('case',['no_witness','current_identity','candidate_identity','microcap','panel','manifest','date','unrelated'])
def test_actual_history_and_identity_counterexamples(actual,case):
    old,new=actual/'daily/checkpoint.json',actual/'formal/checkpoint.json'
    a,b=h.verify(old),h.verify(new);rec=record(actual);doc=witness(old,new,[rec])
    prior=h.read_bound(a,'history');latest=h.read_bound(b,'history')
    if case=='no_witness':doc=None
    if case=='current_identity':doc['current_checkpoint_sha256']='a'*64
    if case=='candidate_identity':doc['candidate_checkpoint_sha256']='a'*64
    if case=='microcap':latest=latest.replace(b'287145.9583492838',b'287146.9583492838')
    if case=='panel':rec['formal']['panel_sha256']='a'*64
    if case=='manifest':rec['formal']['manifest_sha256']='a'*64
    if case=='date':latest=latest.replace(b'2026-11-10',b'2026-11-09')
    if case=='unrelated':doc['records']=[rec,copy.deepcopy(rec)]
    with pytest.raises(ValueError):p.bridge_history(current=a,candidate=b,current_raw=prior,candidate_raw=latest,
        current_period=json.loads(h.read_bound(a,'period')),candidate_period=json.loads(h.read_bound(b,'period')),
        current_checkpoint_raw=old.read_bytes(),candidate_checkpoint_raw=new.read_bytes(),witness=doc)


@pytest.mark.parametrize('case',['prefix','checkpoint_blob','candidate_history','candidate_period','candidate_state',
                                 'current_state','derived_state','origin','anchor_length','model_state'])
def test_archived_original_bindings_are_rechecked_after_rehash(actual,case):
    cp=materialize_actual_bridge(actual); obj=h.verify(cp);period=json.loads(h.read_bound(obj,'period'))
    proof=period[p.KEY];anchor=proof['anchors'][0]
    if case=='prefix':
        raw=h.read_bound(obj,'history').replace(b'6579.36',b'6579.35');replace_part(cp,'history',raw)
    elif case=='checkpoint_blob':
        raw=json.loads(p._blob(anchor['candidate_checkpoint']));raw['history']['sha256']='a'*64
        anchor['candidate_checkpoint']=p.blob(h.encode(raw))
    elif case=='candidate_history':anchor['candidate_history']=p.blob(p._blob(anchor['candidate_history']).replace(b'6579.356',b'6579.357'))
    elif case=='candidate_period':
        raw=json.loads(p._blob(anchor['candidate_period']));raw['source_manifest_sha256']='a'*64
        anchor['candidate_period']=p.blob(h.encode(raw))
    elif case=='candidate_state':anchor['candidate_version_state']=p.blob(b'{}')
    elif case=='current_state':anchor['current_version_state']=p.blob(b'{}')
    elif case=='derived_state':anchor['derived_version_state']=p.blob(b'{}')
    elif case=='origin':anchor['new_record_dates']=[]
    elif case=='anchor_length':anchor['history_bytes']-=1
    elif case=='model_state':
        raw=json.loads(p._blob(anchor['derived_version_state']));raw['versions']['v2.0']['next_holding']='changed'
        anchor['derived_version_state']=p.blob(h.encode(raw));anchor['derived_version_state_sha256']=h.digest(h.encode(raw))
    replace_part(cp,'period',h.encode(period))
    with pytest.raises(ValueError):h.verify(cp)


def test_unchanged_477_reader_preserves_proof_and_next_day_anchor(actual):
    cp=materialize_actual_bridge(actual);obj=h.verify(cp)
    period_raw=h.read_bound(obj,'period');period=json.loads(period_raw)
    # Exercise the actual 477 implementation, not an imitation of its copy loop.
    next_cp=advance_with_original_477(cp,actual/'nextday','2026-11-11','2026-11-12')
    assert h.read_bound(next_cp,'period')==period_raw
    assert h.read_bound(next_cp,'history').startswith(h.read_bound(obj,'history'))
    assert len(h._history_values(h.read_bound(next_cp,'history')))==4091
    # A newly rehashed checkpoint still cannot alter the old proof's anchor.
    replace_part(actual/'nextday/checkpoint.json','history',h.read_bound(next_cp,'history').replace(b'6579.36',b'6579.35',1))
    with pytest.raises(ValueError):h.verify(actual/'nextday/checkpoint.json')


def timeline(root, monkeypatch, *, late=False):
    """Extend the frozen actual prefix with expressly synthetic 11/12 Nov rows."""
    current, candidate=root/'daily/checkpoint.json',root/'formal/checkpoint.json'
    for path in (current,candidate):
        cp=h.verify(path);raw=h.read_bound(cp,'history')
        last=h._history_values(raw)[-1]
        days=['2026-11-11','2026-11-12'] if path==candidate or late else ['2026-11-11']
        for dt in days:
            raw=p._append(raw,dict(date=dt,microcap=str(last['microcap']),hedge='6580.00'))
        replace_part(path,'history',raw)
        doc=json.loads(path.read_bytes());doc['as_of_date']=days[-1]
        doc['next_trade_date']='2026-11-13' if days[-1]=='2026-11-12' else '2026-11-12';path.write_bytes(h.encode(doc))
        prices=json.loads((path.parent/'member_prices.json').read_bytes())
        prices['as_of_date']=days[-1]
        for row in prices['rows']:row['previous_raw_date']=days[-1]
        replace_part(path,'prices',h.encode(prices))
    cp=json.loads(candidate.read_bytes());cp['_directory']=candidate.parent
    period=json.loads(h.read_bound(cp,'period'))
    event=dict(period['rebalance_events'][-1])
    for key in ('rebalance_date','execution_date','effective_date','constraint_trade_date'):event[key]='2026-11-12'
    event['return_start_date']='2026-11-13'
    period.update(package_id='explicit-software-R12',anchor_day='2026-11-12',effective_from='2026-11-13',rebalance_events=[event])
    replace_part(candidate,'period',h.encode(period))
    raw=(candidate.parent/'turnover.csv').read_bytes()
    header=next(csv.reader(io.StringIO(raw.decode('utf-8-sig'))))
    stream=io.StringIO(newline='');csv.DictWriter(stream,fieldnames=header,lineterminator='\n').writerow(event)
    replace_part(candidate,'turnover',raw+stream.getvalue().encode())
    for path in (current,candidate):
        cp=json.loads(path.read_bytes());cp['_directory']=path.parent
        replace_part(path,'version_state',h.encode(dict(schema='member_version_state_v1',versions=states(h.read_bound(cp,'history'),h.read_bound(cp,'turnover')))))
    frame=pd.read_csv(current.parent/'history.csv')
    sessions=tuple(date.fromisoformat(dt) for dt in frame.date if dt<'2026-11-11')+(date(2026,11,11),date(2026,11,12),date(2026,11,13),date(2026,11,26),date(2026,11,27))
    from scripts import exchange_calendar
    monkeypatch.setattr(exchange_calendar,'sessions_for_day',lambda _:sessions)
    doc=witness(current,candidate,[record(root)])
    path=root/'witness.json';path.write_bytes(h.encode(doc))
    return current,candidate,path


@pytest.mark.parametrize('late',[False,True])
def test_native_handoff_early_and_same_close_recovery(actual,monkeypatch,late):
    current,candidate,proof=timeline(actual,monkeypatch,late=late)
    old=h.verify(current);before={path:path.read_bytes() for directory in ('daily','formal') for path in (actual/directory).iterdir()}
    with pytest.raises(ValueError,match='earned'):h.handoff(current,candidate,'2026-11-12','2026-11-13',actual/'default-exact')
    h.handoff(current,candidate,'2026-11-12','2026-11-13',actual/'R12',hedge_precision_witness=proof)
    result=h.verify(actual/'R12/checkpoint.json');raw=h.read_bound(result,'history')
    assert raw.startswith(h.read_bound(old,'history'))
    assert h.read_bound(result,'version_state')==h.read_bound(old,'version_state') if late else len(h._history_values(raw))==4092
    period=json.loads(h.read_bound(result,'period'));assert period[p.KEY]['records']==[record(actual)]
    assert all(path.read_bytes()==content for path,content in before.items())
    # A second same-close invocation reuses the original proof rather than
    # nesting the just-published period or duplicating its anchor.
    h.handoff(actual/'R12/checkpoint.json',candidate,'2026-11-12','2026-11-13',actual/'same-close-retry')
    retried=h.verify(actual/'same-close-retry/checkpoint.json')
    assert json.loads(h.read_bound(retried,'period'))[p.KEY]==period[p.KEY]
    command=[sys.executable,'-B',str(ROOT/'scripts/member_period_handoff.py'),'verify','--checkpoint',str(actual/'R12/checkpoint.json')]
    assert subprocess.run(command,cwd=actual,capture_output=True).returncode==0
    next_cp=advance_with_original_477(actual/'R12/checkpoint.json',actual/'daily13','2026-11-13','2026-11-16')
    assert h.read_bound(next_cp,'period')==h.read_bound(result,'period')
    assert h.read_bound(next_cp,'history').startswith(raw)


@pytest.mark.parametrize('case',['current_holding','next_holding','strategy_revision'])
def test_bridge_rejects_native_model_identity_changes(actual,monkeypatch,case):
    current,candidate,proof=timeline(actual,monkeypatch)
    original=daily.calculate_native
    def changed(version,module,history,turnover):
        row,out=original(version,module,history,turnover)
        if len(history)==4092 and version=='v2.3':row[case]='explicit counterexample'
        return row,out
    monkeypatch.setattr(daily,'calculate_native',changed)
    with pytest.raises(ValueError,match='changes native model state'):
        h.handoff(current,candidate,'2026-11-12','2026-11-13',actual/'bad-state',hedge_precision_witness=proof)


def test_next_rebalance_reuses_flat_original_witness_without_new_input(actual,monkeypatch):
    current,candidate,proof=timeline(actual,monkeypatch)
    h.handoff(current,candidate,'2026-11-12','2026-11-13',actual/'R12',hedge_precision_witness=proof)
    bridged=h.verify(actual/'R12/checkpoint.json');period=json.loads(h.read_bound(bridged,'period'))
    raw=h.read_bound(bridged,'history')
    for dt in ('2026-11-13','2026-11-26'):
        raw=p._append(raw,dict(date=dt,microcap='287145.9583492838',hedge='6580.00'))
    # Directly exercise the next bridge's source proof. R26 full calendar/
    # turnover is covered by the unchanged handoff gate, not forged here.
    source_current=dict(bridged,as_of_date='2026-11-26',next_trade_date='2026-11-27')
    candidate_raw=h.read_bound(h.verify(candidate),'history')
    for dt in ('2026-11-13','2026-11-26'):
        candidate_raw=p._append(candidate_raw,dict(date=dt,microcap='287145.9583492838',hedge='6580.00'))
    source_candidate=dict(h.verify(candidate),as_of_date='2026-11-26')
    derived,new_proof,new_dates=p.bridge_history(current=source_current,candidate=source_candidate,current_raw=raw,
        candidate_raw=candidate_raw,current_period=period,candidate_period=json.loads(h.read_bound(h.verify(candidate),'period')),
        current_checkpoint_raw=b'unused without new witness',candidate_checkpoint_raw=b'unused without new witness')
    assert derived==raw and new_dates==[]
    assert new_proof['records']==period[p.KEY]['records']
    assert new_proof['anchors']==period[p.KEY]['anchors']
    # No source record or anchor contains an embedded previous period/proof.
    assert all('previous_period' not in anchor for anchor in new_proof['anchors'])
