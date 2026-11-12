"""Synthetic software contracts, not observed market prices or strategy metrics."""
import copy
import gzip
import hashlib
import io
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

import microcap_top100_mom16_biweekly_live_v2_0 as v
from scripts import scoped_forward_consumption as m
from scripts import scoped_cap_boundary as boundary
from scripts import scoped_source_consumer as source
from scripts import stock_return_pipeline as pipeline


def put(root,name,raw):
    path=root/name;path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(raw)
    return path


def csv(frame):return frame.to_csv(index=False,date_format='%Y-%m-%d').encode()


@pytest.fixture
def forward(tmp_path,monkeypatch):
    base=v.base_mod;scope=[f'{i:06d}' for i in range(1,301)];requested=set(scope)|{'000400'}
    dates=pd.DatetimeIndex(['2026-10-28','2026-10-29','2026-10-30'])
    calendar=tuple(d.date() for d in dates)
    monkeypatch.setattr(pipeline,'sessions_for_day',lambda *a:calendar)
    import scripts.exchange_calendar as exchange
    monkeypatch.setattr(exchange,'sessions_for_day',lambda *a:calendar)
    master=pd.DataFrame(dict(symbol=sorted(requested),list_date=pd.Timestamp('2000-01-01'),delist_date=pd.NaT))
    monkeypatch.setattr(base.freq_mod,'load_security_master',lambda:master)
    returns=pd.DataFrame(0.0,index=dates,columns=scope)
    buyable=pd.DataFrame(True,index=dates,columns=scope);sellable=buyable.copy()
    sellable.loc[dates[1],'000001']=False
    caps={dates[1]:{s:float((1000+i)*10) for i,s in enumerate(scope,1)}}
    caps[dates[1]]['000001']=100000000.0
    targets=base.build_live_target_members_map(caps,dates[1:2],{},top_n=100)
    initial=scope[:100]
    index,turnover,effective=base.freq_mod.simulate_rebalance_path(trading_dates=dates,
        returns_df=returns,target_members_map=targets,rebalance_dates=dates[1:2],
        buyable_df=buyable,sellable_df=sellable,one_side_cost_rate=0.003,top_n=100,
        execution_timing=base.EXECUTION_TIMING,initial_members=initial,delist_dates={})
    inputs=m.capture(trading_dates=dates,returns_df=returns,caps_by_date=caps,buyable_df=buyable,
        sellable_df=sellable,initial_members=initial,target_members_map=targets,rebalance_dates=dates[1:2],
        index_df=index,turnover_df=turnover,effective_members_map=effective,delist_dates={})
    directory=m.PREFIX+'fixture/boundary/2026-10-29'
    rows=[]
    for symbol in sorted(requested):
        cap=caps[dates[1]].get(symbol,1000000000.0)
        rows.append(dict(SECURITY_CODE=symbol,SECUCODE=symbol+'.SZ',TRADE_DATE='2026-10-29',
            TOTAL_SHARES=cap/10,TOTAL_MARKET_CAP=cap,CLOSE_PRICE=10))
    raw=source.encode(dict(success=True,result=dict(count=len(rows),pages=1,data=rows)))
    receipt=source.encode(dict(url=boundary.URL,params=boundary.request_params('2026-10-29',1),
        sha256=source.sha(raw),tls_verify=True,fetched_at='2026-10-29T16:00:00+08:00'))
    files={'page-01.json':source.sha(raw),'page-01.receipt.json':source.sha(receipt)}
    for name,body in zip(files,(raw,receipt)):put(tmp_path,directory+'/'+name,body)
    spec=dict(directory=directory,files=files)
    checked=boundary.verify(tmp_path,base,'2026-10-29',inputs['caps']['2026-10-29'],scope,spec,['synthetic-authority','1'*64])
    inputs['boundaries']={'2026-10-29':checked}
    old_meta=dict(end_date='2026-10-28',return_basis_consumption=dict(revision='stock_return_consumption_v1',
        full_rebuild_anchor='2026-10-28',tail_extensions=[]))
    old_index=csv(index.iloc[:1]);old_costed=b'date,nav_net\n2026-10-28,1.0\n'
    old_members=pd.DataFrame(dict(rebalance_date='2026-10-28',rank=range(1,101),symbol=initial))
    old_effective=old_members.drop(columns='rebalance_date').assign(as_of_date='2026-10-28')
    old_turnover=turnover.copy().iloc[:1]
    old_turnover['rebalance_date']=dates[0];old_turnover['constraint_trade_date']=dates[0]
    old_turnover['execution_date']=dates[0];old_turnover['effective_date']=dates[0]
    old_turnover['return_start_date']=dates[1]
    old_payloads=dict(index=old_index,costed=old_costed,meta=source.encode(old_meta),
        members=csv(old_members),turnover=csv(old_turnover),effective=csv(old_effective))
    final=index.attrs['terminal_effective_members']
    fresh_members=pd.DataFrame(dict(rebalance_date='2026-10-29',rank=range(1,101),symbol=targets[dates[1]]))
    new_payloads=dict(index=csv(index),costed=old_costed+b'2026-10-29,1.0\n2026-10-30,1.0\n',
        meta=source.encode(dict(end_date='2026-10-30')),members=csv(pd.concat([old_members,fresh_members])),
        turnover=csv(pd.concat([old_turnover,turnover])),
        effective=csv(pd.DataFrame(dict(as_of_date='2026-10-30',rank=range(1,101),symbol=final))))
    proof=m.append(tmp_path,tmp_path/m.PREFIX/'fixture',old_meta,old_payloads,new_payloads,inputs,scope,'a'*64)
    meta=dict(end_date='2026-10-30',return_basis_consumption=proof)
    for role,name in m.FILES.items():put(tmp_path,name,source.encode(meta) if role=='meta' else new_payloads[role])
    return tmp_path,meta,proof,inputs,requested,base


def requirements(fixture):
    root,meta,proof,inputs,requested,base=fixture
    path=root/m.FILES['meta']
    return m.requirements(root,'2026-10-30',requested,meta,path,path.read_bytes(),base=base)


def test_actual_blocked_execution_and_300_horizons_are_independently_replayed(forward):
    full,horizons,stable=requirements(forward)
    assert full=='2026-10-28'
    assert horizons['000001']==horizons['000300']=='2026-10-30'
    assert horizons['000400']=='2026-10-28'
    assert len(stable)>=18
    assert forward[3]['terminal'][-1]=='000001'
    assert '000101' not in forward[3]['terminal']


@pytest.mark.parametrize('fault',['snapshot','native_input','current_member','scope','gap','terminal','target','level','boundary'])
def test_corrupt_forward_authority_never_inherits_a_legacy_pass(forward,fault):
    root,meta,proof,inputs,requested,base=forward;segment=proof['segments'][0]
    if fault=='snapshot':(root/segment['snapshot']['files']['effective']['path']).write_bytes(b'corrupt')
    elif fault=='native_input':(root/segment['inputs']['path']).write_bytes(b'corrupt')
    elif fault=='current_member':(root/m.FILES['effective']).write_bytes(b'corrupt')
    elif fault=='scope':segment['symbols']=segment['symbols'][:-1]
    elif fault=='gap':segment['start_exclusive']='2026-10-27'
    else:
        value=copy.deepcopy(inputs)
        if fault=='terminal':value['terminal'][0]='000300'
        elif fault=='target':value['targets']['2026-10-29'][0]='000300'
        elif fault=='level':value['index']['values'][-1][0]=99999.0
        elif fault=='boundary':value['boundaries']={}
        raw=source.encode(value);put(root,segment['inputs']['path'],raw);segment['inputs']['sha256']=source.sha(raw)
    with pytest.raises((ValueError,AssertionError,KeyError)):
        requirements(forward)


def test_boundary_truncated_market_and_eligible_pool_escape_fail_closed(forward,monkeypatch):
    root,meta,proof,inputs,requested,base=forward
    spec=inputs['boundaries']['2026-10-29']['snapshot']
    # Even a self-consistent provider count cannot omit an active native issuer.
    master=base.freq_mod.load_security_master()
    monkeypatch.setattr(base.freq_mod,'load_security_master',lambda:pd.concat([master,master.iloc[[0]].assign(symbol='000500')]))
    with pytest.raises(ValueError,match='missing active native A-share issuers: 000500'):
        boundary.verify(root,base,'2026-10-29',inputs['caps']['2026-10-29'],proof['segments'][0]['symbols'],spec,['fixture','1'*64])


def test_new_eligible_low_cap_stock_requires_roster_recall(forward,monkeypatch):
    root,meta,proof,inputs,requested,base=forward
    spec=copy.deepcopy(inputs['boundaries']['2026-10-29']['snapshot'])
    path=root/spec['directory']/'page-01.json';payload=json.loads(path.read_bytes())
    next(row for row in payload['result']['data'] if row['SECURITY_CODE']=='000400').update(
        TOTAL_SHARES=100,TOTAL_MARKET_CAP=1000)
    raw=source.encode(payload);path.write_bytes(raw);spec['files'][path.name]=source.sha(raw)
    path=path.with_name('page-01.receipt.json');receipt=json.loads(path.read_bytes());receipt['sha256']=source.sha(raw)
    path.write_bytes(source.encode(receipt));spec['files'][path.name]=source.sha(path.read_bytes())
    monkeypatch.setattr(boundary,'st_exclusions',lambda *a,**k:({},{}))
    with pytest.raises(ValueError,match='Recall potentially eligible issuers.*000400'):
        boundary.verify(root,base,'2026-10-29',inputs['caps']['2026-10-29'],proof['segments'][0]['symbols'],spec,['fixture','1'*64])


def test_retired_pool_member_keeps_its_last_consumed_horizon_after_rotation(forward,monkeypatch):
    root,meta,proof,first,requested,base=forward
    calendar=tuple(pd.Timestamp(d).date() for d in ['2026-10-28','2026-10-29','2026-10-30','2026-11-02'])
    monkeypatch.setattr(pipeline,'sessions_for_day',lambda *a:calendar)
    import scripts.exchange_calendar as exchange
    monkeypatch.setattr(exchange,'sessions_for_day',lambda *a:calendar)
    scope=[s for s in proof['segments'][0]['symbols'] if s!='000300']+['000400']
    dates=pd.DatetimeIndex(['2026-10-30','2026-11-02']);returns=pd.DataFrame(0.0,index=dates,columns=scope)
    flags=pd.DataFrame(True,index=dates,columns=scope)
    index,turnover,effective=base.freq_mod.simulate_rebalance_path(trading_dates=dates,returns_df=returns,
        target_members_map={},rebalance_dates=pd.DatetimeIndex([]),buyable_df=flags,sellable_df=flags,
        one_side_cost_rate=0.003,top_n=100,execution_timing=base.EXECUTION_TIMING,
        initial_members=first['terminal'],delist_dates={})
    captured=m.capture(trading_dates=dates,returns_df=returns,caps_by_date={},buyable_df=flags,
        sellable_df=flags,initial_members=first['terminal'],target_members_map={},rebalance_dates=pd.DatetimeIndex([]),
        index_df=index,turnover_df=turnover,effective_members_map=effective,delist_dates={})
    captured['boundaries']={}
    old={role:(root/name).read_bytes() for role,name in m.FILES.items()}
    new=dict(old);new['index']=old['index']+csv(index.iloc[1:]).split(b'\n',1)[1]
    new['costed']=old['costed']+b'2026-11-02,1.0\n'
    new['effective']=csv(pd.DataFrame(dict(as_of_date='2026-11-02',rank=range(1,101),symbol=first['terminal'])))
    new['meta']=source.encode(dict(end_date='2026-11-02'))
    updated=m.append(root,root/m.PREFIX/'rotated',meta,old,new,captured,scope,'b'*64)
    meta=dict(end_date='2026-11-02',return_basis_consumption=updated)
    for role,name in m.FILES.items():put(root,name,source.encode(meta) if role=='meta' else new[role])
    path=root/m.FILES['meta']
    full,horizons,_=m.requirements(root,'2026-11-02',requested,meta,path,path.read_bytes(),base=base)
    assert full=='2026-10-28' and horizons['000300']=='2026-10-30'
    assert horizons['000400']=='2026-11-02' and horizons[first['terminal'][0]]=='2026-11-02'


def test_real_m8_turnover_mixed_midnight_is_read_without_changing_original_bytes():
    # Exact original M8 v4 archive member, not a regenerated historical CSV.
    path=Path(__file__).parent/'fixtures/stock_return_basis_real/m8_turnover_a87ee0_20261110.csv.gz'
    compressed=path.read_bytes();raw=gzip.decompress(compressed)
    identity='a87ee0f76c90f08304bceda6805eca277bf101ca05dcbe2b3b0e0d3d63ed68d7'
    assert hashlib.sha256(raw).hexdigest()==identity
    frame=pd.read_csv(io.BytesIO(raw),dtype=str)
    assert len(frame)==434 and frame.return_start_date.iloc[-1]=='2026-10-30 00:00:00'
    assert frame.return_start_date.iloc[:-1].str.fullmatch(r'\d{4}-\d{2}-\d{2}').all()
    m._history(raw,raw,'rebalance_date','2026-11-10',turnover=True)
    # The parsed comparison recognizes the same day; neither authority is
    # rewritten or given the other representation's hash.
    bare=raw.replace(b'2026-10-30 00:00:00',b'2026-10-30')
    assert bare!=raw
    m._history(raw,bare,'rebalance_date','2026-11-10',turnover=True)
    assert path.read_bytes()==compressed and hashlib.sha256(raw).hexdigest()==identity


def dated_turnover():
    return pd.DataFrame([dict(rebalance_date='2026-10-29',execution_timing='close',
        constraint_trade_date='2026-10-29',execution_date='2026-10-29',
        effective_date='2026-10-29',return_start_date='2026-10-30',
        entry_count=12,exit_count=12,blocked_entry_count=2,blocked_exit_count=2,
        buy_turnover_frac=0.12,sell_turnover_frac=0.12,two_side_cost_rate=0.00072,
        holding_count_after=100)])


@pytest.mark.parametrize('field',['rebalance_date','constraint_trade_date','execution_date',
    'effective_date','return_start_date'])
def test_history_iso_midnight_forms_compare_as_calendar_days(field):
    before=dated_turnover();after=before.copy()
    before.loc[0,field]+=' 00:00:00'
    after.loc[0,field]+='T00:00:00.000000000'
    m._history(csv(before),csv(after),'rebalance_date','2026-10-29',turnover=True)


@pytest.mark.parametrize('field,value',[
    ('rebalance_date','2026-10-29 00:00:01'),
    ('constraint_trade_date','2026-10-29 09:30:00'),
    ('execution_date','2026-10-29T00:00:00+00:00'),
    ('effective_date','2026-10-29T00:00:00Z'),
    ('return_start_date','2026-10-30 00:00:00.000000001'),
    ('return_start_date','2026-02-30'),
    ('return_start_date','2026-10-30 junk'),
    ('return_start_date','10/30/2026'),
])
def test_history_rejects_non_midnight_timezone_and_invalid_calendar_dates(field,value):
    before=dated_turnover();after=before.copy();after.loc[0,field]=value
    with pytest.raises(ValueError):
        m._history(csv(before),csv(after),'rebalance_date','2026-10-29',turnover=True)


@pytest.mark.parametrize('state',['still_pending','next_session','false_next_session','predates_bridge'])
def test_history_pending_close_first_return_rules_are_unchanged(monkeypatch,state):
    import scripts.exchange_calendar as exchange
    calendar=tuple(pd.Timestamp(d).date() for d in ['2026-10-29','2026-10-30','2026-11-02'])
    monkeypatch.setattr(exchange,'sessions_for_day',lambda *a:calendar)
    before=dated_turnover();before.loc[0,'return_start_date']=None
    after=before.copy()
    if state=='next_session':after.loc[0,'return_start_date']='2026-10-30 00:00:00'
    elif state=='false_next_session':after.loc[0,'return_start_date']='2026-11-02'
    elif state=='predates_bridge':
        before.loc[0,'execution_date']='2026-10-28';after=before.copy()
    old_raw,new_raw=csv(before),csv(after)
    if state in ('false_next_session','predates_bridge'):
        with pytest.raises(ValueError,match='false first-return|predates the bridge'):
            m._history(old_raw,new_raw,'rebalance_date','2026-10-29',turnover=True)
    else:
        m._history(old_raw,new_raw,'rebalance_date','2026-10-29',turnover=True)
    assert csv(before)==old_raw and csv(after)==new_raw


@pytest.mark.parametrize('change',['calendar_day','turnover_count','cost','member'])
def test_history_midnight_compatibility_never_allows_economic_or_member_rewrites(change):
    if change=='member':
        before=pd.DataFrame([dict(rebalance_date='2026-10-29',rank=1,symbol='000001')])
        after=before.copy();after.loc[0,'symbol']='000002'
    else:
        before=dated_turnover();after=before.copy()
        if change=='calendar_day':after.loc[0,'return_start_date']='2026-11-02'
        elif change=='turnover_count':after.loc[0,'entry_count']=13
        elif change=='cost':after.loc[0,'two_side_cost_rate']=0.00073
    with pytest.raises(AssertionError):
        m._history(csv(before),csv(after),'rebalance_date','2026-10-29',turnover=change!='member')
