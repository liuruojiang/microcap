"""Offline contracts for member handoff; synthetic values never market evidence."""
import csv
import io
import json
from pathlib import Path
import pytest
from scripts import member_period_handoff as h


def package(tmp_path, name='base', day='2026-10-29', members=None, history=None):
    members = members or [dict(rank=i+1, symbol=f'{i+1:06d}', name='fixture', target_weight=.01) for i in range(100)]
    period = dict(schema='member_period_v1', package_id=name, anchor_day=day, effective_from='2026-10-30',
                  effective_members=members, rebalance_events=[dict(rebalance_date=day, execution_timing='close',
                  execution_date=day, effective_date=day, return_start_date='2026-10-30', blocked_entry_count=2, blocked_exit_count=2)])
    state = dict(schema='member_version_state_v1', versions={v:dict(date=day,strategy_revision=r) for v,r in h.REVISIONS.items()})
    prices = dict(schema='member_price_basis_v1',as_of_date=day,rows=[dict(symbol=m['symbol'],previous_raw_close=10.,previous_raw_date=day) for m in members])
    files = {'period':('period.json',h.encode(period)), 'history':('history.csv',history or f'date,microcap,hedge\n{day},1000,2000\n'.encode()),
             'turnover':('turnover.csv',b'rebalance_date,execution_timing\n'), 'version_state':('version_state.json',h.encode(state)), 'prices':('member_prices.json',h.encode(prices))}
    h._write_package(tmp_path/name,files,dict(as_of_date=day,next_trade_date='2026-10-30',strategy_sha='a'*40))
    return tmp_path/name/'checkpoint.json'


def mutate(path,key,func):
    cp=json.loads(path.read_bytes()); p=path.parent/cp[key]['path']; obj=json.loads(p.read_bytes()); func(obj); raw=h.encode(obj); p.write_bytes(raw); cp[key]['sha256']=h.digest(raw);path.write_bytes(h.encode(cp))


def test_complete_package_round_trip(tmp_path):
    cp=package(tmp_path)
    assert h.verify(cp)['as_of_date']=='2026-10-29'


def test_detects_transport_tamper(tmp_path):
    cp=package(tmp_path);(cp.parent/'history.csv').write_bytes(b'corrupt')
    with pytest.raises(ValueError,match='hash'):h.verify(cp)


def test_rejects_escape_even_valid_hash(tmp_path):
    cp=package(tmp_path);obj=json.loads(cp.read_bytes());raw=b'outside';(tmp_path/'outside').write_bytes(raw)
    obj['history']={'path':'../outside','sha256':h.digest(raw)};cp.write_bytes(h.encode(obj))
    with pytest.raises(ValueError,match='escapes'):h.verify(cp)


def test_no_99_member_average(tmp_path):
    cp=package(tmp_path);mutate(cp,'period',lambda p:p['effective_members'].pop())
    with pytest.raises(ValueError,match='100'):h.verify(cp)


def test_rejects_stale_price_basis(tmp_path):
    cp=package(tmp_path);mutate(cp,'prices',lambda p:p['rows'][0].update(previous_raw_date='2026-10-28'))
    with pytest.raises(ValueError,match='close'):h.verify(cp)


def test_rejects_wrong_version_identity(tmp_path):
    cp=package(tmp_path);mutate(cp,'version_state',lambda p:p['versions']['v2.3'].update(strategy_revision='retired'))
    with pytest.raises(ValueError,match='revision'):h.verify(cp)


def test_pointer_compare_and_swap_and_owned_lock(tmp_path):
    cp=package(tmp_path);pointer=tmp_path/'active.json';h.publish(cp,pointer)
    original=pointer.read_bytes()
    with pytest.raises(ValueError,match='pointer changed'):h.publish(cp,pointer)
    assert pointer.read_bytes()==original
    h.publish(cp,pointer,h.digest(original))
    lock=pointer.with_name(pointer.name+'.lock');lock.write_bytes(b'other owner')
    with pytest.raises(FileExistsError):h.publish(cp,pointer,h.digest(original))
    assert lock.read_bytes()==b'other owner'


def test_atomic_handoff_preserves_earned_history_and_state(tmp_path,monkeypatch):
    from scripts import exchange_calendar
    from datetime import date
    monkeypatch.setattr(exchange_calendar,'sessions_for_day',lambda d:(date(2026,10,28),date(2026,10,29),date(2026,10,30),date(2026,11,2)))
    old=package(tmp_path,'old');new=package(tmp_path,'new')
    mutate(new,'period',lambda p:p.update(package_id='new-period'))
    h.handoff(old,new,'2026-10-29','2026-10-30',tmp_path/'result')
    result=h.verify(tmp_path/'result'/'checkpoint.json')
    assert h.read_bound(result,'history')==h.read_bound(h.verify(old),'history')
    assert h.read_bound(result,'version_state')==h.read_bound(h.verify(old),'version_state')
    period=json.loads(h.read_bound(result,'period'))
    assert period['package_id']=='new-period' and period['rebalance_events'][0]['blocked_entry_count']==2


def test_handoff_rejects_changed_earned_returns(tmp_path,monkeypatch):
    from scripts import exchange_calendar
    from datetime import date
    monkeypatch.setattr(exchange_calendar,'sessions_for_day',lambda d:(date(2026,10,28),date(2026,10,29),date(2026,10,30)))
    old=package(tmp_path,'old');new=package(tmp_path,'new',history=b'date,microcap,hedge\n2026-10-29,999,2000\n')
    with pytest.raises(ValueError,match='earned'):h.handoff(old,new,'2026-10-29','2026-10-30',tmp_path/'bad')


def test_schedule_uses_native_holiday_week_rule(monkeypatch):
    from scripts import exchange_calendar
    from datetime import date
    monkeypatch.setattr(exchange_calendar,'sessions_for_day',lambda d:tuple(date.fromisoformat(s) for s in ['2026-10-28','2026-10-29','2026-10-30','2026-11-02']))
    assert h.schedule(Path(__file__).resolve().parents[1],'2026-10-29')['is_rebalance'] is True
    assert h.schedule(Path(__file__).resolve().parents[1],'2026-10-30')['is_rebalance'] is False


def formal_root(tmp_path):
    root=tmp_path/'source'; output=root/'outputs';output.mkdir(parents=True)
    def write(name,text):
        path=output/name;path.write_bytes(text.encode());return path
    dt='2026-11-05';prefix=h.PREFIX
    effective='as_of_date,rank,symbol\n'+''.join(f'2026-10-29,{i+1},{i+1:06d}\n' for i in range(100))
    write(prefix+'_proxy_effective_members.csv',effective)
    member='rebalance_date,rank,symbol,name,market_cap\n'+''.join(f'2026-10-29,{i+1},{i+1:06d},fixture,1000\n' for i in range(100))
    write(prefix+'_proxy_members.csv',member)
    write('wind_microcap_top_100_biweekly_thursday_16y_cached.csv',f'date,close\n{dt},1000\n')
    write(prefix+'_panel_refreshed.csv',f'date,1.000852\n{dt},2000\n')
    write(prefix+'_proxy_turnover.csv','rebalance_date,execution_timing,return_start_date\n2026-10-29,close,2026-10-30\n')
    for v,r in h.REVISIONS.items():
        write('microcap_top100_mom16_biweekly_live_'+v.replace('.','_')+'_latest_signal.csv',f'date,strategy_revision\n{dt},{r}\n')
    prices=root/'.microcap_index_cache'/'prices_raw';prices.mkdir(parents=True)
    for i in range(100):(prices/f'{i+1:06d}.csv').write_text(f'date,close_raw\n{dt},10\n')
    refs={p.relative_to(root).as_posix():h.digest(p.read_bytes()) for p in output.iterdir()}
    manifest=dict(ok=True,status='complete',scope='whole_workspace_delivery',expected_date=dt,inputs=refs,artifacts={})
    (output/'top100_delivery_manifest.json').write_bytes(h.encode(manifest))
    return root


def test_bootstrap_reads_only_manifest_bound_exports_and_exact_member_day(tmp_path,monkeypatch):
    from types import SimpleNamespace
    root=formal_root(tmp_path)
    monkeypatch.setattr(h,'calendar_links',lambda anchor:('2026-11-06','2026-11-12'))
    monkeypatch.setattr(h.subprocess,'run',lambda *a,**kw:SimpleNamespace(stdout='a'*40))
    h.bootstrap(root,tmp_path/'export')
    cp=h.verify(tmp_path/'export'/'checkpoint.json')
    assert cp['next_trade_date']=='2026-11-06'
    assert len(h.read_bound(cp,'history'))<100
    assert len(json.loads(h.read_bound(cp,'prices'))['rows'])==100


def test_bootstrap_does_not_accept_mutated_historical_export(tmp_path,monkeypatch):
    root=formal_root(tmp_path)
    (root/'outputs'/'wind_microcap_top_100_biweekly_thursday_16y_cached.csv').write_text('date,close\n2026-11-05,999\n')
    with pytest.raises(ValueError,match='formal export hash'):h.bootstrap(root,tmp_path/'export')


def test_bootstrap_does_not_borrow_old_raw_close(tmp_path,monkeypatch):
    root=formal_root(tmp_path)
    (root/'.microcap_index_cache'/'prices_raw'/'000001.csv').write_text('date,close_raw\n2026-11-04,10\n')
    with pytest.raises(ValueError,match='raw close missing'):h.bootstrap(root,tmp_path/'export')


def test_rebalance_close_handoff_then_next_session_uses_new_basket(tmp_path,monkeypatch):
    # Deterministic software fixture; no prices/results are production evidence.
    from datetime import datetime
    import numpy as np
    import pandas as pd
    from scripts import member_daily_signal as daily, exchange_calendar
    dates=pd.bdate_range(end='2026-10-28',periods=400)
    sessions=list(dates.date)+[pd.Timestamp(d).date() for d in ('2026-10-29','2026-10-30','2026-11-02')]
    monkeypatch.setattr(exchange_calendar,'sessions_for_day',lambda d:tuple(sessions))
    x=np.arange(len(dates));frame=pd.DataFrame(dict(microcap=1000*np.exp(.0008*x+.02*np.sin(x/19)),hedge=1000*np.exp(.0002*x)),index=dates)
    buf=io.StringIO();frame.rename_axis('date').to_csv(buf,date_format='%Y-%m-%d')
    initial=package(tmp_path,'initial',day='2026-10-28',history=buf.getvalue().encode())
    mutate(initial,'period',lambda p:p.update(anchor_day='2026-10-15',effective_from='2026-10-16'))
    cp=json.loads(initial.read_bytes());cp['next_trade_date']='2026-10-29';initial.write_bytes(h.encode(cp))
    turnover=pd.read_csv(initial.parent/'turnover.csv');turnover['two_side_cost_rate']=pd.Series(dtype=float);turnover.to_csv(initial.parent/'turnover.csv',index=False)
    cp=json.loads(initial.read_bytes());cp['turnover']['sha256']=h.digest((initial.parent/'turnover.csv').read_bytes());initial.write_bytes(h.encode(cp))
    states={v:daily.clean(daily.calculate_native(v,m,frame,turnover)[0].iloc[0].to_dict()) for v,m in daily.native_modules().items()}
    mutate(initial,'version_state',lambda v:v.update(versions=states))
    def observation(day,previous,symbols):
        quotes=dict(market_date=day,collected_at=day+'T16:00:00+08:00',close_confirmed=True,rows=[dict(symbol=s,trade_date=day,previous_date=previous,previous_raw_close=10.,raw_close=10.,previous_close_adj=10.,close_adj=10.,return_basis_revision=daily.RETURN_BASIS,source='offline fixture') for s in symbols])
        hedge=dict(market_date=day,collected_at=quotes['collected_at'],close_confirmed=True,close=float(frame.hedge.iloc[-1]),source='offline fixture')
        return quotes,hedge
    symbols=[f'{i+1:06d}' for i in range(100)]
    quotes,hedge=observation('2026-10-29','2026-10-28',symbols)
    result=daily.build(Path(__file__).resolve().parents[1],initial,quotes,hedge,'2026-10-29','close_confirmed',tmp_path/'R',now=datetime.fromisoformat('2026-10-29T16:01:00+08:00'),calendar_sessions=sessions)
    assert result['period_status']=='overdue_reference' and result['checkpoint']=='checkpoint.json'
    r=h.verify(tmp_path/'R/checkpoint.json');assert r['period_status']=='awaiting_handoff'
    # Candidate changes only the future-effective membership, not R earned NAV.
    candidate=tmp_path/'candidate';candidate.mkdir()
    for f in (tmp_path/'R').iterdir():
        if f.is_file():(candidate/f.name).write_bytes(f.read_bytes())
    def new_period(p):
        p.update(package_id='new-period',anchor_day='2026-10-29',effective_from='2026-10-30')
        for m in p['effective_members']:m['symbol']=f"{int(m['symbol'])+100:06d}"
        p['rebalance_events']=[dict(rebalance_date='2026-10-29',execution_timing='close',execution_date='2026-10-29',effective_date='2026-10-29',return_start_date='2026-10-30',blocked_entry_count=2,blocked_exit_count=2)]
    mutate(candidate/'checkpoint.json','period',new_period)
    mutate(candidate/'checkpoint.json','prices',lambda p:[row.update(symbol=f"{int(row['symbol'])+100:06d}") for row in p['rows']])
    h.handoff(tmp_path/'R/checkpoint.json',candidate/'checkpoint.json','2026-10-29','2026-10-30',tmp_path/'handoff')
    adopted=h.verify(tmp_path/'handoff/checkpoint.json');assert adopted['period_status']=='current'
    assert h.read_bound(adopted,'history')==h.read_bound(r,'history')
    quotes,hedge=observation('2026-10-30','2026-10-29',[f'{i+101:06d}' for i in range(100)])
    result=daily.build(Path(__file__).resolve().parents[1],tmp_path/'handoff/checkpoint.json',quotes,hedge,'2026-10-30','close_confirmed',tmp_path/'N',now=datetime.fromisoformat('2026-10-30T16:01:00+08:00'),calendar_sessions=sessions)
    assert result['period_status']=='current' and result['status']=='OK'
    h.verify(tmp_path/'N/checkpoint.json')


@pytest.mark.parametrize('case',['valid','prefix_changed','prior_state_changed','no_native_acceptance'])
def test_1510_native_candidate_can_extend_previous_day_before_daily_report(tmp_path,monkeypatch,case):
    # R maintenance is first: there is no R daily checkpoint yet.
    from datetime import date
    import numpy as np
    import pandas as pd
    from scripts import exchange_calendar,member_daily_signal as daily
    dates=pd.bdate_range(end='2026-10-28',periods=400)
    sessions=list(dates.date)+[date(2026,10,29),date(2026,10,30),date(2026,11,2)]
    monkeypatch.setattr(exchange_calendar,'sessions_for_day',lambda d:tuple(sessions))
    x=np.arange(len(dates));frame=pd.DataFrame(dict(microcap=1000*np.exp(.001*x+.02*np.sin(x/17)),hedge=1000*np.exp(.0001*x)),index=dates)
    buf=io.StringIO();frame.rename_axis('date').to_csv(buf,date_format='%Y-%m-%d')
    old=package(tmp_path,'before_R',day='2026-10-28',history=buf.getvalue().encode())
    cp=json.loads(old.read_bytes());cp['next_trade_date']='2026-10-29';old.write_bytes(h.encode(cp))
    # Native full-history states at D are inherited, never reset to cash.
    turnover=pd.DataFrame(columns=['rebalance_date','two_side_cost_rate']);turnover.to_csv(old.parent/'turnover.csv',index=False)
    cp=json.loads(old.read_bytes());cp['turnover']['sha256']=h.digest((old.parent/'turnover.csv').read_bytes());old.write_bytes(h.encode(cp))
    state_d={v:daily.clean(daily.calculate_native(v,m,frame,turnover)[0].iloc[0].to_dict()) for v,m in daily.native_modules().items()}
    mutate(old,'version_state',lambda p:p.update(versions=state_d))
    extended=frame.copy();extended.loc[pd.Timestamp('2026-10-29')]=[float(frame.microcap.iloc[-1])*1.01,float(frame.hedge.iloc[-1])*1.002]
    buf=io.StringIO();extended.rename_axis('date').to_csv(buf,date_format='%Y-%m-%d')
    candidate=package(tmp_path,'native_R',day='2026-10-29',history=buf.getvalue().encode())
    state_r={v:daily.clean(daily.calculate_native(v,m,extended,turnover)[0].iloc[0].to_dict()) for v,m in daily.native_modules().items()}
    mutate(candidate,'version_state',lambda p:p.update(versions=state_r))
    mutate(candidate,'period',lambda p:p.update(source_manifest_sha256='b'*64,source_files={'outputs/native.csv':'c'*64}))
    cp=json.loads(candidate.read_bytes());cp['bootstrap_source']='existing_formal_exports';candidate.write_bytes(h.encode(cp))
    if case=='prefix_changed':
        cp=json.loads(candidate.read_bytes());path=candidate.parent/cp['history']['path'];f=pd.read_csv(path);f.loc[0,'microcap']+=1;f.to_csv(path,index=False);cp['history']['sha256']=h.digest(path.read_bytes());candidate.write_bytes(h.encode(cp))
    if case=='prior_state_changed':mutate(old,'version_state',lambda p:p['versions']['v2.3'].update(next_holding='unmatched'))
    if case=='no_native_acceptance':
        cp=json.loads(candidate.read_bytes());cp.pop('bootstrap_source');candidate.write_bytes(h.encode(cp))
    if case!='valid':
        with pytest.raises(ValueError):h.handoff(old,candidate,'2026-10-29','2026-10-30',tmp_path/'adopted')
        return
    h.handoff(old,candidate,'2026-10-29','2026-10-30',tmp_path/'adopted')
    adopted=h.verify(tmp_path/'adopted/checkpoint.json')
    assert adopted['handoff_mode']=='native_close_extends_previous'
    assert adopted['as_of_date']=='2026-10-29' and adopted['next_trade_date']=='2026-10-30'
    assert h.read_bound(adopted,'history')==h.read_bound(h.verify(candidate),'history')
    assert h.read_bound(adopted,'version_state')==h.read_bound(h.verify(candidate),'version_state')
