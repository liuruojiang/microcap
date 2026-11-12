"""Native forward replay receipts with issuer-specific historical horizons.

The legacy full-universe anchor stays frozen. Each new segment authenticates its
original inputs, replay, executed authorities and complete session coverage.
This is a transport/continuation contract, never a historical-universe shrink.
"""
from __future__ import annotations

import copy
import io
import json
from pathlib import Path

import numpy as np
import pandas as pd

from scripts import scoped_source_consumer as sources

REVISION='stock_return_consumption_v2'
PREFIX='.microcap_index_cache/forward_consumption/'
FILES=dict(index='outputs/wind_microcap_top_100_biweekly_thursday_16y_cached.csv',
    meta='outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_meta.json',
    members='outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_members.csv',
    turnover='outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_turnover.csv',
    effective='outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_effective_members.csv',
    costed='outputs/microcap_top100_mom16_hedge_zz1000_0p8x_biweekly_thursday_16y_v2_0_base_costed_nav.csv')


def _frame(frame):
    return dict(dates=[str(pd.Timestamp(d).date()) for d in frame.index],index_name=frame.index.name,
        symbols=list(frame.columns),values=frame.astype(object).where(frame.notna(),None).values.tolist())


def _restore(value):
    frame=pd.DataFrame(value['values'],index=pd.DatetimeIndex(value['dates']),columns=value['symbols'])
    frame.index.name=value.get('index_name')
    if frame.index.has_duplicates or frame.columns.has_duplicates or not frame.index.is_monotonic_increasing:
        raise ValueError('Forward input matrix has ambiguous dates or symbols')
    return frame


def capture(*, trading_dates,returns_df,caps_by_date,buyable_df,sellable_df,
            initial_members,target_members_map,rebalance_dates,index_df,turnover_df,
            effective_members_map,delist_dates):
    """Serialize original native values, not an alternate execution model."""
    return dict(schema='native_forward_inputs_v1',dates=[str(pd.Timestamp(d).date()) for d in trading_dates],
        returns=_frame(returns_df),buyable=_frame(buyable_df),sellable=_frame(sellable_df),
        caps={str(pd.Timestamp(d).date()):{s:float(v) for s,v in caps.items()} for d,caps in caps_by_date.items()},
        initial=list(initial_members),rebalance_dates=[str(pd.Timestamp(d).date()) for d in rebalance_dates],
        targets={str(pd.Timestamp(d).date()):list(v) for d,v in target_members_map.items()},
        effective={str(pd.Timestamp(d).date()):list(v) for d,v in effective_members_map.items()},
        terminal=list(index_df.attrs['terminal_effective_members']),
        delist_dates={s:str(pd.Timestamp(d).date()) for s,d in delist_dates.items()},
        index=_frame(index_df.set_index('date')),
        turnover=turnover_df.to_json(orient='table',date_format='iso',double_precision=15))


def replay(inputs,scope,base):
    if inputs.get('schema')!='native_forward_inputs_v1':
        raise ValueError('Unknown native forward input contract')
    dates=pd.DatetimeIndex(inputs['dates'])
    if dates.has_duplicates or not dates.is_monotonic_increasing or len(dates)<2:
        raise ValueError('Forward segment must advance complete ordered sessions')
    frames={k:_restore(inputs[k]) for k in ('returns','buyable','sellable')}
    for key,frame in frames.items():
        if not frame.index.equals(dates) or not set(frame.columns)<=set(scope):
            raise ValueError('Forward matrix escapes its native session/scope')
        if key!='returns' and any(type(v) not in (bool,np.bool_) for v in frame.to_numpy().ravel()):
            raise ValueError('Native trade constraints must be explicit booleans')
    initial=inputs['initial']
    if len(initial)!=100 or len(set(initial))!=100 or not set(initial)<=set(scope):
        raise ValueError('Forward seed is not the actual complete held portfolio')
    scheduled=base.build_biweekly_rebalance_dates(dates)
    scheduled=scheduled[scheduled>dates[0]]
    if list(scheduled.strftime('%Y-%m-%d'))!=inputs['rebalance_dates']:
        raise ValueError('Forward segment changed the native rebalance calendar')
    caps={pd.Timestamp(d):v for d,v in inputs['caps'].items()}
    if set(caps)!=set(scheduled) or any(not set(v)<=set(scope) for v in caps.values()):
        raise ValueError('Forward native ranking scope/date differs')
    targets=base.build_live_target_members_map(caps,scheduled,{},top_n=100)
    if {str(d.date()):v for d,v in targets.items()}!=inputs['targets']:
        raise ValueError('Forward target differs from original cap/symbol ranking')
    index,turnover,effective=base.freq_mod.simulate_rebalance_path(trading_dates=dates,
        returns_df=frames['returns'],target_members_map=targets,rebalance_dates=scheduled,
        buyable_df=frames['buyable'],sellable_df=frames['sellable'],one_side_cost_rate=0.003,
        top_n=100,execution_timing=base.EXECUTION_TIMING,initial_members=initial,
        delist_dates={s:pd.Timestamp(d) for s,d in inputs['delist_dates'].items()})
    if index.attrs['terminal_effective_members']!=inputs['terminal'] or {
            str(d.date()):v for d,v in effective.items()}!=inputs['effective']:
        raise ValueError('Forward executed members differ from native constrained replay')
    expected=_restore(inputs['index'])
    pd.testing.assert_frame_equal(index.set_index('date'),expected,check_dtype=False,
        check_exact=False,rtol=8*np.finfo(float).eps,atol=8*np.finfo(float).eps)
    expected_turnover=pd.read_json(io.StringIO(inputs['turnover']),orient='table')
    if len(turnover.columns) or len(expected_turnover.columns) or not (turnover.empty and expected_turnover.empty):
        pd.testing.assert_frame_equal(turnover,expected_turnover,check_dtype=False,
            check_exact=False,rtol=8*np.finfo(float).eps,atol=8*np.finfo(float).eps)
    return index,turnover


def snapshot(root,directory,payloads):
    directory=Path(directory)
    relative=directory.relative_to(root).as_posix()
    if not relative.startswith(PREFIX) or set(payloads)!=set(FILES):
        raise ValueError('Forward snapshot has an unsupported role or directory')
    files={}
    for role,name in FILES.items():
        path=directory/name;raw=payloads[role]
        sources.atomic(path,raw)
        files[role]=dict(path=path.relative_to(root).as_posix(),sha256=sources.sha(raw))
    return dict(directory=relative,files=files)


def _snapshot(root,spec,stable):
    if (not isinstance(spec.get('directory'),str) or not spec['directory'].startswith(PREFIX)
            or set(spec.get('files',{}))!=set(FILES)):
        raise ValueError('Forward authority snapshot contract differs')
    directory=sources.bound(root,spec['directory']);payloads={}
    for role,name in FILES.items():
        expected=spec['directory']+'/'+name;item=spec['files'][role]
        if item.get('path')!=expected:
            raise ValueError('Forward snapshot source path was rebound')
        path=sources.bound(root,expected);raw=path.read_bytes()
        if sources.sha(raw)!=item.get('sha256'):
            raise ValueError('Forward authority snapshot bytes changed')
        stable[path]=raw;payloads[role]=raw
    return directory,payloads


def _table(raw,*,symbols=False):
    return pd.read_csv(io.BytesIO(raw),dtype={'symbol':str} if symbols else None)


def _members(raw):
    frame=_table(raw,symbols=True)
    if (len(frame)!=100 or frame.symbol.nunique()!=100 or set(pd.to_numeric(frame['rank']))!=set(range(1,101))
            or frame.as_of_date.nunique()!=1):
        raise ValueError('Forward executed authority is not a dated 100-member set')
    return frame.sort_values('rank').symbol.tolist()


def _history(before,after,column,end,*,turnover=False):
    def calendar_days(values):
        # Native CSV writers may retain an exact midnight suffix on only the
        # newest row. Parse that ISO representation without inferring one
        # format from the first row, or accepting intraday/timezone values.
        present=values.dropna().astype(str)
        if not present.str.fullmatch(r'\d{4}-\d{2}-\d{2}(?:[ T]00:00:00(?:\.0{1,9})?)?').all():
            raise ValueError('Forward history date must be an ISO calendar day or exact midnight')
        return pd.to_datetime(values,format='ISO8601',errors='raise')

    old=_table(before,symbols=True);new=_table(after,symbols=True)
    for frame in (old,new):
        frame[column]=calendar_days(frame[column])
    new=new.loc[new[column]<=pd.Timestamp(end)].reset_index(drop=True)
    if turnover:
        for name in ('constraint_trade_date','execution_date','effective_date','return_start_date'):
            for frame in (old,new):frame[name]=calendar_days(frame[name])
        # The original native close event may acquire its first return date in
        # the following segment. No execution or turnover count is rewritten.
        pending=old.return_start_date.isna()
        if pending.any():
            if (old.loc[pending,'execution_date']!=pd.Timestamp(end)).any():
                raise ValueError('Unresolved historical turnover predates the bridge')
            expected=new.loc[pending,'return_start_date']
            if expected.notna().any():
                from scripts.exchange_calendar import sessions_for_day
                calendar=sorted(d for d in sessions_for_day(pd.Timestamp(end).date()) if d>pd.Timestamp(end).date())
                if not calendar or (expected.dropna()!=pd.Timestamp(calendar[0])).any():
                    raise ValueError('A close event was assigned a false first-return date')
            old.loc[pending,'return_start_date']=expected
    pd.testing.assert_frame_equal(old.reset_index(drop=True),new,check_dtype=False,
        check_exact=False,rtol=8*np.finfo(float).eps,atol=8*np.finfo(float).eps)


def append(root,work,prior,old_payloads,new_payloads,inputs,scope,bundle_sha):
    """Called after native calculation and exact old economic-prefix checks."""
    root=Path(root).resolve();work=Path(work)
    proof=prior.get('return_basis_consumption')
    if not proof or proof.get('revision') not in ('stock_return_consumption_v1',REVISION):
        raise ValueError('Forward continuation lacks an authenticated legacy consumption seed')
    if proof['revision']==REVISION:
        result=copy.deepcopy(proof)
    else:
        result=dict(revision=REVISION,legacy_target=prior['end_date'],
            legacy=snapshot(root,work/'legacy',old_payloads),segments=[])
    raw=json.dumps(inputs,ensure_ascii=False,separators=(',',':'),allow_nan=False).encode()
    path=work/'native_inputs.json';sources.atomic(path,raw)
    result['segments'].append(dict(start_exclusive=inputs['dates'][0],end=inputs['dates'][-1],
        symbols=list(scope),source_bundle_sha256=bundle_sha,
        inputs=dict(path=path.relative_to(root).as_posix(),sha256=sources.sha(raw)),
        snapshot=snapshot(root,work/'native_result',new_payloads)))
    return result


def dependency_names(root,proof):
    """Exact active proof dependencies for the existing full-byte memo gate."""
    names=set()
    for spec in [proof['legacy']]+[s['snapshot'] for s in proof['segments']]:
        names.update(item['path'] for item in spec['files'].values())
    for segment in proof['segments']:
        item=segment['inputs'];names.add(item['path'])
        raw=sources.bound(root,item['path']).read_bytes()
        if sources.sha(raw)!=item['sha256']:raise ValueError('Forward dependency receipt changed')
        for boundary in json.loads(raw).get('boundaries',{}).values():
            names.update(boundary['source_files'])
    for name in names:sources.bound(root,name)
    return sorted(names)


def requirements(root,target,requested,meta,meta_path,meta_raw,*,base=None):
    from scripts.stock_return_pipeline import _consumption_requirements
    from scripts.exchange_calendar import sessions_for_day
    if base is None:
        import microcap_top100_mom16_biweekly_live_v2_0 as v
        base=v.base_mod
    root=Path(root);proof=meta['return_basis_consumption'];stable={meta_path:meta_raw}
    directory,previous=_snapshot(root,proof['legacy'],stable)
    legacy=json.loads(previous['meta'])
    if legacy.get('return_basis_consumption',{}).get('revision')!='stock_return_consumption_v1':
        raise ValueError('Forward legacy seed must be original v1 consumption')
    cursor=proof['legacy_target']
    if (legacy.get('end_date')!=cursor or cursor>target
            or str(pd.Timestamp(_table(previous['index']).date.iloc[-1]).date())!=cursor
            or str(pd.Timestamp(_table(previous['costed']).date.iloc[-1]).date())!=cursor):
        raise ValueError('Forward legacy date differs from its original native seed')
    full,live,observed=_consumption_requirements(directory,cursor,requested)
    stable.update(observed);horizons={s:cursor for s in live}
    initial=_members(previous['effective'])
    calendar={d.isoformat() for d in sessions_for_day(pd.Timestamp(target).date())}
    if not isinstance(proof.get('segments'),list) or not proof['segments']:
        raise ValueError('Forward consumption has no native segments')
    for segment in proof['segments']:
        end=segment['end'];scope=segment['symbols']
        if (segment['start_exclusive']!=cursor or not cursor<end<=target or end not in calendar
                or len(scope)!=300 or len(set(scope))!=300 or not set(scope)<=set(requested)):
            raise ValueError('Forward scope or contiguous exchange-session coverage differs')
        item=segment['inputs'];path=sources.bound(root,item['path'])
        if not item['path'].startswith(PREFIX):raise ValueError('Forward input receipt escaped its journal')
        raw=path.read_bytes()
        if sources.sha(raw)!=item['sha256']:raise ValueError('Forward native inputs changed')
        stable[path]=raw;inputs=json.loads(raw)
        expected=sorted(d for d in calendar if cursor<=d<=end)
        if inputs['dates']!=expected or inputs['initial']!=initial:
            raise ValueError('Forward native seed/session coverage differs from prior execution')
        replayed,turnover=replay(inputs,scope,base)
        if set(inputs.get('boundaries',{}))!=set(inputs['rebalance_dates']):
            raise ValueError('Forward rebalance lacks its dated whole-market boundary proof')
        from scripts.scoped_cap_boundary import verify
        for day,boundary in inputs['boundaries'].items():
            for name,digest in boundary['source_files'].items():
                p=sources.bound(root,name);body=p.read_bytes()
                if sources.sha(body)!=digest:raise ValueError('Forward boundary source bytes changed')
                stable[p]=body
            checked=verify(root,base,day,inputs['caps'][day],scope,boundary['snapshot'],boundary['authority'])
            if checked!=boundary:raise ValueError('Forward whole-market boundary decision differs')
        _,current=_snapshot(root,segment['snapshot'],stable)
        if (json.loads(current['meta']).get('end_date')!=end
                or str(pd.Timestamp(_table(current['costed']).date.iloc[-1]).date())!=end):
            raise ValueError('Forward native outputs do not share their certified target')
        if not current['index'].startswith(previous['index']):
            raise ValueError('Forward extension rewrites the frozen proxy prefix')
        current_index=_table(current['index']);current_index['date']=pd.to_datetime(current_index.date)
        tail=current_index.loc[current_index.date>pd.Timestamp(cursor)].set_index('date')
        if tail.index.strftime('%Y-%m-%d').tolist()!=expected[1:]:
            raise ValueError('Forward proxy does not cover every new exchange session')
        np.testing.assert_allclose(tail.daily_return,replayed.daily_return.iloc[1:],rtol=0,atol=8*np.finfo(float).eps)
        bridge_level=float(_table(previous['index']).close.iloc[-1])
        expected_level=replayed.close.iloc[1:].to_numpy()*(bridge_level/float(replayed.close.iloc[0]))
        np.testing.assert_allclose(tail.close,expected_level,rtol=8*np.finfo(float).eps,atol=0)
        if tail.holding_count.tolist()!=replayed.holding_count.iloc[1:].tolist():
            raise ValueError('Forward proxy holding counts differ from native replay')
        if 'holding_effective' in tail and tail.holding_effective.tolist()!=tail.holding_count.gt(0).tolist():
            raise ValueError('Forward proxy effective-holding flag differs from its native count')
        if not current['costed'].startswith(previous['costed']):
            raise ValueError('Forward extension rewrites the approved costed NAV prefix')
        initial=_members(current['effective'])
        if initial!=inputs['terminal'] or not set(initial)<=set(scope):
            raise ValueError('Forward final holdings differ from actual native execution')
        _history(previous['members'],current['members'],'rebalance_date',cursor)
        _history(previous['turnover'],current['turnover'],'rebalance_date',cursor,turnover=True)
        new_members=_table(current['members'],symbols=True)
        for day,symbols in inputs['targets'].items():
            rows=new_members.loc[new_members.rebalance_date.astype(str).str[:10].eq(day)].sort_values('rank')
            if rows.symbol.tolist()!=symbols:raise ValueError('Saved forward target differs from native ranking')
        new_turnover=_table(current['turnover'])
        fresh=new_turnover.loc[pd.to_datetime(new_turnover.rebalance_date)>pd.Timestamp(cursor)].reset_index(drop=True)
        native=turnover.copy()
        for frame in (fresh,native):
            for c in ('rebalance_date','constraint_trade_date','execution_date','effective_date','return_start_date'):
                if c in frame:frame[c]=pd.to_datetime(frame[c],format='ISO8601',errors='raise')
        if not (fresh.empty and native.empty):
            pd.testing.assert_frame_equal(fresh,native,check_dtype=False,check_exact=False,
                rtol=8*np.finfo(float).eps,atol=8*np.finfo(float).eps)
        # Conservative: keep all 300 ranked/maintained issuers certified, not
        # only the final 100. Removed issuers retain their last segment horizon.
        for symbol in scope:horizons[symbol]=end
        previous=current;cursor=end
    if cursor!=target:raise ValueError('Forward proof stops before the delivered proxy anchor')
    for role in ('index','members','turnover','effective','costed'):
        path=sources.bound(root,FILES[role]);raw=path.read_bytes();stable[path]=raw
        if raw!=previous[role]:raise ValueError('Current executed authority differs from the final native snapshot')
    return full,horizons,stable
