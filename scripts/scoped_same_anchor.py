"""Native semantic replay when new morning evidence has the same close date.

New observations never acquire an old clock. Source/metadata adoption must first
protect every consumed historical value. This module recomputes the last native
window, earns actual fingerprints, and leaves all economic CSV bytes unchanged.
"""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

from scripts import scoped_source_consumer as sources
from scripts import scoped_forward_consumption as forward


def window(root,prior,effective):
    proof=prior.get('return_basis_consumption',{})
    if proof.get('revision')==forward.REVISION:
        segment=proof['segments'][-1];item=segment['inputs']
        raw=sources.bound(root,item['path']).read_bytes()
        if sources.sha(raw)!=item['sha256']:raise ValueError('Same-anchor original input receipt changed')
        data=json.loads(raw)
        return data['dates'],segment['symbols'],data['initial'],data
    tails=proof.get('tail_extensions')
    if proof.get('revision')!='stock_return_consumption_v1' or not tails:
        raise ValueError('Same-anchor refresh requires a previously authenticated native tail, not an implicit full rebuild')
    from scripts.exchange_calendar import sessions_for_day
    tail=tails[-1]
    days=[d.isoformat() for d in sessions_for_day(pd.Timestamp(prior['end_date']).date())
          if tail['start_exclusive']<=d.isoformat()<=prior['end_date']]
    if set(tail['symbols'])!=set(effective):raise ValueError('Same-anchor held portfolio differs from original tail')
    return days,list(effective),list(effective),None


def compare(root,base,dates,index,members,turnover,terminal,paths):
    full=pd.read_csv(root/forward.FILES['index'],parse_dates=['date'])
    expected=full.loc[full.date.isin(pd.DatetimeIndex(dates))].set_index('date')
    observed=index.set_index('date')
    if not observed.index.equals(expected.index):raise ValueError('Same-anchor replay lacks complete original sessions')
    np.testing.assert_allclose(observed.daily_return.iloc[1:],expected.daily_return.iloc[1:],rtol=0,
                               atol=8*np.finfo(float).eps)
    levels=observed.close.iloc[1:].to_numpy()*(float(expected.close.iloc[0])/float(observed.close.iloc[0]))
    np.testing.assert_allclose(expected.close.iloc[1:],levels,rtol=8*np.finfo(float).eps,atol=0)
    if observed.holding_count.iloc[1:].tolist()!=expected.holding_count.iloc[1:].tolist():
        raise ValueError('Same-anchor actual holding counts changed')
    effective=pd.read_csv(paths['proxy_effective_members'],dtype={'symbol':str}).sort_values('rank').symbol.tolist()
    if terminal!=effective:raise ValueError('Same-anchor constrained execution changed the actual portfolio')
    saved=pd.read_csv(paths['proxy_members'],dtype={'symbol':str})
    for day,group in members.groupby('rebalance_date') if not members.empty else []:
        original=saved.loc[pd.to_datetime(saved.rebalance_date).eq(pd.Timestamp(day))].sort_values('rank')
        if original.symbol.tolist()!=group.sort_values('rank').symbol.tolist():
            raise ValueError('Same-anchor replay changed a consumed target')
    saved_turnover=pd.read_csv(paths['proxy_turnover'])
    expected_turnover=saved_turnover.loc[pd.to_datetime(saved_turnover.rebalance_date)>pd.Timestamp(dates[0])].reset_index(drop=True)
    if not (turnover.empty and expected_turnover.empty):
        left=turnover.copy();right=expected_turnover.copy()
        for frame in (left,right):
            for key in ('rebalance_date','constraint_trade_date','execution_date','effective_date','return_start_date'):
                if key in frame:frame[key]=pd.to_datetime(frame[key],format='ISO8601',errors='raise')
        pd.testing.assert_frame_equal(left,right,check_dtype=False,check_exact=False,
            rtol=8*np.finfo(float).eps,atol=8*np.finfo(float).eps)


def recompute(root,base,args,paths,target,prior,effective,context):
    from scripts.st_metadata_refresh import frozen_notice_sources
    dates,scope,initial,old=window(root,prior,effective)
    # Cross the unchanged native source refresh hook after the old-state gate.
    base.refresh_price_cache_tail(pd.Timestamp(target),args.max_workers,context['plan']['symbols'],force_refresh=False)
    context['capture_only']=True
    try:
        with frozen_notice_sources(root):
            index,members,turnover,meta=base.build_local_proxy_bundle(args=args,
                trading_dates=pd.DatetimeIndex(dates),symbols=scope,initial_members=initial)
    finally:context['capture_only']=False
    data=context['forward_inputs']
    if data is None:raise ValueError('Same-anchor replay did not retain original native calculation inputs')
    if old is not None:
        if data['targets']!=old['targets'] or set(data['caps'])!=set(old['caps']):
            raise ValueError('Same-anchor original ranking/qualification changed')
        for day,values in old['caps'].items():
            if set(values)!=set(data['caps'][day]):raise ValueError('Same-anchor historical qualification changed')
            for symbol,value in values.items():
                if abs(value-data['caps'][day][symbol])>0.01:raise ValueError('Same-anchor historical capitalization changed')
    compare(root,base,dates,index,members,turnover,meta['terminal_effective_members'],paths)
    return meta['core_params'],dict(schema='native_same_anchor_replay_v1',status='passed',target=target,
        scope=scope,native_inputs=data,earned_core_params=meta['core_params'],
        previous_proxy_sha256=sources.sha(sources.bound(root,forward.FILES['meta']).read_bytes()),
        source_bundle_sha256=context['config']['bundle_sha256'],economic_csv_bytes_changed=False)


def retire_on_advance(prior,updated,target):
    """A same-close receipt remains evidence of its original state only."""
    if pd.Timestamp(target)<=pd.Timestamp(prior['end_date']):return
    spec=updated.get('same_anchor_native_revalidation')
    if spec is not None and spec!=prior.get('same_anchor_native_revalidation'):
        raise ValueError('Advancing native state inherited an unexplained same-anchor receipt')
    updated.pop('same_anchor_native_revalidation',None)


def _checked_receipt(root,proxy,marker):
    spec=proxy.get('same_anchor_native_revalidation')
    if not spec:return None
    path=sources.bound(root,spec['path']);raw=path.read_bytes()
    if sources.sha(raw)!=spec['sha256']:raise ValueError('Same-anchor replay receipt changed')
    receipt=json.loads(raw)
    if (receipt.get('schema')!='native_same_anchor_replay_v1' or receipt.get('status')!='passed'
            or receipt.get('target')!=proxy['end_date'] or receipt.get('earned_core_params')!=proxy['core_params']
            or receipt.get('previous_proxy_sha256')!=marker['original_proxy']['sha256']
            or receipt.get('source_bundle_sha256')!=marker['source_bundle_sha256']
            or receipt.get('economic_csv_bytes_changed') is not False):
        raise ValueError('Same-anchor replay binding differs from the actual native state')
    return spec,raw,receipt


def archived_receipt_files(root,proxy,owner):
    """Authenticate old bytes against their old proxy/marker, never today's state."""
    if not proxy.get('same_anchor_native_revalidation'):return {}
    if not owner:raise ValueError('Archived same-anchor replay has no original metadata owner')
    body=sources.bound(root,owner['path']).read_bytes()
    if sources.sha(body)!=owner['sha256']:raise ValueError('Archived same-anchor metadata owner changed')
    marker=json.loads(body)
    if (marker.get('schema')!='native_scoped_metadata_adoption_v1' or marker.get('status')!='complete'
            or marker.get('target')!=proxy['end_date']
            or proxy.get('scoped_metadata_adoption')!={
                'path':'.microcap_index_cache/scoped_native_consumer.json','sha256':owner['sha256']}):
        raise ValueError('Archived same-anchor proxy and original metadata owner differ')
    spec,raw,_receipt=_checked_receipt(root,proxy,marker)
    original=marker['original_proxy'];previous=sources.bound(root,original['path']).read_bytes()
    if sources.sha(previous)!=original['sha256']:raise ValueError('Archived same-anchor original proxy changed')
    return {owner['path']:owner['sha256'],original['path']:original['sha256'],spec['path']:sources.sha(raw)}


def validate(root,base,proxy,marker,paths):
    checked=_checked_receipt(root,proxy,marker)
    if checked is None:return {}
    spec,raw,receipt=checked
    prior_path=sources.bound(root,marker['original_proxy']['path']);prior=json.loads(prior_path.read_bytes())
    effective=pd.read_csv(paths['proxy_effective_members'],dtype={'symbol':str}).sort_values('rank').symbol.tolist()
    dates,scope,initial,old=window(root,prior,effective)
    data=receipt['native_inputs']
    if data['dates']!=dates or data['initial']!=initial or receipt['scope']!=scope:
        raise ValueError('Same-anchor replay changed its independently proven original window')
    if old is not None:
        if data['targets']!=old['targets'] or set(data['caps'])!=set(old['caps']):
            raise ValueError('Same-anchor receipt changed original qualification')
        for day,values in old['caps'].items():
            if set(values)!=set(data['caps'][day]) or any(abs(value-data['caps'][day][s])>0.01 for s,value in values.items()):
                raise ValueError('Same-anchor receipt changed a consumed capitalization')
    index,turnover=forward.replay(data,scope,base)
    members=pd.DataFrame([dict(rebalance_date=day,rank=i+1,symbol=s)
        for day,symbols in data['targets'].items() for i,s in enumerate(symbols)])
    compare(root,base,dates,index,members,turnover,data['terminal'],paths)
    return {spec['path']:sources.sha(raw)}
