"""Staged whole-universe metadata renewal after authenticated notice append.

This is not an atomic whole-strategy transaction. An incomplete marker survives
failure and must be reconciled before another strategy refresh can proceed.
"""
from datetime import date, timedelta
from pathlib import Path
import json
import os
import uuid

from scripts.st_notice_refresh import _atomic, _bytes, _sha, _bound, FRESHNESS
from scripts.st_notice_evidence import StNoticeEvidenceError
from scripts.st_history_scope import resolve_archive_reference, source_closure

MARKER = '.microcap_index_cache/st_metadata_refresh.json'


def _fail(message):
    raise StNoticeEvidenceError(message)


def frozen_notice_sources(root):
    from scripts.st_history_scope import validated_source_batch
    return validated_source_batch(root)


def _history_equal(old, new, boundary):
    """Compare exact inclusive ST and active semantics at every interval boundary."""
    points={date(1900,1,1),date.fromisoformat(boundary)}
    for payload in (old,new):
        for key in ('list_date','delist_date'):
            if payload.get(key): points.add(date.fromisoformat(payload[key]))
        for interval in payload.get('st_intervals',[]):
            start=date.fromisoformat(interval['start']);points.add(start)
            if interval.get('end'):
                end=date.fromisoformat(interval['end']);points.update((end,end+timedelta(days=1)))
    cutoff=date.fromisoformat(boundary)
    def state(payload, day):
        active=(not payload.get('list_date') or day>=date.fromisoformat(payload['list_date'])) and (not payload.get('delist_date') or day<date.fromisoformat(payload['delist_date']))
        st=any(day>=date.fromisoformat(i['start']) and (not i.get('end') or day<=date.fromisoformat(i['end'])) for i in payload.get('st_intervals',[]))
        return active,st
    return all(state(old,p)==state(new,p) for p in points if p<=cutoff)


def _refresh_metadata_after_notice(*, root, freq_mod, target, notice_result, consumed_through):
    root=Path(root).resolve();marker=root/MARKER
    if Path(getattr(freq_mod,'ROOT',root)).resolve()!=root:
        _fail('Metadata builder belongs to another runtime root')
    pending=json.loads(marker.read_bytes()) if marker.exists() else None
    symbols=sorted(str(s).zfill(6) for s in freq_mod.list_backtest_universe_symbols())
    if not symbols or len(symbols)!=len(set(symbols)):
        _fail('ST metadata universe is empty or duplicated')
    if pending and pending['status']=='complete' and pending['authority']==list(resolve_archive_reference(root)) and pending['target']==str(target):
        if sorted(pending['new_hashes'])!=symbols:
            _fail('Completed metadata transaction universe changed')
        for symbol,digest in pending['new_hashes'].items():
            if _sha((root/'.microcap_index_cache/security_meta'/(symbol+'.json')).read_bytes())!=digest:
                _fail('Completed metadata transaction was changed externally')
        return {'rebuilt':False,'requires_full_rebuild':True}
    import pandas as pd
    if not notice_result['authority_changed'] and not (pending and pending['status']!='complete'):
        current_hash=resolve_archive_reference(root)[1]
        existing=list((root/'.microcap_index_cache/security_meta').glob('*.json'))
        if sorted(p.stem for p in existing)==symbols and all((lambda payload: payload.get('st_notice_archive_sha256')==current_hash and freq_mod._notice_horizon_covers(payload,pd.Timestamp(target)))(json.loads(p.read_bytes())) for p in existing):
            return {'rebuilt':False,'requires_full_rebuild':False}
    if consumed_through is None:
        _fail('Cannot refresh metadata without independently read consumed proxy end')
    consumed=date.fromisoformat(str(consumed_through))
    ref=resolve_archive_reference(root)
    destination=root/'.microcap_index_cache/security_meta'
    if pending and pending['status']!='complete':
        if pending['authority']!=list(ref) or pending['target']!=str(target) or pending['symbols']!=symbols:
            _fail('Unfinished ST metadata transaction must be resumed at its exact authority and target')
        work=_bound(root,pending['work']);baseline=pending
    else:
        proof=json.loads((root/FRESHNESS).read_bytes());previous=proof['previous_authority']
        oldmanifest=json.loads(_bound(root,previous[0]).read_bytes())
        boundary=oldmanifest['daily_increments'][-1]['date']
        work=root/'.microcap_index_cache/st_metadata_refresh'/uuid.uuid4().hex
        (work/'backup').mkdir(parents=True)
        baseline=dict(status='incomplete',authority=list(ref),previous_authority=previous,target=str(target),boundary=boundary,symbols=symbols,work=work.relative_to(root).as_posix(),old_hashes={})
        for symbol in symbols:
            path=destination/(symbol+'.json');raw=path.read_bytes();old=json.loads(raw)
            if old.get('st_notice_archive_sha256')!=previous[1]:
                _fail('Metadata seed is not bound to previous archive: '+symbol)
            (work/'backup'/path.name).write_bytes(raw);baseline['old_hashes'][symbol]=_sha(raw)
        _atomic(marker,_bytes(baseline))
    stage=work/('stage_'+uuid.uuid4().hex);stage.mkdir()
    old_dir=freq_mod.SECURITY_META_DIR
    builder_globals=freq_mod.build_security_meta.__globals__
    global_restore={}
    blocked={}
    def no_network(*args,**kwargs):_fail('Network fallback forbidden in frozen metadata batch')
    try:
        freq_mod.SECURITY_META_DIR=stage
        if 'SECURITY_META_DIR' in builder_globals:
            global_restore['SECURITY_META_DIR']=builder_globals['SECURITY_META_DIR'];builder_globals['SECURITY_META_DIR']=stage
        if 'SHARED_SECURITY_META_DIR' in builder_globals:
            global_restore['SHARED_SECURITY_META_DIR']=builder_globals['SHARED_SECURITY_META_DIR'];builder_globals['SHARED_SECURITY_META_DIR']=None
        for name in ('fetch_cninfo_st_notices','fetch_sz_name_change_history'):
            if hasattr(freq_mod,name):blocked[name]=getattr(freq_mod,name);setattr(freq_mod,name,no_network)
            if name in builder_globals:global_restore[name]=builder_globals[name];builder_globals[name]=no_network
        with frozen_notice_sources(root):
            for symbol in symbols:
                oldraw=(work/'backup'/(symbol+'.json')).read_bytes()
                if _sha(oldraw)!=baseline['old_hashes'][symbol]:_fail('ST metadata backup changed')
                old=json.loads(oldraw)
                new=freq_mod.build_security_meta(symbol,as_of=target)
                if not new or new.get('st_notice_archive_sha256')!=ref[1] or not new.get('notice_source_complete'):
                    _fail('Incomplete rebuilt ST metadata: '+symbol)
                if not freq_mod._notice_horizon_covers(new,__import__('pandas').Timestamp(target)):
                    _fail('Rebuilt ST notice horizon incomplete: '+symbol)
                old_end=date.fromisoformat(old['notice_query_end_date'])
                old_boundary=date.fromisoformat(baseline['boundary'])
                required=min(consumed,date.fromisoformat(old['delist_date'])) if old.get('delist_date') else consumed
                if min(old_end,old_boundary)<required:
                    _fail('Metadata seed has an unverified gap inside consumed proxy history: '+symbol)
                protected=min(old_end,old_boundary).isoformat()
                if new.get('st_notice_evidence_sha256')!=old.get('st_notice_evidence_sha256') or not _history_equal(old,new,protected):
                    _fail('Historical ST/active semantics changed; exact-hash migration required: '+symbol)
                if json.loads((stage/(symbol+'.json')).read_bytes())!=new:_fail('ST builder persisted different metadata')
    finally:
        freq_mod.SECURITY_META_DIR=old_dir
        builder_globals.update(global_restore)
        for name,value in blocked.items():setattr(freq_mod,name,value)
    if list(resolve_archive_reference(root))!=baseline['authority']:_fail('ST pointer changed before metadata install')
    new_hashes={s:_sha((stage/(s+'.json')).read_bytes()) for s in symbols}
    # Recovery accepts only bytes previously installed by this transaction.
    prior_new=baseline.get('new_hashes',{})
    for s in symbols:
        if _sha((destination/(s+'.json')).read_bytes()) not in {baseline['old_hashes'][s],prior_new.get(s)}:
            _fail('Concurrent ST metadata modification: '+s)
    baseline.update(status='installing',new_hashes=new_hashes,stage=stage.relative_to(root).as_posix())
    _atomic(marker,_bytes(baseline))
    for s in symbols:_atomic(destination/(s+'.json'),(stage/(s+'.json')).read_bytes())
    baseline['status']='complete';_atomic(marker,_bytes(baseline))
    return {'rebuilt':True,'requires_full_rebuild':True,'symbols':len(symbols),'boundary':baseline['boundary']}


def refresh_metadata_after_notice(*, root, freq_mod, target, notice_result, consumed_through):
    root=Path(root).resolve()
    lock=root/'.microcap_index_cache/st_metadata_refresh.lock'
    fd=os.open(lock,os.O_CREAT|os.O_EXCL|os.O_WRONLY)
    os.close(fd)
    try:
        return _refresh_metadata_after_notice(root=root,freq_mod=freq_mod,target=target,notice_result=notice_result,consumed_through=consumed_through)
    finally:
        lock.unlink()

def complete_terminal_return_dates(turnover, dates, anchor):
    """Resolve only a replay's terminal close execution using exchange sessions."""
    import pandas as pd
    from scripts.exchange_calendar import sessions_for_day
    out = turnover.copy()
    missing = pd.to_datetime(out['return_start_date'], format='mixed', errors='raise').isna()
    if not missing.any():
        return out
    anchor = pd.Timestamp(anchor).normalize()
    for field in ('rebalance_date', 'execution_date', 'effective_date'):
        if not pd.to_datetime(out.loc[missing, field], format='mixed', errors='raise').eq(anchor).all():
            _fail('Missing return start outside terminal close execution')
    if not out.loc[missing, 'execution_timing'].eq('close').all():
        _fail('Missing return start is not a close execution')
    following = min(pd.Timestamp(day) for day in sessions_for_day(pd.Timestamp(max(dates)).date()) if pd.Timestamp(day) > anchor)
    if following not in set(pd.DatetimeIndex(dates)):
        _fail('Terminal return session absent from verified panel')
    out.loc[missing, 'return_start_date'] = following
    return out


def try_cached_history_rebuild(*, root, base, args, paths, panel_path, target):
    """Replay an unchanged ST history from certified caches, then native short tail.

    This recomputes the proxy; it never edits fingerprints to accept old outputs.
    Unknown metadata changes or a new scheduled rebalance retain the full route.
    """
    import copy
    import io
    import pandas as pd
    root=Path(root);target=pd.Timestamp(target).normalize()
    marker_path=root/MARKER
    if not marker_path.exists() or not paths['proxy_meta'].exists():return False
    marker=json.loads(marker_path.read_bytes())
    if marker.get('status')!='complete' or marker.get('target')!=str(target.date()):return False
    prior=json.loads(paths['proxy_meta'].read_bytes())
    if base.proxy_meta_matches_execution_model(prior):return False
    proof=prior.get('return_basis_consumption',{})
    anchor=proof.get('full_rebuild_anchor')
    if proof.get('revision')!='stock_return_consumption_v1' or not anchor:return False
    anchor=pd.Timestamp(anchor)
    if not anchor<target or (target-anchor).days>10:return False
    dates=pd.to_datetime(pd.read_csv(panel_path)['date'],errors='raise')
    dates=pd.DatetimeIndex(dates[dates<=target])
    scheduled=base.build_biweekly_rebalance_dates(dates)
    if ((scheduled>anchor)&(scheduled<=target)).any():return False
    allowed={'notice_query_end_date','st_notice_archive_sha256','st_notice_archive_manifest_path','notice_count','last_trade_date'}
    backup=_bound(root,marker['work'])/'backup'
    for symbol,digest in marker['new_hashes'].items():
        oldraw=(backup/(symbol+'.json')).read_bytes()
        raw=(root/'.microcap_index_cache/security_meta'/(symbol+'.json')).read_bytes()
        if _sha(oldraw)!=marker['old_hashes'][symbol] or _sha(raw)!=digest:_fail('Changed cached replay metadata: '+symbol)
        old,new=json.loads(oldraw),json.loads(raw)
        if not _history_equal(old,new,str(target.date())):return False
        if any(old.get(k)!=new.get(k) for k in (old.keys()|new.keys())-allowed):return False
    stage=root/'.microcap_index_cache/scoped_proxy_rebuild'/uuid.uuid4().hex;stage.mkdir(parents=True)
    keys=('proxy_members','proxy_turnover','proxy_meta','proxy_effective_members')
    originals={p:p.read_bytes() for p in [args.index_csv,args.costed_nav_csv,*[paths[k] for k in keys]]}
    for p,raw in originals.items():(stage/('before_'+p.name)).write_bytes(raw)
    staged_args=copy.copy(args);staged_args.index_csv=stage/args.index_csv.name;staged_args.costed_nav_csv=stage/args.costed_nav_csv.name
    staged_paths={k:stage/p.name for k,p in paths.items()}
    print('[cached-replay] local historical rebuild through '+str(anchor.date())+'; no universe network refresh',flush=True)
    with frozen_notice_sources(root):
        index,members,turnover,meta=base.build_local_proxy_bundle(staged_args,dates[dates<=anchor])
    turnover = complete_terminal_return_dates(turnover, dates, anchor)
    from scripts.top100_data_contracts import normalise_turnover_table
    previous_turnover = pd.read_csv(io.BytesIO(originals[paths['proxy_turnover']]))
    pd.testing.assert_frame_equal(normalise_turnover_table(previous_turnover), normalise_turnover_table(turnover), check_exact=True, check_dtype=False)
    for frame,p in [(index,staged_args.index_csv),(members,staged_paths['proxy_members']),(turnover,staged_paths['proxy_turnover'])]:base._atomic_to_csv(frame,p,index=False,encoding='utf-8')
    base._atomic_write_json(staged_paths['proxy_meta'],meta,encoding='utf-8')
    # Preserve exact approved event-table bytes after independently recomputing
    # and checking every event, including the previously unknown terminal date.
    _atomic(staged_paths['proxy_turnover'], originals[paths['proxy_turnover']])
    base._write_proxy_effective_members(staged_paths['proxy_effective_members'],anchor,meta['terminal_effective_members'])
    base.rebuild_costed_nav_from_proxy_turnover(staged_args,staged_paths,panel_path,target_end_date=anchor)
    if not base.try_extend_proxy_index_without_rebalance(staged_args,staged_paths,panel_path,target):_fail('Native cached replay tail rejected')
    if not base.try_extend_costed_nav_without_turnover(staged_args,panel_path,target,staged_paths['proxy_turnover']):_fail('Native cached costed tail rejected')
    installs=[(args.index_csv,staged_args.index_csv),(args.costed_nav_csv,staged_args.costed_nav_csv)]+[(paths[k],staged_paths[k]) for k in keys]
    for oldp,newp in installs[:2]+[(paths[k],staged_paths[k]) for k in ('proxy_members','proxy_turnover')]:
        old=pd.read_csv(io.BytesIO(originals[oldp]));new=pd.read_csv(newp)
        pd.testing.assert_frame_equal(old,new.iloc[:len(old)].reset_index(drop=True),check_exact=True,check_dtype=False)
    for p,raw in originals.items():
        if p.read_bytes()!=raw:_fail('Concurrent cached replay output change')
    _atomic(stage/'receipt.json',_bytes(dict(status='exact_historical_parity_pass',anchor=str(anchor.date()),target=str(target.date()),outputs={p.name:_sha(q.read_bytes()) for p,q in installs})))
    for p,q in installs:_atomic(p,q.read_bytes())
    print('[cached-replay] exact historical parity and native member tail passed',flush=True)
    return True
