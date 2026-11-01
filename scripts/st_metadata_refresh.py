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
