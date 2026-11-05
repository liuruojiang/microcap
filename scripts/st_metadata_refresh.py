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


def preserve_verified_replay_prefix(base, oldraw, candidate_path, *, proxy=False):
    """Use the existing rewrite audit, retaining approved serialization bytes."""
    import io
    import numpy as np
    import pandas as pd
    old = pd.read_csv(io.BytesIO(oldraw))
    new = pd.read_csv(candidate_path)
    count = min(len(old), len(new))
    previous, candidate = old.iloc[:count].copy(), new.iloc[:count].copy()
    if proxy:
        pd.testing.assert_frame_equal(previous.drop(columns='close'), candidate.drop(columns='close'), check_exact=True, check_dtype=False)
        np.testing.assert_array_max_ulp(previous['close'].to_numpy(), candidate['close'].to_numpy(), maxulp=2)
    else:
        base.assert_no_historical_rewrite(previous=previous, candidate=candidate,
            key_columns=[c for c in previous.columns if c != 'date'], allowed_tail_rows=0, label='cached replay complete historical prefix')
        for column in ('return', 'return_net'):
            if column in previous:
                np.testing.assert_allclose(previous[column], candidate[column], rtol=0,
                    atol=8*np.finfo(float).eps, equal_nan=True)
    # No values are rounded and no new history is accepted. Reuse the approved
    # bytes only after an independent replay and unchanged economic decisions.
    prefix = b''.join(oldraw.splitlines(keepends=True)[:count + 1])
    _atomic(candidate_path, prefix)
    if len(new) > count:
        base._atomic_append_csv_rows(candidate_path, new.iloc[count:])


def _semantic_metadata_binding(root, base, prior, target):
    """Authenticate a completed renewal without reinterpreting approved history."""
    import hashlib
    import pandas as pd
    root = Path(root).resolve()
    target = pd.Timestamp(target).normalize()
    marker_path = root / MARKER
    raw_marker = marker_path.read_bytes()
    marker = json.loads(raw_marker)
    symbols = sorted(str(x).zfill(6) for x in base.freq_mod.list_backtest_universe_symbols())
    if (not symbols or len(symbols) != len(set(symbols))
            or marker.get('status') != 'complete' or marker.get('target') != str(target.date())
            or marker.get('symbols') != symbols
            or set(marker.get('old_hashes', {})) != set(symbols)
            or set(marker.get('new_hashes', {})) != set(symbols)):
        _fail('Incomplete semantic metadata transaction')
    current_ref = list(resolve_archive_reference(root))
    previous_ref = marker.get('previous_authority')
    if (marker.get('authority') != current_ref or not isinstance(previous_ref, list)
            or len(previous_ref) != 2):
        _fail('Semantic metadata archive identity mismatch')
    for ref in (previous_ref, current_ref):
        if _sha(_bound(root, ref[0]).read_bytes()) != ref[1]:
            _fail('Semantic metadata archive bytes changed')
    consumed = pd.Timestamp(base.read_csv_last_date(prior['_index_path'])).normalize()
    if not consumed < target or marker.get('boundary') != str(consumed.date()):
        _fail('Semantic metadata boundary does not match consumed proxy')
    work_path = root / marker['work']
    for path in (work_path, *work_path.parents):
        if path == root: break
        if path.is_symlink() or getattr(path, 'is_junction', lambda: False)():
            _fail('Linked semantic metadata transaction')
    backup = _bound(root, marker['work']) / 'backup'
    allowed = {'notice_query_end_date', 'st_notice_archive_sha256',
               'st_notice_archive_manifest_path', 'notice_count', 'last_trade_date',
               'current_st_snapshot_name', 'st_intervals'}
    if sorted(p.stem for p in (root / '.microcap_index_cache/security_meta').glob('*.json')) != symbols:
        _fail('Semantic metadata directory universe mismatch')
    old_digest, new_digest = hashlib.sha256(), hashlib.sha256()
    observed = {marker_path: raw_marker}
    for symbol in symbols:
        old_path = backup / (symbol + '.json')
        new_path = root / '.microcap_index_cache/security_meta' / (symbol + '.json')
        for leaf in (old_path, new_path):
            for path in (leaf, *leaf.parents):
                if path == root: break
                if path.is_symlink() or getattr(path, 'is_junction', lambda: False)():
                    _fail('Linked semantic metadata evidence')
        oldraw, newraw = old_path.read_bytes(), new_path.read_bytes()
        if (_sha(oldraw) != marker['old_hashes'][symbol]
                or _sha(newraw) != marker['new_hashes'][symbol]):
            _fail('Semantic metadata transaction bytes changed: ' + symbol)
        old, new = json.loads(oldraw), json.loads(newraw)
        for payload, ref, horizon in ((old, previous_ref, consumed), (new, current_ref, target)):
            if (payload.get('symbol') != symbol or not payload.get('notice_source_complete')
                    or payload.get('st_notice_archive_sha256') != ref[1]
                    or payload.get('st_notice_archive_manifest_path') != ref[0]
                    or not base.freq_mod._notice_horizon_covers(payload, horizon)
                    or pd.Timestamp(payload['notice_query_end_date']) > horizon
                    or pd.Timestamp(payload['last_trade_date']) > horizon):
                _fail('Unverified semantic metadata horizon: ' + symbol)
        if (not _history_equal(old, new, str(target.date()))
                or any(old.get(k) != new.get(k) for k in (old.keys() | new.keys()) - allowed)
                or new.get('current_st_history_resolved') is not True):
            _fail('Metadata semantics changed or unknown field: ' + symbol)
        before, after = old['st_intervals'], new['st_intervals']
        if before != after:
            if len(before) != len(after):
                _fail('Unreviewed interval shape: ' + symbol)
            for left, right in zip(before, after):
                if left == right:
                    continue
                if (left.get('end') is not None or right.get('end') != str(target.date())
                        or {k:v for k,v in left.items() if k != 'end'} !=
                           {k:v for k,v in right.items() if k != 'end'}):
                    _fail('Unreviewed interval transition: ' + symbol)
        for digest, raw in ((old_digest, oldraw), (new_digest, newraw)):
            digest.update(symbol.encode('ascii') + b'\0' + raw + b'\0')
        observed[old_path], observed[new_path] = oldraw, newraw
    fingerprint = lambda digest: dict(present_count=len(symbols), missing_count=0, sha256=digest.hexdigest())
    if prior.get('core_params', {}).get('security_meta_cache_fingerprint') != fingerprint(old_digest):
        _fail('Old proxy does not authenticate metadata backups')
    current = fingerprint(new_digest)
    if base.security_meta_cache_fingerprint() != current:
        _fail('Metadata universe differs from completed transaction')
    return current, observed, current_ref


def try_semantic_metadata_tail(*, root, base, args, paths, panel_path, target):
    """Native tail-only continuation after an exact, economically unchanged renewal.

    Builds candidate outputs in a stage, never calls the historical proxy builder,
    and installs only after source/CAS checks and exact approved-prefix readback.
    """
    import copy
    import pandas as pd
    from scripts.realtime_state_bundle import stock_input_evidence_matches_validated_source
    root = Path(root).resolve()
    target = pd.Timestamp(target).normalize()
    from scripts.realtime_state_bundle import validate_archive_session
    validate_archive_session(target.date())
    lock = root / '.microcap_index_cache/st_metadata_refresh.lock'
    fd = os.open(lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, 'O_BINARY', 0))
    owner = _bytes(dict(pid=os.getpid(), owner=uuid.uuid4().hex, target=str(target.date())))
    os.write(fd, owner)
    os.fsync(fd)
    identity = os.fstat(fd)
    keep_lock = False
    def check_owner():
        observed = lock.stat()
        if ((observed.st_dev, observed.st_ino) != (identity.st_dev, identity.st_ino)
                or lock.read_bytes() != owner):
            _fail('Semantic tail lock ownership changed')
    try:
        prior = json.loads(paths['proxy_meta'].read_bytes())
        if not base._proxy_meta_core_matches_execution_model(prior):
            _fail('Semantic continuation execution model mismatch')
        if not stock_input_evidence_matches_validated_source(
                prior['core_params'].get('stock_input_evidence_fingerprint'),
                base.stock_input_evidence_fingerprint()):
            _fail('Semantic continuation stock input mismatch')
        current_end = pd.Timestamp(base.read_csv_last_date(args.index_csv)).normalize()
        if pd.Timestamp(base.read_csv_last_date(args.costed_nav_csv)).normalize() != current_end:
            _fail('Semantic continuation proxy/NAV anchors differ')
        dates = pd.DatetimeIndex(pd.to_datetime(pd.read_csv(panel_path)['date'], errors='raise'))
        if (target not in dates or current_end not in dates or not dates.is_unique
                or not dates.is_monotonic_increasing or dates.max() > target
                or ((base.build_biweekly_rebalance_dates(dates) > current_end)
                    & (base.build_biweekly_rebalance_dates(dates) <= target)).any()):
            _fail('Semantic continuation requires a verified no-rebalance tail')
        effective = base._continuation_members_at_bridge(paths, current_end)
        if len(effective) != base.TOP_N or len(set(effective)) != base.TOP_N:
            _fail('Semantic tail effective members are incomplete')
        consumption = prior.get('return_basis_consumption', {})
        if consumption.get('revision') != 'stock_return_consumption_v1':
            _fail('Semantic tail consumption proof missing')
        chain_end = consumption.get('full_rebuild_anchor')
        for entry in consumption.get('tail_extensions', []):
            if (entry.get('start_exclusive') != chain_end
                    or pd.Timestamp(entry['end']) <= pd.Timestamp(chain_end)):
                _fail('Semantic tail consumption chain is broken')
            chain_end = entry['end']
        if chain_end != str(current_end.date()):
            _fail('Semantic tail consumption does not reach old proxy')
        if consumption.get('tail_extensions'):
            last = consumption['tail_extensions'][-1]
            for field, key in (('effective_members_sha256','proxy_effective_members'),
                               ('proxy_members_sha256','proxy_members'),
                               ('proxy_turnover_sha256','proxy_turnover')):
                if last.get(field) != _sha(paths[key].read_bytes()):
                    _fail('Semantic tail consumed member authority changed')
            if last.get('symbols') != sorted(effective):
                _fail('Semantic tail consumed member set changed')
        prior['_index_path'] = args.index_csv
        with frozen_notice_sources(root):
            fingerprint, observed, archive_ref = _semantic_metadata_binding(root, base, prior, target)
            prior.pop('_index_path')
            source_paths = [args.index_csv, args.costed_nav_csv, paths['proxy_meta'],
                            paths['proxy_members'], paths['proxy_turnover'], paths['proxy_effective_members'],
                            panel_path, base.FROZEN_TAIL_AUTHORITY_PATH, base.freq_mod.CURRENT_ST]
            originals = {p:p.read_bytes() for p in source_paths}
            stage = root / '.microcap_index_cache/semantic_metadata_tail' / uuid.uuid4().hex
            stage.mkdir(parents=True)
            staged_args = copy.copy(args)
            staged_args.index_csv = stage / args.index_csv.name
            staged_args.costed_nav_csv = stage / args.costed_nav_csv.name
            staged_paths = dict(paths)
            for key in ('proxy_meta', 'proxy_members', 'proxy_turnover', 'proxy_effective_members'):
                staged_paths[key] = stage / paths[key].name
            for destination, source in [(staged_args.index_csv, args.index_csv),
                    (staged_args.costed_nav_csv, args.costed_nav_csv)] + [
                    (staged_paths[k], paths[k]) for k in ('proxy_meta','proxy_members','proxy_turnover','proxy_effective_members')]:
                _atomic(destination, originals[source])
            # A derived candidate identity, authenticated above; original metadata
            # and all published outputs remain untouched until both native tails pass.
            candidate = copy.deepcopy(prior)
            candidate['core_params']['security_meta_cache_fingerprint'] = fingerprint
            base._atomic_write_json(staged_paths['proxy_meta'], candidate, encoding='utf-8')
            if not base.try_extend_proxy_index_without_rebalance(staged_args, staged_paths, panel_path, target):
                _fail('Native semantic proxy tail rejected')
            if not base.try_extend_costed_nav_without_turnover(staged_args, panel_path, target, staged_paths['proxy_turnover']):
                _fail('Native semantic NAV tail rejected')
            for old, new, proxy in ((args.index_csv, staged_args.index_csv, True),
                                    (args.costed_nav_csv, staged_args.costed_nav_csv, False)):
                preserve_verified_replay_prefix(base, originals[old], new, proxy=proxy)
                if not new.read_bytes().startswith(originals[old]):
                    _fail('Semantic tail changed approved prefix bytes')
                if pd.Timestamp(base.read_csv_last_date(new)).normalize() != target:
                    _fail('Semantic tail did not reach target')
            for key in ('proxy_members','proxy_turnover','proxy_effective_members'):
                if staged_paths[key].read_bytes() != originals[paths[key]]:
                    _fail('Semantic tail changed member authority: ' + key)
            result_meta = json.loads(staged_paths['proxy_meta'].read_bytes())
            if (result_meta['core_params']['security_meta_cache_fingerprint'] != fingerprint
                    or not base.proxy_meta_matches_execution_model(result_meta)):
                _fail('Semantic tail final fingerprint mismatch')
            if list(resolve_archive_reference(root)) != archive_ref:
                _fail('Semantic tail archive changed')
            for path, raw in {**observed, **originals}.items():
                if path.read_bytes() != raw:
                    _fail('Concurrent semantic tail evidence change: ' + path.name)
        # The source context also rechecks its complete closure before installation.
        installs = [(args.index_csv, staged_args.index_csv),
                    (args.costed_nav_csv, staged_args.costed_nav_csv),
                    (paths['proxy_meta'], staged_paths['proxy_meta'])]
        for path, raw in {**observed, **originals}.items():
            if path.read_bytes() != raw:
                _fail('Concurrent semantic tail preinstall change: ' + path.name)
        rebound, _, reref = _semantic_metadata_binding(
            root, base, dict(prior, _index_path=args.index_csv), target)
        if rebound != fingerprint or reref != archive_ref:
            _fail('Semantic metadata binding changed before install')
        check_owner()
        attempted = []
        try:
            for destination, candidate_path in installs:
                check_owner()
                payload = candidate_path.read_bytes()
                attempted.append((destination, payload))
                _atomic(destination, payload)
                if destination.read_bytes() != payload:
                    _fail('Semantic tail install readback mismatch')
            _atomic(stage / 'receipt.json', _bytes(dict(status='verified_semantic_tail_only',
                target=str(target.date()), old_proxy_sha256=_sha(originals[paths['proxy_meta']]),
                metadata_fingerprint=fingerprint, outputs={p.name:_sha(p.read_bytes()) for p,_ in installs})))
            check_owner()
        except BaseException as exc:
            try:
                check_owner()
                for destination, payload in reversed(attempted):
                    current = destination.read_bytes()
                    if current not in (payload, originals[destination]):
                        _fail('Concurrent semantic tail change prevents rollback')
                    if current != originals[destination]:
                        _atomic(destination, originals[destination])
                    if destination.read_bytes() != originals[destination]:
                        _fail('Semantic tail rollback readback mismatch')
                _atomic(stage / 'failure_receipt.json', _bytes(dict(status='rolled_back',
                    error=type(exc).__name__ + ': ' + str(exc))))
            except BaseException:
                keep_lock = True
                raise
            raise
        return True
    finally:
        import sys
        pending_error = sys.exc_info()[1]
        owner_error = None
        try:
            check_owner()
        except BaseException as exc:
            keep_lock = True
            owner_error = exc
        os.close(fd)
        if owner_error is not None:
            raise owner_error from pending_error
        if not keep_lock:
            check_owner()
            lock.unlink()



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
    if prior.get('core_params', {}).get('security_meta_cache_fingerprint') is not None:
        return try_semantic_metadata_tail(root=root, base=base, args=args, paths=paths, panel_path=panel_path, target=target)
    proof=prior.get('return_basis_consumption',{})
    anchor=proof.get('full_rebuild_anchor')
    if proof.get('revision')!='stock_return_consumption_v1' or not anchor:return False
    anchor=pd.Timestamp(anchor)
    if not anchor<target or (target-anchor).days>10:return False
    dates=pd.to_datetime(pd.read_csv(panel_path)['date'],errors='raise')
    dates=pd.DatetimeIndex(dates[dates<=target])
    scheduled=base.build_biweekly_rebalance_dates(dates)
    if ((scheduled>anchor)&(scheduled<=target)).any():return False
    allowed={'notice_query_end_date','st_notice_archive_sha256','st_notice_archive_manifest_path','notice_count','last_trade_date','current_st_snapshot_name'}
    backup=_bound(root,marker['work'])/'backup'
    for symbol,digest in marker['new_hashes'].items():
        oldraw=(backup/(symbol+'.json')).read_bytes()
        raw=(root/'.microcap_index_cache/security_meta'/(symbol+'.json')).read_bytes()
        if _sha(oldraw)!=marker['old_hashes'][symbol] or _sha(raw)!=digest:_fail('Changed cached replay metadata: '+symbol)
        old,new=json.loads(oldraw),json.loads(raw)
        if not _history_equal(old,new,str(target.date())):return False
        if (old.get('current_st_snapshot_name') != new.get('current_st_snapshot_name')
                and new.get('current_st_history_resolved') is not True):return False
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
    persisted_turnover = pd.read_csv(io.StringIO(turnover.to_csv(index=False)))
    pd.testing.assert_frame_equal(normalise_turnover_table(previous_turnover), normalise_turnover_table(persisted_turnover), check_exact=True, check_dtype=False)
    for frame,p in [(index,staged_args.index_csv),(members,staged_paths['proxy_members']),(turnover,staged_paths['proxy_turnover'])]:base._atomic_to_csv(frame,p,index=False,encoding='utf-8')
    base._atomic_write_json(staged_paths['proxy_meta'],meta,encoding='utf-8')
    # Preserve exact approved event-table bytes after independently recomputing
    # and checking every event, including the previously unknown terminal date.
    _atomic(staged_paths['proxy_turnover'], originals[paths['proxy_turnover']])
    base._write_proxy_effective_members(staged_paths['proxy_effective_members'],anchor,meta['terminal_effective_members'])
    base.rebuild_costed_nav_from_proxy_turnover(staged_args,staged_paths,panel_path,target_end_date=anchor)
    preserve_verified_replay_prefix(base, originals[args.index_csv], staged_args.index_csv, proxy=True)
    preserve_verified_replay_prefix(base, originals[args.costed_nav_csv], staged_args.costed_nav_csv)
    if not base.try_extend_proxy_index_without_rebalance(staged_args,staged_paths,panel_path,target):_fail('Native cached replay tail rejected')
    preserve_verified_replay_prefix(base, originals[args.index_csv], staged_args.index_csv, proxy=True)
    if not base.try_extend_costed_nav_without_turnover(staged_args,panel_path,target,staged_paths['proxy_turnover']):_fail('Native cached costed tail rejected')
    preserve_verified_replay_prefix(base, originals[args.costed_nav_csv], staged_args.costed_nav_csv)
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
