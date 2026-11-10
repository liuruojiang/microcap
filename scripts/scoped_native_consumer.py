"""Opt-in native no-rebalance consumer for an authenticated 300-issuer source run.

The old native source gate runs before lazy stock adoption. Metadata renewal is
derived from a whole-universe historical binding, while the original native
proxy/NAV continuation computes the new rows. This is not a release override.
"""
from __future__ import annotations
from contextlib import ExitStack
import copy
import csv
from datetime import datetime, timezone
import io
import json
from pathlib import Path
import subprocess
import uuid
import zipfile

import pandas as pd

from scripts import scoped_source_consumer as sources
from scripts.scoped_metadata_consumer import metadata_fingerprint, plan_metadata_renewal
from scripts.st_history_scope import resolve_archive_reference, source_closure, POINTER_PATH
from scripts.st_notice_refresh import FRESHNESS
from scripts.st_metadata_refresh import preserve_verified_replay_prefix

ACTIVE = '.microcap_index_cache/scoped_native_consumer.json'
_CONTEXT = None


def maintained_candidate_symbols(root,current_end,target):
    context=_CONTEXT
    if context is None or not context.get('forward_mode'):
        return None
    if (Path(root).resolve()!=context['root'] or str(pd.Timestamp(current_end).date())!=context['current_end']
            or str(pd.Timestamp(target).date())!=context['target']):
        raise ValueError('Native candidate selection escaped its authenticated forward window')
    return list(context['plan']['symbols'])


def capture_forward_calculation(root,**values):
    context=_CONTEXT
    if context is None or not (context.get('forward_mode') or context.get('capture_only')):
        return
    if Path(root).resolve()!=context['root'] or context.get('forward_inputs') is not None:
        raise ValueError('Unexpected or duplicate native forward calculation')
    from scripts.scoped_forward_consumption import capture
    from scripts.scoped_cap_boundary import verify
    data=capture(**values);boundaries={}
    if context.get('capture_only'):
        context['forward_inputs']=data
        return
    for day in data['rebalance_dates']:
        spec=context['config'].get('boundary_snapshots',{}).get(day)
        if not spec:raise ValueError('A new rebalance needs its real dated whole-market boundary pages: '+day)
        source=Path(spec['source_root']).resolve()
        directory=(context['forward_directory']/'boundary'/day).relative_to(context['root']).as_posix()
        for name,digest in spec['files'].items():
            if Path(name).name!=name or not name.startswith('page-') or not name.endswith('.json'):
                raise ValueError('Unsupported boundary source page role')
            raw=sources.bound(source,spec['directory']+'/'+name).read_bytes()
            if sources.sha(raw)!=digest:raise ValueError('Configured boundary page bytes changed')
            sources.atomic(sources.bound(context['root'],directory+'/'+name),raw)
        saved=dict(directory=directory,files=spec['files'])
        boundaries[day]=verify(context['root'],context['base'],day,data['caps'][day],
            context['plan']['symbols'],saved,list(resolve_archive_reference(context['root'])),materialize=True)
    data['boundaries']=boundaries;context['forward_inputs']=data


def reuse_verified_sources(root, symbols, target):
    """Called only at the original native price/share/ratio refresh boundary."""
    context = _CONTEXT
    if context is None:
        return False
    if (Path(root).resolve() != context['root'] or target != context['target']
            or not set(symbols) <= set(context['plan']['symbols'])):
        raise ValueError('Native refresh requested inputs outside the authenticated scope/date')
    if not context['adopted']:
        context['stack'].enter_context(sources.adoption_transaction(context['plan']))
        context['adopted'] = True
    for symbol in symbols:
        for name in sources.mutable_files(symbol):
            if sources.sha(sources.bound(root, name).read_bytes()) != context['plan']['files'][name]:
                raise ValueError('Native scoped input pointer changed: ' + symbol)
    # The staged reader already validated each complete native closure. The
    # unchanged native tail then validates the actual consumed return/trade legs.
    return True


def original_notice_chain(source, previous, target):
    """Locate original immutable receipts by byte identity, never by a new clock."""
    raw = sources.bound(source, FRESHNESS).read_bytes()
    candidates = sorted((source / '.microcap_index_cache/st_notice_sources').glob('refresh_*/proof.json'))
    matches = [p for p in candidates if p.read_bytes() == raw]
    if not matches:
        raise ValueError('Current notice proof has no original immutable receipt')
    path = matches[0]; chain = []
    for _ in range(32):
        body = path.read_bytes(); proof = json.loads(body)
        chain.append(dict(path=path.relative_to(source).as_posix(), sha256=sources.sha(body)))
        if proof['previous_authority'] == list(previous):
            break
        path = path.with_name('previous_freshness.json')
        # The old receipt itself points at its real generation. Its predecessor
        # backup is held by that original generation, not by the latest folder.
        old = json.loads(path.read_bytes())
        if old['authority'] != proof['previous_authority']:
            raise ValueError('Original notice receipt predecessor identity differs')
        if old['previous_authority'] == list(previous):
            chain.append(dict(path=path.relative_to(source).as_posix(), sha256=sources.sha(path.read_bytes())))
            break
        old_generation = sources.bound(source, old['authority'][0]).parent
        original_path = old_generation / 'proof.json'
        if original_path.read_bytes() != path.read_bytes():
            raise ValueError('Original older notice proof is unavailable')
        path = original_path
    else:
        raise ValueError('Notice predecessor chain exceeds bounded recovery depth')
    return sources.verify_notice_chain(source, previous, chain[::-1], target), chain[::-1]


def checked_publication_universe(source,names,floor,symbols):
    if names.get('schema')!='current_publication_names_v2' or names.get('publication_name_scope')!='maintained_plus_ST_conflicts':
        raise ValueError('Scoped publication names require an explicit maintained-pool observation')
    close=datetime.fromisoformat(names['target']+'T15:00:00+08:00');observed=datetime.fromisoformat(names['observed_at'])
    if observed.tzinfo is None or not max(close,floor)<=observed<=datetime.now(timezone.utc):
        raise ValueError('Scoped publication name observation clock differs')
    for key in ('st_observed_at','universe_observed_at'):
        stamp=datetime.fromisoformat(names[key])
        if stamp.tzinfo is None or not max(close,floor)<=stamp<=observed:
            raise ValueError('Scoped publication name sources predate the required observation window')
    name='.microcap_index_cache/active_universe.csv';universe=sources.bound(source,name).read_bytes()
    active=list(csv.DictReader(io.StringIO(universe.decode('utf-8-sig'))));mapping={r['code']:r['name'].strip() for r in active}
    st=sources.bound(source,'.microcap_index_cache/current_st.csv').read_bytes()
    rows=list(csv.DictReader(io.StringIO(st.decode('utf-8-sig'))))
    import re
    if (sources.sha(universe)!=names.get('universe_sha256') or len(active)!=names.get('universe_count')
            or not active or len(mapping)!=len(active) or sources.sha(st)!=names['cache_sha256']
            or len(rows)!=names['count'] or not rows
            or any(not re.fullmatch('[0-9]{6}',s) or not n for s,n in mapping.items())
            or any(r['code'] not in mapping or mapping[r['code']]!=r['name'].strip()
                   for r in rows if re.fullmatch('[036][0-9]{5}',r['code']))):
        raise ValueError('Scoped publication names and ST board disagree')
    files={name:sources.sha(universe)}
    if names.get('publication_name_scope')=='maintained_plus_ST_conflicts':
        witness=names['scoped_observation'];prior=witness['prior_universe']
        if prior['path']!='.microcap_index_cache/local_current_names/observations/'+prior['sha256']+'.universe.csv':
            raise ValueError('Original publication name evidence has another file role')
        previous=sources.bound(source,prior['path']).read_bytes()
        if sources.sha(previous)!=prior['sha256']:raise ValueError('Original publication name evidence changed')
        old_rows=list(csv.DictReader(io.StringIO(previous.decode('utf-8-sig'))));old={r['code']:r['name'].strip() for r in old_rows}
        quotes={r['code']:r for r in witness['quote_rows']}
        conflicts={r['code'] for r in rows if re.fullmatch('[036][0-9]{5}',r['code']) and old.get(r['code'])!=r['name'].strip()}
        if (not old_rows or len(old)!=len(old_rows) or set(witness['maintained_symbols'])!=set(symbols)
                or len(witness['maintained_symbols'])!=300 or len(set(symbols))!=300
                or set(quotes)!=set(witness['requested_symbols']) or len(quotes)!=len(witness['quote_rows'])
                or set(witness['conflict_symbols'])!=conflicts or len(witness['conflict_symbols'])!=len(conflicts)
                or set(quotes)!=set(symbols)|set(witness['conflict_symbols'])):
            raise ValueError('Scoped publication name quote coverage differs')
        for row in quotes.values():
            fetched=datetime.fromisoformat(row['fetched_at'])
            if (fetched.tzinfo is None or not max(close,floor)<=fetched<=observed
                    or row.get('quote_source')!='tencent_batch_free' or not row.get('name','').strip()):
                raise ValueError('Scoped publication quote source or freshness differs')
        old.update({s:r['name'].strip() for s,r in quotes.items()})
        if old!=mapping:raise ValueError('Publication names changed outside the witnessed scope')
        files[prior['path']]=prior['sha256']
    return files


def checked_producer(source, config, manifest):
    runtime = subprocess.check_output(['git', '-C', str(source), 'rev-parse', 'HEAD'], text=True).strip()
    if runtime not in sources.PRODUCERS or runtime!=manifest['runtime_sha']:
        raise ValueError('Scoped producer runtime differs from the reviewed source package')
    from scripts.stock_input_contract import source_fingerprint
    if source_fingerprint(source) != manifest['producer_context']['native_sources']:
        raise ValueError('Scoped producer calculation code changed')
    receipt_path = Path(config['producer_metadata_receipt']).resolve()
    receipt_raw = receipt_path.read_bytes(); receipt = json.loads(receipt_raw)
    if (sources.sha(receipt_raw) != config['producer_metadata_receipt_sha256']
            or receipt.get('status') != 'complete'
            or receipt.get('context') != manifest['producer_context']):
        raise ValueError('Completed producer metadata receipt identity differs')
    for name, digest in receipt['files'].items():
        if sources.sha(sources.bound(source, name).read_bytes()) != digest:
            raise ValueError('Completed producer metadata bytes changed')
    name_path = '.microcap_index_cache/local_current_names/' + manifest['target'] + '.json'
    raw = sources.bound(source, name_path).read_bytes(); names = json.loads(raw)
    if names.get('schema') not in (None,'current_publication_names_v2'):
        raise ValueError('Unreviewed current publication name receipt schema')
    legacy_morning=(runtime=='706af7ea9186bdb9b3948738deb5a5afda7a331e' and manifest['target']=='2026-11-09'
        and config['producer_metadata_receipt_sha256']=='56e734d24b0698bad1696304cb07ef80c59aeb691b46e6452faad8468eac244c')
    if names.get('schema') is None and runtime!=sources.PRODUCER and not legacy_morning:
        raise ValueError('New source observations require scoped current publication name evidence')
    current = sources.bound(source, '.microcap_index_cache/current_st.csv').read_bytes()
    rows = list(csv.DictReader(io.StringIO(current.decode('utf-8-sig'))))
    observed = datetime.fromisoformat(names['observed_at'])
    close = datetime.fromisoformat(manifest['target'] + 'T15:00:00+08:00')
    floor = datetime.fromisoformat(manifest['producer_context']['observation_floor'])
    if (names.get('target') != manifest['target'] or observed.tzinfo is None
            or not max(close, floor) <= observed <= datetime.now(timezone.utc)
            or names.get('cache_sha256') != sources.sha(current)
            or names.get('native_source_sha256') != sources.sha(
                (source / 'microcap_top100_mom16_biweekly_live_v2_0.py').read_bytes().replace(b'\r\n', b'\n'))
            or not rows or names.get('count') != len(rows)
            or len({row['code'] for row in rows}) != len(rows)):
        raise ValueError('Scoped current-name source clock/identity is unverified')
    if names.get('schema')=='current_publication_names_v2':
        checked_publication_universe(source,names,floor,manifest['symbols'])
    return receipt, name_path


def consume_no_rebalance(**kwargs):
    return _consume(**kwargs,allow_forward=False)


def consume_sources(**kwargs):
    return _consume(**kwargs,allow_forward=True)


def _consume(*, root, base, args, paths, panel_path, target, config,allow_forward=False):
    """Earn a forward state from a complete staged source run, never a full rebuild."""
    global _CONTEXT
    if _CONTEXT is not None:
        raise ValueError('A native scoped consumer is already active')
    root = Path(root).resolve(); source = Path(config['source_root']).resolve()
    target = str(pd.Timestamp(target).date())
    if source == root or config.get('schema') != 'microcap_native_scoped_consumer_v1':
        raise ValueError('Explicit independent source-only consumer configuration is required')
    bundle = Path(config['bundle']).resolve()
    if sources.sha(bundle.read_bytes()) != config['bundle_sha256']:
        raise ValueError('Configured scoped source bundle bytes changed')
    with zipfile.ZipFile(bundle) as archive:
        manifest = json.loads(archive.read('manifest.json'))
    if manifest['target'] != target:
        raise ValueError('Scoped source bundle does not match the independent native panel target')
    sources.validate_source_contract(root, manifest)
    stage = root / '.microcap_index_cache/local_input_imports' / config['bundle_sha256']
    relative_stage='.microcap_index_cache/local_input_imports/'+config['bundle_sha256']
    if json.loads(sources.bound(root,relative_stage+'/manifest.json').read_bytes()) != manifest:
        raise ValueError('Native staged manifest differs from immutable source bundle')
    staged_receipt=json.loads(sources.bound(root,relative_stage+'/receipt.json').read_bytes())
    if (staged_receipt.get('status')!='native_sources_staged'
            or staged_receipt.get('source_bundle_sha256')!=config['bundle_sha256']
            or staged_receipt.get('files')!=manifest['files']
            or staged_receipt.get('production_pointers_changed') is not False
            or staged_receipt.get('native_late_adoption_required') is not True):
        raise ValueError('Independent native staged-source readback receipt is incomplete')
    receipt, name_path = checked_producer(source, config, manifest)
    prior_raw = paths['proxy_meta'].read_bytes(); prior = json.loads(prior_raw)
    current_end = str(pd.Timestamp(base.read_csv_last_date(args.index_csv)).date())
    same_anchor=False
    if current_end == target:
        active=json.loads(sources.bound(root,ACTIVE).read_bytes())
        if active.get('source_bundle_sha256')==config['bundle_sha256'] or not allow_forward:
            return _reuse_completed(root, base, prior, config, target)
        same_anchor=True
    if current_end > target:
        raise ValueError('Scoped consumer cannot replace newer native outputs')
    dates = pd.DatetimeIndex(pd.to_datetime(pd.read_csv(panel_path)['date'], errors='raise'))
    scheduled = base.build_biweekly_rebalance_dates(dates)
    crosses=((scheduled > pd.Timestamp(current_end)) & (scheduled <= pd.Timestamp(target))).any()
    if (target not in dates or pd.Timestamp(current_end) not in dates
            or dates.has_duplicates or not dates.is_monotonic_increasing or dates.max() > pd.Timestamp(target)
            or (crosses and not allow_forward)):
        raise ValueError('Scoped no-rebalance consumer cannot certify a rebalance boundary')
    forward_mode=bool(not same_anchor and (crosses or prior.get('return_basis_consumption',{}).get('revision')=='stock_return_consumption_v2'))
    consumed, observed = sources.consumed_boundaries(root, base, args, paths)
    # The actual old core/data/metadata gate has just passed, before any input
    # or authority mutation. The original canonical proxy remains byte-identical.
    for name in manifest['producer_context']['native_sources']:
        observed[root / name] = (root / name).read_bytes()
    observed[panel_path] = panel_path.read_bytes()
    source_plan = sources.plan_adoption(root, stage / 'root', manifest, consumed=consumed, observed=observed)
    effective = base._continuation_members_at_bridge(paths, pd.Timestamp(current_end))
    if len(effective) != 100 or len(set(effective)) != 100 or not set(effective) <= set(manifest['symbols']):
        raise ValueError('Scoped maintenance excludes an actually executed member')
    old_ref = resolve_archive_reference(root)
    chain, specs = original_notice_chain(source, old_ref, target)
    if chain['authority'] != list(resolve_archive_reference(source)):
        raise ValueError('Scoped notice chain does not reach the real current producer authority')
    meta_plan = plan_metadata_renewal(root, source, symbols=manifest['symbols'], target=target,
        previous_fingerprint=prior['core_params']['security_meta_cache_fingerprint'],
        current_authority=chain['authority'], consumed=consumed,
        universe=base.freq_mod.list_backtest_universe_symbols(), native_freq=base.freq_mod)
    notice_files = source_closure(source)
    notice_files.update(chain['source_files'])
    names=json.loads(sources.bound(source,name_path).read_bytes())
    publication_universe=('.microcap_index_cache/active_universe.csv'
                          if names.get('schema')=='current_publication_names_v2' else None)
    if publication_universe:notice_files[publication_universe]=names['universe_sha256']
    publication_files={}
    if publication_universe:
        publication_files=checked_publication_universe(source,names,
            datetime.fromisoformat(manifest['producer_context']['observation_floor']),manifest['symbols'])
        notice_files.update(publication_files)
        publication_files[name_path]=sources.sha(sources.bound(source,name_path).read_bytes())
    for name in (FRESHNESS, POINTER_PATH, '.microcap_index_cache/current_st.csv', name_path):
        notice_files[name] = sources.sha(sources.bound(source, name).read_bytes())
    meta_files = {'.microcap_index_cache/security_meta/' + s + '.json':sources.sha(raw)
                  for s,raw in meta_plan['new_records'].items()}
    files = {**notice_files, **meta_files}
    allowed_prefixes = ('.microcap_index_cache/st_notice_sources/',
        '.microcap_index_cache/security_meta/', '.microcap_index_cache/local_current_names/')
    if any(not name.startswith(allowed_prefixes) and name not in
            (POINTER_PATH, '.microcap_index_cache/current_st.csv','.microcap_index_cache/active_universe.csv') for name in files):
        raise ValueError('Scoped notice/metadata transport includes another file role')
    mutable = set(meta_files) | {POINTER_PATH, FRESHNESS, '.microcap_index_cache/current_st.csv'}
    if publication_universe:mutable.add(publication_universe)
    old_files = {name:sources.bound(root, name).read_bytes() for name in mutable}
    for s, raw in meta_plan['old_records'].items():
        old_files['.microcap_index_cache/security_meta/' + s + '.json'] = raw
    # A first current-name receipt is immutable evidence; a later observation
    # for the same target is included as an explicitly backed-up pointer.
    if sources.bound(root, name_path).exists():
        mutable.add(name_path); old_files[name_path] = sources.bound(root, name_path).read_bytes()
    for name, digest in files.items():
        path = sources.bound(root, name)
        if name not in mutable and path.exists() and sources.sha(path.read_bytes()) != digest:
            raise ValueError('Immutable native notice/metadata evidence collision')
    authority_plan = dict(root=root, staged_root=source, files=files, originals=old_files,
        mutable=mutable, symbols=manifest['symbols'], target=target,
        consumed={s:consumed[s] for s in manifest['symbols']}, transaction_role='scoped_native_metadata')
    keys = ('proxy_meta', 'proxy_members', 'proxy_turnover', 'proxy_effective_members')
    outputs = [args.index_csv, args.costed_nav_csv, *[paths[k] for k in keys]]
    originals = {path:path.read_bytes() for path in outputs}
    old_active = sources.bound(root, ACTIVE).read_bytes() if sources.bound(root, ACTIVE).exists() else None
    published = {}; work = None
    try:
        with sources.adoption_transaction(authority_plan) as work, ExitStack() as stack:
            if base.security_meta_cache_fingerprint() != meta_plan['fingerprint']:
                raise ValueError('Installed scoped metadata differs from historical binding')
            candidate_dir = sources.extended(work / 'native_candidate'); candidate_dir.mkdir()
            staged_args = copy.copy(args)
            staged_args.index_csv = candidate_dir / args.index_csv.name
            staged_args.costed_nav_csv = candidate_dir / args.costed_nav_csv.name
            staged_paths = dict(paths)
            for key in keys:
                staged_paths[key] = candidate_dir / paths[key].name
            for dest, original in [(staged_args.index_csv,args.index_csv),
                    (staged_args.costed_nav_csv,args.costed_nav_csv)] + [(staged_paths[k],paths[k]) for k in keys]:
                sources.atomic(dest, originals[original])
            candidate = copy.deepcopy(prior)
            candidate['core_params']['security_meta_cache_fingerprint'] = meta_plan['fingerprint']
            base._atomic_write_json(staged_paths['proxy_meta'], candidate, encoding='utf-8')
            from scripts import scoped_forward_consumption as forward
            forward_directory=root/forward.PREFIX/uuid.uuid4().hex
            _CONTEXT = dict(root=root, target=target, plan=source_plan, stack=stack, adopted=False,
                forward_mode=forward_mode,current_end=current_end,base=base,config=config,
                forward_directory=forward_directory,forward_inputs=None,capture_only=False)
            revalidation=None
            try:
                if same_anchor:
                    from scripts.scoped_same_anchor import recompute
                    earned,revalidation=recompute(root,base,staged_args,staged_paths,target,prior,effective,_CONTEXT)
                    candidate['core_params']=earned
                    base._atomic_write_json(staged_paths['proxy_meta'],candidate,encoding='utf-8')
                elif forward_mode:
                    from scripts.st_metadata_refresh import frozen_notice_sources
                    with frozen_notice_sources(root):
                        base.extend_index_recent_window(staged_args,staged_paths,panel_path,pd.Timestamp(target))
                    if _CONTEXT['forward_inputs'] is None:
                        raise ValueError('Native forward computation did not produce its original input receipt')
                elif not base.try_extend_proxy_index_without_rebalance(staged_args, staged_paths, panel_path, pd.Timestamp(target)):
                    raise ValueError('Original native scoped proxy continuation rejected')
                if not _CONTEXT['adopted']:
                    raise ValueError('Native continuation did not cross its source adoption boundary')
                if not same_anchor and not base.try_extend_costed_nav_without_turnover(staged_args, panel_path, pd.Timestamp(target), staged_paths['proxy_turnover']):
                    if forward_mode:
                        base.rebuild_costed_nav_from_proxy_turnover(staged_args,staged_paths,panel_path,target_end_date=pd.Timestamp(target))
                    else:
                        raise ValueError('Original native scoped costed NAV continuation rejected')
                captured=_CONTEXT['forward_inputs']
            finally:
                _CONTEXT = None
            for old, new, proxy in ((args.index_csv,staged_args.index_csv,True),
                                    (args.costed_nav_csv,staged_args.costed_nav_csv,False)):
                preserve_verified_replay_prefix(base, originals[old], new, proxy=proxy)
                if not new.read_bytes().startswith(originals[old]) or str(pd.Timestamp(base.read_csv_last_date(new)).date()) != target:
                    raise ValueError('Native scoped continuation changed its approved prefix or target')
            for key in (() if forward_mode else ('proxy_members','proxy_turnover','proxy_effective_members')):
                if staged_paths[key].read_bytes() != originals[paths[key]]:
                    raise ValueError('No-rebalance scoped continuation changed executed member authority')
            result_meta = json.loads(staged_paths['proxy_meta'].read_bytes())
            if revalidation is not None:
                path=work/'same_anchor_native_replay.json';body=sources.encode(revalidation);sources.atomic(path,body)
                result_meta['same_anchor_native_revalidation']=dict(path=path.relative_to(root).as_posix(),sha256=sources.sha(body))
            else:
                from scripts.scoped_same_anchor import retire_on_advance
                retire_on_advance(prior,result_meta,target)
            if forward_mode:
                old_payloads=dict(index=originals[args.index_csv],costed=originals[args.costed_nav_csv],
                    **{k:originals[paths['proxy_'+k]] for k in ('meta','members','turnover')},
                    effective=originals[paths['proxy_effective_members']])
                new_payloads=dict(index=staged_args.index_csv.read_bytes(),costed=staged_args.costed_nav_csv.read_bytes(),
                    **{k:staged_paths['proxy_'+k].read_bytes() for k in ('meta','members','turnover')},
                    effective=staged_paths['proxy_effective_members'].read_bytes())
                result_meta['return_basis_consumption']=forward.append(root,forward_directory,prior,
                    old_payloads,new_payloads,captured,manifest['symbols'],config['bundle_sha256'])
            if not base.proxy_meta_matches_execution_model(result_meta):
                raise ValueError('Native scoped continuation did not earn the actual new fingerprints')
            original_proxy = work / 'original_proxy.json'; sources.atomic(original_proxy, prior_raw)
            previous_marker=None
            if old_active is not None:
                previous_path=work/'original_active_marker.json';sources.atomic(previous_path,old_active)
                previous_marker=dict(path=previous_path.relative_to(root).as_posix(),sha256=sources.sha(old_active))
            marker = dict(schema='native_scoped_metadata_adoption_v1', status='complete', target=target,
                symbols=manifest['symbols'], previous_authority=list(old_ref), authority=chain['authority'],
                original_proxy=dict(path=original_proxy.relative_to(root).as_posix(),sha256=sources.sha(prior_raw)),
                backup_directory=(work / 'backup/.microcap_index_cache/security_meta').relative_to(root).as_posix(),
                changes=meta_plan['changes'], previous_fingerprint=meta_plan['previous_fingerprint'],
                fingerprint=meta_plan['fingerprint'], source_bundle_sha256=config['bundle_sha256'],
                original_notice_receipts=specs, source_files=chain['source_files'],
                source_input_files=manifest['files'], final_strategy_delivery=False)
            if publication_files:
                snapshot={}
                for name,digest in publication_files.items():
                    raw=sources.bound(root,name).read_bytes()
                    if sources.sha(raw)!=digest:raise ValueError('Publication snapshot changed before archival commit')
                    destination=work/'publication_snapshot'/name;sources.atomic(destination,raw)
                    snapshot[name]=dict(path=destination.relative_to(root).as_posix(),sha256=digest)
                marker.update(publication_files=publication_files,
                    publication_snapshot=snapshot,
                    publication_observation_floor=manifest['producer_context']['observation_floor'])
            if previous_marker is not None:marker['previous_marker']=previous_marker
            marker_raw = sources.encode(marker)
            result_meta['scoped_metadata_adoption'] = dict(path=ACTIVE,sha256=sources.sha(marker_raw))
            base._atomic_write_json(staged_paths['proxy_meta'], result_meta, encoding='utf-8')
            for destination in outputs:
                if destination.read_bytes() != originals[destination]:
                    raise ValueError('Concurrent native output modification before scoped publish')
            installs=[(args.index_csv,staged_args.index_csv),
                (args.costed_nav_csv,staged_args.costed_nav_csv),(paths['proxy_meta'],staged_paths['proxy_meta'])]
            if forward_mode:installs += [(paths[k],staged_paths[k]) for k in ('proxy_members','proxy_turnover','proxy_effective_members')]
            for destination, staged in installs:
                payload = staged.read_bytes(); published[destination] = payload
                sources.atomic(destination, payload)
            published[sources.bound(root,ACTIVE)] = marker_raw
            sources.atomic(sources.bound(root,ACTIVE), marker_raw)
            scoped_transport_files(root)
        return dict(status=('native_scoped_same_anchor_revalidation_passed' if same_anchor else
                    'native_scoped_forward_continuation_passed' if forward_mode else 'native_scoped_no_rebalance_continuation_passed'), target=target,
                    maintained_issuers=300, final_strategy_delivery=False)
    except BaseException:
        for destination, installed in reversed(list(published.items())):
            current = destination.read_bytes()
            old = old_active if destination == sources.bound(root,ACTIVE) else originals[destination]
            if current not in (installed, old):
                raise ValueError('Concurrent scoped output change prevents rollback')
            if old is None:
                # Preserve the failed new pointer as evidence in the owned journal.
                destination.replace(sources.extended(work / 'failed_active_marker.json'))
            else:
                sources.atomic(destination, old)
        raise


def _reuse_completed(root, base, meta, config, target):
    if not base.proxy_meta_matches_execution_model(meta):
        raise ValueError('Existing scoped native state no longer passes its actual source gate')
    marker = json.loads(sources.bound(root, ACTIVE).read_bytes())
    if marker.get('source_bundle_sha256') != config['bundle_sha256'] or marker.get('target') != target:
        raise ValueError('A different completed-day source adoption requires semantic revalidation')
    scoped_transport_files(root)
    return dict(status='reused_verified_native_scoped_state',target=target,final_strategy_delivery=False)


def scoped_transport_files(root):
    """Authenticate mixed metadata only under the explicit native adoption binding."""
    root = Path(root).resolve(); active = sources.bound(root, ACTIVE)
    raw = active.read_bytes(); marker = json.loads(raw)
    proxy = json.loads((root/'outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_meta.json').read_bytes())
    if (marker.get('schema') != 'native_scoped_metadata_adoption_v1' or marker.get('status') != 'complete'
            or proxy.get('scoped_metadata_adoption') != dict(path=ACTIVE,sha256=sources.sha(raw))
            or marker.get('target') != proxy.get('end_date')
            or marker.get('authority') != list(resolve_archive_reference(root))):
        raise ValueError('Native scoped metadata adoption binding is absent or changed')
    symbols = marker['symbols']
    if len(symbols) != 300 or len(set(symbols)) != 300 or set(marker['changes']) != set(symbols):
        raise ValueError('Native scoped metadata roster is incomplete')
    old_proxy = json.loads(sources.bound(root,marker['original_proxy']['path']).read_bytes())
    if sources.sha(sources.bound(root,marker['original_proxy']['path']).read_bytes()) != marker['original_proxy']['sha256']:
        raise ValueError('Native scoped original proxy bytes changed')
    directory = root/'.microcap_index_cache/security_meta'
    current = {p.stem:p.read_bytes() for p in directory.glob('*.json')}; old = dict(current)
    files = {ACTIVE:sources.sha(raw),**marker['source_files'],marker['original_proxy']['path']:marker['original_proxy']['sha256']}
    publication=marker.get('publication_files',{})
    if publication:
        for name,digest in publication.items():
            if sources.sha(sources.bound(root,name).read_bytes())!=digest:raise ValueError('Current publication name evidence changed')
        name_path='.microcap_index_cache/local_current_names/'+marker['target']+'.json'
        names=json.loads(sources.bound(root,name_path).read_bytes())
        if names['target']!=marker['target']:raise ValueError('Current publication name target differs')
        checked=checked_publication_universe(root,names,datetime.fromisoformat(marker['publication_observation_floor']),symbols)
        checked[name_path]=sources.sha(sources.bound(root,name_path).read_bytes())
        if checked!=publication:raise ValueError('Current publication evidence roles differ')
        files.update(publication)
        if set(marker.get('publication_snapshot',{}))!=set(publication):raise ValueError('Publication archival snapshot roles differ')
        for name,spec in marker['publication_snapshot'].items():
            if spec['sha256']!=publication[name] or sources.sha(sources.bound(root,spec['path']).read_bytes())!=spec['sha256']:
                raise ValueError('Publication archival snapshot changed')
            files[spec['path']]=spec['sha256']
    for symbol in symbols:
        relative = marker['backup_directory']+'/'+symbol+'.json'
        before = sources.bound(root,relative).read_bytes()
        if (sources.sha(before) != marker['changes'][symbol]['old_sha256']
                or sources.sha(current[symbol]) != marker['changes'][symbol]['new_sha256']):
            raise ValueError('Native scoped metadata transaction bytes changed')
        payload=json.loads(current[symbol])
        if (payload.get('notice_query_end_date') != marker['target']
                or payload.get('st_notice_archive_sha256') != marker['authority'][1]
                or payload.get('current_st_history_resolved') is not True):
            raise ValueError('Native scoped current metadata coverage is unresolved')
        old[symbol]=before;files[relative]=sources.sha(before)
    if (metadata_fingerprint(old) != marker['previous_fingerprint']
            or marker['previous_fingerprint'] != old_proxy['core_params']['security_meta_cache_fingerprint']
            or metadata_fingerprint(current) != marker['fingerprint']
            or marker['fingerprint'] != proxy['core_params']['security_meta_cache_fingerprint']):
        raise ValueError('Native scoped whole-universe historical metadata binding differs')
    from scripts.st_metadata_refresh import _scoped_ancestor_boundary
    checked={}
    for symbol, body in current.items():
        payload=json.loads(body)
        if symbol not in set(symbols):
            _scoped_ancestor_boundary(root,payload,marker['authority'],checked)
    chain=sources.verify_notice_chain(root,marker['previous_authority'],marker['original_notice_receipts'],marker['target'])
    if chain['authority'] != marker['authority'] or chain['source_files'] != marker['source_files']:
        raise ValueError('Native scoped original notice chain differs')
    for relative,digest in marker['source_files'].items():
        if sources.sha(sources.bound(root,relative).read_bytes()) != digest:
            raise ValueError('Native scoped notice source changed')
    transported=set()
    for payload in (json.loads(body) for body in current.values()):
        ref=(payload['st_notice_archive_manifest_path'],payload['st_notice_archive_sha256'])
        if ref not in transported:
            files.update(source_closure(root,*ref));transported.add(ref)
    effective=pd.read_csv(root/'outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_effective_members.csv',dtype={'symbol':str})
    if len(effective)!=100 or not set(effective.symbol)<=set(symbols):
        raise ValueError('Actual executed members escape the maintained metadata scope')
    import microcap_top100_mom16_biweekly_live_v2_0 as v
    from scripts.scoped_same_anchor import validate,archived_receipt_files
    paths={key:root/name for key,name in (
        ('proxy_effective_members','outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_effective_members.csv'),
        ('proxy_members','outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_members.csv'),
        ('proxy_turnover','outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_turnover.csv'))}
    files.update(validate(root,v.base_mod,proxy,marker,paths))
    # The immediately previous proxy can hold the last same-close replay.
    # Its owner is the archived previous marker, not this new-day marker.
    files.update(archived_receipt_files(root,old_proxy,marker.get('previous_marker')))
    # Preserve original pointer receipts/metadata needed to explain earlier
    # adoptions. They are archival byte evidence, not current ST observations.
    previous=marker.get('previous_marker');seen=set()
    while previous is not None:
        if previous['path'] in seen:raise ValueError('Cyclic archived metadata adoption')
        seen.add(previous['path']);p=sources.bound(root,previous['path']);body=p.read_bytes()
        if sources.sha(body)!=previous['sha256']:raise ValueError('Archived metadata adoption marker changed')
        files[previous['path']]=previous['sha256'];old_marker=json.loads(body)
        for spec in old_marker.get('publication_snapshot',{}).values():
            if sources.sha(sources.bound(root,spec['path']).read_bytes())!=spec['sha256']:
                raise ValueError('Archived publication name snapshot changed')
            files[spec['path']]=spec['sha256']
        for spec in [old_marker['original_proxy']]:
            raw=sources.bound(root,spec['path']).read_bytes()
            if sources.sha(raw)!=spec['sha256']:raise ValueError('Archived original proxy changed')
            files[spec['path']]=spec['sha256']
            old_proxy=json.loads(raw)
            files.update(archived_receipt_files(root,old_proxy,old_marker.get('previous_marker')))
        for symbol,change in old_marker['changes'].items():
            name=old_marker['backup_directory']+'/'+symbol+'.json'
            if sources.sha(sources.bound(root,name).read_bytes())!=change['old_sha256']:
                raise ValueError('Archived original security metadata changed')
            files[name]=change['old_sha256']
        for name,digest in old_marker['source_files'].items():
            if sources.sha(sources.bound(root,name).read_bytes())!=digest:raise ValueError('Archived notice source changed')
            files[name]=digest
        previous=old_marker.get('previous_marker')
    if proxy.get('return_basis_consumption',{}).get('revision')=='stock_return_consumption_v2':
        from scripts.scoped_forward_consumption import requirements
        path=root/'outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_meta.json'
        universe=set(v.base_mod.freq_mod.list_backtest_universe_symbols())
        _,_,stable=requirements(root,marker['target'],universe,proxy,path,path.read_bytes())
        for path,body in stable.items():
            relative=sources.extended(path).relative_to(sources.extended(root)).as_posix()
            files[relative]=sources.sha(body)
    return files
