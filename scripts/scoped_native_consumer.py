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


def checked_producer(source, config, manifest):
    runtime = subprocess.check_output(['git', '-C', str(source), 'rev-parse', 'HEAD'], text=True).strip()
    if runtime != sources.PRODUCER:
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
    return receipt, name_path


def consume_no_rebalance(*, root, base, args, paths, panel_path, target, config):
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
    if current_end == target:
        return _reuse_completed(root, base, prior, config, target)
    if current_end > target:
        raise ValueError('Scoped consumer cannot replace newer native outputs')
    dates = pd.DatetimeIndex(pd.to_datetime(pd.read_csv(panel_path)['date'], errors='raise'))
    scheduled = base.build_biweekly_rebalance_dates(dates)
    if (target not in dates or pd.Timestamp(current_end) not in dates
            or dates.has_duplicates or not dates.is_monotonic_increasing or dates.max() > pd.Timestamp(target)
            or ((scheduled > pd.Timestamp(current_end)) & (scheduled <= pd.Timestamp(target))).any()):
        raise ValueError('Scoped no-rebalance consumer cannot certify a rebalance boundary')
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
    for name in (FRESHNESS, POINTER_PATH, '.microcap_index_cache/current_st.csv', name_path):
        notice_files[name] = sources.sha(sources.bound(source, name).read_bytes())
    meta_files = {'.microcap_index_cache/security_meta/' + s + '.json':sources.sha(raw)
                  for s,raw in meta_plan['new_records'].items()}
    files = {**notice_files, **meta_files}
    allowed_prefixes = ('.microcap_index_cache/st_notice_sources/',
        '.microcap_index_cache/security_meta/', '.microcap_index_cache/local_current_names/')
    if any(not name.startswith(allowed_prefixes) and name not in
            (POINTER_PATH, '.microcap_index_cache/current_st.csv') for name in files):
        raise ValueError('Scoped notice/metadata transport includes another file role')
    mutable = set(meta_files) | {POINTER_PATH, FRESHNESS, '.microcap_index_cache/current_st.csv'}
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
            _CONTEXT = dict(root=root, target=target, plan=source_plan, stack=stack, adopted=False)
            try:
                if not base.try_extend_proxy_index_without_rebalance(staged_args, staged_paths, panel_path, pd.Timestamp(target)):
                    raise ValueError('Original native scoped proxy continuation rejected')
                if not _CONTEXT['adopted']:
                    raise ValueError('Native continuation did not cross its source adoption boundary')
                if not base.try_extend_costed_nav_without_turnover(staged_args, panel_path, pd.Timestamp(target), staged_paths['proxy_turnover']):
                    raise ValueError('Original native scoped costed NAV continuation rejected')
            finally:
                _CONTEXT = None
            for old, new, proxy in ((args.index_csv,staged_args.index_csv,True),
                                    (args.costed_nav_csv,staged_args.costed_nav_csv,False)):
                preserve_verified_replay_prefix(base, originals[old], new, proxy=proxy)
                if not new.read_bytes().startswith(originals[old]) or str(pd.Timestamp(base.read_csv_last_date(new)).date()) != target:
                    raise ValueError('Native scoped continuation changed its approved prefix or target')
            for key in ('proxy_members','proxy_turnover','proxy_effective_members'):
                if staged_paths[key].read_bytes() != originals[paths[key]]:
                    raise ValueError('No-rebalance scoped continuation changed executed member authority')
            result_meta = json.loads(staged_paths['proxy_meta'].read_bytes())
            if not base.proxy_meta_matches_execution_model(result_meta):
                raise ValueError('Native scoped continuation did not earn the actual new fingerprints')
            original_proxy = work / 'original_proxy.json'; sources.atomic(original_proxy, prior_raw)
            marker = dict(schema='native_scoped_metadata_adoption_v1', status='complete', target=target,
                symbols=manifest['symbols'], previous_authority=list(old_ref), authority=chain['authority'],
                original_proxy=dict(path=original_proxy.relative_to(root).as_posix(),sha256=sources.sha(prior_raw)),
                backup_directory=(work / 'backup/.microcap_index_cache/security_meta').relative_to(root).as_posix(),
                changes=meta_plan['changes'], previous_fingerprint=meta_plan['previous_fingerprint'],
                fingerprint=meta_plan['fingerprint'], source_bundle_sha256=config['bundle_sha256'],
                original_notice_receipts=specs, source_files=chain['source_files'],
                source_input_files=manifest['files'], final_strategy_delivery=False)
            marker_raw = sources.encode(marker)
            result_meta['scoped_metadata_adoption'] = dict(path=ACTIVE,sha256=sources.sha(marker_raw))
            base._atomic_write_json(staged_paths['proxy_meta'], result_meta, encoding='utf-8')
            for destination in outputs:
                if destination.read_bytes() != originals[destination]:
                    raise ValueError('Concurrent native output modification before scoped publish')
            for destination, staged in ((args.index_csv,staged_args.index_csv),
                    (args.costed_nav_csv,staged_args.costed_nav_csv),(paths['proxy_meta'],staged_paths['proxy_meta'])):
                payload = staged.read_bytes(); published[destination] = payload
                sources.atomic(destination, payload)
            published[sources.bound(root,ACTIVE)] = marker_raw
            sources.atomic(sources.bound(root,ACTIVE), marker_raw)
            scoped_transport_files(root)
        return dict(status='native_scoped_no_rebalance_continuation_passed', target=target,
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
    return files
