"""Explicit native late-adoption contract for staged, source-only issuer inputs.

Planning is read-only. Adoption is available only after the old global state has
passed its native gate; failure restores the exact old mutable source bytes.
This module does not grant a strategy release, ST renewal or email permission.
"""
from __future__ import annotations

from contextlib import contextmanager
from datetime import date, datetime
from decimal import Decimal
import csv
import hashlib
import io
import json
import os
from pathlib import Path, PurePosixPath
import re
import uuid

import numpy as np
import pandas as pd

from scripts.st_notice_refresh import archived_freshness_source_files

CACHE = '.microcap_index_cache/'
SCHEMA = 'microcap_scoped_source_inputs_v1'
PRODUCER = '63c7b16d77600c4aa9dadf58b86d31e034477e44'
PRODUCERS = frozenset({PRODUCER,'706af7ea9186bdb9b3948738deb5a5afda7a331e',
    'a016ce27ea86fca1fa0b5199f802b198c0ed99de','be89e87a0da728b27be0308d7c4c9e57a212b815',
    'f6970d665d5c7a39b37c866cf13dc3d4cb8fdcc5',
    '3fe2e15ee9e3f2ee7e25bbc328479f1fd9910d36',
    'd2d15cbaf5763333c74239b1ded3d1ac467adb3c',
    '3344e403ff01e17ef45b745a704a240e0917720b'})
ARCHIVED_PROOF_READER = (
    'a9ee19c96df52517d45b0d7e56f82a4b7c924cdaaa72f2818c0a32f7d772a83e',
    '0091610545f494d3c3e347596049bee39494fc9c4a4b557fa0dbdcb838443351')
SEALED_BASIS_READER = (
    '19eb18eecf7a335ce791c3194060caa53f2170a54598f16dd9172fc2dc4a031d',
    '24f668a1fea1570b92c31dcace7f01a0f1e2f8f0f59725166f3a413ce0e706f3')
FORWARD_CONSUMPTION_READER=(SEALED_BASIS_READER[0],
    '3c248e53500ea3952057f953bfe8e87e03f47eeb076d7b1a4a224ddf9b599ecb')
TOTAL_NEUTRAL_SHARE_READER=(
    '996e3eb846894acc03cd0ac62e87bc5bf8fafe16dae686e83f65db71585b1112',
    '3dfb3ad0facbda0b8994af3767272fd35e22b16554993a60f6942d3e8bd74b7b')
FUTURE_PLAN_SHARE_READER=(TOTAL_NEUTRAL_SHARE_READER[1],
    '21ab5dddc118c73d118a12dcc5250dde21b767677c42692b5a055eeed051f34f')
FUTURE_PLAN_REGISTRY_READER=(
    '972825f752eb59d274c14e6a009f1b92cbed2e4a3f45ad5adce027f3b174e2e1',
    '86c353677ec35023bc451c00ca1fd13966f117e3cc7352958d0d5b28bd1b7aa8')
LATER_QUOTE_READER = {
    'scripts/share_change_source.py': (
        '21ab5dddc118c73d118a12dcc5250dde21b767677c42692b5a055eeed051f34f',
        'be9e393f09196a812d60b9eb867d21e32b6afbd1e7862d7605accb8bf809d93c'),
    'scripts/share_unchanged_tail.py': (
        '85bb77979ecdb179c2f3a71ce17261dd9c4bd0cd60b9a8c90c83d183a79146b2',
        'd5824797c483d0b466684a5bebbe19595b7108fb393cd7815dc7b870688dd7ae'),
}
TYPED_QUOTE_READER = {
    'scripts/share_change_source.py': (
        'be9e393f09196a812d60b9eb867d21e32b6afbd1e7862d7605accb8bf809d93c',
        'fdbc6292597ced431cf76bfdd9ad564c70042f8d603ee867f79d5da2bed2339e'),
    'scripts/share_unchanged_tail.py': (
        'd5824797c483d0b466684a5bebbe19595b7108fb393cd7815dc7b870688dd7ae',
        'bbfcbcbfd1654915488164b7af347297fc158987dd03909f68d0eb794e225f88'),
}
BACKUP_QUOTE_READER = {
    'scripts/share_unchanged_tail.py': (
        'bbfcbcbfd1654915488164b7af347297fc158987dd03909f68d0eb794e225f88',
        '80ca426b78cfda4e63fc32be0157be188ebbe31d68e2f9758664b2d7d2342491'),
}


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def extended(path):
    path = Path(path)
    if os.name == 'nt' and not str(path).startswith('\\\\?\\'):
        return Path('\\\\?\\' + str(path.resolve()))
    return path


def bound(root, name):
    """Portable, regular root-relative source paths, including linked ancestors."""
    root = Path(root).resolve()
    if (not isinstance(name, str) or '\\' in name or ':' in name
            or PurePosixPath(name).is_absolute() or '..' in PurePosixPath(name).parts
            or PurePosixPath(name).as_posix() != name):
        raise ValueError('Nonportable scoped source path')
    path = root / name
    for parent in (path, *path.parents):
        if parent == root:
            break
        probe = extended(parent)
        if probe.is_symlink() or getattr(probe, 'is_junction', lambda: False)():
            raise ValueError('Linked scoped source path')
    if not path.resolve().is_relative_to(root):
        raise ValueError('Scoped source path escapes root')
    return extended(path)


def atomic(path, raw):
    path = extended(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + '.' + uuid.uuid4().hex + '.tmp')
    with temporary.open('xb') as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


def encode(value):
    return (json.dumps(value, ensure_ascii=False, indent=2) + '\n').encode()


def validate_source_contract(root, manifest):
    """Permit only the unchanged producer or this exact receipt-reader successor."""
    from scripts.stock_input_contract import SOURCE_FILES, source_fingerprint
    context = manifest.get('producer_context', {})
    old = context.get('native_sources')
    if (manifest.get('runtime_sha') not in PRODUCERS or context.get('runtime_sha') != manifest.get('runtime_sha')
            or not isinstance(old, dict) or set(old) != set(SOURCE_FILES)):
        raise ValueError('Scoped source producer/code contract differs')
    current = source_fingerprint(Path(root))
    changed = {name for name in old if old[name] != current[name]}
    archived={'scripts/st_notice_refresh.py':ARCHIVED_PROOF_READER}
    cached={**archived,'scripts/stock_return_pipeline.py':SEALED_BASIS_READER}
    forward={**archived,'scripts/stock_return_pipeline.py':FORWARD_CONSUMPTION_READER}
    original=(archived,cached,forward)
    neutral={'scripts/share_change_source.py':TOTAL_NEUTRAL_SHARE_READER}
    plans={'scripts/share_change_source.py':FUTURE_PLAN_SHARE_READER,
           'data/share_source_future_plan_reviews.json':FUTURE_PLAN_REGISTRY_READER}
    old_plans={**plans,'scripts/share_change_source.py':(
        TOTAL_NEUTRAL_SHARE_READER[0],FUTURE_PLAN_SHARE_READER[1])}
    reviews=(*original,neutral,*({**r,**neutral} for r in original),plans,old_plans,
             *({**r,**p} for r in original for p in (plans,old_plans)))
    # Preserve only already reviewed predecessor paths ending at the exact P1
    # parser, followed by this exact two-file observation-only successor.
    successors = [LATER_QUOTE_READER]
    for review in reviews:
        shared = set(review) & set(LATER_QUOTE_READER)
        if any(review[name][1] != LATER_QUOTE_READER[name][0] for name in shared):
            continue
        successors.append({**review, **{name:(review[name][0] if name in review else before, after)
            for name,(before,after) in LATER_QUOTE_READER.items()}})
    reviews = (*reviews, *successors)
    # This transport-only successor preserves every reviewed predecessor pair;
    # no arbitrary source hash or partially matching file set is admitted.
    typed_successors = [TYPED_QUOTE_READER]
    for review in reviews:
        shared = set(review) & set(TYPED_QUOTE_READER)
        if any(review[name][1] != TYPED_QUOTE_READER[name][0] for name in shared):
            continue
        typed_successors.append({**review, **{name:(review[name][0] if name in review else before, after)
            for name,(before,after) in TYPED_QUOTE_READER.items()}})
    reviews = (*reviews, *typed_successors)
    budget_successors = [BACKUP_QUOTE_READER]
    for review in reviews:
        shared = set(review) & set(BACKUP_QUOTE_READER)
        if any(review[name][1] != BACKUP_QUOTE_READER[name][0] for name in shared):
            continue
        budget_successors.append({**review, **{name:(review[name][0] if name in review else before, after)
            for name,(before,after) in BACKUP_QUOTE_READER.items()}})
    reviews = (*reviews, *budget_successors)
    if changed and not any(changed==set(review) and all((old[n],current[n])==pair for n,pair in review.items()) for review in reviews):
        raise ValueError('Unreviewed scoped source consumer code successor')
    return current


def verify_notice_chain(root, previous, receipts, target):
    """Authenticate original 5->6->9 receipts without rewriting their identities."""
    cursor = list(previous)
    files = {}
    last_completed = None
    if not receipts:
        raise ValueError('Original ST refresh receipt chain is required')
    for spec in receipts:
        raw = bound(root, spec['path']).read_bytes()
        if sha(raw) != spec['sha256']:
            raise ValueError('ST refresh chain receipt bytes changed')
        proof = json.loads(raw)
        if proof.get('previous_authority') != cursor:
            raise ValueError('ST refresh receipt chain has a missing or reordered link')
        if last_completed is not None and datetime.fromisoformat(proof['required_asof']) < last_completed:
            raise ValueError('ST refresh chain observation clocks overlap backwards')
        verified = archived_freshness_source_files(root, spec['path'], spec['sha256'], proof['authority'])
        for name, digest in verified.items():
            if name in files and files[name] != digest:
                raise ValueError('ST refresh chain has conflicting source identities')
            files[name] = digest
        cursor = proof['authority']
        last_completed = datetime.fromisoformat(proof['completed_at'])
    if proof['target'] != target:
        raise ValueError('ST refresh receipt chain does not reach the requested target')
    return dict(previous_authority=list(previous), authority=cursor, target=target,
                source_files=files, receipt_count=len(receipts), original_clocks_retained=True)


def mutable_files(symbol):
    names = [CACHE + group + '/' + symbol + suffix for group, suffix in (
        ('prices_raw', '.csv'), ('share_change', '.csv'), ('share_sources', '.json'),
        ('trade_status', '.json'), ('return_basis/manifests', '.json'),
        ('return_basis/prices_adj', '.csv'))]
    return names + [CACHE + 'return_basis/sources/' + symbol + '/' + name
                    for name in ('qfq.js', 'hfq.js', 'qfq.json', 'hfq.json')]


def source_role(name, symbols):
    parts = PurePosixPath(name).parts
    if len(parts) < 3 or parts[0] != CACHE.rstrip('/'):
        return False
    if parts[1] in ('prices_raw', 'share_change'):
        return len(parts) == 3 and parts[2] in {s + '.csv' for s in symbols}
    if parts[1] in ('share_sources', 'trade_status'):
        if len(parts) == 3:
            return parts[2] in {s + '.json' for s in symbols}
        # Native provider/baseline evidence objects have no global issuer pointer.
        return parts[-1].endswith(('.json', '.csv', '.raw', '.bin')) and not any(
            p in ('locks', 'incomplete', 'failures') for p in parts)
    if parts[1] == 'return_basis' and len(parts) >= 4:
        if parts[2] in ('manifests', 'prices_adj'):
            return len(parts) == 4 and parts[3] in {s + ('.json' if parts[2] == 'manifests' else '.csv') for s in symbols}
        return parts[2] in ('sources', 'bundles') and parts[3] in symbols
    return False


def consumed_boundaries(root, base, args, paths):
    """Derive issuer horizons from authenticated original native consumption."""
    from scripts.stock_return_pipeline import _consumption_requirements
    root = Path(root).resolve()
    meta = json.loads(paths['proxy_meta'].read_bytes())
    if not base.proxy_meta_matches_execution_model(meta):
        raise ValueError('Old native global execution/data/metadata gate failed before adoption')
    end = str(pd.Timestamp(base.read_csv_last_date(args.index_csv)).date())
    if str(pd.Timestamp(base.read_csv_last_date(args.costed_nav_csv)).date()) != end:
        raise ValueError('Old proxy and costed NAV anchors differ')
    universe = set(base.freq_mod.list_backtest_universe_symbols())
    full, latest, stable = _consumption_requirements(root, end, universe)
    # The native validator authenticates all member hashes, actual executions,
    # tail sessions and the complete chain. Cold candidates inherit full only.
    horizons = {symbol:(latest.get(symbol,full) if isinstance(latest,dict)
                       else end if symbol in latest else full) for symbol in universe}
    stable[args.costed_nav_csv] = args.costed_nav_csv.read_bytes()
    return horizons, stable


def preserve_csv_history(oldraw, newraw, key, value, cutoff, *, carried=False):
    def rows(raw):
        records = list(csv.DictReader(io.StringIO(raw.decode('utf-8-sig'))))
        pairs = [(row[key][:10], Decimal(row[value])) for row in records]
        if any(not number.is_finite() or number <= 0 for _, number in pairs):
            raise ValueError('Nonpositive/nonfinite source history')
        if [day for day, _ in pairs] != sorted(set(day for day, _ in pairs)):
            raise ValueError('Duplicate or unordered source history')
        for day, _ in pairs:
            date.fromisoformat(day)
        return dict(pairs)
    old, new = rows(oldraw), rows(newraw)
    left = right = None
    for day in sorted(set(old) | set(new)):
        if day > cutoff:
            break
        if carried:
            left, right = old.get(day, left), new.get(day, right)
            if left != right:
                raise ValueError('Adoption changes consumed historical share capitalization')
        elif old.get(day) != new.get(day):
            raise ValueError('Adoption changes consumed historical raw prices')


def preserve_return_history(oldraw, newraw, cutoff):
    """Compare every consumed return, permitting only floating arithmetic noise."""
    def series(raw):
        frame = pd.read_csv(io.BytesIO(raw))
        if not {'date', 'close_adj'}.issubset(frame):
            raise ValueError('Adjusted source lacks native return columns')
        frame['date'] = pd.to_datetime(frame.date, errors='raise')
        if (frame.date.isna().any() or frame.date.duplicated().any()
                or not frame.date.is_monotonic_increasing):
            raise ValueError('Adjusted source dates are malformed')
        values = pd.to_numeric(frame.close_adj, errors='raise')
        if not np.isfinite(values).all() or values.le(0).any():
            raise ValueError('Adjusted source prices are invalid')
        return pd.Series(values.to_numpy(), index=frame.date).loc[:pd.Timestamp(cutoff)]
    old, new = series(oldraw), series(newraw)
    if not old.index.equals(new.index):
        raise ValueError('Adoption changes consumed adjusted-date coverage')
    np.testing.assert_allclose(old.pct_change(fill_method=None).to_numpy(),
        new.pct_change(fill_method=None).to_numpy(), rtol=0,
        atol=8*np.finfo(float).eps, equal_nan=True,
        err_msg='Adoption changes consumed stock returns')


def plan_adoption(root, staged_root, manifest, *, consumed, observed):
    """Rehash a staged native candidate and protect issuer-specific consumed history.

    Caller authenticates the source manifest against the configured immutable
    source bundle and validates its native code/lifecycle contract first.
    Returned observations are compared again under the adoption writer lock.
    """
    root, staged_root = Path(root).resolve(), Path(staged_root).resolve()
    symbols = manifest.get('symbols')
    if (manifest.get('schema') != SCHEMA or manifest.get('source_inputs_only') is not True
            or manifest.get('native_ST_refresh_required_at_consumer') is not True
            or manifest.get('final_strategy_delivery') is not False
            or not isinstance(symbols, list) or len(symbols) != 300
            or len(set(symbols)) != 300
            or any(not isinstance(s, str) or re.fullmatch(r'[0-9]{6}', s) is None for s in symbols)):
        raise ValueError('Late adoption requires an exact 300-issuer native source manifest')
    target = manifest['target']; date.fromisoformat(target)
    master_name = CACHE + 'security_master.csv'
    master_raw = bound(root, master_name).read_bytes()
    if sha(master_raw) != manifest.get('master_sha256'):
        raise ValueError('Scoped source master/lifecycle identity differs from consumer')
    mutable = {name for s in symbols for name in mutable_files(s)}
    files = manifest['files']
    if not mutable <= set(files):
        raise ValueError('Scoped source lacks complete native mutable issuer inputs')
    originals = {str(p.relative_to(root).as_posix()): raw for p, raw in observed.items()}
    originals[master_name] = master_raw
    for name, digest in files.items():
        if (not source_role(name, set(symbols))
                or re.fullmatch(r'[a-f0-9]{64}', digest) is None):
            raise ValueError('Unsupported scoped source object role')
        source = bound(staged_root, name)
        raw = source.read_bytes()
        if sha(raw) != digest:
            raise ValueError('Staged source bytes changed: ' + name)
        destination = bound(root, name)
        previous = destination.read_bytes() if destination.exists() else None
        if name in mutable:
            if previous is None:
                raise ValueError('No installed native issuer baseline for late adoption')
            originals[name] = previous
        elif previous is not None and previous != raw:
            raise ValueError('Immutable source collision during late adoption')
    for s in symbols:
        cutoff = consumed.get(s)
        if not isinstance(cutoff, str) or cutoff > target:
            raise ValueError('Missing or future issuer consumption horizon: ' + s)
        date.fromisoformat(cutoff)
        raw_name, share_name, _, _, _, adjusted_name = mutable_files(s)[:6]
        old_price = list(csv.DictReader(io.StringIO(originals[raw_name].decode('utf-8-sig'))))
        observed_price_end = max(row['date'][:10] for row in old_price)
        if observed_price_end > target:
            raise ValueError('Late adoption would replace newer installed raw inputs')
        preserve_csv_history(originals[raw_name], bound(staged_root, raw_name).read_bytes(),
                             'date', 'close_raw', observed_price_end)
        share_state = json.loads(originals[CACHE + 'share_sources/' + s + '.json'])
        share_cutoff = max(cutoff, share_state['query_end'])
        if share_cutoff > target:
            raise ValueError('Late adoption would replace a newer share query')
        preserve_csv_history(originals[share_name], bound(staged_root, share_name).read_bytes(),
                             'change_date', 'total_shares_10k', share_cutoff, carried=True)
        preserve_return_history(originals[adjusted_name], bound(staged_root, adjusted_name).read_bytes(), cutoff)
    # Observe immutable manifest/code/source gate artifacts supplied by native
    # caller as well as every old mutable object. No old identity is rewritten.
    if any(bound(root, name).read_bytes() != raw for name, raw in originals.items()):
        raise ValueError('Old state changed while planning scoped adoption')
    return dict(root=root, staged_root=staged_root, files=dict(files), originals=originals,
                mutable=mutable, symbols=list(symbols), target=target,
                consumed={s:consumed[s] for s in symbols})


@contextmanager
def adoption_transaction(plan):
    """Rollback sources unless the caller completes and revalidates native adoption.

    Native strategy outputs and ST authority require their own encompassing
    transaction. This source transaction intentionally never calls a full rebuild.
    """
    root = plan['root']; cache = root / CACHE
    role = plan.get('transaction_role', 'scoped_native_adoption')
    if role not in ('scoped_native_adoption', 'scoped_native_metadata'):
        raise ValueError('Unknown native scoped transaction role')
    lock = cache / (role + '.lock')
    owner = encode(dict(pid=os.getpid(), owner=uuid.uuid4().hex))
    fd = os.open(lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, 'O_BINARY', 0))
    os.write(fd, owner); os.fsync(fd)
    identity = os.fstat(fd)
    work = cache / role / uuid.uuid4().hex
    work.mkdir(parents=True)
    attempted = []; keep_lock = False
    def check_owner():
        stat = lock.stat()
        if ((stat.st_dev, stat.st_ino) != (identity.st_dev, identity.st_ino)
                or lock.read_bytes() != owner):
            raise ValueError('Scoped adoption writer ownership changed')
    try:
        check_owner()
        for name, raw in plan['originals'].items():
            if bound(root, name).read_bytes() != raw:
                raise ValueError('Old native state changed before late adoption')
        # Stage all backups before changing even the first mutable pointer.
        for name in plan['mutable']:
            atomic(bound(work / 'backup', name), plan['originals'][name])
        atomic(work / 'receipt.json', encode(dict(status='installing', target=plan['target'],
            symbols=plan['symbols'], consumed=plan['consumed'], files=plan['files'],
            old_mutable_hashes={n:sha(plan['originals'][n]) for n in sorted(plan['mutable'])},
            final_strategy_delivery=False)))
        for name, digest in plan['files'].items():
            check_owner()
            source_root = plan.get('source_roots', {}).get(name, plan['staged_root'])
            raw = bound(source_root, name).read_bytes()
            if sha(raw) != digest:
                raise ValueError('Scoped staged source changed during adoption')
            destination = bound(root, name)
            if name in plan['mutable']:
                if destination.read_bytes() != plan['originals'][name]:
                    raise ValueError('Concurrent source mutation before pointer install')
                attempted.append((name, raw))
                atomic(destination, raw)
            elif destination.exists():
                if destination.read_bytes() != raw:
                    raise ValueError('Concurrent immutable source collision')
            else:
                atomic(destination, raw)
        yield work
        check_owner()
        if any(sha(bound(root, name).read_bytes()) != digest for name, digest in plan['files'].items()):
            raise ValueError('Adopted sources changed during native consumer readback')
        atomic(work / 'receipt.json', encode(dict(status='native_sources_adopted',
            target=plan['target'], symbols=plan['symbols'], files=plan['files'],
            final_strategy_delivery=False)))
    except BaseException as exc:
        try:
            check_owner()
            for name, installed in reversed(attempted):
                destination = bound(root, name); old = plan['originals'][name]
                if destination.read_bytes() not in (installed, old):
                    raise ValueError('Concurrent mutation prevents scoped adoption rollback')
                atomic(destination, old)
                if destination.read_bytes() != old:
                    raise ValueError('Scoped source rollback readback failed')
            atomic(work / 'failure_receipt.json', encode(dict(status='rolled_back',
                error=type(exc).__name__ + ': ' + str(exc), immutable_evidence_retained=True,
                final_strategy_delivery=False)))
        except BaseException:
            keep_lock = True
            raise
        raise
    finally:
        try:
            check_owner()
        except BaseException:
            keep_lock = True
            raise
        finally:
            os.close(fd)
        if not keep_lock:
            lock.unlink()
