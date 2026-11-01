"""Transactional, source-bound daily notice refresh; never adjudicates ST events."""
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
import copy
import hashlib
import json
import os
import uuid

from scripts.st_history_scope import POINTER_PATH, resolve_archive_reference, source_closure
from scripts.st_notice_evidence import StNoticeEvidenceError
from scripts.st_notice_incremental import collect_days, verify_day, _provider_time

PREFIX = '.microcap_index_cache/st_notice_sources'
FRESHNESS = PREFIX + '/freshness.json'


def _sha(raw):
    return hashlib.sha256(raw).hexdigest()


def _bytes(value):
    return (json.dumps(value, ensure_ascii=False, indent=2) + '\n').encode('utf-8')


def _bound(root, relative):
    path = (root / relative).resolve()
    if Path(relative).is_absolute() or not path.is_relative_to(root):
        raise StNoticeEvidenceError('ST refresh path escapes root')
    return path


def _atomic(path, raw):
    temporary = path.with_name(path.name + '.' + uuid.uuid4().hex + '.tmp')
    with temporary.open('xb') as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


def _native(folder, day):
    verify_day(folder, day)
    manifest = json.loads((folder / 'manifest.json').read_bytes())
    rows = {}
    for page in manifest['pages']:
        for row in json.loads((folder / page['response']).read_bytes())['data']['list']:
            rows[row['art_code']] = row
    return rows


def _boundary_precision_drift(previous, current):
    """Only tolerate sub-second same-day provider display-time precision drift."""
    error = 'ST boundary native events changed; exact-hash history review required; pointer preserved'
    if previous.keys() != current.keys():
        raise StNoticeEvidenceError(error)
    changed, maximum, crossed = [], 0, 0
    for identity, old in previous.items():
        new = current[identity]
        if old == new:
            continue
        if ({k: value for k, value in old.items() if k != 'display_time'}
                != {k: value for k, value in new.items() if k != 'display_time'}):
            raise StNoticeEvidenceError(error)
        before, after = _provider_time(old.get('display_time')), _provider_time(new.get('display_time'))
        delta = abs(after - before)
        close = before.replace(hour=15, minute=0, second=0, microsecond=0)
        if before.date() != after.date() or delta >= timedelta(seconds=1) or (before < close) != (after < close):
            raise StNoticeEvidenceError(error)
        micros = delta // timedelta(microseconds=1)
        changed.append(identity)
        maximum = max(maximum, micros)
        crossed += before.replace(microsecond=0) != after.replace(microsecond=0)
    return dict(policy='display_time_same_day_lt_1s_no_close_crossing_v1',
                changed_event_count=len(changed), maximum_delta_microseconds=maximum,
                crossed_integer_second_count=crossed,
                changed_event_ids_sha256=_sha(json.dumps(sorted(changed), separators=(',', ':')).encode()))


def freshness_source_files(root):
    """Verified portable closure for the latest refresh proof, separate from authority."""
    root = Path(root).resolve()
    path = root / FRESHNESS
    if not path.exists():
        return {}
    raw = path.read_bytes()
    proof = json.loads(raw)
    if proof.get('schema') != 'st_notice_refresh_v1':
        raise StNoticeEvidenceError('Unsupported ST freshness schema')
    target = date.fromisoformat(proof['target'])
    required = datetime.fromisoformat(proof['required_asof'])
    completed = datetime.fromisoformat(proof['completed_at'])
    if required.tzinfo is None or completed.tzinfo is None or not required <= completed <= datetime.now(timezone.utc):
        raise StNoticeEvidenceError('Invalid ST freshness observation interval')
    reference = resolve_archive_reference(root)
    if tuple(proof['authority']) != tuple(reference):
        raise StNoticeEvidenceError('ST freshness proof belongs to another authority')
    files = {FRESHNESS: _sha(raw)}
    for relative, expected in proof['source_files'].items():
        if _sha(_bound(root, relative).read_bytes()) != expected:
            raise StNoticeEvidenceError('ST freshness evidence changed: ' + relative)
        files[relative] = expected
    previous = proof['previous_authority']
    for ref in (previous, reference):
        if len(ref) != 2 or proof['source_files'].get(ref[0]) != ref[1]:
            raise StNoticeEvidenceError('ST freshness authority source omitted')
        source_closure(root, *ref)
    old = json.loads(_bound(root, previous[0]).read_bytes())
    current = json.loads(_bound(root, reference[0]).read_bytes())
    if old.get('schema_version') != 3 or not old.get('daily_increments'):
        raise StNoticeEvidenceError('ST freshness previous daily boundary missing')
    scope = json.loads(_bound(root, old['scope']['path']).read_bytes())
    cursor = date.fromisoformat(scope['end_date']) + timedelta(days=1)
    for item in old['daily_increments']:
        if item['date'] != cursor.isoformat():
            raise StNoticeEvidenceError('ST freshness previous coverage gap')
        cursor += timedelta(days=1)
    boundary = cursor - timedelta(days=1)
    if target < boundary:
        raise StNoticeEvidenceError('ST freshness target before boundary')
    dates = [(boundary + timedelta(days=n)).isoformat() for n in range((target-boundary).days+1)]
    if [item['date'] for item in proof['queried_days']] != dates:
        raise StNoticeEvidenceError('ST freshness queried days incomplete')
    expected_authority = copy.deepcopy(old)
    for day in proof['queried_days']:
        folder = _bound(root, day['manifest_path']).parent
        verified = verify_day(folder, day['date'], required_asof=datetime.fromisoformat(proof['required_asof']))
        if datetime.fromisoformat(verified['observation_end']) > completed:
            raise StNoticeEvidenceError('ST freshness completed before query observations')
        expected_files = {day['manifest_path']: verified['manifest_sha256']}
        expected_files.update({(folder / name).relative_to(root).as_posix(): digest
                              for name, digest in verified['source_files'].items()})
        if any(proof['source_files'].get(path) != digest for path, digest in expected_files.items()):
            raise StNoticeEvidenceError('ST freshness proof omits queried source files')
        if day['date'] == boundary.isoformat():
            old_folder = _bound(root, old['daily_increments'][-1]['manifest']['path']).parent
            drift = _boundary_precision_drift(_native(old_folder, day['date']), _native(folder, day['date']))
            recorded = proof.get('boundary_display_time_precision_drift')
            if ((recorded is None and drift['changed_event_count'])
                    or (recorded is not None and recorded != drift)
                    or proof.get('boundary_native_content_unchanged') is not (drift['changed_event_count'] == 0)):
                raise StNoticeEvidenceError('ST freshness boundary precision summary differs from source')
        else:
            expected_authority['daily_increments'].append(dict(date=day['date'], manifest=dict(
                path=day['manifest_path'], sha256=verified['manifest_sha256'])))
    if current != expected_authority or current['daily_increments'][-1]['date'] != target.isoformat():
        raise StNoticeEvidenceError('ST freshness authority is not the exact contiguous append')
    return files


def refresh_notice_archive(*, root, target, session=None):
    """Re-query last certified daily boundary; additions/revisions there block promotion.

    Caller must use its independently verified panel target. Baseline-only archives
    cannot be refreshed here because category-only history is not an all-class boundary.
    """
    root = Path(root).resolve()
    target = date.fromisoformat(str(target))
    parent = root / PREFIX
    parent.mkdir(parents=True, exist_ok=True)
    lock = parent / 'REFRESH.lock'
    fd = os.open(lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    os.write(fd, str(os.getpid()).encode())
    os.close(fd)
    try:
        pointer = root / POINTER_PATH
        original_pointer = pointer.read_bytes()  # Portable installed seed is mandatory.
        freshness_path = root / FRESHNESS
        previous_freshness = freshness_path.read_bytes() if freshness_path.exists() else None
        reference = resolve_archive_reference(root)
        old_closure = source_closure(root)
        manifest_path = _bound(root, reference[0])
        old_raw = manifest_path.read_bytes()
        old = json.loads(old_raw)
        if old.get('schema_version') != 3 or not old.get('daily_increments'):
            raise StNoticeEvidenceError('ST refresh requires an installed all-class daily boundary; baseline-only archive is insufficient')
        scope = json.loads(_bound(root, old['scope']['path']).read_bytes())
        cursor = date.fromisoformat(scope['end_date']) + timedelta(days=1)
        daily = old['daily_increments']
        for item in daily:
            if item['date'] != cursor.isoformat():
                raise StNoticeEvidenceError('Existing ST daily coverage is not contiguous and ordered')
            cursor += timedelta(days=1)
        boundary = cursor - timedelta(days=1)
        if target < boundary:
            raise StNoticeEvidenceError('ST refresh target precedes installed daily boundary')
        if target > boundary and (os.environ.get('MICROCAP_ST_NOTICE_ARCHIVE') or os.environ.get('MICROCAP_ST_NOTICE_ARCHIVE_SHA256')):
            raise StNoticeEvidenceError('ST daily advancement requires portable pointer ownership, not pinned environment overrides')
        started = datetime.now(timezone.utc)
        generation = parent / ('refresh_' + started.strftime('%Y%m%dT%H%M%S%fZ') + '_' + uuid.uuid4().hex[:12])
        generation.mkdir()
        collect_days(generation, boundary.isoformat(), target.isoformat(), session=session, required_asof=started)
        boundary_folder = _bound(root, daily[-1]['manifest']['path']).parent
        boundary_drift = _boundary_precision_drift(
            _native(boundary_folder, boundary.isoformat()), _native(generation / boundary.isoformat(), boundary.isoformat()))
        candidate = copy.deepcopy(old)
        queried, files = [], {}
        day = boundary
        while day <= target:
            folder = generation / day.isoformat()
            verified = verify_day(folder, day.isoformat(), required_asof=started)
            mp = folder / 'manifest.json'
            relative = mp.relative_to(root).as_posix()
            files[relative] = _sha(mp.read_bytes())
            for name, digest in verified['source_files'].items():
                files[(folder / name).relative_to(root).as_posix()] = digest
            queried.append(dict(date=day.isoformat(), manifest_path=relative))
            if day > boundary:
                candidate['daily_increments'].append(dict(date=day.isoformat(), manifest=dict(path=relative, sha256=files[relative])))
            day += timedelta(days=1)
        new_reference = reference
        if target > boundary:
            candidate_path = generation / 'archive.json'
            candidate_raw = _bytes(candidate)
            candidate_path.write_bytes(candidate_raw)
            new_reference = (candidate_path.relative_to(root).as_posix(), _sha(candidate_raw))
            source_closure(root, *new_reference)
        # Re-read all old source bytes and pointer immediately before commit.
        current_freshness = freshness_path.read_bytes() if freshness_path.exists() else None
        if pointer.read_bytes() != original_pointer or source_closure(root) != old_closure or current_freshness != previous_freshness:
            raise StNoticeEvidenceError('ST authority changed concurrently; pointer not replaced')
        backup = generation / 'previous_pointer.json'
        backup.write_bytes(original_pointer)
        if previous_freshness is not None:
            (generation / 'previous_freshness.json').write_bytes(previous_freshness)
        files[backup.relative_to(root).as_posix()] = _sha(original_pointer)
        files[reference[0]] = reference[1]
        files[new_reference[0]] = new_reference[1]
        proof = dict(schema='st_notice_refresh_v1', required_asof=started.isoformat(),
                     completed_at=datetime.now(timezone.utc).isoformat(), target=target.isoformat(),
                     previous_authority=list(reference), authority=list(new_reference),
                     boundary_native_content_unchanged=boundary_drift['changed_event_count'] == 0,
                     boundary_display_time_precision_drift=boundary_drift, queried_days=queried, source_files=files)
        proof_raw = _bytes(proof)
        (generation / 'proof.json').write_bytes(proof_raw)
        new_pointer = _bytes(dict(manifest_path=new_reference[0], manifest_sha256=new_reference[1]))
        try:
            if new_reference != reference:
                _atomic(pointer, new_pointer)
            _atomic(freshness_path, proof_raw)
            verified_freshness = freshness_source_files(root)
        except Exception:
            # Restore only bytes this transaction still owns; never overwrite another writer.
            if new_reference != reference and pointer.read_bytes() == new_pointer:
                _atomic(pointer, original_pointer)
            if freshness_path.exists() and freshness_path.read_bytes() == proof_raw:
                if previous_freshness is None:
                    freshness_path.unlink()
                else:
                    _atomic(freshness_path, previous_freshness)
            raise
        return dict(authority_changed=new_reference != reference, target=target.isoformat(),
                    manifest_path=new_reference[0], manifest_sha256=new_reference[1],
                    freshness_files=verified_freshness)
    finally:
        lock.unlink()
