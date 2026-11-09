"""Authenticate a scoped forward metadata renewal against the whole old universe.

Only the maintained issuers may advance. All other historical records retain
their original bytes and authenticated ancestor archive; no current coverage is
claimed for those cold records. Historical universe membership never narrows.
"""
from pathlib import Path
import hashlib
import json

import pandas as pd

from scripts.scoped_source_consumer import bound, sha
from scripts.st_metadata_refresh import _history_equal


def metadata_fingerprint(records):
    digest = hashlib.sha256()
    for symbol, raw in sorted(records.items()):
        digest.update(symbol.encode('ascii') + b'\0' + raw + b'\0')
    return dict(present_count=len(records), missing_count=0, sha256=digest.hexdigest())


def plan_metadata_renewal(root, source, *, symbols, target, previous_fingerprint,
                          current_authority, consumed, universe, native_freq):
    """Read-only binding after native notice-chain and producer-source validation.

    The caller verifies the whole original source chain and the completed native
    producer receipt. Those immutable identities remain prerequisites at install.
    """
    root, source = Path(root).resolve(), Path(source).resolve()
    if (len(symbols) != 300 or len(set(symbols)) != 300
            or not set(symbols) <= set(universe)):
        raise ValueError('Scoped metadata renewal requires 300 existing historical issuers')
    directory = root / '.microcap_index_cache/security_meta'
    previous = {p.stem:p.read_bytes() for p in directory.glob('*.json')}
    if set(previous) != set(universe) or metadata_fingerprint(previous) != previous_fingerprint:
        raise ValueError('Old whole-universe metadata no longer matches the consumed proxy')
    current = dict(previous)
    allowed = {'notice_query_end_date', 'st_notice_archive_sha256',
        'st_notice_archive_manifest_path', 'notice_count', 'last_trade_date',
        'current_st_snapshot_name', 'st_intervals', 'st_presample_name_anchor'}
    changes = {}
    for symbol in sorted(symbols):
        path = bound(source, '.microcap_index_cache/security_meta/' + symbol + '.json')
        raw = path.read_bytes(); old, new = json.loads(previous[symbol]), json.loads(raw)
        old_end = old.get('notice_query_end_date')
        required = consumed.get(symbol)
        if (old.get('symbol') != symbol or new.get('symbol') != symbol
                or not isinstance(required, str) or not isinstance(old_end, str)
                or old_end < required or old_end > target
                or not old.get('notice_source_complete')
                or not new.get('notice_source_complete')
                or new.get('current_st_history_resolved') is not True
                or new.get('st_notice_archive_manifest_path') != current_authority[0]
                or new.get('st_notice_archive_sha256') != current_authority[1]
                or new.get('notice_query_end_date') != target
                or not native_freq._notice_horizon_covers(new, pd.Timestamp(target))):
            raise ValueError('Unverified scoped metadata horizon: ' + symbol)
        if (not _history_equal(old, new, old_end)
                or any(old.get(k) != new.get(k) for k in (old.keys() | new.keys()) - allowed)):
            raise ValueError('Scoped metadata changes protected historical semantics: ' + symbol)
        # A pre-sample anchor is historical evidence, not a display field.
        if old.get('st_presample_name_anchor') != new.get('st_presample_name_anchor'):
            raise ValueError('Scoped metadata changes its historical name anchor: ' + symbol)
        current[symbol] = raw
        changes[symbol] = dict(old_sha256=sha(previous[symbol]), new_sha256=sha(raw),
                               protected_through=old_end, consumed_through=required)
    if any((directory / (s + '.json')).read_bytes() != raw for s,raw in previous.items()):
        raise ValueError('Whole-universe metadata changed during scoped planning')
    return dict(symbols=sorted(symbols), target=target, authority=list(current_authority),
        previous_fingerprint=previous_fingerprint, fingerprint=metadata_fingerprint(current),
        old_records=previous, new_records={s:current[s] for s in symbols}, changes=changes,
        cold_count=len(previous)-len(symbols), historical_universe_count=len(previous),
        final_strategy_delivery=False)
