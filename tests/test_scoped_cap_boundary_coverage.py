"""Frozen original market pages plus explicit software source/master faults.

No current data, strategy refresh, historical rewrite or external connection.
The old validator is the exact released source, not a test reimplementation.
"""
import base64
import copy
import gzip
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import socket

import pandas as pd
import pytest

import microcap_top100_mom16_biweekly_live_v2_0 as v
from scripts import scoped_cap_boundary as boundary
from scripts import scoped_source_consumer as source

DAY = '2026-10-29'
MISSING = '301098'
FIXTURE = Path(__file__).parent/'fixtures/scoped_cap_boundary_20261029.json.gz'
FIXTURE_SHA = '5c71eda7d92266495dfb32148d8e1f0352423c6a8392dd301c1d4a07e1a78936'


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def fail(*args, **kwargs):
        raise AssertionError('Frozen boundary regressions must never collect market data')
    monkeypatch.setattr(socket.socket, 'connect', fail)
    monkeypatch.setattr(socket, 'create_connection', fail)


@pytest.fixture
def original(tmp_path, monkeypatch):
    raw = FIXTURE.read_bytes()
    assert hashlib.sha256(raw).hexdigest() == FIXTURE_SHA
    packet = json.loads(gzip.decompress(raw))
    assert packet['scope'] == 'frozen_software_regression_only_not_current_market_data'
    for name, item in packet['objects'].items():
        body = base64.b64decode(item['base64'], validate=True)
        assert hashlib.sha256(body).hexdigest() == item['sha256']
        path = tmp_path/name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(body)
    spec = dict(directory='boundary/'+DAY, files={
        Path(name).name:item['sha256'] for name,item in packet['objects'].items()
        if name.startswith('boundary/')})
    master_path = tmp_path/'.microcap_index_cache/security_master.csv'
    # Embedded freq_mod is a namespace, not the function's globals dictionary.
    # Configure the real loader itself; do not accidentally read repo caches.
    native_globals = v.base_mod.freq_mod.load_security_master.__globals__
    monkeypatch.setitem(native_globals, 'SECURITY_MASTER_CACHE', master_path)
    monkeypatch.setitem(native_globals, 'SHARED_SECURITY_MASTER_CACHE', None)
    monkeypatch.setitem(native_globals, '_SECURITY_MASTER_MEMO', None)
    rows, _ = boundary.read_snapshot(tmp_path, spec, DAY)
    return tmp_path, packet, spec, rows


def scoped_inputs(packet, rows, *, omit=False):
    """Reuse the audit's scope fault while preserving the actual current 100."""
    caps = packet['native_caps']
    ranking = sorted(caps, key=lambda symbol:(caps[symbol], symbol))
    assert ranking.index(MISSING) == 28 and MISSING not in packet['initial_members']
    scope = set(ranking[:150]) | set(packet['initial_members'])
    threshold = (caps[ranking[100]], ranking[100])
    scope.update(row['SECURITY_CODE'] for row in rows
        if not row['SECUCODE'].endswith('.BJ')
        and not row['SECURITY_CODE'].startswith(('200', '900'))
        and (row['TOTAL_MARKET_CAP'], row['SECURITY_CODE']) <= threshold)
    if omit:
        scope.remove(MISSING)
    for symbol in packet['native_scope'] + [row['SECURITY_CODE'] for row in rows]:
        if len(scope) == 300:
            break
        if not omit or symbol != MISSING:
            scope.add(symbol)
    assert len(scope) == 300 and set(packet['initial_members']) <= scope
    return sorted(scope), {symbol:value for symbol,value in caps.items() if symbol in scope}


def write_pages(original, rows):
    root, packet, _, _ = original
    files = {}
    for page, offset in enumerate(range(0, len(rows), 5000), 1):
        name = f'page-{page:02d}'
        receipt = json.loads(base64.b64decode(
            packet['objects'][f'boundary/{DAY}/{name}.receipt.json']['base64']))
        raw = source.encode(dict(success=True, result=dict(count=len(rows),
            pages=(len(rows)+4999)//5000, data=rows[offset:offset+5000])))
        receipt['sha256'] = source.sha(raw)
        for suffix, body in (('.json', raw), ('.receipt.json', source.encode(receipt))):
            path = root/'boundary'/DAY/(name+suffix)
            path.write_bytes(body)
            files[path.name] = source.sha(body)
    return dict(directory='boundary/'+DAY, files=files)


def old_validator(root):
    path = root/'baseline/scoped_cap_boundary.py'
    spec = importlib.util.spec_from_file_location('released_boundary_1634a27c', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def verify(original, spec, *, omit=False, module=boundary):
    root, packet, _, rows = original
    scope, caps = scoped_inputs(packet, rows, omit=omit)
    return module.verify(root, v.base_mod, DAY, caps, scope, spec,
        ['unused_no_outside_candidates', '1'*64])


def test_original_5577_pages_keep_exact_previous_boundary_result(original):
    root, packet, spec, rows = original
    master = v.base_mod.freq_mod.load_security_master()
    assert master.columns.tolist() == ['symbol','exchange','board','list_date','delist_date','name','source']
    assert len(master) == 5533 and len(rows) == 5577
    assert len(boundary._active_boundary_symbols(master, DAY)) == 5168
    expected = verify(original, spec, module=old_validator(root))
    actual = verify(original, spec)
    assert actual == expected
    caps = packet['native_caps']
    assert actual['top100'] == sorted(caps, key=lambda symbol:(caps[symbol],symbol))[:100]


def test_real_rank29_omission_that_passed_old_guard_now_fails(original):
    root, _, _, rows = original
    faulty = [row for row in rows if row['SECURITY_CODE'] != MISSING]
    assert len(faulty) == 5576
    spec = write_pages(original, faulty)
    assert verify(original, spec, omit=True, module=old_validator(root))['status'] == 'native_scope_boundary_passed'
    with pytest.raises(ValueError, match='missing active native A-share issuers: 301098'):
        verify(original, spec, omit=True)


@pytest.mark.parametrize('noise', ['BJ', 'B_SH', 'B_SZ', 'other_inactive_A'])
def test_extra_rows_cannot_compensate_for_missing_active_issuer(original, noise):
    _, _, _, rows = original
    replacement = copy.deepcopy(rows[0])
    symbol, venue = {'BJ':('920999','BJ'), 'B_SH':('900999','SH'),
                     'B_SZ':('200999','SZ'), 'other_inactive_A':('699999','SH')}[noise]
    assert symbol not in {row['SECURITY_CODE'] for row in rows}
    replacement.update(SECURITY_CODE=symbol, SECUCODE=symbol+'.'+venue)
    faulty = sorted([row for row in rows if row['SECURITY_CODE'] != MISSING] + [replacement],
        key=lambda row:row['SECURITY_CODE'])
    assert len(faulty) == len(rows)
    spec = write_pages(original, faulty)
    with pytest.raises(ValueError, match='missing active native A-share issuers: 301098'):
        verify(original, spec, omit=True)


def test_duplicate_source_issuer_remains_rejected_after_rehash(original):
    _, _, _, rows = original
    faulty = sorted(rows+[copy.deepcopy(rows[0])], key=lambda row:row['SECURITY_CODE'])
    spec = write_pages(original, faulty)
    with pytest.raises(ValueError, match='missing, duplicate or unordered issuers'):
        verify(original, spec)


def write_master(original, monkeypatch, mutate):
    root = original[0]
    path = root/'.microcap_index_cache/security_master.csv'
    master = pd.read_csv(path, dtype={'symbol':str})
    changed = mutate(master)
    changed.to_csv(path, index=False, date_format='%Y-%m-%d')
    monkeypatch.setitem(v.base_mod.freq_mod.load_security_master.__globals__, '_SECURITY_MASTER_MEMO', None)


@pytest.mark.parametrize('listed,delisted,required', [
    ('2026-10-28', None, True),
    ('2026-10-29', None, True),
    ('2026-10-30', None, False),
    ('2000-01-01', '2026-10-28', False),
    ('2000-01-01', '2026-10-29', False),
    ('2000-01-01', '2026-10-30', True),
])
def test_listing_inclusive_and_delisting_exclusive_source_coverage(original, monkeypatch, listed, delisted, required):
    def lifecycle(master):
        selected = master.symbol.eq(MISSING)
        master.loc[selected, 'list_date'] = listed
        master.loc[selected, 'delist_date'] = delisted
        return master
    write_master(original, monkeypatch, lifecycle)
    spec = write_pages(original, [row for row in original[3] if row['SECURITY_CODE'] != MISSING])
    if required:
        with pytest.raises(ValueError, match='missing active native A-share issuers: 301098'):
            verify(original, spec, omit=True)
    else:
        assert verify(original, spec, omit=True)['status'] == 'native_scope_boundary_passed'


@pytest.mark.parametrize('fault', ['duplicate', 'missing_symbol', 'integer_symbol', 'missing_list_date', 'missing_column'])
def test_ambiguous_native_master_cannot_silently_reduce_coverage(original, monkeypatch, fault):
    def corrupt(master):
        selected = master.symbol.eq(MISSING)
        if fault == 'duplicate':
            return pd.concat([master, master.loc[selected]], ignore_index=True)
        if fault == 'missing_column':
            return master.drop(columns='list_date')
        if fault == 'missing_symbol':
            master.loc[selected, 'symbol'] = None
        if fault == 'integer_symbol':
            # A lost leading zero cannot be silently normalized into another issuer.
            master.loc[master.symbol.eq('000001'), 'symbol'] = '1'
        if fault == 'missing_list_date':
            master.loc[selected, 'list_date'] = None
        return master
    write_master(original, monkeypatch, corrupt)
    # The real loader requires dates at parsing time. Either it or the new
    # validator must reject an unusable native master; neither collects data.
    with pytest.raises((ValueError, KeyError)):
        verify(original, original[2])


def test_ST_name_does_not_exempt_a_missing_active_source_row(original, monkeypatch):
    def current_name(master):
        master.loc[master.symbol.eq(MISSING), 'name'] = '*ST explicit software fixture'
        return master
    write_master(original, monkeypatch, current_name)
    spec = write_pages(original, [row for row in original[3] if row['SECURITY_CODE'] != MISSING])
    with pytest.raises(ValueError, match='missing active native A-share issuers: 301098'):
        verify(original, spec, omit=True)


def test_native_BJ_and_B_share_rows_are_not_A_share_source_obligations(original, monkeypatch):
    def add_other_boards(master):
        template = master.iloc[0].to_dict()
        extras = [dict(template, symbol=symbol, exchange=exchange, list_date='2000-01-01', delist_date=None)
            for symbol,exchange in [('920999','BSE'),('900999','SSE'),('200999','SZSE')]]
        return pd.concat([master, pd.DataFrame(extras)], ignore_index=True)
    write_master(original, monkeypatch, add_other_boards)
    rows = [row for row in original[3] if not row['SECUCODE'].endswith('.BJ')
            and not row['SECURITY_CODE'].startswith(('200','900'))]
    spec = write_pages(original, rows)
    assert verify(original, spec)['status'] == 'native_scope_boundary_passed'
