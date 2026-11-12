"""Real D11 target + D12 quotes/Sina; no event execution or market request."""
import base64
import copy
from datetime import datetime
from decimal import Decimal
from functools import cache
import gzip
import json
from pathlib import Path
import subprocess
import types

import pytest
import requests

from scripts import share_change_source as source
from scripts import share_unchanged_tail as native
from frozen_exchange_calendar import frozen_exchange_sessions

ROOT = Path(__file__).resolve().parents[1]
SYMBOLS = ('300535', '300644')


@pytest.fixture(autouse=True)
def offline(monkeypatch, frozen_exchange_sessions):
    monkeypatch.setattr(requests.Session, 'request', lambda *a, **kw: pytest.fail('no market/network request'))
    monkeypatch.delenv(native.QUOTE_CORROBORATION_ENV, raising=False)
    return frozen_exchange_sessions


@cache
def recorded():
    return json.loads(gzip.decompress((ROOT/'tests/fixtures/share_later_quote_20261112.json.gz').read_bytes()))


def case(symbol):
    item = copy.deepcopy(next(c for c in recorded()['cases'] if c['symbol'] == symbol))
    bodies = {p:base64.b64decode(raw) for p, raw in item['objects_b64'].items()}
    return item, bodies


def bound_for(bodies):
    def bound(path, digest):
        raw = bodies[path]
        if native.sha(raw) != digest:
            raise ValueError('frozen object hash differs')
        return raw
    return bound


def verify(value, mode='dated', root=ROOT):
    item, bodies = value
    manifest = copy.deepcopy(item['manifest'])
    candidate = base64.b64decode(item['previous_candidate_b64'])
    kwargs = dict(symbol=item['symbol'], previous_day=item['previous_day'], end=item['end'],
                  bound=bound_for(bodies), root=root, now=datetime.fromisoformat(recorded()['observed_at']))
    if mode == 'dated':
        return source.verify_dated_window(manifest, previous_candidate=candidate, **kwargs)
    manifest['schema'] = native.KIND
    manifest['files']['historical_close'] = manifest['files']['dated_window']
    return candidate, native.verify_manifest(manifest, candidate=candidate, **kwargs)


def rewrite(value, role, mutate, receipt=False):
    item, bodies = value
    entry = item['manifest']['files'][role]
    if isinstance(entry, list):
        entry = entry[0]
    key = 'receipt_path' if receipt else 'raw_path'
    obj = json.loads(bodies[entry[key]])
    mutate(obj)
    raw = json.dumps(obj, ensure_ascii=False).encode()
    if receipt:
        bodies[entry[key]] = raw
        entry['receipt_sha256'] = native.sha(raw)
    else:
        rewrite_raw(value, entry, raw)


def rewrite_raw(value, entry, raw):
    bodies = value[1]
    bodies[entry['raw_path']] = raw
    entry['raw_sha256'] = native.sha(raw)
    receipt = json.loads(bodies[entry['receipt_path']])
    receipt.update(raw_sha256=native.sha(raw), bytes=len(raw))
    body = json.dumps(receipt).encode()
    bodies[entry['receipt_path']] = body
    entry['receipt_sha256'] = native.sha(body)


@pytest.mark.parametrize('symbol', SYMBOLS)
@pytest.mark.parametrize('mode', ('dated', 'unchanged'))
def test_real_later_provider_observations_keep_target_and_all_csv_bytes(symbol, mode):
    value = case(symbol)
    candidate, facts = verify(value, mode)
    assert candidate == base64.b64decode(value[0]['previous_candidate_b64'])
    assert b'2026-11-12' not in candidate
    proof = facts['quote_dated_corroboration']
    assert proof['target_session'] == '2026-11-11'
    assert proof['later_provider_date'] == '2026-11-12'
    assert proof['basis'] == 'later_provider_observation_reconciled'
    assert proof['execution_or_legal_effect_not_asserted'] is True
    assert proof['f84_independent_asof_guarantee_proven'] is False
    assert proof['sina_after_raw_sha256'] == value[0]['manifest']['files']['quote_dated_corroboration']['raw_sha256']
    assert Decimal(proof['target_total_shares']) == (107019863 if symbol == '300535' else 110933506)
    assert Decimal(proof['later_total_shares']) == (108177343 if symbol == '300535' else 110973506)


@pytest.mark.parametrize('symbol', SYMBOLS)
def test_original_f697_source_rejects_these_exact_real_inputs(symbol):
    # Read the immutable producer, not a simplified replica of its old gate.
    raw = subprocess.run(['git', '-C', str(ROOT), 'show',
                          'f6970d665d5c7a39b37c866cf13dc3d4cb8fdcc5:scripts/share_change_source.py'],
                         check=True, capture_output=True).stdout
    old = types.ModuleType('f697_source_readonly')
    old.__file__ = str(ROOT/'scripts/share_change_source.py')
    exec(compile(raw, old.__file__, 'exec'), old.__dict__)
    item, bodies = case(symbol)
    with pytest.raises(ValueError, match='quote total differs from dated terminal total'):
        old.verify_dated_window(item['manifest'], symbol=symbol, previous_day=item['previous_day'], end=item['end'],
            previous_candidate=base64.b64decode(item['previous_candidate_b64']), bound=bound_for(bodies), root=ROOT,
            now=datetime.fromisoformat(recorded()['observed_at']))


FAULTS = ('no_opt_in', 'unknown_schema', 'missing_after', 'old_sina', 'old_after_clock', 'next_after_day',
          'after_before_quote', 'after_tls', 'after_http', 'after_hash', 'after_total', 'after_unknown_date',
          'quote_total', 'quote_same_total_wrong_day', 'quote_before_plan', 'unused_bridge',
          'target_total', 'target_missing', 'target_cap', 'unknown_future', 'pdf')


@pytest.mark.parametrize('mode', ('dated', 'unchanged'))
@pytest.mark.parametrize('fault', FAULTS)
def test_explicit_bridge_preserves_date_source_inventory_and_target_gates(mode, fault):
    value = case('300535')
    item, bodies = value
    files = item['manifest']['files']
    if fault == 'no_opt_in':
        item['manifest'].pop('quote_dated_corroboration')
    elif fault == 'unknown_schema':
        item['manifest']['quote_dated_corroboration']['schema'] += '_unknown'
    elif fault == 'missing_after':
        files.pop('quote_dated_corroboration')
    elif fault == 'old_sina':
        files['quote_dated_corroboration'] = files['sina']
    elif fault in ('old_after_clock', 'next_after_day', 'after_before_quote', 'after_tls', 'after_http'):
        def change(r):
            if fault == 'after_tls': r['tls_verification'] = False
            elif fault == 'after_http': r['http_status'] = 503
            else:
                stamp = {'old_after_clock':'2026-11-11T15:00:00+00:00',
                         'next_after_day':'2026-11-13T02:00:00+00:00',
                         'after_before_quote':'2026-11-12T01:00:00+00:00'}[fault]
                r.update(http_start=stamp, http_complete=stamp)
        rewrite(value, 'quote_dated_corroboration', change, receipt=True)
    elif fault == 'after_hash':
        entry = files['quote_dated_corroboration']
        bodies[entry['raw_path']] += b'changed'
    elif fault in ('after_total', 'after_unknown_date'):
        entry = files['quote_dated_corroboration']
        raw = bodies[entry['raw_path']]
        needle, replacement = ((b'10817.7343', b'10817.7344') if fault == 'after_total'
                               else (b'2026-11-12', b'2026-11-13'))
        assert needle in raw
        rewrite_raw(value, entry, raw.replace(needle, replacement))
    elif fault in ('quote_total', 'quote_same_total_wrong_day', 'quote_before_plan', 'unused_bridge'):
        def quote(q):
            if fault == 'quote_total': q['data']['f84'] += 1
            elif fault == 'unused_bridge': q['data']['f84'] = 107019863
            else:
                day = '2026-11-13' if fault == 'quote_same_total_wrong_day' else '2026-11-11'
                q['data']['f86'] = int(datetime.fromisoformat(day+'T15:30:00+08:00').timestamp())
        rewrite(value, 'quote', quote)
    elif fault.startswith('target_'):
        def target(obj):
            row = obj['result']['data'][0]
            if fault == 'target_missing':
                obj['result']['data'].pop(0); obj['result']['count'] -= 1
            elif fault == 'target_cap': row['TOTAL_MARKET_CAP'] += 1
            else:
                row['TOTAL_SHARES'] += 1
                row['TOTAL_MARKET_CAP'] = float(Decimal(row['TOTAL_SHARES'])*Decimal(str(row['CLOSE_PRICE'])))
        rewrite(value, 'dated_window', target)
    elif fault == 'unknown_future':
        def future(obj):
            row = copy.deepcopy(obj['result']['data'][0])
            row.update(END_DATE='2026-11-13 00:00:00', LISTING_DATE='2026-11-13 00:00:00', INFO_CODE='UNREVIEWED')
            obj['result']['data'].insert(0, row); obj['result']['count'] += 1
        rewrite(value, 'unfiltered', future)
    elif fault == 'pdf':
        entry = item['manifest']['future_plan_documents'][0]
        rewrite_raw(value, entry, bodies[entry['raw_path']]+b'changed')
    else:
        raise AssertionError(fault)
    with pytest.raises((ValueError, KeyError)):
        verify(value, mode)


@pytest.mark.parametrize('mode', ('dated', 'unchanged'))
@pytest.mark.parametrize('fault', ('delta', 'multiple_plans', 'before', 'after', 'plan_not_arrived'))
def test_exact_registry_bridge_is_not_a_manifest_assertion(tmp_path, mode, fault):
    data = json.loads((ROOT/'data/share_source_future_plan_reviews.json').read_bytes())
    plan = next(r for r in data['records'] if r['symbol'] == '300535')
    if fault == 'delta': plan['change_total_shares'] += 1
    elif fault == 'multiple_plans': data['records'].append(copy.deepcopy(plan))
    elif fault == 'before': plan['before_total_shares'] += 1
    elif fault == 'after': plan['after_total_shares'] += 1
    else: plan['planned_date'] = '2026-11-13'
    root = tmp_path/'registry'; (root/'data').mkdir(parents=True)
    (root/'data/share_source_future_plan_reviews.json').write_text(json.dumps(data), encoding='utf8')
    with pytest.raises(ValueError):
        verify(case('300535'), mode, root)


def materialize(tmp_path, value):
    root = source._extended(tmp_path)
    item, bodies = value
    directory = root/'.microcap_index_cache/share_sources'
    for name, raw in {**{p:base64.b64decode(v) for p,v in item['prefix_objects_b64'].items()}, **bodies}.items():
        path = directory/name; path.parent.mkdir(parents=True, exist_ok=True); path.write_bytes(raw)
    cache = root/'.microcap_index_cache/share_change'/(item['symbol']+'.csv')
    cache.parent.mkdir(parents=True, exist_ok=True)
    cache.write_bytes(base64.b64decode(item['previous_candidate_b64']))
    (root/'data').mkdir(exist_ok=True)
    (root/'data/share_source_future_plan_reviews.json').write_bytes((ROOT/'data/share_source_future_plan_reviews.json').read_bytes())
    return root, cache, directory


@pytest.mark.parametrize('symbol', SYMBOLS)
def test_real_original_inputs_and_imported_after_source_roundtrip_public_native_chain(tmp_path, monkeypatch, symbol):
    value = case(symbol)
    root, cache, directory = materialize(tmp_path, value)
    old, old_state = cache.read_bytes(), (directory/(symbol+'.json')).read_bytes()
    monkeypatch.setenv('MICROCAP_SHARE_DATED_WINDOW', '1')
    monkeypatch.setenv(native.QUOTE_CORROBORATION_ENV, '1')
    source.refresh_share_change(root=root, symbol=symbol, cache_path=cache, as_of='2026-11-11', allow_network=True)
    result = source.verify_share_cache(root=root, symbol=symbol, as_of='2026-11-11')
    state = json.loads((directory/(symbol+'.json')).read_bytes())
    manifest = json.loads((directory/state['evidence_manifest']['path']).read_bytes())
    assert result['candidate_sha256'] == native.sha(old) and cache.read_bytes() == old
    assert (directory/state['previous_state']['path']).read_bytes() == old_state
    assert manifest['verified_facts']['quote_dated_corroboration']['execution_or_legal_effect_not_asserted']
    # The new observation is in the normal recursive hash-bound closure.
    after = directory/manifest['files']['quote_dated_corroboration']['raw_path']
    after.write_bytes(after.read_bytes()+b'changed')
    with pytest.raises(ValueError):
        source.verify_share_cache(root=root, symbol=symbol, as_of='2026-11-11')


@pytest.mark.parametrize('opt_in', (False, True))
def test_collector_only_supplements_after_source_and_keeps_default_strict(tmp_path, monkeypatch, opt_in):
    value = case('300535')
    root, cache, directory = materialize(tmp_path, value)
    entry = value[0]['manifest']['files']['quote_dated_corroboration']
    for field in ('raw_path', 'receipt_path'):
        (directory/entry[field]).unlink()
    calls = []
    def save(**kwargs):
        calls.append((kwargs['url'], kwargs['params'], kwargs['purpose']))
        assert kwargs['url'] == native.SINA_URL.format(symbol='300535')
        assert kwargs['params'] == {} and kwargs['purpose'] == 'sina_quote_date_2026-11-12'
        result = {}
        for kind in ('raw', 'receipt'):
            field = 'raw_path' if kind == 'raw' else 'receipt_path'
            path = kwargs['directory']/(kwargs['purpose']+('.raw' if kind == 'raw' else '.receipt.json'))
            body = value[1][entry[field]]; path.write_bytes(body)
            result[field] = path.relative_to(kwargs['source']).as_posix()
            result['raw_sha256' if kind == 'raw' else 'receipt_sha256'] = native.sha(body)
        return result, value[1][entry['raw_path']]
    monkeypatch.setattr(native, '_save_public_observation', save)
    if opt_in:
        monkeypatch.setenv(native.QUOTE_CORROBORATION_ENV, '1')
        candidate, _ = source.collect_dated_window(root=root, symbol='300535', previous_day='2026-11-10',
                                                  end='2026-11-11', candidate=cache.read_bytes())
        assert candidate == cache.read_bytes() and len(calls) == 1
    else:
        with pytest.raises(ValueError, match='quote total differs'):
            source.collect_dated_window(root=root, symbol='300535', previous_day='2026-11-10',
                                         end='2026-11-11', candidate=cache.read_bytes())
        assert calls == []


@pytest.mark.parametrize('opt_in', (False, True))
def test_unchanged_collector_uses_the_same_explicit_protocol_without_recollecting_target(tmp_path, monkeypatch, opt_in):
    value = case('300644')
    root, cache, _ = materialize(tmp_path, value)
    files = value[0]['manifest']['files']
    pdf = value[0]['manifest']['future_plan_documents'][0]
    roles = {'unfiltered_1':files['unfiltered'][0], 'bounded_1':files['bounded'][0],
             'pdf_'+pdf['announcement_id']:pdf, 'sina':files['sina'], 'quote':files['quote'],
             'historical_close':files['dated_window'],
             'sina_quote_date_2026-11-12':files['quote_dated_corroboration']}
    calls = []
    def save(**kwargs):
        purpose = kwargs['purpose']; calls.append(purpose)
        entry = roles[purpose]; result = {}
        for field, suffix, digest in (('raw_path','.raw','raw_sha256'),
                                      ('receipt_path','.receipt.json','receipt_sha256')):
            body = value[1][entry[field]]; path = kwargs['directory']/(purpose+suffix)
            path.write_bytes(body)
            result[field] = path.relative_to(kwargs['source']).as_posix()
            result[digest] = native.sha(body)
        return result, value[1][entry['raw_path']]
    monkeypatch.setattr(native, '_save_public_observation', save)
    if opt_in:
        monkeypatch.setenv(native.QUOTE_CORROBORATION_ENV, '1')
        ref = native.collect_manifest(root=root, symbol='300644', previous_day='2026-11-10',
                                      end='2026-11-11', candidate=cache.read_bytes())
        path = root/'.microcap_index_cache/share_sources'/ref['path']
        manifest = json.loads(path.read_bytes())
        assert manifest['quote_dated_corroboration'] == {'schema':native.QUOTE_CORROBORATION_KIND}
        assert calls[-1] == 'sina_quote_date_2026-11-12'
    else:
        with pytest.raises(ValueError, match='quote total differs'):
            native.collect_manifest(root=root, symbol='300644', previous_day='2026-11-10',
                                      end='2026-11-11', candidate=cache.read_bytes())
        assert 'sina_quote_date_2026-11-12' not in calls
    assert len(calls) == len(set(calls))
