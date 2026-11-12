"""Independent typed-quote attacks using frozen real bodies, software receipts.

No market requests occur. Software EM quotes below test only the old ABI;
they are explicitly not authentic EM observations or publication evidence.
"""
import base64
from copy import deepcopy
from datetime import datetime, timezone
import gzip
import json
import os
from pathlib import Path
import re
import sys
from urllib.parse import urlencode

import pytest
import requests

ROOT = Path(os.environ.get('MICROCAP_TYPED_QUOTE_TEST_ROOT', Path(__file__).resolve().parents[1])).resolve()
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / 'tests'))
from scripts import share_change_source as shares
from scripts import share_unchanged_tail as quote
from frozen_exchange_calendar import frozen_exchange_sessions


TARGET = '2026-11-12'
MORNING = datetime.fromisoformat('2026-11-13T01:30:00+00:00')


@pytest.fixture
def case():
    fixture = json.loads(gzip.decompress((ROOT / 'tests/fixtures/share_typed_quote_20261112.json.gz').read_bytes()))
    dated = fixture['dated']['000953']
    bodies = {name: base64.b64decode(value) for name, value in dated['bodies'].items()}
    candidate = base64.b64decode(dated['candidate'])
    files = {}
    for purpose in ('dated_window', 'unfiltered_1', 'bounded_1', 'sina'):
        name = dated['prefix'] + purpose + '.raw'
        entry = dict(raw_path=name, raw_sha256=quote.sha(bodies[name]),
            receipt_path=dated['prefix'] + purpose + '.receipt.json',
            receipt_sha256=quote.sha(bodies[dated['prefix'] + purpose + '.receipt.json']))
        files[{'unfiltered_1': 'unfiltered', 'bounded_1': 'bounded'}.get(purpose, purpose)] = [entry] if purpose.endswith('_1') else entry

    # Test-only transport envelope; original source bodies are unchanged.
    failure_raw = b'Explicit software HTTP 502 fixture'
    bodies['software-main.raw'] = failure_raw
    bodies['software-main.receipt'] = json.dumps(dict(url=quote.QUOTE_URL,
        params=quote.quote_params('000953'), http_status=502, tls_verification=True,
        http_start='2026-11-12T10:39:00+00:00', http_complete='2026-11-12T10:39:01+00:00',
        bytes=len(failure_raw), raw_sha256=quote.sha(failure_raw))).encode()
    spec = lambda name: dict(path=name, sha256=quote.sha(bodies[name]))
    bodies['software-main-ledger.json'] = json.dumps(dict(schema='quote_primary_transport_failure_v1',
        symbol='000953', target=TARGET, attempts=[dict(raw=spec('software-main.raw'),
        receipt=spec('software-main.receipt'))])).encode()
    for provider, key, url in zip(('tencent', 'sina'), ('tencent_quote', 'sina_quote'), quote.typed_quote_urls(fixture['symbols'])):
        raw = base64.b64decode(fixture['quotes'][provider]['raw'])
        original = fixture['quotes'][provider]['receipt']
        name, rn = 'software-' + provider + '.raw', 'software-' + provider + '.receipt'
        bodies[name] = raw
        bodies[rn] = json.dumps(dict(schema='share_unchanged_tail_receipt_v1', url=url, actual_url=url,
            params={}, http_status=200, tls_verification=True, raw_sha256=quote.sha(raw), bytes=len(raw),
            http_start=original['http_start'], http_complete=original['http_complete'])).encode()
        files[key] = dict(raw_path=name, raw_sha256=quote.sha(raw), receipt_path=rn, receipt_sha256=quote.sha(bodies[rn]))
    manifest = dict(schema=shares.DATED_WINDOW_KIND, symbol='000953', previous_day='2026-11-11',
        query_end=TARGET, previous_candidate_sha256=quote.sha(candidate), future_plan_documents=[],
        quote_source=quote.TYPED_QUOTE, quote_symbols=fixture['symbols'],
        quote_primary_failure=spec('software-main-ledger.json'), files=files)
    return dict(manifest=manifest, bodies=bodies, candidate=candidate)


def update(case, key, *, raw=None, receipt=None):
    entry = case['manifest']['files'][key]
    old = json.loads(case['bodies'][entry['receipt_path']])
    if raw is not None:
        case['bodies'][entry['raw_path']] = raw
        entry['raw_sha256'] = quote.sha(raw)
        old.update(raw_sha256=quote.sha(raw), bytes=len(raw))
    if receipt:
        old.update(receipt)
    case['bodies'][entry['receipt_path']] = json.dumps(old).encode()
    entry['receipt_sha256'] = quote.sha(case['bodies'][entry['receipt_path']])


def field(case, key, variable, index, value):
    entry = case['manifest']['files'][key]
    text = case['bodies'][entry['raw_path']].decode('gb18030')
    pattern = re.compile(r'(' + re.escape(variable) + r'=")([^"\r\n]*)(";)')
    separator = '~' if key == 'tencent_quote' else ','
    def replace(match):
        parts = match[2].split(separator)
        parts[index] = value
        return match[1] + separator.join(parts) + match[3]
    text, count = pattern.subn(replace, text)
    assert count == 1
    update(case, key, raw=text.encode('gb18030'))


def morning(case):
    for key in ('tencent_quote', 'sina_quote'):
        update(case, key, receipt=dict(http_start='2026-11-13T01:10:00+00:00',
            http_complete='2026-11-13T01:10:02+00:00'))


def verify(case, *, now=MORNING):
    reads = []
    def bound(name, digest):
        raw = case['bodies'][name]
        assert quote.sha(raw) == digest
        reads.append(name)
        return raw
    candidate, facts = shares.verify_dated_window(case['manifest'], symbol='000953',
        previous_day='2026-11-11', end=TARGET, previous_candidate=case['candidate'],
        bound=bound, root=ROOT, now=now)
    return candidate, facts, reads


@pytest.mark.parametrize('fresh_provider', ['tencent', 'sina'])
def test_next_morning_mixed_frames_and_renamed_issuer_pass_entire_dated_gate(case, frozen_exchange_sessions, fresh_provider):
    morning(case)
    field(case, 'tencent_quote', 'v_sz000953', 1, '河化股份')
    if fresh_provider == 'tencent':
        field(case, 'tencent_quote', 'v_sz000953', 30, '20261113090500')
    else:
        field(case, 'sina_quote', 'hq_str_sz000953', 30, '2026-11-13')
        field(case, 'sina_quote', 'hq_str_sz000953', 31, '09:05:00')
    before = case['candidate']
    candidate, facts, reads = verify(case)
    typed = facts['typed_quote']
    assert candidate == before and facts['old_prefix_preserved'] is True
    assert typed['total_shares'] == '366122195'
    assert typed['source_market_dates'][fresh_provider] == '2026-11-13'
    assert typed['source_market_dates'][{'tencent': 'sina', 'sina': 'tencent'}[fresh_provider]] == TARGET
    assert typed['source_market_dates_agree'] is False and 'market_date' not in typed
    assert typed['source_names_agree'] is False and typed['source_names']['tencent'] == '河化股份'
    assert typed['independent_capital_asof_guarantee_proven'] is False
    assert all(any(purpose in name for name in reads) for purpose in ('dated_window', 'unfiltered_1', 'bounded_1', 'sina.raw'))


@pytest.mark.parametrize('fault', ['before_target_close', 'after_own_receipt', 'later_not_observation_day',
    'naive_receipt', 'future_receipt', 'one_source_stale', 'issuer_conflict', 'secondary_info_not_A'])
def test_bad_per_source_dates_or_identity_fail_with_real_dated_inventory(case, frozen_exchange_sessions, fault):
    morning(case)
    now = MORNING
    if fault == 'before_target_close':
        field(case, 'tencent_quote', 'v_sz000953', 30, '20261112145959')
    elif fault == 'after_own_receipt':
        field(case, 'sina_quote', 'hq_str_sz000953', 30, '2026-11-13')
        field(case, 'sina_quote', 'hq_str_sz000953', 31, '09:10:03')
    elif fault == 'later_not_observation_day':
        field(case, 'tencent_quote', 'v_sz000953', 30, '20261113090500')
        update(case, 'tencent_quote', receipt=dict(http_start='2026-11-14T01:10:00+00:00', http_complete='2026-11-14T01:10:02+00:00'))
        now = datetime.fromisoformat('2026-11-14T01:30:00+00:00')
    elif fault == 'naive_receipt':
        update(case, 'tencent_quote', receipt=dict(http_complete='2026-11-13T01:10:02'))
    elif fault == 'future_receipt':
        update(case, 'sina_quote', receipt=dict(http_complete='2026-11-13T01:30:01+00:00'))
    elif fault == 'one_source_stale':
        field(case, 'sina_quote', 'hq_str_sz000953', 30, '2026-11-11')
    elif fault == 'issuer_conflict':
        field(case, 'tencent_quote', 'v_sz000953', 2, '000545')
    else:
        field(case, 'sina_quote', 'hq_str_sz000953_i', 0, 'I')
    with pytest.raises(ValueError, match='later typed quote is not from observation day' if fault == 'later_not_observation_day' else None):
        verify(case, now=now)


def test_two_matching_wrong_current_totals_cannot_replace_dated_terminal(case, frozen_exchange_sessions):
    morning(case)
    field(case, 'tencent_quote', 'v_sz000953', 30, '20261113090500')
    field(case, 'tencent_quote', 'v_sz000953', 73, '366122196')
    field(case, 'sina_quote', 'hq_str_sz000953_i', 7, '36612.2196')
    with pytest.raises(ValueError, match='dated terminal total'):
        verify(case)


@pytest.mark.parametrize('fault', ['ambiguous_old_quote', 'private_headers', 'primary_200', 'primary_403'])
def test_typed_manifest_cannot_hide_unknown_or_semantic_primary_failure(case, frozen_exchange_sessions, fault):
    if fault == 'ambiguous_old_quote':
        case['manifest']['files']['quote'] = deepcopy(case['manifest']['files']['tencent_quote'])
    elif fault == 'private_headers':
        update(case, 'sina_quote', receipt=dict(request_headers={'Cookie': 'explicit software secret fixture'}))
    else:
        name = 'software-main.receipt'
        receipt = json.loads(case['bodies'][name]);receipt['http_status'] = 200 if fault == 'primary_200' else 403
        case['bodies'][name] = json.dumps(receipt).encode()
        ledger = json.loads(case['bodies']['software-main-ledger.json'])
        ledger['attempts'][0]['receipt']['sha256'] = quote.sha(case['bodies'][name])
        case['bodies']['software-main-ledger.json'] = json.dumps(ledger).encode()
        case['manifest']['quote_primary_failure']['sha256'] = quote.sha(case['bodies']['software-main-ledger.json'])
    with pytest.raises(ValueError):
        verify(case)


def test_plain_original_EM_ABI_remains_valid_with_software_quote_envelope(case, frozen_exchange_sessions):
    manifest = case['manifest']
    for key in ('quote_source', 'quote_symbols', 'quote_primary_failure'):
        manifest.pop(key)
    for key in ('tencent_quote', 'sina_quote'):
        manifest['files'].pop(key)
    raw = json.dumps(dict(rc=0, data=dict(f57='000953', f58='software ABI fixture', f84=366122195,
        f86=int(datetime.fromisoformat('2026-11-12T16:30:00+08:00').timestamp())))).encode()
    params = quote.quote_params('000953')
    receipt = dict(schema='share_unchanged_tail_receipt_v1', url=quote.QUOTE_URL,
        actual_url=quote.QUOTE_URL + '?' + urlencode(params), params=params, http_status=200,
        tls_verification=True, http_start='2026-11-12T10:40:00+00:00', http_complete='2026-11-12T10:40:01+00:00',
        raw_sha256=quote.sha(raw), bytes=len(raw))
    case['bodies']['software-old-EM.raw'] = raw
    case['bodies']['software-old-EM.receipt'] = json.dumps(receipt).encode()
    manifest['files']['quote'] = dict(raw_path='software-old-EM.raw', raw_sha256=quote.sha(raw),
        receipt_path='software-old-EM.receipt', receipt_sha256=quote.sha(case['bodies']['software-old-EM.receipt']))
    candidate, facts, _reads = verify(case)
    assert candidate == case['candidate'] and facts['old_prefix_preserved'] is True
    assert 'typed_quote' not in facts


@pytest.mark.parametrize('fault', ['SSL', '403', '200_semantic_bad'])
def test_real_collector_entry_never_switches_on_SSL_refusal_or_HTTP200(case, tmp_path, monkeypatch, fault):
    monkeypatch.setattr(quote, '_QUOTE_ENDPOINT_FAILURES', {})
    calls = []
    class Response:
        status_code = 403 if fault == '403' else 200
        content = b'Explicit software malformed HTTP200 main quote'
        headers = {}
        url = quote.QUOTE_URL + '?' + urlencode(quote.quote_params('000953'))
        def close(self):
            pass
        def raise_for_status(self):
            if self.status_code == 403:
                raise requests.HTTPError('Explicit software 403')
    def get(url, **_kwargs):
        calls.append(url)
        assert url == quote.QUOTE_URL, 'A semantic or security failure may not select another provider'
        if fault == 'SSL':
            raise requests.exceptions.SSLError('Explicit software TLS failure')
        return Response()
    monkeypatch.setattr(quote.requests, 'get', get)
    folder = tmp_path / 'observation';folder.mkdir()
    if fault in {'SSL', '403'}:
        with pytest.raises((requests.exceptions.SSLError, requests.HTTPError)):
            quote.collect_quote_inputs(directory=folder, source=tmp_path, symbol='000953', end=TARGET)
    else:
        metadata, files = quote.collect_quote_inputs(directory=folder, source=tmp_path, symbol='000953', end=TARGET)
        assert metadata == {} and set(files) == {'quote'}
    assert calls == [quote.QUOTE_URL]
    assert not (tmp_path / 'quote_failover/000953' / TARGET / 'primary_failure.json').exists()


@pytest.fixture
def recovery_transport(case, tmp_path, monkeypatch):
    """Real collector entry, real frozen provider bodies, explicit software HTTP."""
    real_datetime = datetime
    state = dict(now=real_datetime.now(timezone.utc), source=tmp_path, symbol='000953',
        calls=[], outcomes={'tencent': [], 'sina': []})
    class Clock(real_datetime):
        @classmethod
        def now(cls, tz=None):
            current = state['now']
            return current.astimezone(tz) if tz is not None else current.replace(tzinfo=None)
    monkeypatch.setattr(quote, 'datetime', Clock)
    monkeypatch.setattr(quote, '_QUOTE_ENDPOINT_FAILURES', {})
    monkeypatch.setattr(quote.time, 'sleep', lambda *_a: pytest.fail('Recovery tests must not sleep'))
    folder = tmp_path / 'collect-observation';folder.mkdir()
    state['directory'] = folder
    def selected_body(provider, url):
        body = case['bodies'][case['manifest']['files'][provider + '_quote']['raw_path']].decode('gb18030')
        prefix = 'v_' if provider == 'tencent' else 'hq_str_'
        tickers = re.findall(r'(?:sz|sh)[0-9]{6}(?:_i)?', url)
        return '\n'.join(re.findall(r'(?:var\s+)?' + prefix + '(?:' + '|'.join(tickers) + r')="[^"\r\n]*";', body)).encode('gb18030')
    class Response:
        def __init__(self, url, params, raw, status):
            self.url = url + ('?' + urlencode(params) if params else '')
            self.content, self.status_code, self.headers = raw, status, {}
        def close(self):
            pass
        def raise_for_status(self):
            if self.status_code >= 400:
                raise requests.HTTPError('Explicit software ' + str(self.status_code))
    def request(url, *, params, **_kwargs):
        if url == quote.QUOTE_URL:
            provider, outcome = 'main', 502
        else:
            provider = 'tencent' if url.startswith('https://qt.gtimg.cn/') else 'sina'
            outcome = state['outcomes'][provider].pop(0) if state['outcomes'][provider] else 200
        state['calls'].append((provider, outcome))
        if outcome == 'unknown':
            raise KeyboardInterrupt('Explicit software interruption before response/receipt')
        if outcome == 'transport':
            raise requests.ConnectionError('Explicit software no-response failure')
        if outcome == 'SSL':
            raise requests.exceptions.SSLError('Explicit software TLS refusal')
        raw = selected_body(provider, url) if outcome == 200 else b'Explicit software failed response'
        if outcome == 'semantic':
            outcome, raw = 200, b'v_sz000953="Explicit software incomplete fields";'
        return Response(url, params, raw, outcome)
    monkeypatch.setattr(quote.requests, 'get', request)
    def collect():
        return quote.collect_quote_inputs(directory=state['directory'], source=tmp_path, symbol=state['symbol'], end=TARGET)
    def bound(name, digest):
        raw = (tmp_path / name).read_bytes();assert quote.sha(raw) == digest;return raw
    def verified():
        metadata, files = collect()
        return quote.verify_typed_quote(dict(metadata, files=files), symbol=state['symbol'], end=TARGET,
            bound=bound, now=state['now'])
    def advance():
        state['now'] += __import__('datetime').timedelta(minutes=31)
    def original_snapshot():
        return {path: path.read_bytes() for path in tmp_path.rglob('*') if path.is_file()
            and ('initial' in path.parts or path.name.startswith('quote_attempt')
                or path.name in ('primary_started.json', 'primary_failure.json'))}
    return state, collect, verified, advance, original_snapshot


def test_successful_first_provider_survives_second_failure_and_one_cooled_recovery(recovery_transport):
    state, collect, verified, advance, snapshot = recovery_transport
    state['outcomes']['sina'] = [502, 200]
    with pytest.raises(quote.QuoteRecoveryPending):
        collect()
    original = snapshot();assert original
    calls = list(state['calls'])
    with pytest.raises(quote.QuoteRecoveryPending):
        collect()
    assert state['calls'] == calls
    advance()
    facts = verified()
    assert facts['total_shares'] == '366122195'
    assert [provider for provider, _outcome in state['calls']].count('main') == 1
    assert [provider for provider, _outcome in state['calls']].count('tencent') == 1
    assert [provider for provider, _outcome in state['calls']].count('sina') == 2
    metadata, files = collect()
    assert '/recovery/' in files['sina_quote']['raw_path']
    assert all(path.read_bytes() == raw for path, raw in original.items())
    calls = list(state['calls']);verified()
    assert state['calls'] == calls and metadata['quote_source'] == quote.TYPED_QUOTE


def test_transport_then_failed_recovery_cannot_get_a_third_attempt(recovery_transport):
    state, collect, _verified, advance, snapshot = recovery_transport
    state['outcomes']['tencent'] = ['transport', 502, 200]
    with pytest.raises(quote.QuoteRecoveryPending):
        collect()
    original = snapshot();advance()
    with pytest.raises(quote.QuoteRecoveryExhausted):
        collect()
    calls = list(state['calls']);advance()
    with pytest.raises(quote.QuoteRecoveryExhausted):
        collect()
    assert state['calls'] == calls
    assert [provider for provider, _outcome in calls].count('tencent') == 2
    assert all(path.read_bytes() == raw for path, raw in original.items())


def test_unknown_initial_outcome_cools_before_one_recovery_and_preserves_intent(recovery_transport):
    state, collect, verified, advance, snapshot = recovery_transport
    state['outcomes']['tencent'] = ['unknown', 200]
    with pytest.raises((KeyboardInterrupt, quote.QuoteRecoveryPending)):
        collect()
    original = snapshot();assert any(path.name == 'intent.json' for path in original)
    calls = list(state['calls'])
    with pytest.raises(quote.QuoteRecoveryPending):
        collect()
    assert state['calls'] == calls
    advance();assert verified()['total_shares'] == '366122195'
    assert [provider for provider, _outcome in state['calls']].count('tencent') == 2
    assert all(path.read_bytes() == raw for path, raw in original.items())


def test_unknown_recovery_outcome_exhausts_budget_without_a_third_request(recovery_transport):
    state, collect, _verified, advance, snapshot = recovery_transport
    state['outcomes']['tencent'] = [502, 'unknown', 200]
    with pytest.raises(quote.QuoteRecoveryPending):
        collect()
    original = snapshot();advance()
    with pytest.raises((KeyboardInterrupt, quote.QuoteRecoveryExhausted)):
        collect()
    calls = list(state['calls']);advance()
    with pytest.raises(quote.QuoteRecoveryExhausted):
        collect()
    assert state['calls'] == calls and all(path.read_bytes() == raw for path, raw in original.items())


@pytest.mark.parametrize('terminal', [403, 'SSL', 'semantic'])
def test_refusal_SSL_and_HTTP200_semantics_are_terminal_across_restarts(recovery_transport, terminal):
    state, _collect, verified, advance, snapshot = recovery_transport
    state['outcomes']['tencent'] = [terminal, 200]
    with pytest.raises(Exception):
        verified()
    calls, original = list(state['calls']), snapshot()
    advance()
    with pytest.raises(Exception):
        verified()
    assert state['calls'] == calls
    assert all(path.read_bytes() == raw for path, raw in original.items())


def test_old_successful_dual_observations_are_reused_without_new_collection(case, tmp_path, monkeypatch):
    folder = tmp_path / 'old-success';folder.mkdir()
    urls = quote.typed_quote_urls(case['manifest']['quote_symbols'])
    for key in ('tencent_quote', 'sina_quote'):
        entry = case['manifest']['files'][key]
        (folder / (key + '.raw')).write_bytes(case['bodies'][entry['raw_path']])
        (folder / (key + '.receipt.json')).write_bytes(case['bodies'][entry['receipt_path']])
    original = {path: path.read_bytes() for path in folder.iterdir()}
    monkeypatch.setattr(quote.requests, 'get', lambda *_a, **_k: pytest.fail('Original successful provider must not be recollected'))
    files = quote.collect_dual_quote(directory=folder, source=tmp_path,
        symbols=case['manifest']['quote_symbols'], end=TARGET)
    def bound(name, digest):
        raw = (tmp_path / name).read_bytes();assert quote.sha(raw) == digest;return raw
    for key, url in zip(('tencent_quote', 'sina_quote'), urls):
        quote._artifact(files[key], bound, url=url, params={}, end=TARGET, now=datetime.now(timezone.utc))
    assert all(path.read_bytes() == raw for path, raw in original.items())


@pytest.mark.parametrize('fault', ['selected_role_alias', 'late_initial_intent'])
def test_attempt_proof_cannot_mix_a_selected_role_or_move_its_clock(recovery_transport, fault):
    state, collect, _verified, advance, _snapshot = recovery_transport
    if fault == 'late_initial_intent':
        state['outcomes']['tencent'] = [502, 200]
        with pytest.raises(quote.QuoteRecoveryPending):
            collect()
        advance()
    metadata, files = collect()
    files = deepcopy(files)
    entry = files['tencent_quote'];spec = entry['attempt_budget']
    root = state['source'];budget = json.loads((root / spec['path']).read_bytes())
    prefix = spec['path'].rsplit('/', 1)[0] + '/'
    changed = {}
    if fault == 'selected_role_alias':
        for role, suffix in (('raw', '.raw'), ('receipt', '.receipt.json')):
            old_path = entry[role + '_path']
            alias = prefix + 'unselected-observation' + suffix
            changed[alias] = (root / old_path).read_bytes()
            entry[role + '_path'] = alias
            budget['success'][role + '_path'] = alias
            budget['files'][alias] = quote.sha(changed[alias])
    else:
        name = prefix + 'initial/intent.json'
        intent = json.loads((root / name).read_bytes())
        # Still <= legitimate verifier now, but after the original failed HTTP.
        intent['created_at'] = state['now'].isoformat()
        changed[name] = json.dumps(intent).encode()
        budget['files'][name] = quote.sha(changed[name])
    changed[spec['path']] = json.dumps(budget).encode()
    spec['sha256'] = quote.sha(changed[spec['path']])
    def bound(name, digest):
        raw = changed[name] if name in changed else (root / name).read_bytes()
        assert quote.sha(raw) == digest
        return raw
    with pytest.raises(ValueError):
        quote.verify_typed_quote(dict(metadata, files=files), symbol='000953', end=TARGET,
            bound=bound, now=state['now'])


def test_endpoint_outage_skip_still_allows_one_cooled_backup_recovery(recovery_transport):
    state, collect, verified, advance, snapshot = recovery_transport
    state['outcomes']['sina'] = [200, 502, 200]
    assert verified()['total_shares'] == '366122195'
    state['symbol'] = '000545'
    state['directory'] = state['source'] / 'second-issuer-observation'
    state['directory'].mkdir()
    with pytest.raises(quote.QuoteRecoveryPending):
        collect()
    original = snapshot();advance()
    assert verified()['total_shares'] == '986833096'
    # The second issuer keeps the first issuer's exact outage identity. A real
    # 30-minute recovery must not be permanently rejected by its old 90s bound.
    assert [provider for provider, _outcome in state['calls']].count('main') == 1
    assert [provider for provider, _outcome in state['calls']].count('tencent') == 2
    assert [provider for provider, _outcome in state['calls']].count('sina') == 3
    assert all(path.read_bytes() == raw for path, raw in original.items())
