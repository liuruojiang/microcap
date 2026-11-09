"""Portable offline transport counterexamples; no prices or mail are fetched."""
from collections import Counter
from datetime import datetime
import importlib.util
import json
from pathlib import Path

import pandas as pd
import pytest
import requests

from scripts import collect_member_daily_inputs as collect
from scripts import stock_daily_alternative as alternative
from scripts import index_history_preflight as index_history
from scripts import member_daily_signal as daily
from scripts.member_period_handoff import verify as verify_checkpoint

ROOT = Path(__file__).resolve().parents[1]
NOW = datetime.fromisoformat('2026-11-09T16:00:00+08:00')


class FrozenClock(datetime):
    @classmethod
    def now(cls, tz=None):
        return NOW.astimezone(tz) if tz else NOW.replace(tzinfo=None)


@pytest.fixture(autouse=True)
def isolated_clock_and_backoff(monkeypatch):
    monkeypatch.setattr(collect, 'datetime', FrozenClock)
    monkeypatch.setattr(alternative, 'datetime', FrozenClock)
    monkeypatch.setattr(collect, 'sleep', lambda seconds: None)
    monkeypatch.setattr(alternative.time, 'sleep', lambda seconds: None)


def http_response(url, status, body):
    response = requests.Response()
    response.status_code, response.url, response._content = status, url, body
    return response


def install_transport(monkeypatch, *, factor_failure=None, price_failure=None,
                      always_fail=False, factor_mismatch=False):
    calls = Counter()
    urls = []
    def get(url, params=None, timeout=None):
        urls.append(url)
        if url == alternative.PRICE_URL:
            calls['price'] += 1
            if price_failure and (always_fail or calls['price'] == 1):
                if price_failure == 'timeout': raise requests.Timeout('offline raw timeout')
                return http_response(url, price_failure, b'offline raw gateway failure')
            code = params['param'].split(',')[0]
            body = json.dumps({'code': 0, 'data': {code: {'day': [
                ['2026-11-09', '10', '10.1', '10.2', '9.9', '100']]}}}).encode()
            return http_response(url, 200, body)
        kind = url.rsplit('/', 1)[-1].split('.')[0]
        calls[kind] += 1
        if kind == 'hfq' and factor_failure and (always_fail or calls[kind] == 1):
            if factor_failure == 'timeout': raise requests.Timeout('offline factor timeout')
            return http_response(url, factor_failure, b'offline factor gateway failure')
        market = url.split('/company/', 1)[1].split('/')[0]
        factor = 2.0 if factor_mismatch and kind == 'hfq' else 1.0
        body = ('var ' + market + kind + '=' + json.dumps({'total': 2, 'data': [
            {'d': '2026-11-06', 'f': 1.0}, {'d': '2026-11-09', 'f': factor}]})).encode()
        return http_response(url, 200, body)
    monkeypatch.setattr(requests, 'get', get)
    def hedge(start, end):
        frame = pd.DataFrame({'date': pd.to_datetime(['2026-11-09']), 'close': [6500.0]})
        frame.attrs.update(independent_history_source='offline adversarial fixture', independent_history_attempts=[])
        return frame
    monkeypatch.setattr(index_history, 'fetch_delivery_history', hedge)
    return calls, urls


def collect_tail(tmp_path, symbols=('600001',)):
    return collect._collect_close_tail(cp={'as_of_date': '2026-11-06'},
        members=[{'symbol': symbol} for symbol in symbols],
        price_rows={symbol: {'previous_raw_date': '2026-11-06', 'previous_raw_close': 10.0} for symbol in symbols},
        days=['2026-11-09'], out=tmp_path, workers=1)


@pytest.mark.parametrize('failure', [504, 'timeout'])
def test_transient_factor_recovers_without_refetching_successful_price_or_qfq(tmp_path, monkeypatch, failure):
    calls, _ = install_transport(monkeypatch, factor_failure=failure)
    value = collect_tail(tmp_path)
    assert len(value['rows']) == 1 and not value['errors']
    assert calls == Counter(price=1, qfq=1, hfq=2)
    folder = tmp_path/'factor_sources/600001'
    first = json.loads((folder/'hfq.attempt-1.json').read_bytes())
    assert (first.get('status') == 504) if failure == 504 else (first['error_type'] == 'Timeout')
    assert json.loads((folder/'hfq.json').read_bytes())['status'] == 200


@pytest.mark.parametrize('failure', [504, 'timeout'])
def test_factor_retry_exhaustion_is_three_gets_and_keeps_successes(tmp_path, monkeypatch, failure):
    calls, _ = install_transport(monkeypatch, factor_failure=failure, always_fail=True)
    value = collect_tail(tmp_path)
    assert not value['rows'] and '600001' in value['errors']
    assert calls == Counter(price=1, qfq=1, hfq=3)
    folder = tmp_path/'factor_sources/600001'
    assert all((folder/f'hfq.attempt-{i}.json').is_file() for i in (1, 2, 3))
    assert (folder/'qfq.js').is_file() and (folder/'daily_raw.json').is_file()


@pytest.mark.parametrize('status', [502, 503, 504])
def test_transient_price_http_failure_recovers_only_the_failed_get(monkeypatch, status):
    calls, _ = install_transport(monkeypatch, price_failure=status)
    value = alternative.query('600001', '2026-11-09', '2026-11-09', ['2026-11-09'], 10.0)
    alternative.verify(value)
    assert value['rows'][0][2] == '10.1' and calls == Counter(price=2)


@pytest.mark.parametrize('failure', [504, 'timeout'])
def test_price_retry_exhaustion_is_three_gets_within_original_budget(monkeypatch, failure):
    calls, _ = install_transport(monkeypatch, price_failure=failure, always_fail=True)
    with pytest.raises((requests.HTTPError, requests.Timeout)):
        alternative.query('600001', '2026-11-09', '2026-11-09', ['2026-11-09'], 10.0)
    assert calls == Counter(price=3)


@pytest.mark.parametrize('status', [403, 429])
def test_factor_access_rejection_stops_factor_gets_for_queued_members(tmp_path, monkeypatch, status):
    calls, urls = install_transport(monkeypatch, factor_failure=status)
    value = collect_tail(tmp_path, ('600001', '600002'))
    assert not value['rows'] and set(value['errors']) == {'600001', '600002'}
    assert str(status) in value['errors']['600001']
    assert 'stopped' in value['errors']['600002']
    assert calls == Counter(price=2, qfq=1, hfq=1)
    assert not any('/sh600002/' in url for url in urls)


@pytest.mark.parametrize('status', [403, 429])
def test_raw_access_rejection_remains_hard_and_unretried(monkeypatch, status):
    calls, _ = install_transport(monkeypatch, price_failure=status)
    with pytest.raises(requests.HTTPError):
        alternative.query('600001', '2026-11-09', '2026-11-09', ['2026-11-09'], 10.0)
    assert calls == Counter(price=1)


def test_adjustment_disagreement_is_not_retried_as_network_failure(tmp_path, monkeypatch):
    calls, _ = install_transport(monkeypatch, factor_mismatch=True)
    value = collect_tail(tmp_path)
    assert not value['rows'] and 'disagreement' in value['errors']['600001']
    assert calls == Counter(price=1, qfq=1, hfq=1)


def test_intraday_cutoff_is_rechecked_before_factor_retry(tmp_path, monkeypatch):
    import threading
    calls, _ = install_transport(monkeypatch, factor_failure=504)
    class BeforeClose(FrozenClock):
        @classmethod
        def now(cls, tz=None): return datetime.fromisoformat('2026-11-09T14:59:59+08:00')
    class AtClose(FrozenClock):
        @classmethod
        def now(cls, tz=None): return datetime.fromisoformat('2026-11-09T15:00:00+08:00')
    monkeypatch.setattr(collect, 'datetime', BeforeClose)
    monkeypatch.setattr(collect, 'sleep', lambda seconds: monkeypatch.setattr(collect, 'datetime', AtClose))
    with pytest.raises(ValueError, match='cutoff'):
        collect.collect_factor('https://finance.sina.com.cn/realstock/company/sh600001/hfq.js',
                               tmp_path, 'hfq', threading.Event(), 'intraday')
    assert calls == Counter(hfq=1)


_spec = importlib.util.spec_from_file_location('_transport_native_fixture', Path(__file__).with_name('test_member_daily_signal.py'))
_fixture = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_fixture)
bundle = _fixture.bundle


def test_two_synthetic_closes_keep_history_and_pending_rebalance_reference(bundle, tmp_path):
    seed, _, quotes, hedge, first, sessions = bundle
    original = (seed/'checkpoint.json').read_bytes()
    first_result = daily.build(ROOT, seed/'checkpoint.json', quotes, hedge, first, 'close_confirmed', tmp_path/'first',
        now=datetime.fromisoformat(first+'T16:01:00+08:00'), calendar_sessions=sessions)
    assert first_result['status'] == 'OK'
    previous = tmp_path/'first'; cp = verify_checkpoint(previous/'checkpoint.json')
    second = cp['next_trade_date']
    prices = json.loads((previous/'member_prices.json').read_bytes())['rows']
    next_quotes = dict(market_date=second, collected_at=second+'T16:00:00+08:00', close_confirmed=True,
        rows=[dict(symbol=r['symbol'], trade_date=second, previous_date=first, previous_raw_close=r['previous_raw_close'],
            raw_close=r['previous_raw_close'], previous_close_adj=r['previous_raw_close'], close_adj=r['previous_raw_close'],
            return_basis_revision=daily.RETURN_BASIS, source='offline synthetic zero-return fixture') for r in prices])
    next_hedge = dict(market_date=second, collected_at=next_quotes['collected_at'], close_confirmed=True,
                     close=hedge['close'], source='offline synthetic zero-return fixture')
    second_result = daily.build(ROOT, previous/'checkpoint.json', next_quotes, next_hedge, second, 'close_confirmed', tmp_path/'second',
        now=datetime.fromisoformat(second+'T16:01:00+08:00'), calendar_sessions=sessions)
    # The existing synthetic calendar's second close is an original rebalance
    # session. Computation may continue, but an unhanded-off old basket must
    # remain a reference rather than being promoted to the new-period target.
    assert second_result['status'] == 'PARTIAL' and second_result['period_status'] == 'overdue_reference'
    assert all(v['status'] == 'reference' and v['signal'] for v in second_result['versions'])
    assert (tmp_path/'second/history.csv').read_bytes().startswith((previous/'history.csv').read_bytes())
    assert (seed/'checkpoint.json').read_bytes() == original
    candidate = verify_checkpoint(tmp_path/'second/checkpoint.json')
    assert candidate['as_of_date'] == second and candidate['period_status'] == 'awaiting_handoff'
