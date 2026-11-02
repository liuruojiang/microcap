"""Synthetic transport faults exercise the actual raw/status atomic publisher."""
import copy
import hashlib
import json
from types import SimpleNamespace

import pandas as pd
import pytest

from scripts import stock_daily_alternative as alt, stock_daily_inputs as daily
from test_stock_daily_inputs_20261030 import prepared


def install_source(monkeypatch, dates, *, suspended=False, fault=None):
    days = [str(d.date()) for d in dates]
    rows = [[d, '10', '10', '10', '10', '100'] for d in days]
    if suspended:
        rows.pop()
    if fault == 'rewrite':
        rows[0][2] = '11'
    if fault == 'missing':
        rows.pop()
    if fault == 'duplicate':
        rows.append(rows[-1])
    record = dict(SECURITY_CODE='000001', SECUCODE='000001.SZ',
                  SUSPEND_START_TIME=days[-1]+' 09:30:00', SUSPEND_END_TIME=None,
                  PREDICT_RESUME_DATE=None)
    if fault == 'resumed':
        record['PREDICT_RESUME_DATE'] = days[-1]+' 00:00:00'
    if fault == 'partial':
        record['SUSPEND_START_TIME'] = days[-1]+' 13:00:00'
    if fault == 'other_issuer':
        record['SECURITY_CODE'] = '000002'
    def get(url, params, timeout):
        assert 0 < timeout[0] <= 5 and 0 < timeout[1] <= 10
        if url == alt.PRICE_URL:
            data = {'code': 0, 'data': {'sz000001': {'day': rows}}}
        else:
            assert url == alt.SUSPENSION_URL
            records = [record] if suspended else []
            data = {'success': True, 'result': {'pages': 1, 'count': len(records), 'data': records}}
        return SimpleNamespace(content=json.dumps(data).encode(), status_code=200, raise_for_status=lambda: None)
    monkeypatch.setattr(alt.requests, 'get', get)
    monkeypatch.setenv('MICROCAP_DAILY_STOCK_PROVIDER', 'tencent')


@pytest.mark.parametrize('suspended', [False, True])
def test_native_alternative_updates_real_pair_and_reuses_offline(tmp_path, monkeypatch, suspended):
    dates, raw, sidecar, counts = prepared(tmp_path, monkeypatch)
    before = pd.read_csv(raw)
    install_source(monkeypatch, dates, suspended=suspended)
    result = daily.refresh_daily_inputs(root=tmp_path, symbols=['000001'], as_of=dates[-1])[0]
    assert result['added_trade_rows'] == (not suspended)
    pd.testing.assert_frame_equal(pd.read_csv(raw).iloc[:6].reset_index(drop=True), before)
    assert counts == {'login': 0, 'query': 0, 'logout': 0}
    monkeypatch.setattr(alt.requests, 'get', lambda *a, **kw: pytest.fail('offline reuse made a network request'))
    assert daily.refresh_daily_inputs(root=tmp_path, symbols=['000001'], as_of=dates[-1], allow_network=False)[0]['status'] == 'existing_evidence_verified'


@pytest.mark.parametrize('fault,suspended', [('rewrite', False), ('missing', False), ('duplicate', False),
                                            ('resumed', True), ('partial', True), ('other_issuer', True)])
def test_alternative_conflict_or_missing_status_never_publishes(tmp_path, monkeypatch, fault, suspended):
    dates, raw, sidecar, counts = prepared(tmp_path, monkeypatch)
    before = raw.read_bytes(), sidecar.read_bytes()
    install_source(monkeypatch, dates, suspended=suspended, fault=fault)
    with pytest.raises(ValueError):
        daily.refresh_daily_inputs(root=tmp_path, symbols=['000001'], as_of=dates[-1])
    assert (raw.read_bytes(), sidecar.read_bytes()) == before
    assert counts['login'] == 0


@pytest.mark.parametrize('fault', ['row', 'body', 'source', 'request'])
def test_normalized_rows_must_reproduce_the_bound_native_response(monkeypatch, fault):
    dates = pd.bdate_range('2026-10-16', periods=7)
    install_source(monkeypatch, dates)
    payload = alt.query('000001', str(dates[0].date()), str(dates[-1].date()),
                        [str(d.date()) for d in dates], 10)
    alt.verify(payload)
    broken = copy.deepcopy(payload)
    if fault == 'row': broken['rows'][-1][2] = '12'
    if fault == 'body': broken['native_evidence']['prices']['body'] += ' '
    if fault == 'source': broken['native_evidence']['prices']['url'] = 'https://example.com'
    if fault == 'request': broken['request']['end'] = '2026-10-27'
    with pytest.raises(ValueError): alt.verify(broken)


def test_failed_parallel_group_joins_active_writer_before_returning():
    import threading
    entered = threading.Event()
    failed = threading.Event()
    finished = threading.Event()
    def task(symbol):
        if symbol == 'bad':
            assert entered.wait(2)
            failed.set()
            raise ValueError('source failed')
        entered.set()
        assert failed.wait(2)
        __import__('time').sleep(0.05)
        finished.set()
        return symbol
    with pytest.raises(ValueError, match='source failed'):
        daily._parallel_alternative(['bad', 'active'], task)
    assert finished.is_set()
