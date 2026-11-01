import hashlib
import json
import sys
from types import SimpleNamespace

import pandas as pd
import pytest

import scripts.stock_daily_inputs as daily


def test_missing_installed_seed_blocks_before_calendar_or_network(tmp_path, monkeypatch):
    cache = tmp_path / '.microcap_index_cache'
    cache.mkdir()
    (cache / 'security_master.csv').write_text('symbol,list_date,delist_date\n000001,1991-04-03,\n')
    def unexpected(*args, **kwargs):
        raise AssertionError('Incomplete seed must fail before source queries')
    monkeypatch.setattr(daily, 'sessions_for_day', unexpected)
    with pytest.raises(ValueError, match='installed input seed is incomplete'):
        daily.refresh_daily_inputs(root=tmp_path, symbols=['000001'], as_of='2026-10-30')
    assert not (cache / 'trade_status').exists()


def prepared(tmp_path, monkeypatch, mode='normal'):
    dates = pd.bdate_range('2026-10-16', periods=7)
    cache = tmp_path / '.microcap_index_cache'; base = cache / 'trade_status'; base.mkdir(parents=True)
    raw = cache / 'prices_raw/000001.csv'; raw.parent.mkdir()
    pd.DataFrame({'date': dates[:6], 'close_raw': [10.] * 6}).to_csv(raw, index=False)
    rows = [[d.strftime('%Y-%m-%d'), 'sz.000001', '10', '10', '100', '1', '0', '3'] for d in dates]
    query = {'request': {'code': 'sz.000001', 'fields': daily.FIELDS, 'start': str(dates[0].date()), 'end': str(dates[5].date()), 'adjustflag': '3', 'frequency': 'd'}, 'fields': daily.FIELDS.split(','), 'rows': rows[:6], 'error_code': '0', 'error_msg': ''}
    source = base / 'old.json'; source.write_text(json.dumps(query))
    sidecar = base / '000001.json'; sidecar.write_text(json.dumps({'schema_version': 1, 'symbol': '000001', 'raw_price_sha256': hashlib.sha256(raw.read_bytes()).hexdigest(), 'lifecycle': {'list_date': '1991-04-03', 'delist_date': ''}, 'provider_queries': [{'path': 'old.json', 'sha256': hashlib.sha256(source.read_bytes()).hexdigest()}]}))
    pd.DataFrame([{'symbol': '000001', 'list_date': '1991-04-03', 'delist_date': None}]).to_csv(cache / 'security_master.csv', index=False)
    monkeypatch.setattr(daily, 'sessions_for_day', lambda _: tuple(dates))
    if mode == 'missing': rows.pop()
    if mode == 'rewrite': rows[0][2] = '11'
    if mode == 'bad_status': rows[-1][5] = '0'
    if mode == 'suspended': rows[-1][4:6] = ['', '0']
    if mode == 'identity': rows[-1][1] = 'sz.000002'
    if mode == 'too_few_overlap': rows = rows[-5:]
    counts = {'login': 0, 'query': 0, 'logout': 0}
    class Result:
        error_code = '0'; error_msg = ''; fields = daily.FIELDS.split(',')
        def __init__(self): self.i = -1
        def next(self): self.i += 1; return self.i < len(rows)
        def get_row_data(self): return rows[self.i]
    def login(): counts['login'] += 1; return SimpleNamespace(error_code='0')
    def query(*a, **kw): counts['query'] += 1; return Result()
    def logout(): counts['logout'] += 1
    monkeypatch.setitem(sys.modules, 'baostock', SimpleNamespace(login=login, query_history_k_data_plus=query, logout=logout))
    return dates, raw, sidecar, counts


@pytest.mark.parametrize('mode', ['normal', 'suspended'])
def test_actual_adapter_updates_pair_and_reuses_without_network(tmp_path, monkeypatch, mode):
    dates, raw, sidecar, counts = prepared(tmp_path, monkeypatch, mode)
    before = pd.read_csv(raw)
    r = daily.refresh_daily_inputs(root=tmp_path, symbols=['000001'], as_of=dates[-1])[0]
    assert r['added_trade_rows'] == (mode == 'normal')
    pd.testing.assert_frame_equal(pd.read_csv(raw).iloc[:6].reset_index(drop=True), before)
    assert len(json.loads(sidecar.read_bytes())['provider_queries']) == 2
    again = daily.refresh_daily_inputs(root=tmp_path, symbols=['000001'], as_of=dates[-1])[0]
    assert again['status'] == 'existing_evidence_verified'
    assert counts == {'login': 1, 'query': 1, 'logout': 1}


@pytest.mark.parametrize('mode', ['missing', 'rewrite', 'bad_status', 'identity', 'too_few_overlap'])
def test_rejected_source_does_not_change_installed_pair(tmp_path, monkeypatch, mode):
    dates, raw, sidecar, counts = prepared(tmp_path, monkeypatch, mode)
    before = raw.read_bytes(), sidecar.read_bytes()
    with pytest.raises(ValueError): daily.refresh_daily_inputs(root=tmp_path, symbols=['000001'], as_of=dates[-1])
    assert (raw.read_bytes(), sidecar.read_bytes()) == before
    assert counts == {'login': 1, 'query': 1, 'logout': 1}
    assert not (sidecar.parent / 'DAILY_PROVIDER.lock').exists()


def test_publication_failure_restores_both_old_files(tmp_path, monkeypatch):
    dates, raw, sidecar, _ = prepared(tmp_path, monkeypatch)
    before = raw.read_bytes(), sidecar.read_bytes(); original_write = daily._write; failed = []
    def fail_once(path, payload):
        if str(path).removeprefix('\\\\?\\') == str(sidecar) and not failed:
            failed.append(True); raise OSError('injected publish interruption')
        original_write(path, payload)
    monkeypatch.setattr(daily, '_write', fail_once)
    with pytest.raises(OSError): daily.refresh_daily_inputs(root=tmp_path, symbols=['000001'], as_of=dates[-1])
    assert (raw.read_bytes(), sidecar.read_bytes()) == before


def test_existing_writer_is_not_stolen(tmp_path, monkeypatch):
    dates, raw, sidecar, counts = prepared(tmp_path, monkeypatch)
    lock = sidecar.parent / 'DAILY_PROVIDER.lock'; lock.write_text('other writer')
    with pytest.raises(FileExistsError): daily.refresh_daily_inputs(root=tmp_path, symbols=['000001'], as_of=dates[-1])
    assert lock.read_text() == 'other writer' and counts['login'] == 0


def test_master_change_during_query_cannot_publish_a_stale_pair(tmp_path, monkeypatch):
    dates, raw, sidecar, _ = prepared(tmp_path, monkeypatch)
    before = raw.read_bytes(), sidecar.read_bytes()
    provider = sys.modules['baostock']; original_query = provider.query_history_k_data_plus
    master = tmp_path / '.microcap_index_cache/security_master.csv'
    def changed(*a, **kw):
        master.write_bytes(master.read_bytes() + b'\n')
        return original_query(*a, **kw)
    monkeypatch.setattr(provider, 'query_history_k_data_plus', changed)
    with pytest.raises(ValueError, match='changed'):
        daily.refresh_daily_inputs(root=tmp_path, symbols=['000001'], as_of=dates[-1])
    assert (raw.read_bytes(), sidecar.read_bytes()) == before
