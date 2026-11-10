import hashlib
import json
from pathlib import Path

import pandas as pd
import pytest

from scripts.tradeability_evidence import enrich_price_with_trade_status
import microcap_top100_mom16_biweekly_live_v2_0 as v
import analyze_top100_rebalance_frequency as standalone


def fixture_bundle(tmp_path):
    dates = pd.bdate_range('2026-10-26', periods=3)
    price = pd.DataFrame({'date': dates[:1], 'close_raw': [10.]})
    raw_path = tmp_path / 'raw.csv'; price.to_csv(raw_path, index=False)
    base = tmp_path / '.microcap_index_cache/trade_status'; base.mkdir(parents=True)
    query = {'request': {'code': 'sz.000001', 'adjustflag': '3', 'frequency': 'd', 'start': '2026-10-26', 'end': '2026-10-28'}, 'error_code': '0', 'fields': ['date', 'code', 'close', 'volume', 'tradestatus', 'adjustflag'], 'rows': [['2026-10-26', 'sz.000001', '10', '100', '1', '3'], ['2026-10-27', 'sz.000001', '10', '', '0', '3'], ['2026-10-28', 'sz.000001', '10', '', '0', '3']]}
    q = base / 'query.json'; q.write_text(json.dumps(query))
    record = {'schema_version': 1, 'symbol': '000001', 'raw_price_sha256': hashlib.sha256(raw_path.read_bytes()).hexdigest(), 'lifecycle': {'list_date': '1991-04-03', 'delist_date': ''}, 'provider_queries': [{'path': 'query.json', 'sha256': hashlib.sha256(q.read_bytes()).hexdigest()}]}
    sidecar = base / '000001.json'; sidecar.write_text(json.dumps(record))
    master = pd.DataFrame([{'symbol': '000001', 'list_date': '1991-04-03', 'delist_date': ''}])
    return dict(root=tmp_path, symbol='000001', raw_path=raw_path, price=price, master=master, trading_dates=dates), sidecar, q, record, query


def test_short_window_retains_actual_anchor_for_a_suspended_bridge(tmp_path):
    args,*_=fixture_bundle(tmp_path)
    days=args['trading_dates'][1:]
    args['price']=v.base_mod.freq_mod.price_window_with_valuation_anchor(args['price'],days.min(),days.max())
    args['trading_dates']=days
    frame=enrich_price_with_trade_status(**args,require_evidence=True)
    assert frame.loc[frame.date.isin(days),'close_raw'].tolist()==[10.0,10.0]
    assert frame.loc[frame.date.isin(days),'tradestatus'].tolist()==[0,0]
    assert len(args['price'])==1 and args['price'].date.iloc[0]<days.min()


def test_short_window_never_invents_a_missing_initial_valuation(tmp_path):
    args,*_=fixture_bundle(tmp_path)
    days=args['trading_dates'][1:]
    args['price']=args['price'].iloc[:0]
    args['trading_dates']=days
    args['price']=v.base_mod.freq_mod.price_window_with_valuation_anchor(args['price'],days.min(),days.max())
    with pytest.raises(ValueError,match='no previous observed valuation'):
        enrich_price_with_trade_status(**args,require_evidence=True)


@pytest.mark.parametrize('module', [v.base_mod.freq_mod, standalone])
def test_real_tradeability_keeps_suspension_tail_valued_but_not_tradeable(tmp_path, monkeypatch, module):
    args, *_ = fixture_bundle(tmp_path)
    enriched = enrich_price_with_trade_status(**args)
    monkeypatch.setattr(module, 'read_ohlc_cache', lambda *a: pd.DataFrame())
    adjusted = pd.DataFrame({'date': args['trading_dates'][:1], 'close_adj': [100.]})
    from scripts.stock_return_basis import RETURN_BASIS_REVISION
    adjusted.attrs['return_basis_manifest'] = {'return_basis_revision': RETURN_BASIS_REVISION, 'symbol': '000001', 'raw_sha256': 'a'*64, 'qfq_sha256': 'b'*64, 'hfq_sha256': 'c'*64}
    returns, traded, buy, sell = module.build_tradeability_series('000001', enriched, args['trading_dates'], return_price=adjusted)
    assert returns.iloc[1:].tolist() == [0., 0.]
    assert traded.tolist() == buy.tolist() == sell.tolist() == [True, False, False]
    assert module.build_active_status_series({'list_date': '1991-04-03', 'delist_date': '2026-10-28'}, args['trading_dates']).tolist() == [True, True, False]


def test_absent_evidence_does_not_invent_suspension(tmp_path):
    args, sidecar, *_ = fixture_bundle(tmp_path); sidecar.unlink()
    assert enrich_price_with_trade_status(**args).equals(args['price'])
    with pytest.raises(ValueError): enrich_price_with_trade_status(**args, require_evidence=True)
    args['price'] = args['price'].assign(tradestatus=0)
    with pytest.raises(ValueError): enrich_price_with_trade_status(**args)


@pytest.mark.parametrize('change', ['raw_hash', 'evidence_hash', 'identity', 'missing_trade', 'missing_session', 'non_session', 'delist', 'volume', 'unbound_review'])
def test_rejects_inconsistent_evidence(tmp_path, change):
    args, sidecar, q, record, query = fixture_bundle(tmp_path)
    if change == 'raw_hash': record['raw_price_sha256'] = '0'*64
    elif change == 'evidence_hash': record['provider_queries'][0]['sha256'] = '0'*64
    elif change == 'identity': query['rows'][0][1] = 'sz.000002'
    elif change == 'missing_trade': query['rows'][1][3:5] = ['100', '1']
    elif change == 'missing_session': query['rows'].pop(1)
    elif change == 'non_session': args['trading_dates'] = pd.DatetimeIndex(['2026-10-26', '2026-10-28'])
    elif change == 'delist': args['master'].loc[0, 'delist_date'] = '2026-10-27'; record['lifecycle']['delist_date'] = '2026-10-27'
    elif change == 'volume': query['rows'][1][3] = '123'
    elif change == 'unbound_review': record['reviewed_suspensions'] = [{'date': '2026-10-27', 'document': record['provider_queries'][0], 'review': record['provider_queries'][0]}]
    if change not in ['evidence_hash', 'unbound_review']:
        q.write_text(json.dumps(query)); record['provider_queries'][0]['sha256'] = hashlib.sha256(q.read_bytes()).hexdigest()
    sidecar.write_text(json.dumps(record))
    with pytest.raises(ValueError): enrich_price_with_trade_status(**args)


@pytest.mark.parametrize('module', [v.base_mod.freq_mod, standalone])
def test_normal_observed_trades_identical_with_or_without_explicit_status(monkeypatch, module):
    dates = pd.bdate_range('2026-10-26', periods=3)
    price = pd.DataFrame({'date': dates, 'close_raw': [10., 10.1, 10.2]})
    adjusted = pd.DataFrame({'date': dates, 'close_adj': [100., 101., 102.]})
    from scripts.stock_return_basis import RETURN_BASIS_REVISION
    adjusted.attrs['return_basis_manifest'] = {'return_basis_revision': RETURN_BASIS_REVISION, 'symbol': '000001', 'raw_sha256': 'a'*64, 'qfq_sha256': 'b'*64, 'hfq_sha256': 'c'*64}
    monkeypatch.setattr(module, 'read_ohlc_cache', lambda *a: pd.DataFrame())
    before = module.build_tradeability_series('000001', price, dates, return_price=adjusted)
    after = module.build_tradeability_series('000001', price.assign(tradestatus=1), dates, return_price=adjusted)
    for a, b in zip(before, after): pd.testing.assert_series_equal(a, b)


@pytest.mark.parametrize('mutation', [None, 'wrong_issuer', 'wrong_request_id', 'wrong_date', 'wrong_price', 'null_volume', 'wrong_raw_binding'])
def test_status_correction_requires_exact_independent_response(tmp_path, mutation):
    args, sidecar, q, record, query = fixture_bundle(tmp_path)
    query['rows'][0][4] = '0'  # Provider flag contradicts its positive volume.
    q.write_text(json.dumps(query)); record['provider_queries'][0]['sha256'] = hashlib.sha256(q.read_bytes()).hexdigest()
    payload = {'data': {'columns': [{'name': n} for n in ['TIME', 'MATCH', 'VOLUME']], 'rows': [['2026-10-26', '10', '100']]}, 'error': None}
    if mutation == 'wrong_date': payload['data']['rows'][0][0] = '2026-10-27'
    if mutation == 'wrong_price': payload['data']['rows'][0][1] = '11'
    if mutation == 'null_volume': payload['data']['rows'][0][2] = None
    response = {'id': 123, 'result': {'isError': False, 'content': [{'type': 'text', 'text': json.dumps(payload)}]}}
    if mutation == 'wrong_request_id': response['id'] = 124
    rp = q.parent / 'wind.txt'; rp.write_text('event: message\ndata: '+json.dumps(response)+'\n\n')
    receipt = {'provider': 'wind', 'status': 200, 'tool': 'get_stock_kline', 'request_id': 123, 'sha256': hashlib.sha256(rp.read_bytes()).hexdigest(), 'arguments': {'windcode': '000001.SZ', 'begin_date': '20261026', 'end_date': '20261026', 'aftype': '2', 'period': '10'}}
    if mutation == 'wrong_issuer': receipt['arguments']['windcode'] = '000002.SZ'
    cp = q.parent / 'wind_receipt.json'; cp.write_text(json.dumps(receipt))
    correction = {'symbol': '000001', 'date': '2026-10-26', 'original_status': 0, 'corrected_status': 1, 'provider_raw_sha256': record['provider_queries'][0]['sha256'], 'wind_response': {'path': rp.name, 'sha256': hashlib.sha256(rp.read_bytes()).hexdigest()}, 'wind_receipt': {'path': cp.name, 'sha256': hashlib.sha256(cp.read_bytes()).hexdigest()}}
    if mutation == 'wrong_raw_binding': correction['provider_raw_sha256'] = '0'*64
    record['provider_status_reviews'] = [correction]; sidecar.write_text(json.dumps(record))
    if mutation is None:
        assert enrich_price_with_trade_status(**args).tradestatus.tolist() == [1, 0, 0]
    else:
        with pytest.raises(ValueError): enrich_price_with_trade_status(**args)
