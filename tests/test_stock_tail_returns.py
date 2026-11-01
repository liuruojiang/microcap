"""Exercise the real tail reader on captured source data and missing-day faults."""
import pandas as pd
import pytest

from test_stock_return_basis_transport import prepare
from scripts import stock_tail_returns as tail


def setup_tail(tmp_path, monkeypatch):
    anchor, cache = prepare(tmp_path, monkeypatch)
    raw = cache / 'prices_raw/000001.csv'
    dates = pd.DatetimeIndex(pd.read_csv(raw, parse_dates=['date']).date)
    monkeypatch.setattr(tail, 'sessions_for_day', lambda _: tuple(dates.date))
    master = pd.read_csv(cache / 'security_master.csv', dtype={'symbol': str})
    return dict(root=tmp_path, symbol='000001', raw_path=raw, master=master,
                tail_dates=dates[-3:])


def test_real_tail_matches_ratio_prices(tmp_path, monkeypatch):
    args = setup_tail(tmp_path, monkeypatch)
    result = tail.load_tail_returns(**args)
    adjusted = tail.load_return_prices(root=tmp_path, symbol='000001',
        raw_path=args['raw_path'], master=args['master'],
        as_of=args['tail_dates'][-1], allow_network=False)
    expected = adjusted.set_index('date').close_adj.reindex(args['tail_dates']).pct_change(fill_method=None)
    expected.iloc[0] = 0.0
    pd.testing.assert_series_equal(result, expected, check_names=False)


def test_omitted_exchange_session_blocks_tail(tmp_path, monkeypatch):
    args = setup_tail(tmp_path, monkeypatch)
    args['tail_dates'] = args['tail_dates'][[0, 2]]
    with pytest.raises(tail.ReturnBasisError, match='omits'):
        tail.load_tail_returns(**args)


def test_default_status_cannot_replace_source_coverage(tmp_path, monkeypatch):
    args = setup_tail(tmp_path, monkeypatch)
    original = tail.enrich_price_with_trade_status
    def missing_proof(**kwargs):
        frame = original(**kwargs)
        frame.attrs['verified_trade_status_dates'] = []
        return frame
    monkeypatch.setattr(tail, 'enrich_price_with_trade_status', missing_proof)
    with pytest.raises(tail.ReturnBasisError, match='coverage is incomplete'):
        tail.load_tail_returns(**args)
