"""Real price fixture through actual embedded/standalone consumers; fault injection is explicit."""
import json
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest
import requests
import socket
import subprocess

import microcap_top100_mom16_biweekly_live_v2_0 as strategy
import fetch_wind_microcap_index as standalone
import analyze_top100_rebalance_frequency as standalone_frequency
from scripts.stock_return_basis import build_ratio_return_basis
from scripts.stock_return_pipeline import ReturnBasisError

F=Path(__file__).parent/"fixtures/stock_return_basis_real"


@pytest.mark.parametrize('module', [strategy.freq_mod, standalone_frequency])
def test_required_stock_history_cannot_silently_shrink_universe(module, monkeypatch):
    namespace = module.load_symbol_cache.__globals__
    monkeypatch.setitem(namespace, 'resolve_cache_path', lambda *args: None)
    dates = pd.DatetimeIndex(['2026-10-28', '2026-10-29'])
    with pytest.raises(RuntimeError, match='Missing required price/share history'):
        module.load_symbol_cache('000001', dates, dates)


@pytest.fixture(autouse=True)
def deny_network_and_persistent_store_writes(monkeypatch):
    import scripts.stock_return_basis_store as store
    def forbidden(*args,**kwargs):raise AssertionError("Entry-point test attempted network/subprocess/persistent write")
    monkeypatch.setattr(requests.sessions.Session,"request",forbidden)
    monkeypatch.setattr(socket.socket,"connect",forbidden)
    monkeypatch.setattr(socket,"create_connection",forbidden)
    monkeypatch.setattr(subprocess,"run",forbidden)
    monkeypatch.setattr(store,"_write",forbidden)


def real_prices():
    raw=pd.read_csv(F/"000418_raw.csv",parse_dates=["date"])
    out,manifest=build_ratio_return_basis(symbol="000418",raw_csv=(F/"000418_raw.csv").read_bytes(),qfq_source=(F/"qfq.js").read_bytes(),hfq_source=(F/"hfq.js").read_bytes(),qfq_receipt=json.loads((F/"qfq.json").read_text(encoding="utf-8")),hfq_receipt=json.loads((F/"hfq.json").read_text(encoding="utf-8")),as_of="2012-06-12",sessions=raw.date.dt.strftime('%Y-%m-%d'),list_date="1993-11-29")
    out.attrs["return_basis_manifest"]=manifest
    return raw,out


@pytest.mark.parametrize("module",[strategy.fetch_mod,standalone])
def test_actual_consumer_rejects_raw_partial_wrong_subject(module):
    raw,adjusted=real_prices();series=raw.set_index("date").close_raw
    actual=module.select_return_close("000418",series,adjusted)
    assert actual.pct_change().iloc[1]==pytest.approx(9.91/9.67-1,abs=1e-12)
    for bad in (None,raw,adjusted.iloc[:2]):
        with pytest.raises(ValueError):module.select_return_close("000418",series,bad)
    with pytest.raises(ValueError,match="identity"):module.select_return_close("000559",series,adjusted)


@pytest.mark.parametrize("module",[strategy.fetch_mod,standalone])
def test_actual_public_proxy_does_not_shrink_after_basis_failure(module,monkeypatch):
    raw,_=real_prices()
    args=SimpleNamespace(start_date="2012-06-04",end_date="2012-06-12",force_refresh=False,exclude_current_st=False,limit_symbols=0,executor="thread",max_workers=1,switch_date="2012-06-05",pre_switch_schedule="x",post_switch_schedule="x")
    namespace=module.build_public_proxy.__globals__
    monkeypatch.setitem(namespace,"ensure_dirs",lambda:None)
    monkeypatch.setitem(namespace,"load_calendar",lambda **_:pd.bdate_range("2012-06-04","2012-06-12"))
    monkeypatch.setitem(namespace,"fetch_active_universe",lambda **_:pd.DataFrame({"code":["000418","000559"]}))
    def worker(symbol,*args):
        if symbol=="000559":raise ReturnBasisError("fault injection: source unavailable")
        return symbol,raw
    monkeypatch.setitem(namespace,"_build_symbol_panel_worker",worker)
    def forbidden(**kwargs):raise AssertionError("Construction continued after basis failure")
    monkeypatch.setitem(namespace,"build_rebalance_dates",forbidden)
    with pytest.raises(RuntimeError,match="Incomplete"):module.build_public_proxy(args)


def test_raw_current_still_revalidates_factor_source(monkeypatch):
    import scripts.stock_daily_inputs as daily
    function=strategy.base_mod.refresh_price_cache_tail;namespace=function.__globals__
    monkeypatch.setattr(namespace['fetch_mod'],"ensure_dirs",lambda:None)
    monkeypatch.setitem(namespace['price_cache_refresh_preflight'].__globals__,"_price_cache_latest_date",lambda _:pd.Timestamp("2026-10-29"))
    calls=[]
    monkeypatch.setattr(daily,'refresh_daily_inputs',lambda **kwargs:calls.append(('daily',kwargs['symbols'],kwargs['as_of'])))
    monkeypatch.setattr(namespace['fetch_mod'],'fetch_share_change',lambda *args:None)
    monkeypatch.setitem(namespace,"refresh_return_basis_sources",lambda *args:calls.append(args))
    function(pd.Timestamp("2026-10-29"),2,["000418"])
    assert calls==[('daily',["000418"],"2026-10-29"),(["000418"],"2026-10-29")]
