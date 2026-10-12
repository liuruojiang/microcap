from datetime import date
from types import SimpleNamespace

import pandas as pd
import pytest

import microcap_top100_mom16_biweekly_live_v2_0 as v20
from scripts import exchange_calendar


def test_panel_shadow_rejects_non_session_before_fetch_or_write(tmp_path, monkeypatch):
    base = v20.base_mod
    panel_path = tmp_path / "source_panel.csv"
    shadow_path = tmp_path / "panel_shadow.csv"
    pd.DataFrame(
        {
            "date": ["2018-06-15", "2018-06-18", "2018-06-19"],
            base.HEDGE_COLUMN: [100.0, 101.0, 102.0],
        }
    ).to_csv(panel_path, index=False)

    monkeypatch.setattr(
        exchange_calendar,
        "sessions_for_day",
        lambda day: (date(2018, 6, 15), date(2018, 6, 19)),
    )
    namespace = base.build_refreshed_panel_shadow.__globals__
    monkeypatch.setitem(
        namespace,
        "fetch_eastmoney_index_history",
        lambda *args, **kwargs: pytest.fail("index history fetched before source-panel validation"),
    )

    with pytest.raises(ValueError, match="2018-06-18"):
        base.build_refreshed_panel_shadow(
            SimpleNamespace(panel_path=panel_path),
            {"panel_shadow": shadow_path},
        )

    assert not shadow_path.exists()



def test_reusable_panel_shadow_rejects_non_session_cache_before_fetch_or_write(tmp_path, monkeypatch):
    base = v20.base_mod
    panel_path = tmp_path / "source_panel.csv"
    shadow_path = tmp_path / "panel_shadow.csv"
    dates = ["2018-06-15", "2018-06-19"]
    pd.DataFrame({"date": dates, base.HEDGE_COLUMN: [100.0, 102.0]}).to_csv(panel_path, index=False)
    pd.DataFrame(
        {"date": ["2018-06-15", "2018-06-18", "2018-06-19"], base.HEDGE_COLUMN: [100.0, 101.0, 102.0]}
    ).to_csv(shadow_path, index=False)
    original_shadow = shadow_path.read_bytes()

    monkeypatch.setattr(
        exchange_calendar,
        "sessions_for_day",
        lambda day: (date(2018, 6, 15), date(2018, 6, 19)),
    )
    namespace = base.build_refreshed_panel_shadow.__globals__
    monkeypatch.setitem(namespace, "panel_shadow_cache_is_reusable", lambda *args, **kwargs: True)
    monkeypatch.setitem(
        namespace,
        "fetch_eastmoney_index_history",
        lambda *args, **kwargs: pytest.fail("index history fetched before cached-panel validation"),
    )

    with pytest.raises(ValueError, match="2018-06-18"):
        base.build_refreshed_panel_shadow(
            SimpleNamespace(panel_path=panel_path),
            {"panel_shadow": shadow_path},
        )

    assert shadow_path.read_bytes() == original_shadow


def test_full_proxy_rebuild_rejects_non_session_before_price_refresh(tmp_path, monkeypatch):
    base = v20.base_mod
    panel_path = tmp_path / "source_panel.csv"
    pd.DataFrame(
        {"date": ["2018-06-15", "2018-06-18", "2018-06-19"]}
    ).to_csv(panel_path, index=False)
    monkeypatch.setattr(
        exchange_calendar,
        "sessions_for_day",
        lambda day: (date(2018, 6, 15), date(2018, 6, 19)),
    )
    namespace = base.ensure_strategy_files.__globals__
    monkeypatch.setitem(
        namespace,
        "refresh_price_cache_tail",
        lambda *args, **kwargs: pytest.fail("price refresh started before panel session validation"),
    )
    args = SimpleNamespace(
        index_csv=tmp_path / "proxy.csv",
        costed_nav_csv=tmp_path / "costed_nav.csv",
        max_workers=1,
        force_refresh=False,
        rebuild_index_if_missing=True,
        allow_full_rebuild=True,
    )
    paths = {
        "proxy_meta": tmp_path / "proxy_meta.json",
        "proxy_members": tmp_path / "proxy_members.csv",
        "proxy_turnover": tmp_path / "proxy_turnover.csv",
    }

    with pytest.raises(ValueError, match="2018-06-18"):
        base.ensure_strategy_files(args, paths, panel_path, pd.Timestamp("2018-06-19"))

    assert not args.index_csv.exists()
    assert not paths["proxy_meta"].exists()


def test_full_proxy_rebuild_uses_frequency_start_date_and_reaches_builder(tmp_path, monkeypatch):
    base = v20.base_mod
    panel_path = tmp_path / "source_panel.csv"
    panel_dates = [pd.Timestamp("2018-06-15"), pd.Timestamp("2018-06-19")]
    pd.DataFrame({"date": panel_dates}).to_csv(panel_path, index=False)
    expected_sessions = (date(2018, 6, 15), date(2018, 6, 19))
    monkeypatch.setattr(exchange_calendar, "sessions_for_day", lambda day: expected_sessions)
    namespace = base.ensure_strategy_files.__globals__
    refresh_calls = []
    builder_calls = []
    monkeypatch.setitem(namespace, "refresh_price_cache_tail", lambda *a, **k: refresh_calls.append((a, k)))

    def build_proxy(args, trading_dates):
        builder_calls.append(pd.DatetimeIndex(trading_dates))
        return (
            pd.DataFrame({"date": trading_dates, "close": [100.0, 101.0]}),
            pd.DataFrame({"rebalance_date": [], "symbol": [], "rank": []}),
            pd.DataFrame({"rebalance_date": []}),
            {"candidate": True},
        )

    monkeypatch.setitem(namespace, "build_local_proxy_bundle", build_proxy)
    monkeypatch.setitem(namespace, "rebuild_costed_nav_from_proxy_turnover", lambda *a, **k: None)
    monkeypatch.setitem(namespace, "assert_proxy_tail_is_actionable", lambda *a, **k: None)
    args = SimpleNamespace(
        index_csv=tmp_path / "proxy.csv",
        costed_nav_csv=tmp_path / "costed_nav.csv",
        max_workers=1,
        force_refresh=False,
        rebuild_index_if_missing=True,
        allow_full_rebuild=True,
    )
    paths = {
        "proxy_meta": tmp_path / "proxy_meta.json",
        "proxy_members": tmp_path / "proxy_members.csv",
        "proxy_turnover": tmp_path / "proxy_turnover.csv",
    }

    base.ensure_strategy_files(args, paths, panel_path, pd.Timestamp("2018-06-19"))

    assert len(refresh_calls) == 1
    assert len(builder_calls) == 1
    assert list(builder_calls[0].date) == list(expected_sessions)
    assert args.index_csv.exists()
    assert paths["proxy_meta"].exists()
