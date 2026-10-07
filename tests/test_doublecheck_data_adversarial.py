"""Second independent data audit; synthetic inputs never become market results.

These cases exercise the real shared functions and real security-meta builder.
All writes are isolated by pytest's task-local --basetemp directory.
"""
from __future__ import annotations

import importlib.util
import json
import os
from pathlib import Path
import sys

import numpy as np
import pandas as pd
import pytest


def _load_strategy(name: str):
    source_root = os.environ.get("MICROCAP_DOUBLECHECK_SOURCE_ROOT")
    if not source_root:
        return __import__(name)
    source = Path(source_root) / f"{name}.py"
    module_name = f"_doublecheck_data_{name}"
    spec = importlib.util.spec_from_file_location(module_name, source)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


v20 = _load_strategy("microcap_top100_mom16_biweekly_live_v2_0")
legacy = _load_strategy("microcap_top100_mom16_biweekly_live")


@pytest.fixture(params=[v20.base_mod, legacy], ids=["shared_v20", "mainline"])
def base(request):
    return request.param


@pytest.mark.parametrize("missing,text", [(np.nan, "nan"), (None, "None"), (pd.NA, "<NA>")])
def test_freeze_distinguishes_missing_from_unparsed_text_with_same_display(base, missing, text):
    previous = pd.DataFrame({"date": pd.bdate_range("2026-09-01", periods=3),
                             "diagnostic": pd.Series([1.0, missing, 2.0], dtype=object)})
    candidate = previous.copy()
    candidate.loc[1, "diagnostic"] = text
    with pytest.raises(RuntimeError, match="historical rewrite"):
        base.assert_no_historical_rewrite(previous, candidate, ["diagnostic"], 0, "doublecheck")


def test_freeze_preserves_genuine_unchanged_warmup_missing(base):
    previous = pd.DataFrame({"date": pd.bdate_range("2026-09-01", periods=3),
                             "diagnostic": [np.nan, 1.0, 2.0]})
    base.assert_no_historical_rewrite(previous, previous.copy(), ["diagnostic"], 0, "doublecheck")


def _price_sources(tmp_path, base):
    dates = pd.bdate_range("2026-08-03", periods=30)
    panel = pd.DataFrame({"date": dates, base.HEDGE_COLUMN: np.arange(30, dtype=float) + 100.0})
    proxy = pd.DataFrame({"date": dates, "close": np.arange(30, dtype=float) + 200.0,
                          "holding_effective": True})
    return panel, proxy, tmp_path / "panel.csv", tmp_path / "proxy.csv"


def test_daily_close_loader_rejects_two_observations_for_one_session(base, tmp_path):
    panel, proxy, panel_path, proxy_path = _price_sources(tmp_path, base)
    extra_panel = panel.iloc[[15]].copy()
    extra_proxy = proxy.iloc[[15]].copy()
    extra_panel["date"] += pd.Timedelta(hours=12)
    extra_proxy["date"] += pd.Timedelta(hours=12)
    pd.concat([panel, extra_panel]).to_csv(panel_path, index=False)
    pd.concat([proxy, extra_proxy]).to_csv(proxy_path, index=False)
    with pytest.raises(ValueError, match="date|session|daily"):
        base.load_close_df(panel_path, proxy_path)


@pytest.mark.parametrize("timing", ["close", "next_open"])
def test_explicit_execution_date_cannot_be_reassigned_to_another_session(timing):
    dates = pd.bdate_range("2026-09-07", periods=3)
    event = pd.DataFrame({"rebalance_date": [dates[0]],
                          "execution_date": [dates[0] + pd.Timedelta(hours=12)],
                          "execution_timing": [timing], "two_side_cost_rate": [0.006]})
    with pytest.raises(ValueError, match="date|session|execution"):
        v20.cost_mod.map_rebalance_apply_costs(dates, event)


@pytest.mark.parametrize("signal_date", ["2026-09-01", "2026-09-18"])
def test_legacy_next_open_event_before_window_requires_calendar_or_fails_closed(signal_date):
    dates = pd.bdate_range("2026-09-21", periods=3)
    event = pd.DataFrame({"rebalance_date": [pd.Timestamp(signal_date)],
                          "execution_timing": ["next_open"], "two_side_cost_rate": [0.006]})
    with pytest.raises(ValueError, match="date|calendar|window|execution"):
        v20.cost_mod.map_rebalance_apply_costs(dates, event)


def test_explicit_execution_date_cannot_precede_rebalance_signal():
    dates = pd.bdate_range("2026-09-01", periods=3)
    event = pd.DataFrame({"rebalance_date": [dates[1]], "execution_date": [dates[0]],
                          "execution_timing": ["next_open"], "two_side_cost_rate": [0.006]})
    with pytest.raises(ValueError, match="date|execution|rebalance"):
        v20.cost_mod.map_rebalance_apply_costs(dates, event)


def test_explicit_non_session_inside_window_is_not_moved_to_monday():
    dates = pd.to_datetime(["2026-09-04", "2026-09-07", "2026-09-08"])
    event = pd.DataFrame({"rebalance_date": [dates[0]], "execution_date": [pd.Timestamp("2026-09-05")],
                          "execution_timing": ["next_open"], "two_side_cost_rate": [0.006]})
    with pytest.raises(ValueError, match="date|session|execution"):
        v20.cost_mod.map_rebalance_apply_costs(dates, event)


@pytest.mark.parametrize("signal_date", [pd.NaT, "invalid-signal-date"])
def test_explicit_execution_date_does_not_hide_invalid_rebalance_date(signal_date):
    dates = pd.bdate_range("2026-09-21", periods=3)
    event = pd.DataFrame({"rebalance_date": [signal_date], "execution_date": [dates[1]],
                          "execution_timing": ["next_open"], "two_side_cost_rate": [0.006]})
    with pytest.raises(ValueError, match="date|Date|NaT|execution"):
        v20.cost_mod.map_rebalance_apply_costs(dates, event)


@pytest.mark.parametrize("timing", ["close", "next_open"])
def test_explicit_already_paid_cost_before_window_stays_outside_window(timing):
    dates = pd.bdate_range("2026-09-21", periods=3)
    event = pd.DataFrame({"rebalance_date": [pd.Timestamp("2026-09-17")],
                          "execution_date": [pd.Timestamp("2026-09-18")],
                          "execution_timing": [timing], "two_side_cost_rate": [0.006]})
    assert v20.cost_mod.map_rebalance_apply_costs(dates, event).eq(0.0).all()


def test_explicit_first_window_execution_retains_its_cost():
    dates = pd.bdate_range("2026-09-21", periods=3)
    event = pd.DataFrame({"rebalance_date": [pd.Timestamp("2026-09-18")], "execution_date": [dates[0]],
                          "execution_timing": ["next_open"], "two_side_cost_rate": [0.006]})
    assert v20.cost_mod.map_rebalance_apply_costs(dates, event).tolist() == pytest.approx([0.006, 0.0, 0.0])


@pytest.mark.parametrize("date_value", [pd.Timestamp("2026-09-01 12:00"),
                                        pd.Timestamp("2026-09-01", tz="UTC")])
def test_daily_frozen_index_rejects_timestamp_and_timezone_labels(base, date_value):
    frame = pd.DataFrame({"date": [date_value], "nav_net": [1.0]})
    with pytest.raises(ValueError, match="date|daily|time|session"):
        base._normalise_dated_frame(frame, "doublecheck daily frozen input")


@pytest.mark.parametrize("mutation", ["obsolete_policy", "snapshot_drift", "short_symbol", "numeric_symbol"])
def test_real_meta_refresh_cannot_erase_historical_st_evidence_after_identity_drift(tmp_path, monkeypatch, mutation):
    namespace = v20.freq_mod.load_security_meta.__globals__
    intervals = [{"start": "2020-01-02", "end": "2021-01-04"}]
    payload = {"symbol": "000001", "meta_version": namespace["SECURITY_META_VERSION"],
               "st_notice_policy_version": namespace["ST_NOTICE_POLICY_VERSION"],
               "current_st_snapshot_name": None, "first_trade_date": "2019-01-02",
               "last_trade_date": "2026-09-30", "name_history_status": "ok",
               "notice_query_status": "ok", "st_intervals": intervals}
    if mutation in {"obsolete_policy", "short_symbol", "numeric_symbol"}:
        payload["st_notice_policy_version"] = "superseded-policy"
        if mutation == "short_symbol":
            payload["symbol"] = "1"
        elif mutation == "numeric_symbol":
            payload["symbol"] = 1
    else:
        payload["current_st_snapshot_name"] = "ST旧名"
    meta_path = tmp_path / "000001.json"
    meta_path.write_text(json.dumps(payload), encoding="utf-8")
    price_path = tmp_path / "prices.csv"
    pd.DataFrame({"date": ["2019-01-02", "2026-09-30"]}).to_csv(price_path, index=False)

    def source_failure(*args, **kwargs):
        raise RuntimeError("isolated history source outage")

    monkeypatch.setitem(namespace, "SECURITY_META_DIR", tmp_path)
    monkeypatch.setitem(namespace, "resolve_security_meta_path", lambda symbol: meta_path)
    monkeypatch.setitem(namespace, "resolve_cache_path", lambda *args: price_path)
    monkeypatch.setitem(namespace, "load_security_master", lambda: pd.DataFrame())
    monkeypatch.setitem(namespace, "load_current_st_name_map", lambda: {})
    monkeypatch.setitem(namespace, "fetch_sz_name_change_history", source_failure)
    monkeypatch.setitem(namespace, "fetch_cninfo_st_notices", source_failure)
    original = meta_path.read_bytes()
    try:
        result = v20.freq_mod.load_security_meta("000001")
    except namespace["SecurityMetaHistoryCorrectionRequired"]:
        assert meta_path.read_bytes() == original
        return
    assert result is None or result.get("st_intervals") == intervals
    assert meta_path.read_bytes() == original, "A failed source refresh cannot overwrite approved historical ST evidence"


def test_matching_meta_identity_preserves_historical_st_evidence_during_outage():
    namespace = v20.freq_mod.load_security_meta.__globals__
    old = {"symbol": "000001", "meta_version": namespace["SECURITY_META_VERSION"],
           "st_notice_policy_version": namespace["ST_NOTICE_POLICY_VERSION"],
           "current_st_snapshot_name": None,
           "st_intervals": [{"start": "2020-01-02", "end": "2021-01-04"}]}
    candidate = {**old, "st_intervals": [], "name_history_status": "error:isolated outage",
                 "notice_query_status": "error:isolated outage"}
    assert namespace["_preserve_security_meta_evidence"](old, candidate) is old


def _meta_success_context(tmp_path, monkeypatch, mutation, revised=False):
    namespace = v20.freq_mod.load_security_meta.__globals__
    intervals = [{"start": "2020-01-02", "end": "2021-01-04", "source": "cninfo_notice"}]
    payload = {"symbol": "000001", "meta_version": namespace["SECURITY_META_VERSION"],
               "st_notice_policy_version": namespace["ST_NOTICE_POLICY_VERSION"],
               "current_st_snapshot_name": None, "first_trade_date": "2019-01-02",
               "last_trade_date": "2026-09-30", "name_history_status": "ok",
               "notice_query_status": "ok", "st_intervals": intervals}
    if mutation == "obsolete_policy":
        payload["st_notice_policy_version"] = "superseded-policy"
    elif mutation == "snapshot_drift":
        payload["current_st_snapshot_name"] = "ST旧名"
    elif mutation == "different_symbol":
        payload["symbol"] = "000002"
    else:
        raise ValueError(mutation)
    meta_path = tmp_path / "000001.json"
    meta_path.write_text(json.dumps(payload), encoding="utf-8")
    price_path = tmp_path / "prices.csv"
    pd.DataFrame({"date": ["2019-01-02", "2026-09-30"]}).to_csv(price_path, index=False)
    notices = pd.DataFrame({"notice_date": ["2020-01-03" if revised else "2020-01-02", "2021-01-04"],
                            "title": ["关于股票交易被实施退市风险警示的公告", "关于撤销退市风险警示的公告"]})
    monkeypatch.setitem(namespace, "SECURITY_META_DIR", tmp_path)
    monkeypatch.setitem(namespace, "resolve_security_meta_path", lambda symbol: meta_path)
    monkeypatch.setitem(namespace, "resolve_cache_path", lambda *args: price_path)
    monkeypatch.setitem(namespace, "load_security_master", lambda: pd.DataFrame())
    monkeypatch.setitem(namespace, "load_current_st_name_map", lambda: {})
    monkeypatch.setitem(namespace, "fetch_sz_name_change_history",
                        lambda: pd.DataFrame(columns=["symbol", "change_date", "old_name", "new_name"]))
    monkeypatch.setitem(namespace, "fetch_cninfo_st_notices", lambda **kwargs: notices)
    return namespace, meta_path, intervals


@pytest.mark.parametrize("mutation", ["obsolete_policy", "snapshot_drift"])
def test_successful_identity_upgrade_preserves_same_historical_st_intervals(tmp_path, monkeypatch, mutation):
    namespace, meta_path, intervals = _meta_success_context(tmp_path, monkeypatch, mutation)
    result = v20.freq_mod.load_security_meta("000001")
    assert result["st_intervals"] == intervals
    assert result["symbol"] == "000001"
    assert result["st_notice_policy_version"] == namespace["ST_NOTICE_POLICY_VERSION"]
    assert not result["current_st_snapshot_name"]
    assert json.loads(meta_path.read_text(encoding="utf-8")) == result


@pytest.mark.parametrize("mutation", ["obsolete_policy", "snapshot_drift"])
def test_successful_source_revision_still_requires_historical_st_migration(tmp_path, monkeypatch, mutation):
    namespace, meta_path, intervals = _meta_success_context(tmp_path, monkeypatch, mutation, revised=True)
    original = meta_path.read_bytes()
    with pytest.raises(namespace["SecurityMetaHistoryCorrectionRequired"], match="histor|lineage|interval|migration"):
        v20.freq_mod.load_security_meta("000001")
    assert meta_path.read_bytes() == original


def test_foreign_symbol_metadata_does_not_bind_this_symbols_historical_intervals(tmp_path, monkeypatch):
    namespace, meta_path, intervals = _meta_success_context(tmp_path, monkeypatch, "different_symbol", revised=True)
    result = v20.freq_mod.load_security_meta("000001")
    assert result["symbol"] == "000001"
    assert result["st_intervals"] == [{"start": "2020-01-03", "end": "2021-01-04", "source": "cninfo_notice"}]
