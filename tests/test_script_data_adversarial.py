"""Isolated data/lineage counterexamples; fixtures are never market results.

MICROCAP_DATA_AUDIT_BASELINE_ROOT optionally runs the same checks against a
recoverable pre-edit source snapshot. No official artifacts or caches are written.
"""
from __future__ import annotations

import importlib.util
import json
import os
from pathlib import Path
import sys
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest


def _load_strategy(name: str):
    baseline_root = os.environ.get("MICROCAP_DATA_AUDIT_BASELINE_ROOT")
    if not baseline_root:
        return __import__(name)
    source = Path(baseline_root) / f"{name}.py"
    module_name = f"_data_adversarial_baseline_{name}"
    spec = importlib.util.spec_from_file_location(module_name, source)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


v20 = _load_strategy("microcap_top100_mom16_biweekly_live_v2_0")
legacy = _load_strategy("microcap_top100_mom16_biweekly_live")


@pytest.fixture(params=[v20.base_mod, legacy], ids=["shared_v20", "mainline"])
def base(request, monkeypatch):
    runtime = request.param
    for name, value in (("pd", pd), ("np", np)):
        monkeypatch.setitem(runtime.latest_closed_history_date.__globals__, name, value)
    return runtime


@pytest.mark.parametrize(
    "clock,expected",
    [("2026-09-30 14:00", "2026-09-29"), ("2026-09-30 16:00", "2026-09-30")],
)
def test_history_selector_never_uses_future_rows(base, clock, expected):
    history = pd.DataFrame({"date": ["2026-09-29", "2026-09-30", "2026-10-08"]})
    assert base.latest_closed_history_date(history, pd.Timestamp(clock)) == pd.Timestamp(expected)


def test_only_future_history_cannot_establish_a_close_anchor(base):
    with pytest.raises(RuntimeError, match="close-confirmed"):
        base.latest_closed_history_date(
            pd.DataFrame({"date": ["2026-10-08"]}), pd.Timestamp("2026-09-30 16:00")
        )


def _performance():
    return pd.DataFrame({"date": pd.bdate_range("2026-09-01", periods=3),
                         "return_net": [0.0, 0.02, -0.01], "nav_net": [1.0, 1.02, 1.0098]})


@pytest.mark.parametrize("column,value", [
    ("return_net", np.nan), ("return_net", np.inf), ("return_net", -np.inf),
    ("return_net", -1.0), ("return_net", -1.1),
    ("nav_net", np.nan), ("nav_net", np.inf), ("nav_net", -np.inf),
    ("nav_net", 0.0), ("nav_net", -1.0),
])
def test_partial_bad_performance_rows_are_not_reported_as_valid(base, column, value):
    frame = _performance()
    frame.loc[1, column] = value
    with pytest.raises(ValueError):
        base.validate_performance_frame(frame, "return_net", "nav_net", "diagnostic")


def test_performance_rejects_conflicting_nav_and_daily_return(base):
    frame = _performance()
    frame.loc[1, "nav_net"] = 1.4
    with pytest.raises(ValueError, match="NAV|nav|inconsistent"):
        base.validate_performance_frame(frame, "return_net", "nav_net", "diagnostic")


def test_clean_performance_preserves_each_observed_row(base):
    frame = _performance()
    pd.testing.assert_frame_equal(
        base.validate_performance_frame(frame, "return_net", "nav_net", "diagnostic"),
        frame.set_index("date"),
    )


@pytest.mark.parametrize("column", ["date", "index"])
def test_invalid_dates_are_not_silently_removed(base, column):
    frame = _performance()
    frame.loc[1, "date"] = pd.NaT
    if column == "index":
        frame = frame.set_index("date")
    with pytest.raises(ValueError, match="date|NaT"):
        base._normalise_dated_frame(frame, "diagnostic")


@pytest.mark.parametrize("before,after", [(1.0, np.inf), (1.0, -np.inf),
                                         (np.inf, 1.0), (-np.inf, 1.0),
                                         (np.inf, -np.inf), (-np.inf, np.inf)])
def test_historical_numeric_rewrite_detects_infinities_in_either_direction(base, before, after):
    previous = _performance()
    previous.loc[1, "nav_net"] = before
    candidate = previous.copy()
    candidate.loc[1, "nav_net"] = after
    with pytest.raises(RuntimeError, match="historical rewrite"):
        base.assert_no_historical_rewrite(previous, candidate, ["nav_net"], 0, "diagnostic")


def test_historical_rewrite_compares_unparsed_cells_in_mixed_numeric_column(base):
    previous = _performance().astype({"nav_net": object})
    previous.loc[1, "nav_net"] = "corrupt_before"
    candidate = previous.copy()
    candidate.loc[1, "nav_net"] = "corrupt_after"
    with pytest.raises(RuntimeError, match="historical rewrite"):
        base.assert_no_historical_rewrite(previous, candidate, ["nav_net"], 0, "diagnostic")


@pytest.mark.parametrize("mutation", ["removed_date", "inserted_date", "missing_key"])
def test_frozen_lineage_cannot_lose_dates_insert_dates_or_lose_key_columns(base, mutation):
    previous = _performance()
    if mutation == "removed_date":
        candidate = previous.drop(index=1)
    elif mutation == "inserted_date":
        candidate = pd.concat([previous, pd.DataFrame({"date": [pd.Timestamp("2026-08-31")],
                                                       "return_net": [0.0], "nav_net": [1.0]})])
    else:
        candidate = previous.drop(columns="nav_net")
    with pytest.raises(RuntimeError, match="historical|schema"):
        base.assert_no_historical_rewrite(previous, candidate, ["nav_net"], 0, "diagnostic")


def test_lineage_allows_new_tail_and_unchanged_warmup_nan(base):
    previous = _performance()
    previous.loc[0, "return_net"] = np.nan
    candidate = pd.concat([previous, pd.DataFrame({"date": [pd.Timestamp("2026-09-04")],
                                                   "return_net": [0.0], "nav_net": [1.0098]})])
    base.assert_no_historical_rewrite(previous, candidate, ["nav_net", "return_net"], 0, "diagnostic")


def _close_sources(tmp_path):
    dates = pd.bdate_range("2026-08-03", periods=30)
    panel = pd.DataFrame({"date": dates, v20.base_mod.HEDGE_COLUMN: np.arange(30, dtype=float) + 100.0})
    proxy = pd.DataFrame({"date": dates, "close": np.arange(30, dtype=float) + 200.0,
                          "holding_effective": True})
    return panel, proxy, tmp_path / "panel.csv", tmp_path / "proxy.csv"


@pytest.mark.parametrize("mutation", ["hedge_missing_date", "hedge_nan", "proxy_missing_date", "proxy_nan"])
def test_aligned_close_loader_does_not_compress_an_internal_session_gap(base, tmp_path, mutation):
    panel, proxy, panel_path, proxy_path = _close_sources(tmp_path)
    if mutation == "hedge_missing_date":
        panel = panel.drop(index=15)
    elif mutation == "proxy_missing_date":
        proxy = proxy.drop(index=15)
    elif mutation == "hedge_nan":
        panel.loc[15, base.HEDGE_COLUMN] = np.nan
    else:
        proxy.loc[15, "close"] = np.nan
    panel.to_csv(panel_path, index=False)
    proxy.to_csv(proxy_path, index=False)
    with pytest.raises(ValueError, match="aligned|missing|NaN"):
        base.load_close_df(panel_path, proxy_path)


def test_aligned_close_loader_preserves_clean_daily_prices(base, tmp_path):
    panel, proxy, panel_path, proxy_path = _close_sources(tmp_path)
    panel.to_csv(panel_path, index=False)
    proxy.to_csv(proxy_path, index=False)
    close = base.load_close_df(panel_path, proxy_path)
    np.testing.assert_array_equal(close.microcap, proxy.close)
    np.testing.assert_array_equal(close.hedge, panel[base.HEDGE_COLUMN])


def test_realtime_proxy_cache_requires_its_metadata_file(base, tmp_path, monkeypatch):
    runtime = base
    namespace = runtime.reusable_cached_proxy_end_for_realtime.__globals__
    paths = {"proxy_meta": tmp_path / "missing.json", "proxy_turnover": tmp_path / "turnover.csv"}
    args = SimpleNamespace(index_csv=tmp_path / "index.csv", costed_nav_csv=tmp_path / "costed.csv",
                           max_stale_anchor_days=5, allow_stale_realtime=False)
    for path in (args.index_csv, args.costed_nav_csv, paths["proxy_turnover"]):
        path.write_text("seed\n", encoding="utf-8")
    monkeypatch.setitem(namespace, "read_csv_last_date", lambda path: pd.Timestamp("2026-09-30"))
    monkeypatch.setitem(namespace, "assess_realtime_anchor_freshness", lambda *a, **k: {"is_stale": False})
    monkeypatch.setitem(namespace, "assess_history_anchor_freshness", lambda *a, **k: {"is_stale": False})
    assert runtime.reusable_cached_proxy_end_for_realtime(args, paths, pd.Timestamp("2026-09-30")) is None


@pytest.mark.parametrize("mutation", ["wrong_symbol", "obsolete_policy", "removed_current_st"])
def test_non_st_security_metadata_cache_requires_matching_identity_and_explicit_policy(tmp_path, monkeypatch, mutation):
    runtime = v20.freq_mod
    namespace = runtime.load_security_meta.__globals__
    payload = {"symbol": "000001", "meta_version": namespace["SECURITY_META_VERSION"],
               "st_notice_policy_version": namespace["ST_NOTICE_POLICY_VERSION"],
               "current_st_snapshot_name": None, "st_intervals": []}
    if mutation == "wrong_symbol":
        payload["symbol"] = "000002"
    elif mutation == "obsolete_policy":
        payload["st_notice_policy_version"] = "superseded-policy"
    else:
        payload["current_st_snapshot_name"] = "ST旧名"
    path = tmp_path / "000001.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    fresh = {"symbol": "000001", "fresh": True}
    monkeypatch.setitem(namespace, "load_current_st_name_map", lambda: {})
    monkeypatch.setitem(namespace, "resolve_security_meta_path", lambda symbol: path)
    monkeypatch.setitem(namespace, "build_security_meta", lambda symbol: fresh)
    assert runtime.load_security_meta("000001") is fresh


@pytest.mark.parametrize("timing", ["close", "next_open"])
def test_explicit_rebalance_execution_date_is_charged_on_that_session(timing):
    dates = pd.bdate_range("2026-09-01", periods=3)
    turnover = pd.DataFrame({"rebalance_date": [dates[0]], "execution_date": [dates[1]],
                             "execution_timing": [timing], "two_side_cost_rate": [0.006]})
    actual = v20.cost_mod.map_rebalance_apply_costs(dates, turnover)
    assert actual.tolist() == pytest.approx([0.0, 0.006, 0.0])


@pytest.mark.parametrize("timing", ["close", "next_open"])
def test_cost_paid_before_a_result_window_is_not_charged_again(timing):
    dates = pd.bdate_range("2026-09-02", periods=3)
    turnover = pd.DataFrame({"rebalance_date": [pd.Timestamp("2026-09-01")],
                             "execution_date": [pd.Timestamp("2026-09-01")],
                             "execution_timing": [timing], "two_side_cost_rate": [0.006]})
    assert v20.cost_mod.map_rebalance_apply_costs(dates, turnover).eq(0.0).all()


@pytest.mark.parametrize("timing,expected", [("close", [0.006, 0.0, 0.0]),
                                           ("next_open", [0.0, 0.006, 0.0])])
def test_legacy_turnover_without_execution_date_keeps_rebalance_timing(timing, expected):
    dates = pd.bdate_range("2026-09-01", periods=3)
    turnover = pd.DataFrame({"rebalance_date": [dates[0]], "execution_timing": [timing],
                             "two_side_cost_rate": [0.006]})
    assert v20.cost_mod.map_rebalance_apply_costs(dates, turnover).tolist() == pytest.approx(expected)


@pytest.mark.parametrize("cost", [np.nan, np.inf, -np.inf, -0.001, 1.0])
def test_nonfinite_or_invalid_cost_cannot_enter_costed_returns(cost):
    dates = pd.bdate_range("2026-09-01", periods=3)
    turnover = pd.DataFrame({"rebalance_date": [dates[0]], "execution_timing": ["close"],
                             "two_side_cost_rate": [cost]})
    with pytest.raises(ValueError, match="cost"):
        v20.cost_mod.map_rebalance_apply_costs(dates, turnover)
