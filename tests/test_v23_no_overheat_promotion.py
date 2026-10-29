"""Synthetic behavior checks; market parity is recorded by the migration runner."""
import numpy as np
import pandas as pd
import pytest
from types import SimpleNamespace

import microcap_top100_mom16_biweekly_live_v2_3 as v


@pytest.mark.parametrize("feature", [0.0, .27, 3.0])
def test_formal_builder_ignores_volatility_but_preserves_momentum_and_costs(monkeypatch, feature):
    idx = pd.bdate_range("2026-01-05", periods=4)
    active = "long_microcap_short_zz1000"
    gross = pd.DataFrame({"holding": ["cash", active, active, "cash"],
                          "next_holding": [active, active, "cash", "cash"],
                          "return": [0., .02, -.01, 0.], "spread_nav": 1.}, index=idx)
    monkeypatch.setattr(v, "build_spread_log_wls_gross", lambda *_: gross.copy())
    monkeypatch.setattr(v, "_overheat_feature_series", lambda _: pd.Series(feature, index=idx))
    out = v.build_v2_3_result(pd.DataFrame(), pd.DataFrame(), idx)
    assert out.holding.equals(gross.holding)
    assert out.next_holding.equals(gross.next_holding)
    entry = v.v2_0.base_mod.freq_mod.cost_mod.ENTRY_COST
    exit_cost = v.v2_0.base_mod.freq_mod.cost_mod.EXIT_COST
    np.testing.assert_allclose(out.return_net, [-entry, .02, .99*(1-exit_cost)-1, 0.], atol=1e-14)
    for field in ["overheat_enabled", "overheat_overlay_enabled", "overheat_risk_off",
                  "overheat_exit_triggered", "overheat_reentry_triggered", "overheat_block_entry_triggered"]:
        assert not out[field].any()
    legacy = v.apply_overheat_defense(gross, pd.DataFrame(), enabled=True)
    if feature >= .26:
        assert legacy.holding.eq("cash").all()


def test_retired_revision_cannot_certify_no_overheat_identity():
    from scripts.top100_delivery import plain_v23_identity
    row = dict(strategy_revision="plain_lb25_hl2p5_r2off_vol10_26_20_20260904",
               target_vol_enabled="False", r2_gate_enabled="False", overheat_enabled="True",
               lookback=25, halflife=2.5, r2_entry_gate=0, signal_spread_hedge_ratio=1,
               execution_hedge_ratio=.8, entry_threshold=0, momentum_gap_exit_buffer=.08,
               cash_day_yield_enabled="False", financing_enabled="False",
               overheat_feature_window=10, overheat_trigger_threshold=.26, overheat_recovery_threshold=.20)
    assert not plain_v23_identity(row)
    row.update(strategy_revision=v.STRATEGY_REVISION)
    assert not plain_v23_identity(row)
    row.update(overheat_enabled="False")
    assert plain_v23_identity(row)
    row.pop("overheat_enabled")
    assert not plain_v23_identity(row)
    row["overheat_overlay_enabled"] = "False"
    assert plain_v23_identity(row)
    row["overheat_enabled"] = "True"
    assert not plain_v23_identity(row)


def test_realtime_builder_also_removes_volatility_constraint(monkeypatch):
    idx = pd.bdate_range("2026-01-05", periods=3)
    active = "long_microcap_short_zz1000"
    gross = pd.DataFrame({"holding": ["cash", active, active],
                          "next_holding": [active, active, active],
                          "return": [0., .02, .01], "spread_nav": 1.}, index=idx)
    base = SimpleNamespace(realtime_close_df=pd.DataFrame({'microcap':1.,'hedge':1.}, index=idx),
                           turnover_df=pd.DataFrame(), reference_summary={}, context={},
                           meta={'snapshot_row_appended':True})
    monkeypatch.setattr(v, 'ensure_output_dir', lambda:None)
    monkeypatch.setattr(v.v2_0.realtime_core, 'load_realtime_base', lambda:base)
    monkeypatch.setattr(v, '_build_realtime_v2_3_official_index', lambda *_:idx)
    monkeypatch.setattr(v, 'build_v2_3_common_index', lambda *_:idx)
    monkeypatch.setattr(v, 'build_spread_log_wls_gross', lambda *_:gross.copy())
    monkeypatch.setattr(v, '_overheat_feature_series', lambda _:pd.Series(3., index=idx))
    monkeypatch.setattr(v, 'build_signal_execution_mismatch_diagnostics', lambda *_:{})
    monkeypatch.setattr(v, 'apply_signal_execution_mismatch_columns', lambda *_:None)
    monkeypatch.setattr(v, '_build_signal_row', lambda out,_:out.tail(1).copy())
    monkeypatch.setattr(v.v2_0.realtime_core.base_mod, 'augment_realtime_signal_with_member_rebalance', lambda row,*args,**kwargs:row)
    monkeypatch.setattr(v.v2_0.overlay_mod, '_apply_realtime_meta_columns_to_signal_row', lambda *_:None)
    monkeypatch.setattr(v, '_atomic_write_text', lambda *args,**kwargs:None)
    signal, _, out = v._build_realtime_v2_3_outputs_unlocked()
    assert out.holding.equals(gross.holding)
    assert not out.overheat_enabled.any()
    assert not out.overheat_block_entry_triggered.any()
    assert not bool(signal.iloc[0].overheat_enabled)
