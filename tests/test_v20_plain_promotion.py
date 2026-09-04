from __future__ import annotations

import itertools
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from hypothesis import given, settings, strategies as st

import microcap_top100_mom16_biweekly_live_v2_0 as v


@settings(max_examples=35, deadline=None)
@given(st.lists(st.tuples(st.booleans(), st.floats(min_value=-.04, max_value=.04, allow_nan=False)), min_size=1, max_size=25))
def test_fixed_one_identity_for_generated_state_sequences(samples):
    index = pd.bdate_range("2024-01-01", periods=len(samples))
    nxt = pd.Series(["long_microcap_short_zz1000" if x[0] else "cash" for x in samples], index=index)
    held = nxt.shift(1, fill_value="cash")
    gross = pd.DataFrame({"holding": held, "next_holding": nxt, "return": [x[1] for x in samples], "momentum_gap": .01}, index=index)
    turnover = pd.DataFrame(columns=["rebalance_date", "two_side_cost_rate"])
    reference = v.base_mod.apply_momentum_gap_no_peak_decay_cost_model(gross, turnover)
    actual = v.overlay_mod.apply_v2_0_execution(gross, turnover)
    np.testing.assert_allclose(actual.return_net, reference.return_net, atol=1e-12, rtol=0)
    np.testing.assert_allclose(actual.nav_net, reference.nav_net, atol=1e-12, rtol=0)
    assert actual.current_execution_scale.equals(held.ne("cash").astype(float))
    assert actual.next_session_actionable_scale.equals(nxt.ne("cash").astype(float))
    assert not actual.target_vol_enabled.any() and not actual.overheat_enabled.any()
    assert actual.financing_cost.eq(0).all() and actual.scale_change_cost.eq(0).all()
    signal = v.overlay_mod._build_signal_row(actual, {})
    assert signal.iloc[0].strategy_revision == v.STRATEGY_REVISION
    assert signal.iloc[0].max_leverage == 1
    assert not signal.iloc[0].target_vol_enabled


def test_path_reconfiguration_cannot_restore_old_targetvol_filename():
    v.overlay_mod.configure_output_paths()
    assert v.overlay_mod.COSTED_NAV_CSV.name == "microcap_top100_mom16_plain_fixed1_v2_0_costed_nav.csv"


@pytest.mark.parametrize("identity", ["spread_nav_log_wls_lb25_vol10_overheat", "microcap_only_log_wls_threshold_no_target_vol"])
def test_shared_signal_builder_does_not_mislabel_sibling_revision(identity):
    net = pd.DataFrame({"holding": ["cash"], "next_holding": ["cash"], "return_net": [0.],
                        "overlay_type": [identity]}, index=pd.DatetimeIndex(["2026-09-04"]))
    signal = v.overlay_mod._build_signal_row(net, {})
    assert signal.iloc[0].strategy_revision == identity


@pytest.mark.parametrize("mutate", ["approved", "source_sha256_lf", "candidate_frame_sha256", "previous_costed_nav_sha256", "unchanged_base_inputs", "authorization"])
def test_strategy_migration_rejects_any_changed_binding(tmp_path, monkeypatch, mutate):
    expected = {"authorization": "user_replace_existing_v2_0", "source_sha256_lf": "a", "candidate_frame_sha256": "b",
                "previous_costed_nav_sha256": "c", "unchanged_base_inputs": {"panel": "d"}}
    ns = v.overlay_mod.v2_0_rewrite_audit_matches_strategy_promotion.__globals__
    monkeypatch.setitem(ns, "strategy_promotion_evidence", lambda *args: expected)
    path = tmp_path / "report.json"
    payload = {"approved": True, **expected}
    path.write_text(json.dumps(payload), encoding="utf-8")
    args = (path, Path("previous"), pd.DataFrame(), Path("audit"))
    assert v.overlay_mod.v2_0_rewrite_audit_matches_strategy_promotion(*args)
    payload[mutate] = False if mutate == "approved" else "tampered"
    path.write_text(json.dumps(payload), encoding="utf-8")
    assert not v.overlay_mod.v2_0_rewrite_audit_matches_strategy_promotion(*args)


def test_strategy_migration_never_implicitly_approves():
    assert not v.overlay_mod.v2_0_rewrite_audit_matches_strategy_promotion(None, Path("x"), pd.DataFrame(), Path("y"))
