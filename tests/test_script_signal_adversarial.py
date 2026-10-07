"""Offline script-contract attacks; synthetic fixtures are diagnostic only."""
from contextlib import contextmanager, nullcontext
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

import microcap_top100_mom16_biweekly_live_v2_3 as v23
import microcap_top100_mom16_biweekly_live_v2_0 as v20
import microcap_top100_mom16_biweekly_live_v2_5 as v25
from scripts import top100_delivery as delivery


ACTIVE = "long_microcap_short_zz1000"


def _realtime_identity(**overrides):
    row = {
        "date": "2026-09-30",
        "quote_trade_date": "2026-09-30",
        "latest_anchor_trade_date": "2026-09-29",
        "snapshot_time": "2026-09-30T15:00:25+08:00",
        "signal_timing": "intraday_hypothetical_if_now_close",
        "official_close_confirmed_signal": False,
        "current_holding": ACTIVE,
        "next_holding": ACTIVE,
        "signal_label": ACTIVE,
        "trade_state": "hold",
        "current_execution_scale": 1.0,
        "next_session_actionable_scale": 1.0,
        "version": v23.VERSION,
        "strategy_version": f"v{v23.VERSION}",
        "strategy_revision": v23.STRATEGY_REVISION,
        "lookback": v23.LOOKBACK,
        "halflife": v23.HALFLIFE,
        "r2_entry_gate": v23.R2_ENTRY_GATE,
        "momentum_gap_entry_threshold": v23.MOMENTUM_GAP_ENTRY_THRESHOLD,
        "momentum_gap_exit_buffer": v23.MOMENTUM_GAP_EXIT_BUFFER,
        "signal_spread_hedge_ratio": v23.SIGNAL_SPREAD_HEDGE_RATIO,
        "execution_hedge_ratio": v23.EXECUTION_HEDGE_RATIO,
        "overheat_feature_window": v23.OVERHEAT_FEATURE_WINDOW,
        "overheat_trigger_threshold": v23.OVERHEAT_TRIGGER_THRESHOLD,
        "overheat_recovery_threshold": v23.OVERHEAT_RECOVERY_THRESHOLD,
        "overheat_enabled": True,
        "r2_gate_enabled": False,
        "target_vol_enabled": False,
        "cash_day_yield_enabled": False,
        "financing_enabled": False,
    }
    row.update(overrides)
    return row


def _write_realtime(path, **overrides):
    pd.DataFrame([_realtime_identity(**overrides)]).to_csv(path, index=False)


def _net(current=ACTIVE, next_holding=ACTIVE, feature=.21):
    idx = pd.DatetimeIndex(["2026-09-29", "2026-09-30"])
    current_scale = float(current != "cash")
    next_scale = float(next_holding != "cash")
    return pd.DataFrame({
        "holding": ["cash", current],
        "next_holding": [current, next_holding],
        "current_execution_scale": [0., current_scale],
        "execution_scale": [0., current_scale],
        "next_session_target_scale": [current_scale, next_scale],
        "next_session_actionable_scale": [current_scale, next_scale],
        "target_vol_scale_next_session": [current_scale, next_scale],
        "annualized_log_wls_score": [.1, .2],
        "momentum_gap": [.1, .2],
        "log_wls_r2": [.6, .7],
        "spread_nav": [1., 1.02],
        "overheat_feature_value": [.2, feature],
        "overheat_risk_off": [False, False],
        "overheat_exit_triggered": [False, False],
        "overheat_reentry_triggered": [False, False],
        "overheat_block_entry_triggered": [False, False],
        "target_vol_enabled": [False, False],
    }, index=idx)


def test_current_summary_cannot_certify_retired_realtime_artifact(tmp_path, monkeypatch):
    realtime = tmp_path / "realtime.csv"
    summary = tmp_path / "summary.json"
    summary.write_text("{}", encoding="utf-8")
    _write_realtime(realtime, strategy_revision="retired_revision", target_vol_enabled=True)
    monkeypatch.setattr(v23, "SUMMARY_JSON", summary)
    monkeypatch.setattr(v23, "REALTIME_SIGNAL_CSV", realtime)
    monkeypatch.setattr(v23, "summary_matches_current_v2_3_base", lambda _summary: True)
    assert v23.incompatible_v2_3_outputs() == [realtime]


def test_retired_realtime_artifact_is_not_unconditionally_protected(tmp_path, monkeypatch):
    realtime = tmp_path / "realtime.csv"
    _write_realtime(realtime, strategy_revision="retired_revision")
    monkeypatch.setattr(v23, "REALTIME_SIGNAL_CSV", realtime)
    assert v23._stale_outputs_to_remove_after_generate([realtime], set()) == [realtime]


@pytest.mark.parametrize("field,bad", [
    ("version", "2.0"), ("strategy_version", "v2.5"),
    ("strategy_revision", "retired_revision"), ("lookback", 17),
    ("halflife", 3.), ("r2_entry_gate", .08),
    ("momentum_gap_entry_threshold", .1), ("momentum_gap_exit_buffer", .09),
    ("signal_spread_hedge_ratio", .8), ("execution_hedge_ratio", 1.),
    ("overheat_feature_window", 60), ("overheat_trigger_threshold", .23),
    ("overheat_recovery_threshold", .19), ("overheat_enabled", False),
    ("r2_gate_enabled", True), ("target_vol_enabled", True),
    ("cash_day_yield_enabled", True), ("financing_enabled", True),
])
def test_realtime_identity_rejects_missing_or_contradictory_core_fields(tmp_path, field, bad):
    realtime = tmp_path / "realtime.csv"
    _write_realtime(realtime)
    assert v23.realtime_signal_matches_current_v2_3(realtime)
    _write_realtime(realtime, **{field: bad})
    assert not v23.realtime_signal_matches_current_v2_3(realtime)
    row = _realtime_identity()
    row.pop(field)
    pd.DataFrame([row]).to_csv(realtime, index=False)
    assert not v23.realtime_signal_matches_current_v2_3(realtime)


@pytest.mark.parametrize("payload", ["", "{broken", "version\n2.3\n"])
def test_malformed_realtime_identity_is_incompatible(tmp_path, payload):
    realtime = tmp_path / "realtime.csv"
    realtime.write_text(payload, encoding="utf-8")
    assert not v23.realtime_signal_matches_current_v2_3(realtime)


def test_realtime_identity_rejects_multiple_final_rows(tmp_path):
    realtime = tmp_path / "realtime.csv"
    pd.DataFrame([_realtime_identity(), _realtime_identity()]).to_csv(realtime, index=False)
    assert not v23.realtime_signal_matches_current_v2_3(realtime)


def test_invalid_realtime_cleanup_keeps_concurrent_current_writer(tmp_path, monkeypatch):
    realtime = tmp_path / "realtime.csv"
    _write_realtime(realtime, strategy_revision="retired_revision")
    monkeypatch.setattr(v23, "REALTIME_SIGNAL_CSV", realtime)

    @contextmanager
    def concurrent_writer():
        _write_realtime(realtime)
        yield

    monkeypatch.setattr(v23, "v2_3_realtime_output_lock", concurrent_writer)
    v23._remove_stale_outputs_after_generate([realtime], set())
    assert realtime.exists()
    assert v23.realtime_signal_matches_current_v2_3(realtime)


def test_invalid_realtime_cleanup_removes_only_invalid_signal(tmp_path, monkeypatch):
    realtime = tmp_path / "realtime.csv"
    unrelated = tmp_path / "unrelated.csv"
    _write_realtime(realtime, strategy_revision="retired_revision")
    unrelated.write_text("preserve", encoding="utf-8")
    monkeypatch.setattr(v23, "REALTIME_SIGNAL_CSV", realtime)
    monkeypatch.setattr(v23, "v2_3_realtime_output_lock", nullcontext)
    v23._remove_stale_outputs_after_generate([realtime], set())
    assert not realtime.exists()
    assert unrelated.read_text(encoding="utf-8") == "preserve"


@pytest.mark.parametrize("feature", [.21, np.nan])
def test_v23_signal_never_inherits_v20_risk_metric(feature):
    poisoned_reference = {"latest_signal": {
        "overheat_metric": 999., "overheat_feature_value": 999.,
        "gap_peak": 777., "gap_decay_ratio": 666.,
        "trade_return_net": 888., "latest_realized_vol": 555.,
        "return_gross_base": 444.,
        "blocked_until_signal_reset": True, "signal_reset_seen": True,
    }}
    signal = v23._build_signal_row(_net(feature=feature), poisoned_reference).iloc[0]
    if pd.isna(feature):
        assert pd.isna(signal["overheat_metric"])
        assert pd.isna(signal["overheat_feature_value"])
    else:
        assert signal["overheat_metric"] == pytest.approx(feature)
        assert signal["overheat_feature_value"] == pytest.approx(feature)
    for field in ("gap_peak", "gap_decay_ratio", "trade_return_net",
                  "latest_realized_vol", "return_gross_base"):
        assert pd.isna(signal[field]), field
    assert not signal["blocked_until_signal_reset"]
    assert not signal["signal_reset_seen"]


@pytest.mark.parametrize("current,next_holding,trade,turnover", [
    ("cash", "cash", "hold", 0.),
    ("cash", ACTIVE, "open", 1.8),
    (ACTIVE, "cash", "close", 1.8),
    (ACTIVE, ACTIVE, "hold", 0.),
])
def test_signal_projection_uses_executed_and_next_holdings(current, next_holding, trade, turnover):
    result = _net(current=current, next_holding=next_holding)
    signal = v23._build_signal_row(result, {"latest_signal": {
        "current_holding": "cash", "next_holding": "cash",
    }}).iloc[0]
    assert signal["current_holding"] == current
    assert signal["next_holding"] == next_holding
    assert signal["signal_label"] == next_holding
    assert signal["trade_state"] == trade
    assert signal["next_session_leg_turnover"] == pytest.approx(turnover)
    assert signal["date"] == result.index[-1]
    assert signal["current_execution_scale"] == float(current != "cash")
    assert signal["next_session_actionable_scale"] == float(next_holding != "cash")


@pytest.mark.parametrize("level", [1.5, 2., 5.])
def test_constant_positive_nav_scores_exactly_zero(level):
    nav = pd.Series(np.full(80, level), index=pd.bdate_range("2025-01-02", periods=80))
    out = v23.log_wls_score_and_r2(nav).dropna()
    assert len(out) == len(nav) - v23.LOOKBACK + 1
    assert out["annualized_log_wls_score"].eq(0.).all()
    assert out["log_wls_r2"].eq(0.).all()


@pytest.mark.parametrize("invalid", [0., -1., np.nan, np.inf, -np.inf, "not-a-number"])
def test_log_wls_rejects_invalid_nav(invalid):
    nav = pd.Series([1.] * 40, dtype=object)
    nav.iloc[10] = invalid
    with pytest.raises(ValueError, match="finite positive"):
        v23.log_wls_score_and_r2(nav)


def test_spread_nav_fails_closed_when_positive_prices_imply_ruin():
    prices = pd.DataFrame({"microcap": [100., 100., 101.], "hedge": [100., 230., 100.]},
                          index=pd.bdate_range("2025-01-02", periods=3))
    with pytest.raises(ValueError, match="spread.*(non-positive|ruin)"):
        v23.always_on_spread_nav(prices)


def test_spread_nav_fails_closed_on_derived_nonfinite_returns():
    prices = pd.DataFrame({"microcap": [1e-300, 1e300], "hedge": [100., 100.]},
                          index=pd.bdate_range("2025-01-02", periods=2))
    with pytest.raises(ValueError, match="spread.*non-finite"):
        v23.always_on_spread_nav(prices)


def test_wls_matches_independent_weighted_regression_and_scale_invariance():
    dates = pd.bdate_range("2025-01-02", periods=60)
    step = np.arange(60, dtype=float)
    nav = pd.Series(np.exp(.002 * step + .01 * np.sin(step / 7)), index=dates)
    out = v23.log_wls_score_and_r2(nav)
    weights = np.asarray(v23.exp_weights())
    x = np.column_stack([np.ones(v23.LOOKBACK), np.arange(v23.LOOKBACK)])
    for end in (24, 31, 59):
        y = np.log(nav.iloc[end - v23.LOOKBACK + 1:end + 1].to_numpy())
        beta = np.linalg.lstsq(x * np.sqrt(weights)[:, None], y * np.sqrt(weights), rcond=None)[0]
        predicted = x @ beta
        ybar = np.average(y, weights=weights)
        r2 = 1. - np.sum(weights * (y - predicted) ** 2) / np.sum(weights * (y - ybar) ** 2)
        assert out.iloc[end]["annualized_log_wls_score"] == pytest.approx(beta[1] * v23.TRADING_DAYS, abs=1e-12)
        assert out.iloc[end]["log_wls_r2"] == pytest.approx(r2, abs=1e-12)
    scaled = v23.log_wls_score_and_r2(nav * 1e100)
    np.testing.assert_allclose(out, scaled, rtol=0., atol=1e-10, equal_nan=True)


@pytest.mark.parametrize("version", ["0", "3", "5"])
@pytest.mark.parametrize("current,next_active", [(False, False), (False, True), (True, False), (True, True)])
def test_delivery_action_gate_accepts_all_normal_native_transitions(version, current, next_active):
    module = {"0": v20.overlay_mod, "3": v23, "5": v25}[version]
    label = "long_microcap_top100" if version == "5" else ACTIVE
    net = _net(label if current else "cash", label if next_active else "cash")
    if version == "0":
        # v2.0 publishes plain momentum fields; its native NAV has no WLS score.
        net = net.drop(columns=["annualized_log_wls_score", "log_wls_r2"])
    if version == "5":
        net["microcap_nav"] = [1., 1.02]
    signal = module._build_signal_row(net, {"latest_signal": {}}).iloc[0].to_dict()
    latest = net.iloc[-1].to_dict()
    delivery.validate_final_signal(signal, latest, version)


def _v25_diagnostic_costed(rows=2):
    # Economic contract fixture, not a strategy/backtest return series.
    dates = pd.bdate_range("2025-01-02", periods=rows)
    return pd.DataFrame({
        "holding": ["long_microcap_top100"] * rows,
        "next_holding": ["long_microcap_top100"] * rows,
        "return_net": [.00697] * rows,
        "total_cost": [.003] * rows,
        "overlay_pre_cost_return": [.01] * rows,
    }, index=dates)


@pytest.mark.parametrize("ret", [-1., -1.1])
def test_independent_v25_attack_rejects_account_ruin(ret):
    costed = _v25_diagnostic_costed()
    costed["return_net"] = ret
    with pytest.raises((ValueError, RuntimeError)):
        v25.apply_no_target_vol(costed)


def test_independent_v25_attack_rejects_cumulative_overflow():
    costed = _v25_diagnostic_costed()
    costed["return_net"] = 1e308
    costed["overlay_pre_cost_return"] = 1e308
    costed["total_cost"] = 0.
    with pytest.raises((ValueError, RuntimeError)):
        v25.apply_no_target_vol(costed)


def test_independent_v25_attack_rejects_net_cost_disagreement():
    costed = _v25_diagnostic_costed()
    costed["return_net"] = .01
    with pytest.raises((ValueError, RuntimeError)):
        v25.apply_no_target_vol(costed)


def test_independent_v25_attack_rejects_cash_gross_leakage():
    costed = _v25_diagnostic_costed()
    costed["holding"] = "cash"
    costed["next_holding"] = "cash"
    costed["total_cost"] = 0.
    costed["return_net"] = .01
    with pytest.raises((ValueError, RuntimeError)):
        v25.apply_no_target_vol(costed)


@pytest.mark.parametrize("levels", [(100., 100.), (100., 150.)])
def test_independent_v25_flat_tail_scores_zero_and_closes(levels):
    dates = pd.bdate_range("2025-01-02", periods=80)
    prices = np.r_[np.linspace(*levels, 25), np.full(55, levels[1])]
    numeric = pd.DataFrame({"microcap": prices}, index=dates)
    text = numeric.astype(str)
    gross = v25.build_microcap_log_wls_gross(numeric)
    text_gross = v25.build_microcap_log_wls_gross(text)
    pd.testing.assert_frame_equal(text_gross, gross)
    assert gross.iloc[-1]["annualized_log_wls_score"] == 0.
    assert gross.iloc[-1]["next_holding"] == "cash"


def _v23_gross_diagnostic(returns):
    dates = pd.bdate_range("2025-01-02", periods=len(returns))
    return pd.DataFrame({"holding": ACTIVE, "next_holding": ACTIVE,
                         "return": returns, "spread_nav": 1.}, index=dates)


def _patch_v23_diagnostic_feature(gross, monkeypatch):
    monkeypatch.setattr(v23, "_overheat_feature_series", lambda _: pd.Series(0., index=gross.index))


@pytest.mark.parametrize("invalid", [-1., -1.1])
def test_v23_overheat_rejects_active_ruin(invalid, monkeypatch):
    gross = _v23_gross_diagnostic([invalid])
    _patch_v23_diagnostic_feature(gross, monkeypatch)
    with pytest.raises(ValueError, match="(return|NAV).*(below|non-positive|ruin)"):
        v23.apply_overheat_defense(gross, pd.DataFrame())


@pytest.mark.parametrize("rate", [-.01, 1., 2., np.nan, np.inf])
def test_v23_overheat_rejects_invalid_mapped_cost(rate, monkeypatch):
    gross = _v23_gross_diagnostic([.01])
    _patch_v23_diagnostic_feature(gross, monkeypatch)
    monkeypatch.setattr(v20.base_mod.freq_mod.cost_mod, "map_rebalance_apply_costs",
                        lambda *_: pd.Series(rate, index=gross.index))
    with pytest.raises(ValueError, match="cost"):
        v23.apply_overheat_defense(gross, pd.DataFrame())


@pytest.mark.parametrize("returns", [[1e308, 1e308], [-.9] * 400], ids=["overflow", "underflow"])
def test_v23_overheat_rejects_nonfinite_or_nonpositive_cumulative_nav(returns, monkeypatch):
    gross = _v23_gross_diagnostic(returns)
    _patch_v23_diagnostic_feature(gross, monkeypatch)
    with pytest.raises(ValueError, match="(NAV|nav).*(non-finite|non-positive)"):
        v23.apply_overheat_defense(gross, pd.DataFrame())


def test_v23_overheat_cash_diagnostic_return_is_not_realized_debt(monkeypatch):
    gross = _v23_gross_diagnostic([-1.1, np.nan, np.inf])
    gross["holding"] = "cash"
    gross["next_holding"] = "cash"
    _patch_v23_diagnostic_feature(gross, monkeypatch)
    out = v23.apply_overheat_defense(gross, pd.DataFrame())
    assert out["return_net"].eq(0.).all()
    assert out["nav_net"].eq(1.).all()
