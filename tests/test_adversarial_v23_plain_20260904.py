"""Isolated adversarial contracts; synthetic fixtures are NOT backtest evidence."""

import itertools

import numpy as np
import pandas as pd
import pytest

import microcap_top100_mom16_biweekly_live_v2_3 as v


ACTIVE = "long_microcap_short_zz1000"


def _diagnostic_prices(periods=100):
    index = pd.bdate_range("2025-01-02", periods=periods)
    steps = np.arange(periods, dtype=float)
    return pd.DataFrame(
        {
            "microcap": 100 * np.cumprod(1 + .003 + .001 * np.sin(steps / 4)),
            "hedge": 200 * np.cumprod(1 + .0002 + .0001 * np.cos(steps / 6)),
        }, index=index,
    )


@pytest.mark.parametrize("diagnostic_window", [1, 60, 120])
def test_disabled_r2_window_cannot_change_signal_calendar(monkeypatch, diagnostic_window):
    """The promoted source explicitly labels R2_WINDOW diagnostic-only when OFF."""
    prices = _diagnostic_prices()
    monkeypatch.setattr(v, "R2_ENTRY_GATE", 0.)
    reference = v.build_spread_log_wls_gross(prices)
    monkeypatch.setattr(v, "R2_WINDOW", diagnostic_window)
    changed = v.build_spread_log_wls_gross(prices)
    pd.testing.assert_frame_equal(
        changed[["annualized_log_wls_score", "holding", "next_holding", "return"]],
        reference[["annualized_log_wls_score", "holding", "next_holding", "return"]],
    )


def test_disabled_r2_nan_cannot_veto_valid_score_entry(monkeypatch):
    """A diagnostic-only R2 missing value must not veto a finite positive score."""
    prices = _diagnostic_prices()
    baseline = v.build_spread_log_wls_gross(prices)
    real_score = v.log_wls_score_and_r2

    def score_with_missing_diagnostic(*args, **kwargs):
        frame = real_score(*args, **kwargs)
        frame["log_wls_r2"] = np.nan
        return frame

    monkeypatch.setattr(v, "log_wls_score_and_r2", score_with_missing_diagnostic)
    actual = v.build_spread_log_wls_gross(prices, baseline.index)
    assert actual["next_holding"].equals(baseline["next_holding"])
    assert actual["r2_gate_pass"].all()


def test_enabled_r2_nan_still_blocks_entry(monkeypatch):
    prices = _diagnostic_prices()
    baseline = v.build_spread_log_wls_gross(prices)
    real_score = v.log_wls_score_and_r2

    def score_with_missing_diagnostic(*args, **kwargs):
        frame = real_score(*args, **kwargs)
        frame["log_wls_r2"] = np.nan
        return frame

    monkeypatch.setattr(v, "R2_ENTRY_GATE", .08)
    monkeypatch.setattr(v, "log_wls_score_and_r2", score_with_missing_diagnostic)
    actual = v.build_spread_log_wls_gross(prices, baseline.index)
    assert actual["next_holding"].eq("cash").all()


def test_overheat_exhaustive_short_state_paths_preserve_lag_and_cost(monkeypatch):
    """Exhaust all 3-session base decisions and cool/band/hot threshold paths."""
    dates = pd.bdate_range("2025-01-02", periods=3)
    for decisions in itertools.product([False, True], repeat=3):
        base_current = [False, *decisions[:-1]]
        gross = pd.DataFrame({
            "holding": np.where(base_current, ACTIVE, "cash"),
            "next_holding": np.where(decisions, ACTIVE, "cash"),
            "return": np.where(base_current, [.01, -.02, .03], 0),
            "spread_nav": [1., 1.01, .99],
        }, index=dates)
        for features in itertools.product([.20, .23, .26], repeat=3):
            monkeypatch.setattr(v, "_overheat_feature_series",
                                lambda _frame, values=features: pd.Series(values, index=dates))
            out = v.apply_overheat_defense(gross, pd.DataFrame())
            active = out.holding.ne("cash")
            next_active = out.next_holding.ne("cash")
            assert active.tolist() == [False, *next_active.tolist()[:-1]]
            assert not (next_active & ~pd.Series(decisions, index=dates)).any()
            expected_raw = gross["return"].where(active, 0.)
            np.testing.assert_allclose(out.overlay_pre_cost_return, expected_raw, atol=0, rtol=0)
            expected_cost = np.where(
                ~active & next_active, v.v2_0.base_mod.freq_mod.cost_mod.ENTRY_COST,
                np.where(active & ~next_active, v.v2_0.base_mod.freq_mod.cost_mod.EXIT_COST, 0.),
            )
            np.testing.assert_allclose(out.total_cost, expected_cost, atol=0, rtol=0)
            np.testing.assert_allclose(out.return_net, (1 + expected_raw) * (1 - expected_cost) - 1,
                                       atol=1e-15, rtol=0)
            assert out.loc[~active & ~next_active, "return_net"].eq(0.).all()


def test_synthetic_price_path_prefix_invariance():
    """Later fixture prices cannot alter any earlier decision or costed return."""
    prices = _diagnostic_prices(140)
    full = v.build_v2_3_result(prices, pd.DataFrame())
    for cut in [40, 65, 95, 120]:
        prefix = v.build_v2_3_result(prices.iloc[:cut], pd.DataFrame())
        pd.testing.assert_frame_equal(prefix, full.loc[prefix.index], check_exact=False,
                                      atol=1e-12, rtol=0)


def test_summary_drawdown_includes_initial_capital_and_year_reset():
    ret = pd.Series([-.10, .01, -.20, .02],
                    index=pd.to_datetime(["2024-12-30", "2024-12-31", "2025-01-02", "2025-01-03"]))
    assert v.summarize_returns(ret)["max_drawdown_pct"] == pytest.approx(-27.28)
    yearly = v.summarize_yearly(ret).set_index("year")
    assert yearly.loc["2024", "max_drawdown_pct"] == pytest.approx(-10.)
    assert yearly.loc["2025", "max_drawdown_pct"] == pytest.approx(-20.)


def test_short_history_is_not_labeled_multiyear_performance():
    ret = pd.Series(.001, index=pd.bdate_range("2025-01-02", periods=20))
    windows = v.summarize_required_windows(ret)
    assert windows[0]["window"] == "full"
    assert windows[0]["days"] == 20
    for row in windows[1:]:
        assert row["days"] == 0
        assert pd.isna(row["annual_pct"])
        assert pd.isna(row["max_drawdown_pct"])
        assert "insufficient history" in row["unavailable_reason"]
