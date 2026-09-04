"""Isolated synthetic counterexamples, not empirical strategy performance.

No output generation, data refresh, quotes, network calls or shared cache writes.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
from hypothesis import given, settings, strategies as st

import microcap_top100_mom16_biweekly_live_v2_0 as v20
import microcap_top100_mom16_biweekly_live_v2_3 as v23


@pytest.mark.parametrize("version", [v20.overlay_mod, v23], ids=["v20", "v23"])
def test_drawdown_counts_loss_from_initial_capital(version):
    ret = pd.Series([-.10, .01, 0.0], index=pd.bdate_range("2024-01-02", periods=3))
    assert version.summarize_returns(ret)["max_drawdown_pct"] == pytest.approx(-10.0)


@pytest.mark.parametrize("version", [v20.overlay_mod, v23], ids=["v20", "v23"])
def test_yearly_drawdown_counts_first_session_loss(version):
    ret = pd.Series([-.10, .01, 0.0], index=pd.bdate_range("2024-01-02", periods=3))
    assert version.summarize_yearly(ret).iloc[0]["max_drawdown_pct"] == pytest.approx(-10.0)


def test_base_metrics_count_initial_capital_loss():
    ret = pd.Series([-.10, .01, 0.0], index=pd.bdate_range("2024-01-02", periods=3))
    assert v20.base_mod.hedge_mod.calc_metrics(ret).max_dd == pytest.approx(-.10)
    assert v20.base_mod.calc_max_drawdown_from_returns(ret) == pytest.approx(-.10)


def test_initial_capital_drawdown_does_not_claim_false_recovery():
    ret = pd.Series([-.10, .01, 0.0], index=pd.bdate_range("2024-01-02", periods=3))
    info = v20.base_mod.freq_mod.cost_mod.calc_drawdown_info(ret)
    assert info["peak_date"] is None
    assert info["peak_basis"] == "initial_capital"
    assert info["peak_nav"] == 1.0
    assert info["trough_date"] == "2024-01-02"
    assert info["recovery_date"] is None


def test_observed_peak_and_initial_capital_recovery_dates():
    dates = pd.bdate_range("2024-01-02", periods=3)
    initial = v20.base_mod.freq_mod.cost_mod.calc_drawdown_info(pd.Series([-.1, .25, 0.], index=dates))
    assert initial["peak_basis"] == "initial_capital"
    assert initial["recovery_date"] == "2024-01-03"
    observed = v20.base_mod.freq_mod.cost_mod.calc_drawdown_info(pd.Series([.1, -.1, .2], index=dates))
    assert observed["peak_date"] == "2024-01-02"
    assert observed["peak_basis"] == "observed_nav"
    assert observed["recovery_date"] == "2024-01-04"


@pytest.mark.parametrize("version", [v20.overlay_mod, v23], ids=["v20", "v23"])
def test_short_history_does_not_claim_ten_year_coverage(version):
    ret = pd.Series(0.0, index=pd.bdate_range("2024-01-02", periods=20))
    windows = {row["window"]: row for row in version.summarize_required_windows(ret)}
    for label in ("last_10y", "last_5y", "last_3y", "last_1y"):
        assert windows[label]["unavailable_reason"], label
        assert pd.isna(windows[label]["annual_pct"]), label
    assert windows["full"]["unavailable_reason"] == ""


def _gross_from_gaps(gaps, microcap_returns):
    index = pd.bdate_range("2024-01-02", periods=len(gaps))
    return pd.DataFrame({"microcap_ret": microcap_returns, "hedge_ret": 0.0,
                         "microcap_mom": .01, "momentum_gap": gaps}, index=index)


@settings(max_examples=30, deadline=None, derandomize=True)
@given(st.lists(st.tuples(st.sampled_from([-.01, 0., .01]),
                         st.floats(min_value=-.04, max_value=.04, allow_nan=False)),
                min_size=2, max_size=35))
def test_future_gaps_cannot_change_realized_prefix(samples):
    """Causality and cash-return invariants across generated signal reversals."""
    raw = _gross_from_gaps([row[0] for row in samples], [row[1] for row in samples])
    turnover = pd.DataFrame(columns=["rebalance_date", "two_side_cost_rate"])
    gross = v20.base_mod.apply_momentum_gap_exit_buffer(raw, 0.0)
    whole = v20.overlay_mod.apply_v2_0_execution(gross, turnover)
    for cut in {1, len(samples) // 2, len(samples) - 1}:
        partial_gross = v20.base_mod.apply_momentum_gap_exit_buffer(raw.iloc[:cut], 0.0)
        prefix = v20.overlay_mod.apply_v2_0_execution(partial_gross, turnover)
        np.testing.assert_allclose(prefix.return_net, whole.return_net.iloc[:cut], rtol=0, atol=1e-12)
    assert whole.holding.iloc[1:].tolist() == whole.next_holding.iloc[:-1].tolist()
    fully_cash = whole.holding.eq("cash") & whole.next_holding.eq("cash")
    assert whole.loc[fully_cash, "return_net"].eq(0.0).all()
    assert whole.loc[whole.holding.eq("cash"), "base_pre_cost_return"].eq(0.0).all()
    np.testing.assert_allclose((1 + whole.base_pre_cost_return) * (1 - whole.total_cost) - 1,
                               whole.return_net, rtol=0, atol=1e-12)


def test_zero_gap_hysteresis_and_nan_signal_exit_are_explicit():
    raw = _gross_from_gaps([0.0, .01, 0.0, np.nan, 0.0], [0.0] * 5)
    gross = v20.base_mod.apply_momentum_gap_exit_buffer(raw, 0.0)
    assert gross.signal_on.tolist() == [False, True, True, False, False]


@pytest.mark.parametrize("bad_price", [0.0, -.01, np.nan, np.inf, -np.inf])
def test_official_close_validation_rejects_invalid_prices(bad_price):
    close = pd.DataFrame({"microcap": [100., bad_price], "hedge": [100., 100.]},
                         index=pd.bdate_range("2024-01-02", periods=2))
    with pytest.raises(ValueError):
        v20.overlay_mod.validate_close_df(close)
