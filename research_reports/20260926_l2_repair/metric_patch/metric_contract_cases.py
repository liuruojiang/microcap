"""The first saved return is measured from initial capital, including its costs."""

import pandas as pd
import pytest

import microcap_top100_mom16_biweekly_live_v2_0 as v20
import microcap_top100_mom16_biweekly_live_v2_3 as v23
import microcap_top100_mom16_biweekly_live_v2_5 as v25


@pytest.mark.parametrize("module", [v20.overlay_mod, v23, v25])
def test_annualization_counts_every_return_day(module):
    returns = pd.Series([-0.10, 0.02], index=pd.bdate_range("2024-01-02", periods=2))
    days_per_year = module.TRADING_DAYS if hasattr(module, "TRADING_DAYS") else module.TARGET_VOL_TRADING_DAYS
    expected = ((0.90 * 1.02) ** (days_per_year / 2) - 1.0) * 100.0
    assert module.summarize_returns(returns)["annual_pct"] == pytest.approx(expected)


@pytest.mark.parametrize("module", [v20.overlay_mod, v23, v25])
def test_yearly_annualization_counts_all_eligible_days(module):
    returns = pd.Series([-.10] + [0.0] * 59, index=pd.bdate_range("2024-01-02", periods=60))
    days_per_year = module.TRADING_DAYS if hasattr(module, "TRADING_DAYS") else module.TARGET_VOL_TRADING_DAYS
    expected = (0.90 ** (days_per_year / 60) - 1.0) * 100.0
    assert module.summarize_yearly(returns).iloc[0]["annual_pct"] == pytest.approx(expected)


def test_v25_drawdown_includes_initial_capital_in_full_and_yearly_windows():
    returns = pd.Series([-.10, .01, 0.0], index=pd.bdate_range("2024-01-02", periods=3))
    assert v25.summarize_returns(returns)["max_drawdown_pct"] == pytest.approx(-10.0)
    assert v25.summarize_yearly(returns).iloc[0]["max_drawdown_pct"] == pytest.approx(-10.0)
