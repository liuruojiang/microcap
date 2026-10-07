"""Independent script arithmetic checks; synthetic fixtures are diagnostic only."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

import microcap_top100_mom16_biweekly_live_v2_3 as v23
import microcap_top100_mom16_biweekly_live_v2_5 as v25


def _prices(periods: int = 160) -> pd.DataFrame:
    dates = pd.bdate_range("2025-01-02", periods=periods)
    t = np.arange(periods, dtype=float)
    return pd.DataFrame(
        {
            "microcap": 100.0 * np.exp(0.08 * np.sin(t / 13.0) + 0.001 * t),
            "hedge": 200.0 * np.exp(0.02 * np.cos(t / 11.0) + 0.0003 * t),
        },
        index=dates,
    )


def _independent_wls(nav: pd.Series, lookback: int, halflife: float, trading_days: int) -> pd.DataFrame:
    """Solve the weighted least-squares system without production's centered formula."""
    x = np.arange(lookback, dtype=float)
    weights = 0.5 ** ((lookback - 1 - x) / halflife)
    design = np.column_stack([np.ones(lookback), x])
    weighted_design = design * np.sqrt(weights[:, None])
    y = np.log(nav.to_numpy(dtype=float))
    rows = []
    for end in range(lookback - 1, len(y)):
        values = y[end - lookback + 1 : end + 1]
        coefficients = np.linalg.lstsq(weighted_design, values * np.sqrt(weights), rcond=None)[0]
        total = np.sum(weights * (values - np.average(values, weights=weights)) ** 2)
        residual = np.sum(weights * (values - design @ coefficients) ** 2)
        r2 = 0.0 if total == 0.0 else float(np.clip(1.0 - residual / total, 0.0, 1.0))
        rows.append([coefficients[1] * trading_days, r2])
    return pd.DataFrame(rows, index=nav.index[lookback - 1 :], columns=["annualized_log_wls_score", "log_wls_r2"])


@pytest.mark.parametrize("module", [v23, v25], ids=["v23", "v25"])
def test_wls_matches_independent_weighted_linear_algebra(module) -> None:
    prices = _prices()
    nav = prices.microcap / prices.microcap.iloc[0]
    expected = _independent_wls(nav, module.LOOKBACK, module.HALFLIFE, module.TRADING_DAYS)
    actual = module.log_wls_score_and_r2(nav).loc[expected.index]
    np.testing.assert_allclose(actual.to_numpy(), expected.to_numpy(), rtol=0.0, atol=2e-12)


@pytest.mark.parametrize("level", [1.5, np.nextafter(1.5, np.inf), 2.0])
def test_v25_constant_nav_has_exact_zero_score(level: float) -> None:
    nav = pd.Series(level, index=pd.bdate_range("2025-01-02", periods=80))
    actual = v25.log_wls_score_and_r2(nav).dropna()
    assert actual.annualized_log_wls_score.eq(0.0).all()
    assert actual.log_wls_r2.eq(0.0).all()


def test_v25_flat_price_window_exits_the_zero_threshold_strategy() -> None:
    prices = pd.DataFrame(
        {"microcap": np.r_[np.linspace(100.0, 150.0, 25), np.full(55, 150.0)]},
        index=pd.bdate_range("2025-01-02", periods=80),
    )
    gross = v25.build_microcap_log_wls_gross(prices)
    assert gross.iloc[-1].annualized_log_wls_score == 0.0
    assert gross.iloc[-1].holding == "cash"
    assert gross.iloc[-1].next_holding == "cash"
    assert gross.iloc[-1]["return"] == 0.0


@pytest.mark.parametrize("include_hedge", [False, True])
def test_v25_numeric_string_prices_match_numeric_prices_without_mutation(include_hedge: bool) -> None:
    numeric = _prices()
    if not include_hedge:
        numeric = numeric[["microcap"]]
    text = numeric.astype(str)
    saved = text.copy(deep=True)
    actual = v25.build_microcap_log_wls_gross(text)
    expected = v25.build_microcap_log_wls_gross(numeric)
    pd.testing.assert_frame_equal(actual, expected, rtol=0.0, atol=1e-12)
    pd.testing.assert_frame_equal(text, saved, check_exact=True)


@pytest.mark.parametrize("timing", ["close", "next_open"])
def test_v25_holding_returns_and_costs_match_independent_daily_ledger(timing: str) -> None:
    prices = _prices()
    dates = prices.index[v25.LOOKBACK - 1 :]
    rebalance_dates = [dates[20], dates[70]]
    execution_dates = rebalance_dates if timing == "close" else [dates[21], dates[71]]
    turnover = pd.DataFrame(
        {
            "rebalance_date": rebalance_dates,
            "execution_date": execution_dates,
            "execution_timing": timing,
            "two_side_cost_rate": [0.006, 0.008],
        }
    )
    nav = prices.microcap / prices.microcap.iloc[0]
    scores = _independent_wls(nav, v25.LOOKBACK, v25.HALFLIFE, v25.TRADING_DAYS).annualized_log_wls_score
    next_active = scores.to_numpy() > 0.0
    active = np.r_[False, next_active[:-1]]
    raw = (prices.microcap / prices.microcap.shift(1) - 1.0).loc[dates].to_numpy()
    realized = np.where(active, raw, 0.0)
    cost_active = next_active if timing == "close" else active
    previous_active = active if timing == "close" else np.r_[False, active[:-1]]
    transition_cost = np.where(
        cost_active & ~previous_active,
        v25.v2_0.base_mod.freq_mod.cost_mod.ENTRY_COST,
        np.where(~cost_active & previous_active, v25.v2_0.base_mod.freq_mod.cost_mod.EXIT_COST, 0.0),
    )
    rebalance_cost = np.zeros(len(dates), dtype=float)
    for dt, cost in zip(execution_dates, [0.006, 0.008], strict=True):
        position = dates.searchsorted(dt, side="left")
        if cost_active[position] and previous_active[position]:
            rebalance_cost[position] += cost
    expected_cost = transition_cost + rebalance_cost
    expected_net = (1.0 + realized) * (1.0 - expected_cost) - 1.0
    actual = v25.build_v2_5_result(prices, turnover)
    assert actual.holding.ne("cash").tolist() == active.tolist()
    assert actual.next_holding.ne("cash").tolist() == next_active.tolist()
    np.testing.assert_allclose(actual.base_pre_cost_return, realized, rtol=0.0, atol=1e-15)
    np.testing.assert_allclose(actual.total_cost, expected_cost, rtol=0.0, atol=0.0)
    np.testing.assert_allclose(actual.return_net, expected_net, rtol=0.0, atol=1e-15)
    np.testing.assert_allclose(actual.nav_net, np.cumprod(1.0 + expected_net), rtol=0.0, atol=1e-13)
    assert actual.financing_cost.eq(0.0).all()
    assert actual.cash_day_yield.eq(0.0).all()
    assert actual.target_vol_enabled.eq(False).all()


def _costed_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "return_net": [-0.003, 0.01],
            "total_cost": [0.003, 0.0],
            "overlay_pre_cost_return": [0.0, 0.01],
            "holding": ["cash", "long_microcap_top100"],
            "next_holding": ["long_microcap_top100", "long_microcap_top100"],
        },
        index=pd.bdate_range("2025-01-02", periods=2),
    )


@pytest.mark.parametrize("column", ["return_net", "total_cost", "overlay_pre_cost_return"])
@pytest.mark.parametrize("bad_value", [np.nan, np.inf, -np.inf, "not-a-number"])
def test_v25_no_target_vol_rejects_nonfinite_costed_inputs(column: str, bad_value: object) -> None:
    frame = _costed_frame()
    frame[column] = frame[column].astype(object)
    frame.loc[frame.index[1], column] = bad_value
    with pytest.raises(ValueError, match=rf"{column}.*non-finite.*2025-01-03"):
        v25.apply_no_target_vol(frame)


@pytest.mark.parametrize("bad_cost", [-0.01, 1.0, 1.01])
def test_v25_no_target_vol_rejects_costs_outside_valid_range(bad_cost: float) -> None:
    frame = _costed_frame()
    frame.iloc[1, frame.columns.get_loc("total_cost")] = bad_cost
    with pytest.raises(ValueError, match="total_cost.*outside.*2025-01-03"):
        v25.apply_no_target_vol(frame)


@pytest.mark.parametrize("bad_return", [np.nan, np.inf, -np.inf])
def test_v25_apply_cost_rejects_corrupt_gross_return(bad_return: float) -> None:
    gross = v25.build_microcap_log_wls_gross(_prices())
    gross.loc[gross.index[-1], "return"] = bad_return
    with pytest.raises(ValueError, match="return.*non-finite"):
        v25.apply_cost(gross, pd.DataFrame())


@pytest.mark.parametrize("bad_nav", [0.0, -1.0, np.nan, np.inf, -np.inf, "not-a-number"])
def test_v25_wls_rejects_nonpositive_or_nonfinite_nav(bad_nav: object) -> None:
    nav = pd.Series(1.0, index=pd.bdate_range("2025-01-02", periods=80), dtype=object)
    nav.iloc[40] = bad_nav
    with pytest.raises(ValueError, match="NAV.*finite.*positive"):
        v25.log_wls_score_and_r2(nav)


@pytest.mark.parametrize("bad_return", [-1.0, -1.01])
def test_v25_no_target_vol_rejects_insolvent_returns(bad_return: float) -> None:
    frame = _costed_frame()
    frame.loc[frame.index[1], "return_net"] = bad_return
    frame.loc[frame.index[1], "overlay_pre_cost_return"] = bad_return
    with pytest.raises(ValueError, match="return_net.*at or below -1"):
        v25.apply_no_target_vol(frame)


@pytest.mark.parametrize("returns", [[1e200, 1e200], [-0.99] * 300], ids=["overflow", "underflow"])
def test_v25_no_target_vol_rejects_nonfinite_or_zero_accumulated_nav(returns: list[float]) -> None:
    frame = pd.DataFrame(
        {
            "return_net": returns,
            "overlay_pre_cost_return": returns,
            "total_cost": 0.0,
            "holding": "long_microcap_top100",
            "next_holding": "long_microcap_top100",
        },
        index=pd.bdate_range("2025-01-02", periods=len(returns)),
    )
    with pytest.raises(ValueError, match="NAV.*finite.*positive"):
        v25.apply_no_target_vol(frame)


def test_v25_no_target_vol_rejects_an_omitted_transaction_cost() -> None:
    frame = _costed_frame()
    frame.loc[frame.index[0], "return_net"] = 0.0
    with pytest.raises(ValueError, match="return_net.*cost arithmetic"):
        v25.apply_no_target_vol(frame)


def test_v25_no_target_vol_rejects_a_cash_day_with_market_return() -> None:
    frame = _costed_frame()
    frame.loc[frame.index[0], "return_net"] = (1.0 + 0.02) * (1.0 - 0.003) - 1.0
    frame.loc[frame.index[0], "overlay_pre_cost_return"] = 0.02
    with pytest.raises(ValueError, match="cash holding.*non-zero pre-cost return"):
        v25.apply_no_target_vol(frame)


def test_v25_future_prices_cannot_change_earlier_holdings_returns_or_costs() -> None:
    prices = _prices()
    full = v25.build_v2_5_result(prices, pd.DataFrame())
    earlier = v25.build_v2_5_result(prices.iloc[:95], pd.DataFrame())
    fields = ["holding", "next_holding", "return_net", "total_cost", "current_execution_scale"]
    pd.testing.assert_frame_equal(earlier[fields], full.loc[earlier.index, fields], rtol=0.0, atol=1e-12)
