from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

import microcap_top100_mom16_biweekly_live_v2_3 as v2_3


ACTIVE_HOLDING = "long_microcap_short_zz1000"


def _nonlinear_close_df(periods: int = 120) -> pd.DataFrame:
    index = pd.bdate_range("2020-01-02", periods=periods)
    step = np.arange(periods, dtype=float)
    microcap_return = 0.0012 + 0.006 * np.sin(step / 5.0) + 0.002 * np.cos(step / 11.0)
    hedge_return = 0.0004 + 0.004 * np.cos(step / 7.0) - 0.001 * np.sin(step / 13.0)
    return pd.DataFrame(
        {
            "microcap": 100.0 * np.cumprod(1.0 + microcap_return),
            "hedge": 200.0 * np.cumprod(1.0 + hedge_return),
        },
        index=index,
    )


def test_numeric_string_close_prices_are_normalized_without_mutating_input() -> None:
    index = pd.bdate_range("2026-01-05", periods=4)
    text_prices = pd.DataFrame(
        {
            "microcap": ["100.0", "101.0", "102.5", "102.0"],
            "hedge": ["200.0", "201.0", "200.5", "202.0"],
        },
        index=index,
    )
    numeric_prices = text_prices.astype(float)

    normalized = v2_3.validate_close_df(text_prices)

    assert normalized is not text_prices
    assert normalized["microcap"].dtype.kind == "f"
    assert normalized["hedge"].dtype.kind == "f"
    assert text_prices["microcap"].dtype == object
    assert text_prices["hedge"].dtype == object
    pd.testing.assert_frame_equal(normalized, numeric_prices)

    actual = v2_3.always_on_spread_nav(text_prices)
    expected = v2_3.always_on_spread_nav(numeric_prices)
    for actual_value, expected_value in zip(actual[:3], expected[:3], strict=True):
        pd.testing.assert_series_equal(actual_value, expected_value)
    assert actual[3] == pytest.approx(expected[3])


@pytest.mark.parametrize("bad_return", [np.nan, np.inf, -np.inf, "not-a-number"])
def test_active_nonfinite_return_fails_closed(
    bad_return: object,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    index = pd.bdate_range("2026-01-05", periods=2)
    gross = pd.DataFrame(
        {
            "return": [bad_return, 0.0],
            "holding": [ACTIVE_HOLDING, ACTIVE_HOLDING],
            "next_holding": [ACTIVE_HOLDING, ACTIVE_HOLDING],
            "spread_nav": [1.0, 1.01],
        },
        index=index,
    )
    monkeypatch.setattr(
        v2_3,
        "_overheat_feature_series",
        lambda _gross: pd.Series(0.0, index=index),
    )

    with pytest.raises(ValueError, match=r"active return is non-finite.*2026-01-05"):
        v2_3.apply_overheat_defense(gross, pd.DataFrame())


def test_default_r2_window_preserves_the_same_window_calculation() -> None:
    close_df = _nonlinear_close_df()
    spread_nav, *_ = v2_3.always_on_spread_nav(close_df)

    implicit = v2_3.log_wls_score_and_r2(spread_nav)
    explicit = v2_3.log_wls_score_and_r2(
        spread_nav,
        lookback=v2_3.LOOKBACK,
        halflife=v2_3.HALFLIFE,
        r2_window=v2_3.LOOKBACK,
    )

    pd.testing.assert_frame_equal(implicit, explicit, check_exact=True)


def test_build_uses_r2_window_independently_from_score_lookback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    close_df = _nonlinear_close_df()
    baseline = v2_3.build_spread_log_wls_gross(close_df)

    monkeypatch.setattr(v2_3, "R2_WINDOW", 7)
    shorter_r2 = v2_3.build_spread_log_wls_gross(close_df)

    pd.testing.assert_series_equal(
        shorter_r2["annualized_log_wls_score"],
        baseline["annualized_log_wls_score"],
        check_exact=True,
    )
    common = baseline["log_wls_r2"].dropna().index.intersection(shorter_r2["log_wls_r2"].dropna().index)
    assert len(common) > 0
    assert not np.allclose(
        baseline.loc[common, "log_wls_r2"],
        shorter_r2.loc[common, "log_wls_r2"],
        rtol=0.0,
        atol=1e-12,
    )
    assert shorter_r2["r2_window"].eq(7).all()
