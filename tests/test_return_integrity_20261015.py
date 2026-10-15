"""Return-integrity regressions; all fixtures are isolated fault injections.

These tests exercise the actual execution and price-building functions without
refreshing, writing, or reporting any production strategy artifacts.
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

import microcap_top100_mom16_biweekly_live_v2_0 as v20


ACTIVE = "long_microcap_short_zz1000"


def _events() -> pd.DataFrame:
    return pd.DataFrame(columns=[
        "rebalance_date", "execution_date", "execution_timing", "two_side_cost_rate",
    ])


def _gross() -> pd.DataFrame:
    return pd.DataFrame({
        "holding": ["cash", ACTIVE, ACTIVE, "cash", "cash"],
        "next_holding": [ACTIVE, ACTIVE, "cash", "cash", "cash"],
        "return": [np.nan, 0.01, -0.02, np.nan, 0.0],
        "momentum_gap": [0.1, 0.1, -0.1, -0.1, np.nan],
    }, index=pd.bdate_range("2026-01-05", periods=5))


@pytest.mark.parametrize("bad", [np.nan, pd.NA, "damaged", np.inf, -np.inf])
def test_official_execution_rejects_invalid_held_return(bad) -> None:
    gross = _gross().astype({"return": object})
    date = gross.index[1]
    gross.at[date, "return"] = bad
    before = gross.copy(deep=True)
    with pytest.raises(ValueError, match=f"non-finite active return at {date.date()}"):
        v20.overlay_mod.apply_v2_0_execution(gross, _events())
    pd.testing.assert_frame_equal(gross, before)


def test_normal_execution_preserves_cash_warmup_and_cost_ledger() -> None:
    gross = _gross()
    events = pd.DataFrame({
        "rebalance_date": [gross.index[1]],
        "execution_date": [gross.index[1]],
        "execution_timing": ["close"],
        "two_side_cost_rate": [0.0012],
    })
    result = v20.overlay_mod.apply_v2_0_execution(gross, events)
    entry = v20.cost_mod.ENTRY_COST
    exit_cost = v20.cost_mod.EXIT_COST
    expected = np.array([-entry, 1.01 * (1.0 - 0.0012) - 1.0,
                         0.98 * (1.0 - exit_cost) - 1.0, 0.0, 0.0])
    np.testing.assert_allclose(result.return_net, expected, rtol=0.0, atol=1e-15)
    np.testing.assert_allclose(result.nav_net, np.cumprod(1.0 + expected), rtol=0.0, atol=1e-15)
    np.testing.assert_array_equal(result.overlay_pre_cost_return, [0.0, 0.01, -0.02, 0.0, 0.0])
    np.testing.assert_array_equal(result.total_cost, [entry, 0.0012, exit_cost, 0.0, 0.0])
    assert result.holding.tolist() == gross.holding.tolist()
    assert result.next_holding.tolist() == gross.next_holding.tolist()


def test_finite_positive_prices_cannot_hide_overflow_as_zero_held_return() -> None:
    dates = pd.bdate_range("2024-01-02", periods=90)
    t = np.arange(len(dates), dtype=float)
    scale = np.where(t < 50, 1e-250, 1e250)
    prices = pd.DataFrame({
        "microcap": scale * np.exp(0.002 * t),
        "hedge": scale * np.exp(0.0005 * t),
    }, index=dates)
    v20.overlay_mod.validate_close_df(prices)
    with np.errstate(over="ignore", invalid="ignore", divide="ignore"):
        gross = v20.base_mod.run_signal(prices)
        gross = v20.base_mod.apply_momentum_gap_exit_buffer(
            gross, v20.V2_0_MOMENTUM_GAP_EXIT_BUFFER,
        )
        assert gross.at[dates[50], "holding"] != "cash"
        assert pd.isna(gross.at[dates[50], "return"])
        with pytest.raises(ValueError, match="non-finite active return"):
            v20.overlay_mod.apply_v2_0_execution(gross, _events())


@pytest.fixture
def raw_price(monkeypatch: pytest.MonkeyPatch) -> pd.DataFrame:
    monkeypatch.setitem(
        v20.freq_mod.build_tradeability_series.__globals__,
        "read_ohlc_cache", lambda *_args: pd.DataFrame(),
    )
    return pd.DataFrame({
        "date": pd.bdate_range("2026-01-05", periods=5),
        "close_raw": [100.0, 102.0, 51.0, 52.0, 53.0],
    })


def _tradeability(raw: pd.DataFrame, adjusted: pd.DataFrame | None, dates=None):
    return v20.freq_mod.build_tradeability_series(
        symbol="600000", price=raw,
        trading_dates=pd.DatetimeIndex(raw.date if dates is None else dates),
        return_price=adjusted,
        trade_constraint_mode=v20.freq_mod.TRADE_CONSTRAINT_MODE_CLOSE,
    )


@pytest.mark.parametrize("return_column", ["close_qfq", "close_adj", "close_raw"])
def test_complete_adjustment_uses_one_price_basis(raw_price, return_column) -> None:
    adjusted = pd.DataFrame({
        "date": raw_price.date,
        return_column: [50.0, 51.0, 51.0, 52.0, 53.0],
    })
    result = _tradeability(raw_price, adjusted)[0]
    np.testing.assert_allclose(
        result, [np.nan, 0.02, 0.0, 52.0 / 51.0 - 1.0, 53.0 / 52.0 - 1.0],
        rtol=0.0, atol=1e-15, equal_nan=True,
    )


def test_partial_identical_adjustment_keeps_raw_returns_and_tradeability(raw_price) -> None:
    adjusted = raw_price.iloc[1:4].rename(columns={"close_raw": "close_qfq"})
    expected = _tradeability(raw_price, None)
    result = _tradeability(raw_price, adjusted)
    for actual, normal in zip(result, expected):
        pd.testing.assert_series_equal(actual, normal, check_exact=True)


@pytest.mark.parametrize("factors", [
    [0.5, 0.5, 0.5], [0.5, 1.0, 1.0], [1.0, 1.0 + 1e-12, 1.0],
])
def test_partial_meaningful_adjustment_rejects_even_constant_ratio(raw_price, factors) -> None:
    adjusted = raw_price.iloc[1:4].rename(columns={"close_raw": "close_qfq"}).copy()
    adjusted["close_qfq"] *= factors
    with pytest.raises(ValueError, match="incomplete adjusted return-price coverage.*600000"):
        _tradeability(raw_price, adjusted)


def test_internal_adjusted_gap_is_not_forward_filled_across_a_raw_observation(raw_price) -> None:
    adjusted = pd.DataFrame({
        "date": raw_price.date,
        "close_qfq": [50.0, 51.0, 51.0, 52.0, 53.0],
    }).drop(index=2)
    with pytest.raises(ValueError, match="missing dates=.*2026-01-07"):
        _tradeability(raw_price, adjusted)


def test_suspended_day_and_outside_listing_dates_keep_existing_semantics(raw_price) -> None:
    raw = raw_price.drop(index=2)
    adjusted = pd.DataFrame({
        "date": raw.date,
        "close_qfq": [50.0, 51.0, 52.0, 53.0],
    })
    dates = pd.bdate_range("2026-01-02", periods=7)
    returns, tradeable, _, _ = _tradeability(raw, adjusted, dates)
    np.testing.assert_allclose(
        returns, [np.nan, np.nan, 0.02, 0.0, 52.0 / 51.0 - 1.0,
                  53.0 / 52.0 - 1.0, np.nan],
        rtol=0.0, atol=1e-15, equal_nan=True,
    )
    assert tradeable.tolist() == [False, True, True, False, True, True, False]


def test_complete_adjustment_preserves_observation_on_a_raw_missing_day(raw_price) -> None:
    raw = raw_price.drop(index=2)
    adjusted = pd.DataFrame({
        "date": raw_price.date,
        "close_qfq": [50.0, 51.0, 51.5, 52.0, 53.0],
    })
    returns, tradeable, _, _ = _tradeability(raw, adjusted, raw_price.date)
    np.testing.assert_allclose(
        returns, [np.nan, 0.02, 51.5 / 51.0 - 1.0, 52.0 / 51.5 - 1.0, 53.0 / 52.0 - 1.0],
        rtol=0.0, atol=1e-15, equal_nan=True,
    )
    assert not tradeable.iloc[2]


def test_partial_adjustment_cannot_discard_difference_on_a_raw_missing_day(raw_price) -> None:
    raw = raw_price.drop(index=2)
    adjusted = raw_price.iloc[1:4].rename(columns={"close_raw": "close_qfq"})
    # The two overlapping prices equal raw, but the extra date differs from the
    # raw calendar's carried close. It is meaningful data, not a redundant cache.
    with pytest.raises(ValueError, match="incomplete adjusted return-price coverage"):
        _tradeability(raw, adjusted, raw_price.date)


@pytest.mark.parametrize("bad", [np.nan, "damaged", np.inf, 0.0])
def test_invalid_adjusted_close_is_not_replaced_by_raw(raw_price, bad) -> None:
    adjusted = raw_price.rename(columns={"close_raw": "close_qfq"}).astype({"close_qfq": object})
    adjusted.at[2, "close_qfq"] = bad
    with pytest.raises(ValueError, match="finite positive closes"):
        _tradeability(raw_price, adjusted)


def test_duplicate_adjusted_dates_are_rejected(raw_price) -> None:
    adjusted = raw_price.rename(columns={"close_raw": "close_qfq"})
    adjusted = pd.concat([adjusted, adjusted.iloc[[2]]], ignore_index=True)
    with pytest.raises(ValueError, match="duplicate dates"):
        _tradeability(raw_price, adjusted)


@pytest.mark.parametrize("attack", [
    "partial_adjustment", "adjusted_nan", "adjusted_text", "adjusted_nat",
    "raw_nan", "raw_text", "raw_nat", "raw_zero", "raw_negative", "raw_inf", "raw_negative_inf",
])
def test_cache_loader_propagates_return_data_failure(raw_price, tmp_path: Path,
                                                   monkeypatch: pytest.MonkeyPatch, attack) -> None:
    paths = {}
    for name in ("PRICE_DIR", "ADJ_PRICE_DIR", "SHARE_DIR"):
        path = tmp_path / name / "600000.csv"
        path.parent.mkdir()
        paths[name] = path
    raw = raw_price.copy().astype({"close_raw": object})
    adjusted = raw_price.rename(columns={"close_raw": "close_qfq"}).astype({"close_qfq": object})
    if attack == "partial_adjustment":
        adjusted = adjusted.iloc[1:4].copy()
        adjusted["close_qfq"] *= 0.5
    elif attack.startswith("adjusted_"):
        if attack == "adjusted_nat":
            adjusted.at[2, "date"] = pd.NaT
        else:
            adjusted.at[2, "close_qfq"] = np.nan if attack == "adjusted_nan" else "damaged"
    else:
        # Dropping this bad raw observation would falsely make the adjusted
        # cache appear complete and reclassify corrupted data as suspension.
        adjusted = adjusted.drop(index=2)
        adjusted["close_qfq"] *= 0.5
        if attack == "raw_nat":
            raw.at[2, "date"] = pd.NaT
        else:
            raw.at[2, "close_raw"] = {
                "raw_nan": np.nan, "raw_text": "damaged", "raw_zero": 0.0,
                "raw_negative": -1.0, "raw_inf": np.inf, "raw_negative_inf": -np.inf,
            }[attack]
    raw.to_csv(paths["PRICE_DIR"], index=False)
    adjusted.to_csv(paths["ADJ_PRICE_DIR"], index=False)
    pd.DataFrame({"change_date": [raw_price.date.iloc[0]], "total_shares_10k": [1.0]}).to_csv(
        paths["SHARE_DIR"], index=False,
    )
    namespace = v20.freq_mod.load_symbol_cache.__globals__
    monkeypatch.setitem(namespace, "resolve_cache_path",
                        lambda local, shared, symbol: paths[next(name for name in paths if namespace[name] == local)])
    monkeypatch.setitem(namespace, "load_security_meta", lambda _symbol: {})
    monkeypatch.setitem(namespace, "build_st_status_series", lambda _meta, dates: pd.Series(False, index=dates))
    monkeypatch.setitem(namespace, "build_active_status_series", lambda _meta, dates: pd.Series(True, index=dates))
    dates = pd.DatetimeIndex(raw_price.date)
    with pytest.raises(RuntimeError, match="symbol cache load failed.*(incomplete adjusted|finite positive|invalid dates|missing or nonnumeric)"):
        v20.freq_mod.load_symbol_cache("600000", dates, dates)


@pytest.mark.parametrize("partial", [False, True])
def test_public_symbol_panel_uses_the_same_coverage_guard(raw_price, monkeypatch, partial) -> None:
    adjusted = pd.DataFrame({
        "date": raw_price.date,
        "close_qfq": [50.0, 51.0, 51.0, 52.0, 53.0],
    })
    if partial:
        adjusted = adjusted.iloc[1:4]
    namespace = v20.fetch_mod.build_symbol_panel.__globals__
    monkeypatch.setitem(namespace, "fetch_price_history", lambda **_kwargs: raw_price.copy())
    monkeypatch.setitem(namespace, "fetch_adjusted_price_history", lambda **_kwargs: adjusted.copy())
    monkeypatch.setitem(namespace, "fetch_share_change", lambda **_kwargs: pd.DataFrame({
        "change_date": [raw_price.date.iloc[0]], "total_shares_10k": [1.0],
    }))
    if partial:
        with pytest.raises(ValueError, match="incomplete adjusted return-price coverage"):
            v20.fetch_mod.build_symbol_panel("600000", "2026-01-05", "2026-01-09")
    else:
        panel = v20.fetch_mod.build_symbol_panel("600000", "2026-01-05", "2026-01-09")
        np.testing.assert_allclose(
            panel["return"], [np.nan, 0.02, 0.0, 52.0 / 51.0 - 1.0, 53.0 / 52.0 - 1.0],
            rtol=0.0, atol=1e-15, equal_nan=True,
        )
        np.testing.assert_array_equal(panel.market_cap, raw_price.close_raw * 10000.0)


@pytest.mark.parametrize("bad", [0.0, -1.0, np.inf, -np.inf])
@pytest.mark.parametrize("with_adjustment", [False, True])
def test_raw_price_guard_applies_before_any_adjustment_fallback(raw_price, bad, with_adjustment) -> None:
    corrupt = raw_price.copy()
    corrupt.at[2, "close_raw"] = bad
    adjusted = raw_price.rename(columns={"close_raw": "close_qfq"}) if with_adjustment else None
    with pytest.raises(ValueError, match="raw return-price data.*finite positive"):
        _tradeability(corrupt, adjusted)


@pytest.mark.parametrize("bad", [0.0, -1.0, np.inf, -np.inf])
def test_public_symbol_panel_rejects_invalid_raw_prices_without_adjustment(raw_price, monkeypatch, bad) -> None:
    raw = raw_price.copy()
    raw.at[2, "close_raw"] = bad
    namespace = v20.fetch_mod.build_symbol_panel.__globals__
    monkeypatch.setitem(namespace, "fetch_price_history", lambda **_kwargs: raw.copy())
    monkeypatch.setitem(namespace, "fetch_adjusted_price_history", lambda **_kwargs: pd.DataFrame())
    monkeypatch.setitem(namespace, "fetch_share_change", lambda **_kwargs: pd.DataFrame({
        "change_date": [raw_price.date.iloc[0]], "total_shares_10k": [1.0],
    }))
    with pytest.raises(ValueError, match="raw return-price data.*finite positive"):
        v20.fetch_mod.build_symbol_panel("600000", "2026-01-05", "2026-01-09")


@pytest.mark.parametrize("prices", [[0.0, 0.0], [-100.0, -101.0]])
def test_no_adjustment_loader_cannot_launder_invalid_prices_into_a_valid_proxy(
    tmp_path, monkeypatch, prices,
) -> None:
    dates = pd.to_datetime(["2026-01-05", "2026-01-06"])
    raw_path, share_path = tmp_path / "raw.csv", tmp_path / "shares.csv"
    pd.DataFrame({"date": dates, "close_raw": prices}).to_csv(raw_path, index=False)
    pd.DataFrame({"change_date": [dates[0]], "total_shares_10k": [1.0]}).to_csv(share_path, index=False)
    namespace = v20.freq_mod.load_symbol_cache.__globals__
    monkeypatch.setitem(namespace, "resolve_cache_path", lambda local, shared, symbol:
                        raw_path if local == namespace["PRICE_DIR"] else
                        share_path if local == namespace["SHARE_DIR"] else None)
    monkeypatch.setitem(namespace, "load_security_meta", lambda _symbol: {})
    monkeypatch.setitem(namespace, "build_st_status_series", lambda _meta, days: pd.Series(False, index=days))
    monkeypatch.setitem(namespace, "build_active_status_series", lambda _meta, days: pd.Series(True, index=days))
    monkeypatch.setitem(namespace, "read_ohlc_cache", lambda *_args: pd.DataFrame())
    with pytest.raises(RuntimeError, match="symbol cache panel load failed.*finite positive"):
        v20.freq_mod.load_cache_panels(
            ["600000"], dates, dates, max_workers=1, trade_constraint_mode="close",
        )
