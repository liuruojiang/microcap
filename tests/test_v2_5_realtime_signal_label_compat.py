from __future__ import annotations

import pandas as pd
import pytest

import microcap_top100_mom16_biweekly_live_v2_5 as v2_5


def _net_df(
    current_holding: str,
    next_holding: str,
    current_scale: float,
    next_scale: float,
) -> pd.DataFrame:
    idx = pd.to_datetime(["2026-07-29", "2026-07-30"])
    return pd.DataFrame(
        {
            "holding": ["cash", current_holding],
            "next_holding": ["cash", next_holding],
            "current_execution_scale": [0.0, current_scale],
            "execution_scale": [0.0, current_scale],
            "next_session_target_scale": [0.0, next_scale],
            "next_session_actionable_scale": [0.0, next_scale],
            "target_vol_scale_next_session": [0.0, next_scale],
            "annualized_log_wls_score": [0.1, 0.6],
            "log_wls_r2": [0.5, 0.6],
            "microcap_nav": [1.0, 1.01],
            "cash_day_yield": [0.0, 0.0],
        },
        index=idx,
    )


def test_active_v2_5_signal_keeps_native_holding_labels() -> None:
    net_df = _net_df("long_microcap_top100", "long_microcap_top100", 0.8, 0.8)

    signal = v2_5._build_signal_row(net_df, {"latest_signal": {}}).iloc[0]

    assert signal["current_holding"] == "long_microcap_top100"
    assert signal["next_holding"] == "long_microcap_top100"
    assert signal["signal_label"] == "long_microcap_top100"
    assert net_df.iloc[-1]["holding"] == "long_microcap_top100"


def test_v2_5_entry_signal_reports_one_sided_turnover() -> None:
    signal = v2_5._build_signal_row(
        _net_df("cash", "long_microcap_top100", 0.0, 0.8),
        {"latest_signal": {}},
    ).iloc[0]

    assert signal["trade_state"] == "open"
    assert signal["next_session_leg_turnover"] == pytest.approx(0.8)
    assert signal["next_session_leg_cost_est_raw"] == pytest.approx(
        0.8 * v2_5.TARGET_VOL_SCALE_CHANGE_ENTRY_COST
    )
    assert signal["next_session_overlay_cost_est"] == pytest.approx(0.0)


def test_v2_5_scale_rebalance_uses_native_cost_rate() -> None:
    signal = v2_5._build_signal_row(
        _net_df("long_microcap_top100", "long_microcap_top100", 0.5, 0.9),
        {"latest_signal": {}},
    ).iloc[0]

    assert signal["scale_trade_state"] == "rebalance_scale"
    assert signal["next_session_leg_turnover"] == pytest.approx(0.4)
    assert signal["next_session_overlay_cost_est"] == pytest.approx(
        0.4 * v2_5.TARGET_VOL_SCALE_CHANGE_ENTRY_COST
    )
    assert signal["fixed_hedge_ratio"] == pytest.approx(0.0)
    assert signal["max_leverage"] == pytest.approx(v2_5.TARGET_VOL_MAX_LEVERAGE)
