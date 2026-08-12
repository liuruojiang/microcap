from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

import microcap_top100_mom16_biweekly_live_v2_5 as v2_5
from scripts import run_microcap_v2_5_env_breadth_cooldown_scan as cooldown
from scripts import run_microcap_v2_5_staged_entry_scan as staged
from scripts import microcap_v2_5_scan_common as scan_common
from scripts import run_microcap_v2_5_zz2000_cyb_volume_scan as volume
from scripts import run_microcap_v2_5_bias_overheat_scan as bias_overheat
from scripts import run_microcap_v2_5_pool_rebalance_frequency_scan as pool_scan


@pytest.fixture
def transition_frame() -> pd.DataFrame:
    idx = pd.bdate_range("2026-01-05", periods=3)
    holding = ["cash", "long_microcap_top100", "cash"]
    next_holding = ["long_microcap_top100", "cash", "cash"]
    total_cost = [0.003, 0.003, 0.0]
    return_net = [(1.0 - cost) - 1.0 for cost in total_cost]
    return pd.DataFrame(
        {
            "holding": holding,
            "next_holding": next_holding,
            "return_net": return_net,
            "return": return_net,
            "total_cost": total_cost,
            "base_trade_cost": total_cost,
            "overlay_pre_cost_return": 0.0,
            "base_pre_cost_return": 0.0,
            "microcap_ret": 0.0,
            "microcap_close": [100.0, 100.0, 100.0],
            "current_execution_scale": [0.0, 1.0, 0.0],
            "next_session_actionable_scale": [1.0, 0.0, 0.0],
        },
        index=idx,
    )


def test_target_vol_replay_preserves_exit_cost(transition_frame: pd.DataFrame) -> None:
    out = v2_5.apply_target_vol(transition_frame, target_vol=0.30)

    assert out.iloc[1]["return_net"] == pytest.approx(-0.003)


def test_no_target_vol_close_entry_cost_scale_matches_scaled_cost(
    transition_frame: pd.DataFrame,
) -> None:
    out = v2_5.apply_no_target_vol(transition_frame)
    entry = out.iloc[0]

    assert entry["holding"] == "cash"
    assert entry["next_holding"] == "long_microcap_top100"
    assert entry["base_trade_cost"] == pytest.approx(0.003)
    assert entry["base_trade_cost_scale"] == pytest.approx(1.0)
    assert entry["base_trade_cost_scaled"] == pytest.approx(
        entry["base_trade_cost"] * entry["base_trade_cost_scale"]
    )
    assert entry["return_net"] == pytest.approx(transition_frame.iloc[0]["return_net"])


def test_staged_none_matches_official_return_net(transition_frame: pd.DataFrame) -> None:
    out = staged.apply_staged_entry_overlay(transition_frame, trigger_scope="none")

    pd.testing.assert_series_equal(
        out["return_net"],
        transition_frame["return_net"],
        check_names=False,
    )


def test_staged_fill_cost_is_not_charged_again_next_day() -> None:
    idx = pd.bdate_range("2026-01-05", periods=4)
    frame = pd.DataFrame(
        {
            "holding": ["cash", "long_microcap_top100", "long_microcap_top100", "long_microcap_top100"],
            "next_holding": ["long_microcap_top100"] * 4,
            "base_pre_cost_return": 0.0,
            "base_trade_cost": [0.003, 0.0, 0.0, 0.0],
            "return_net": [-0.003, 0.0, 0.0, 0.0],
            "microcap_close": [100.0, 101.0, 100.0, 100.0],
            "current_execution_scale": [0.0, 1.0, 1.0, 1.0],
            "next_session_actionable_scale": [1.0, 1.0, 1.0, 1.0],
        },
        index=idx,
    )

    out = staged.apply_staged_entry_overlay(frame, trigger_scope="cash_only")

    assert out.iloc[2]["staged_entry_trade_cost"] == pytest.approx(0.0015)
    assert out.iloc[3]["scale_change_cost_actual"] == pytest.approx(0.0)


def test_scaled_condition_cancels_base_entry_without_roundtrip_cost(transition_frame: pd.DataFrame) -> None:
    condition = pd.Series([True, True, True], index=transition_frame.index)

    out = cooldown._apply_scaled_condition(
        transition_frame,
        condition,
        scale=0.0,
        candidate="blocked_entry",
        filter_group="test",
    )

    assert out.iloc[0]["return_net"] == pytest.approx(0.0)
    assert out.iloc[1]["return_net"] == pytest.approx(0.0)
    assert out.iloc[1]["holding"] == "cash"


def test_volume_scale_cancels_base_entry_without_roundtrip_cost(transition_frame: pd.DataFrame) -> None:
    execution_day = pd.Series([False, True, True], index=transition_frame.index)

    candidate, _, overlay_cost = volume._apply_volume_scale(
        transition_frame,
        execution_day,
        0.0,
        next_execution_day=execution_day.shift(-1, fill_value=False),
    )

    assert candidate.iloc[0]["return_net"] == pytest.approx(0.0)
    assert candidate.iloc[1]["return_net"] == pytest.approx(0.0)
    assert overlay_cost.iloc[1] == pytest.approx(0.0)
    assert candidate.iloc[1]["holding"] == "cash"


def test_scaled_condition_uses_last_close_signal_for_next_session_entry() -> None:
    idx = pd.bdate_range("2026-01-05", periods=2)
    frame = pd.DataFrame(
        {
            "holding": ["cash", "cash"],
            "next_holding": ["cash", "long_microcap_top100"],
            "base_pre_cost_return": 0.0,
            "base_trade_cost": [0.0, 0.003],
            "return_net": [0.0, -0.003],
            "current_execution_scale": [0.0, 0.0],
            "next_session_actionable_scale": [0.0, 1.0],
        },
        index=idx,
    )
    condition = pd.Series([False, True], index=idx)

    out = cooldown._apply_scaled_condition(
        frame,
        condition,
        scale=0.0,
        candidate="last_close_block",
        filter_group="test",
    )

    assert out.iloc[-1]["next_holding"] == "cash"
    assert out.iloc[-1]["next_session_actionable_scale"] == pytest.approx(0.0)
    assert out.iloc[-1]["return_net"] == pytest.approx(0.0)


def test_volume_scale_uses_last_close_signal_for_next_session_entry() -> None:
    idx = pd.bdate_range("2026-01-05", periods=2)
    frame = pd.DataFrame(
        {
            "holding": ["cash", "cash"],
            "next_holding": ["cash", "long_microcap_top100"],
            "base_pre_cost_return": 0.0,
            "base_trade_cost": [0.0, 0.003],
            "return_net": [0.0, -0.003],
            "current_execution_scale": [0.0, 0.0],
            "next_session_actionable_scale": [0.0, 1.0],
        },
        index=idx,
    )
    execution_day = pd.Series([False, False], index=idx)
    next_execution_day = pd.Series([False, True], index=idx)

    out, _, _ = volume._apply_volume_scale(
        frame,
        execution_day,
        0.0,
        next_execution_day=next_execution_day,
    )

    assert out.iloc[-1]["next_holding"] == "cash"
    assert out.iloc[-1]["next_session_actionable_scale"] == pytest.approx(0.0)
    assert out.iloc[-1]["return_net"] == pytest.approx(0.0)


def test_cooldown_does_not_double_charge_base_transition(transition_frame: pd.DataFrame) -> None:
    out = cooldown._apply_cooldown(transition_frame, cooldown_days=3)

    assert out.iloc[1]["return_net"] == pytest.approx(-0.003)


def test_cooldown_cancels_blocked_entry_without_roundtrip_and_preserves_tail_signal() -> None:
    idx = pd.bdate_range("2026-01-05", periods=4)
    frame = pd.DataFrame(
        {
            "holding": ["long_microcap_top100", "cash", "long_microcap_top100", "long_microcap_top100"],
            "next_holding": ["cash", "long_microcap_top100", "long_microcap_top100", "long_microcap_top100"],
            "base_pre_cost_return": [0.0, 0.0, 0.02, 0.01],
            "base_trade_cost": [0.003, 0.003, 0.0, 0.0],
            "total_cost": [0.003, 0.003, 0.0, 0.0],
            "return_net": [-0.003, -0.003, 0.02, 0.01],
            "current_execution_scale": [1.0, 0.0, 1.0, 1.0],
            "next_session_actionable_scale": [0.0, 1.0, 1.0, 1.0],
        },
        index=idx,
    )

    out = cooldown._apply_cooldown(frame, cooldown_days=3)

    assert out.iloc[1]["return_net"] == pytest.approx(0.0)
    assert out.iloc[2]["return_net"] == pytest.approx(0.0)
    assert out.iloc[1]["filter_overlay_cost"] == pytest.approx(0.0)
    assert out.iloc[2]["filter_overlay_cost"] == pytest.approx(0.0)
    assert out.iloc[-1]["holding"] == "cash"
    assert out.iloc[-1]["next_holding"] == "long_microcap_top100"
    assert out.iloc[-1]["next_session_actionable_scale"] == pytest.approx(1.0)
    scan_common.assert_candidate_state_consistent(out, "cooldown regression")


def test_legacy_static_preflight_is_rejected(tmp_path) -> None:
    legacy = tmp_path / "microcap_top100_mom16_biweekly_live_v2_5_scan_preflight_20260601_costed_nav.csv"
    legacy.write_text("date,return_net\n2026-06-01,0\n", encoding="utf-8")

    with pytest.raises(RuntimeError, match="official v2.5"):
        scan_common.reject_legacy_preflight(legacy)


def test_breadth_rejects_raw_only_price_cache(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw_only = tmp_path / "000001.csv"
    raw_only.write_text("date,close_raw\n2026-01-05,10\n", encoding="utf-8")
    monkeypatch.setattr(
        cooldown.v25.v2_0.freq_mod,
        "resolve_cache_path",
        lambda *_args, **_kwargs: raw_only,
    )

    with pytest.raises(RuntimeError, match="adjusted breadth cache schema"):
        cooldown._load_price_series("000001")


def test_breadth_rejects_undercovered_member_dates(monkeypatch: pytest.MonkeyPatch) -> None:
    idx = pd.bdate_range("2026-01-05", periods=30)
    rebalance = idx[0]
    members = [f"{number:06d}" for number in range(1, 95)]
    history_idx = pd.bdate_range("2025-11-03", periods=75)
    price = pd.Series(range(100, 175), index=history_idx, dtype=float)
    monkeypatch.setattr(cooldown, "_load_members_by_rebalance", lambda: {rebalance: members})
    monkeypatch.setattr(cooldown, "_load_price_series", lambda _symbol: price)

    with pytest.raises(RuntimeError, match="coverage below 95"):
        cooldown._build_breadth_frame(idx)


def test_breadth_ma20_width_uses_only_ma_valid_members(monkeypatch: pytest.MonkeyPatch) -> None:
    history_idx = pd.bdate_range("2025-11-03", periods=25)
    nav_idx = history_idx[-1:]
    valid_members = [f"{number:06d}" for number in range(1, 96)]
    warmup_members = [f"{number:06d}" for number in range(96, 101)]
    members = valid_members + warmup_members
    monkeypatch.setattr(cooldown, "_load_members_by_rebalance", lambda: {history_idx[0]: members})

    def load_price(symbol: str) -> pd.Series:
        rows = 25 if symbol in valid_members else 10
        return pd.Series(np.arange(1, rows + 1, dtype=float), index=history_idx[-rows:])

    monkeypatch.setattr(cooldown, "_load_price_series", load_price)

    breadth, _meta = cooldown._build_breadth_frame(nav_idx)

    assert breadth.iloc[0]["top100_ma20_valid_members"] == 95
    assert breadth.iloc[0]["top100_ma20_width"] == pytest.approx(1.0)


def test_breadth_rejects_undercovered_ma20_history(monkeypatch: pytest.MonkeyPatch) -> None:
    history_idx = pd.bdate_range("2025-11-03", periods=25)
    nav_idx = history_idx[-1:]
    valid_members = [f"{number:06d}" for number in range(1, 95)]
    warmup_members = [f"{number:06d}" for number in range(95, 101)]
    members = valid_members + warmup_members
    monkeypatch.setattr(cooldown, "_load_members_by_rebalance", lambda: {history_idx[0]: members})

    def load_price(symbol: str) -> pd.Series:
        rows = 25 if symbol in valid_members else 10
        return pd.Series(np.arange(1, rows + 1, dtype=float), index=history_idx[-rows:])

    monkeypatch.setattr(cooldown, "_load_price_series", load_price)

    with pytest.raises(RuntimeError, match="MA20 coverage below 95"):
        cooldown._build_breadth_frame(nav_idx)


@pytest.mark.parametrize("gap", ["tail", "middle"])
def test_pool_variant_rejects_incomplete_candidate_calendar(
    monkeypatch: pytest.MonkeyPatch,
    gap: str,
) -> None:
    official_dates = pd.bdate_range("2026-01-05", periods=30)
    candidate_dates = official_dates[:-1] if gap == "tail" else official_dates.delete(20)
    index_df = pd.DataFrame(
        {
            "date": candidate_dates,
            "close": 100.0 * np.exp(np.arange(len(candidate_dates), dtype=float) * 0.001),
            "holding_count": 100,
        }
    )
    turnover = pd.DataFrame(columns=["rebalance_date", "two_side_cost_rate"])
    monkeypatch.setattr(
        pool_scan.v25.v2_0.base_mod,
        "build_live_target_members_map",
        lambda **_kwargs: {candidate_dates[0]: ["000001"]},
    )
    monkeypatch.setattr(
        pool_scan.v25.v2_0.freq_mod,
        "build_target_members_frame",
        lambda *_args, **_kwargs: pd.DataFrame(
            {"rebalance_date": [candidate_dates[0]], "symbol": ["000001"]}
        ),
    )
    monkeypatch.setattr(
        pool_scan.v25.v2_0.freq_mod,
        "simulate_rebalance_path",
        lambda **_kwargs: (index_df.copy(), turnover.copy(), pd.DataFrame()),
    )
    monkeypatch.setattr(
        pool_scan.v25.v2_0.base_mod,
        "trim_proxy_history",
        lambda index, members, costs: (index, members, costs, candidate_dates[0]),
    )
    base = pd.DataFrame(
        {"hedge_close": np.linspace(100.0, 101.0, len(official_dates))},
        index=official_dates,
    )

    with pytest.raises(RuntimeError, match="candidate close history"):
        pool_scan._build_variant_output(
            candidate=f"calendar_{gap}",
            top_n=1,
            rebalance_frequency="biweekly",
            schedules={"biweekly": pd.DatetimeIndex([candidate_dates[0]])},
            returns_df=pd.DataFrame(index=candidate_dates),
            caps_by_date={},
            buyable_df=pd.DataFrame(),
            sellable_df=pd.DataFrame(),
            name_map={},
            official_index=official_dates,
            base_gross_cached=base,
        )


def test_pool_cache_inputs_use_formal_historical_universe_and_st_lineage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    trading_dates = pd.bdate_range("2026-01-05", periods=10)
    calls: dict[str, object] = {}
    monkeypatch.setattr(pool_scan.v25.v2_0.freq_mod, "load_universe", lambda: ["000001", "000002"])
    monkeypatch.setattr(
        pool_scan.v25.v2_0.freq_mod,
        "load_current_universe",
        lambda: pytest.fail("current-universe loader must not be used by historical pool scans"),
    )

    def load_cache_panels(**kwargs):
        calls.update(kwargs)
        empty = pd.DataFrame(index=trading_dates)
        return empty, {}, empty.copy(), empty.copy()

    monkeypatch.setattr(pool_scan.v25.v2_0.freq_mod, "load_cache_panels", load_cache_panels)
    monkeypatch.setattr(pool_scan.v25.v2_0.base_mod, "load_name_map", lambda: {})

    *_, symbols = pool_scan._prepare_cache_inputs(1, trading_dates)

    assert symbols == ["000001", "000002"]
    assert calls["symbols"] == symbols
    assert calls["exclude_historical_st_from_caps"] is True


def test_bias_risk_off_state_matches_executed_scale_and_cash_return() -> None:
    idx = pd.bdate_range("2026-01-05", periods=3)
    frame = pd.DataFrame(
        {
            "holding": ["long_microcap_top100"] * 3,
            "next_holding": ["long_microcap_top100"] * 3,
            "base_pre_cost_return": [0.01, 0.02, 0.03],
            "base_trade_cost": [0.0, 0.0, 0.0],
            "return_net": [0.01, 0.02, 0.03],
            "current_execution_scale": [1.0, 1.0, 1.0],
            "next_session_actionable_scale": [1.0, 1.0, 1.0],
        },
        index=idx,
    )
    bias = pd.Series([0.20, 0.21, 0.10], index=idx)

    out = bias_overheat._apply_bias_overheat_overlay(
        frame,
        bias=bias,
        hot_threshold=0.15,
        cool_threshold=0.05,
        one_side_trade_cost=0.003,
    )

    scan_common.assert_candidate_state_consistent(out, "bias regression")
    risk_off = out["bias_overheat_risk_off"]
    assert out.loc[risk_off, "holding"].eq("cash").all()
    assert out.loc[risk_off, "actual_execution_scale"].eq(0.0).all()
    cost_free_cash = risk_off & out["overlay_trade_cost"].eq(0.0)
    assert out.loc[cost_free_cash, "return_net"].eq(0.0).all()


def test_bias_hot_signal_cancels_base_entry_cost_without_roundtrip() -> None:
    idx = pd.bdate_range("2026-01-05", periods=2)
    frame = pd.DataFrame(
        {
            "holding": ["cash", "long_microcap_top100"],
            "next_holding": ["long_microcap_top100", "long_microcap_top100"],
            "base_pre_cost_return": [0.0, 0.02],
            "base_trade_cost": [0.003, 0.0],
            "return_net": [-0.003, 0.02],
            "current_execution_scale": [0.0, 1.0],
            "next_session_actionable_scale": [1.0, 1.0],
        },
        index=idx,
    )

    out = bias_overheat._apply_bias_overheat_overlay(
        frame,
        bias=pd.Series([0.20, 0.21], index=idx),
        hot_threshold=0.15,
        cool_threshold=0.05,
        one_side_trade_cost=0.003,
    )

    assert out.iloc[0]["return_net"] == pytest.approx(0.0)
    assert out.iloc[1]["return_net"] == pytest.approx(0.0)
    assert out["overlay_trade_cost"].eq(0.0).all()
    assert out["holding"].eq("cash").all()


@pytest.mark.parametrize("module", [cooldown, pool_scan, bias_overheat])
def test_v2_5_scan_metrics_use_formal_calendar_year_annualization(module) -> None:
    idx = pd.to_datetime(["2020-01-02", "2021-01-04", "2022-01-03"])
    returns = pd.Series([0.10, -0.05, 0.20], index=idx)
    expected = v2_5.summarize_returns(returns)["annual_pct"] / 100.0

    assert module._metrics(returns)["ann_return"] == pytest.approx(expected)


def test_volume_scan_rejects_feature_end_date_truncation(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    nav_idx = pd.bdate_range("2026-01-05", periods=5)
    amount_idx = nav_idx[:-1]
    nav = pd.DataFrame({"return_net": 0.0}, index=nav_idx)
    amount = pd.Series(1.0, index=amount_idx)
    monkeypatch.setattr(volume, "_load_v2_5_nav", lambda: ({}, nav))
    monkeypatch.setattr(volume, "_load_csi2000_amount", lambda: amount.rename("csi2000_amount"))
    monkeypatch.setattr(volume, "_load_cyb_amount", lambda: amount.rename("cyb_amount"))

    with pytest.raises(RuntimeError, match="end date must equal"):
        volume._scan(tmp_path)


@pytest.mark.parametrize("gap", ["start", "middle"])
def test_volume_scan_rejects_incomplete_full_sample_feature_coverage(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
    gap: str,
) -> None:
    nav_idx = pd.bdate_range("2026-01-05", periods=5)
    amount_idx = nav_idx[1:] if gap == "start" else nav_idx.delete(2)
    nav = pd.DataFrame({"return_net": 0.0}, index=nav_idx)
    amount = pd.Series(1.0, index=amount_idx)
    monkeypatch.setattr(volume, "_load_v2_5_nav", lambda: ({}, nav))
    monkeypatch.setattr(volume, "_load_csi2000_amount", lambda: amount.rename("csi2000_amount"))
    monkeypatch.setattr(volume, "_load_cyb_amount", lambda: amount.rename("cyb_amount"))

    with pytest.raises(RuntimeError, match="full sample"):
        volume._scan(tmp_path)


def test_bias_overheat_loader_uses_fresh_official_v25(monkeypatch: pytest.MonkeyPatch) -> None:
    expected = pd.DataFrame({"return_net": [0.0]}, index=pd.to_datetime(["2026-01-05"]))
    calls = []
    monkeypatch.setattr(
        bias_overheat.scan_common,
        "load_fresh_official_v25",
        lambda: (calls.append(True) or ({"version": "2.5"}, expected)),
    )

    summary, frame = bias_overheat._load_v2_5_shadow()

    assert calls == [True]
    assert summary["version"] == "2.5"
    pd.testing.assert_frame_equal(frame, expected)
