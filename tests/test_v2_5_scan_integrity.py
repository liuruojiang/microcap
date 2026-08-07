from __future__ import annotations

import pandas as pd
import pytest

import microcap_top100_mom16_biweekly_live_v2_5 as v2_5
from scripts import run_microcap_v2_5_env_breadth_cooldown_scan as cooldown
from scripts import run_microcap_v2_5_staged_entry_scan as staged
from scripts import microcap_v2_5_scan_common as scan_common
from scripts import run_microcap_v2_5_zz2000_cyb_volume_scan as volume
from scripts import run_microcap_v2_5_bias_overheat_scan as bias_overheat


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
