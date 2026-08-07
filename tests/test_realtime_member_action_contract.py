from __future__ import annotations

from types import SimpleNamespace

import pandas as pd
import pytest

import microcap_top100_mom16_biweekly_live_v2_0 as v2_0
import microcap_top100_mom16_biweekly_live_v2_3 as v2_3
import microcap_top100_mom16_biweekly_live_v2_5 as v2_5


def test_standalone_realtime_refresh_route_exports_default_anchor_age(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from scripts import realtime_state_bundle

    calls: list[tuple[object, int, int]] = []

    def fake_refresh_state(root, *, max_workers, max_anchor_age_days):
        calls.append((root, max_workers, max_anchor_age_days))
        return {"ok": True}

    monkeypatch.delenv("TOP100_REALTIME_REQUIRE_STATE", raising=False)
    monkeypatch.setattr(realtime_state_bundle, "refresh_state", fake_refresh_state)

    assert v2_0.run_realtime_query_with_fresh_state(lambda: "emitted") == "emitted"
    assert v2_0.DEFAULT_MAX_STALE_ANCHOR_DAYS == v2_0.base_mod.DEFAULT_MAX_STALE_ANCHOR_DAYS
    assert calls == [(v2_0.ROOT, 8, v2_0.DEFAULT_MAX_STALE_ANCHOR_DAYS)]


def _official_changes(official_rebalance: bool) -> pd.DataFrame:
    changes = pd.DataFrame({"action": ["enter"] * 7 + ["exit"] * 7})
    if not official_rebalance:
        changes["official_rebalance"] = False
    return changes


def _base_signal_row() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "trade_state": "hold",
                "current_holding": "cash",
                "next_holding": "cash",
            }
        ]
    )


def _fake_realtime_base(
    *,
    latest_anchor_trade_date: str,
    quote_trade_date: str,
    official_rebalance: bool,
) -> SimpleNamespace:
    anchor = pd.Timestamp(latest_anchor_trade_date)
    snapshot = pd.Timestamp(f"{quote_trade_date} 14:55:00")
    close_df = pd.DataFrame(
        {"microcap": [100.0, 101.0], "hedge": [200.0, 201.0]},
        index=pd.DatetimeIndex([anchor, snapshot]),
    )
    meta = {
        "latest_anchor_trade_date": latest_anchor_trade_date,
        "quote_trade_date": quote_trade_date,
        "snapshot_time": str(snapshot),
        "snapshot_row_appended": pd.Timestamp(quote_trade_date) > anchor,
        "member_price_count": 100,
        "member_count": 100,
    }
    return SimpleNamespace(
        realtime_close_df=close_df,
        base_gross=close_df.copy(),
        turnover_df=pd.DataFrame(),
        reference_summary={"latest_signal": {}},
        meta=meta,
        context={
            "latest_rebalance": pd.Timestamp("2026-08-06"),
            "changes_df": _official_changes(official_rebalance),
        },
    )


def _patch_native_realtime_builder(
    monkeypatch: pytest.MonkeyPatch,
    version: str,
    realtime_base: SimpleNamespace,
):
    monkeypatch.setattr(v2_0.realtime_core, "load_realtime_base", lambda: realtime_base)
    passthrough = pd.DataFrame(
        {"holding": ["cash"], "next_holding": ["cash"]},
        index=pd.DatetimeIndex([pd.Timestamp(realtime_base.meta["quote_trade_date"])]),
    )

    if version == "v2.0":
        overlay_globals = v2_0._build_realtime_v2_0_outputs_unlocked.__globals__
        monkeypatch.setitem(overlay_globals, "validate_close_df", lambda *args, **kwargs: None)
        monkeypatch.setattr(
            v2_0.embedded_context.base_mod,
            "apply_momentum_gap_exit_buffer",
            lambda *args, **kwargs: passthrough,
        )
        monkeypatch.setitem(overlay_globals, "apply_volatility_overheat_exit", lambda *args, **kwargs: passthrough)
        monkeypatch.setitem(overlay_globals, "apply_target_vol_scaling", lambda *args, **kwargs: passthrough)
        monkeypatch.setitem(overlay_globals, "_load_realtime_v2_0_official_index", lambda: passthrough.index)
        monkeypatch.setitem(overlay_globals, "assert_realtime_target_vol_lag_fresh", lambda *args, **kwargs: None)
        monkeypatch.setitem(overlay_globals, "_build_signal_row", lambda *args, **kwargs: _base_signal_row())
        monkeypatch.setitem(overlay_globals, "_atomic_write_text", lambda *args, **kwargs: None)
        return v2_0.build_realtime_v2_0_outputs

    if version == "v2.3":
        monkeypatch.setattr(v2_3, "_build_realtime_v2_3_official_index", lambda *args, **kwargs: passthrough.index)
        monkeypatch.setattr(v2_3, "build_v2_3_common_index", lambda *args, **kwargs: passthrough.index)
        monkeypatch.setattr(v2_3, "build_spread_log_wls_gross", lambda *args, **kwargs: passthrough)
        monkeypatch.setattr(v2_3, "apply_overheat_defense", lambda *args, **kwargs: passthrough)
        monkeypatch.setattr(v2_3, "build_signal_execution_mismatch_diagnostics", lambda *args, **kwargs: {})
        monkeypatch.setattr(v2_3, "apply_signal_execution_mismatch_columns", lambda *args, **kwargs: None)
        monkeypatch.setattr(v2_3, "_build_signal_row", lambda *args, **kwargs: _base_signal_row())
        monkeypatch.setattr(v2_3, "_atomic_write_text", lambda *args, **kwargs: None)
        return v2_3._build_realtime_v2_3_outputs_unlocked

    monkeypatch.setattr(v2_5, "_close_df_from_realtime", lambda *args, **kwargs: realtime_base.realtime_close_df)
    monkeypatch.setattr(v2_5, "_build_realtime_v2_5_official_index", lambda *args, **kwargs: passthrough.index)
    monkeypatch.setattr(v2_5, "build_v2_5_common_index", lambda *args, **kwargs: passthrough.index)
    monkeypatch.setattr(v2_5, "build_microcap_log_wls_gross", lambda *args, **kwargs: passthrough)
    monkeypatch.setattr(v2_5, "apply_cost", lambda *args, **kwargs: passthrough)
    monkeypatch.setattr(v2_5, "apply_no_target_vol", lambda *args, **kwargs: passthrough)
    monkeypatch.setattr(v2_5, "_build_signal_row", lambda *args, **kwargs: _base_signal_row())
    monkeypatch.setattr(v2_5, "current_base_fingerprint", lambda: {})
    monkeypatch.setattr(v2_5, "current_realtime_fingerprint", lambda: {})
    monkeypatch.setattr(v2_5, "_atomic_write_text", lambda *args, **kwargs: None)
    return v2_5._build_realtime_v2_5_outputs_unlocked


@pytest.mark.parametrize("version", ["v2.0", "v2.3", "v2.5"])
@pytest.mark.parametrize(
    (
        "latest_anchor_trade_date",
        "quote_trade_date",
        "official_rebalance",
        "expected_actionable",
        "expected_execution_date",
    ),
    [
        ("2026-08-06", "2026-08-07", True, True, "2026-08-07"),
        ("2026-08-07", "2026-08-10", True, False, ""),
        ("2026-08-06", "2026-08-07", False, False, ""),
    ],
)
def test_native_realtime_rows_publish_dated_official_member_actions(
    monkeypatch: pytest.MonkeyPatch,
    version: str,
    latest_anchor_trade_date: str,
    quote_trade_date: str,
    official_rebalance: bool,
    expected_actionable: bool,
    expected_execution_date: str,
) -> None:
    realtime_base = _fake_realtime_base(
        latest_anchor_trade_date=latest_anchor_trade_date,
        quote_trade_date=quote_trade_date,
        official_rebalance=official_rebalance,
    )
    builder = _patch_native_realtime_builder(monkeypatch, version, realtime_base)

    signal_df, _, _ = builder()
    row = signal_df.iloc[0]

    assert row["member_enter_count"] == 7
    assert row["member_exit_count"] == 7
    assert bool(row["member_rebalance_required"]) is True
    assert row["member_rebalance_signal_date"] == "2026-08-06"
    assert row["member_rebalance_execution_date"] == expected_execution_date
    assert bool(row["member_rebalance_actionable"]) is expected_actionable
    assert bool(row["member_rebalance_official"]) is official_rebalance
