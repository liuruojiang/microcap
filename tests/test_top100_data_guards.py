from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd
import pytest

import microcap_top100_mom16_biweekly_live_v2_0 as v2_0
import microcap_top100_mom16_biweekly_live_v2_3 as v2_3
import microcap_top100_mom16_biweekly_live_v2_5 as v2_5


def test_full_proxy_bundle_uses_backtest_universe_when_symbols_are_omitted(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, list[str]] = {}
    trading_dates = pd.DatetimeIndex(pd.to_datetime(["2026-06-17", "2026-06-18", "2026-06-19"]))
    rebalance_dates = pd.DatetimeIndex([pd.Timestamp("2026-06-18")])

    monkeypatch.setattr(v2_0.freq_mod, "load_current_universe", lambda: ["999999"])
    monkeypatch.setattr(v2_0.freq_mod, "load_universe", lambda: ["000001", "000002"])
    monkeypatch.setattr(v2_0.base_mod, "build_biweekly_rebalance_dates", lambda _dates: rebalance_dates)
    monkeypatch.setattr(v2_0.base_mod, "load_name_map", lambda: {})
    monkeypatch.setattr(
        v2_0.base_mod,
        "build_live_target_members_map",
        lambda caps_by_date, rebalance_dates, name_map, top_n: {pd.Timestamp("2026-06-18"): ["000001"]},
    )
    monkeypatch.setattr(
        v2_0.freq_mod,
        "build_target_members_frame",
        lambda target_members_map, caps_by_date, name_map=None: pd.DataFrame(
            {"rebalance_date": [pd.Timestamp("2026-06-18")], "symbol": ["000001"]}
        ),
    )
    monkeypatch.setattr(
        v2_0.base_mod,
        "trim_proxy_history",
        lambda index_df, members_df, turnover_df: (index_df, members_df, turnover_df, pd.Timestamp("2026-06-17")),
    )

    def fake_load_cache_panels(
        *,
        symbols: list[str],
        trading_dates: pd.DatetimeIndex,
        cap_dates: pd.DatetimeIndex,
        max_workers: int,
        trade_constraint_mode: str,
        exclude_historical_st_from_caps: bool,
    ):
        captured["symbols"] = list(symbols)
        returns = pd.DataFrame(index=trading_dates)
        caps = {pd.Timestamp("2026-06-18"): {"000001": 1.0}}
        flags = pd.DataFrame(index=trading_dates)
        return returns, caps, flags, flags.copy()

    monkeypatch.setattr(v2_0.freq_mod, "load_cache_panels", fake_load_cache_panels)
    monkeypatch.setattr(
        v2_0.freq_mod,
        "simulate_rebalance_path",
        lambda **_kwargs: (
            pd.DataFrame(
                {
                    "date": trading_dates,
                    "close": [1.0, 1.01, 1.02],
                    "daily_return": [0.0, 0.01, 0.0099],
                    "holding_count": [100, 100, 100],
                }
            ),
            pd.DataFrame({"rebalance_date": [pd.Timestamp("2026-06-18")], "turnover": [0.0]}),
            None,
        ),
    )

    _index, _members, _turnover, meta = v2_0.base_mod.build_local_proxy_bundle(
        argparse.Namespace(max_workers=1),
        trading_dates,
    )

    assert captured["symbols"] == ["000001", "000002"]
    assert meta["universe_source"] == "backtest_cache_security_master"
    assert meta["universe_symbol_count"] == 2


def test_refresh_price_cache_tail_uses_backtest_universe_when_symbols_are_omitted(monkeypatch: pytest.MonkeyPatch) -> None:
    refreshed: list[str] = []

    monkeypatch.setattr(v2_0.freq_mod, "load_current_universe", lambda: ["999999"])
    monkeypatch.setattr(v2_0.freq_mod, "load_universe", lambda: ["000001", "000002"])
    monkeypatch.setattr(v2_0.fetch_mod, "ensure_dirs", lambda: None)
    monkeypatch.setattr(
        v2_0.fetch_mod,
        "fetch_price_history",
        lambda symbol, start_date, end_date, force_refresh: refreshed.append(symbol),
    )
    monkeypatch.setattr(v2_0.fetch_mod, "fetch_share_change", lambda *_args, **_kwargs: None)

    v2_0.base_mod.refresh_price_cache_tail(
        pd.Timestamp("2026-06-26"),
        max_workers=1,
        symbols=None,
        force_refresh=False,
    )

    assert refreshed == ["000001", "000002"]


def test_file_state_fingerprint_reads_turnover_dates_and_json_meta(tmp_path: Path) -> None:
    turnover_path = tmp_path / "turnover.csv"
    turnover_path.write_text(
        "rebalance_date,turnover\n2026-06-11,0.3\n2026-06-25,0.4\n",
        encoding="utf-8",
    )
    meta_path = tmp_path / "proxy_meta.json"
    meta_path.write_text(
        json.dumps({"start_date": "2010-01-01", "end_date": "2026-06-26", "rebalance_dates_count": 425}),
        encoding="utf-8",
    )

    turnover_state = v2_0.overlay_mod._file_state_fingerprint(turnover_path)
    meta_state = v2_0.overlay_mod._file_state_fingerprint(meta_path)

    assert turnover_state["latest_date"] == "2026-06-25"
    assert turnover_state["row_count"] == 2
    assert turnover_state["date_column"] == "rebalance_date"
    assert meta_state["latest_date"] == "2026-06-26"
    assert meta_state["row_count"] == 425
    assert meta_state["date_column"] == "end_date"


def test_freshness_proof_blocks_misaligned_daily_stream_and_stale_turnover() -> None:
    files = {
        "base_panel_shadow": {"exists": True, "latest_date": "2026-06-26", "row_count": 10},
        "base_index_csv": {"exists": True, "latest_date": "2026-06-17", "row_count": 9},
        "base_costed_nav": {"exists": True, "latest_date": "2026-06-26", "row_count": 10},
        "v2_0_costed_nav": {"exists": True, "latest_date": "2026-06-26", "row_count": 10},
        "base_proxy_turnover": {"exists": True, "latest_date": "2026-06-11", "row_count": 424},
    }

    with pytest.raises(RuntimeError, match="base_index_csv.*2026-06-17.*2026-06-26"):
        v2_0.overlay_mod.validate_top100_freshness_proof(
            files,
            expected_latest_date="2026-06-26",
            expected_latest_rebalance_date="2026-06-25",
        )

    files["base_index_csv"]["latest_date"] = "2026-06-26"

    with pytest.raises(RuntimeError, match="base_proxy_turnover.*2026-06-11.*2026-06-25"):
        v2_0.overlay_mod.validate_top100_freshness_proof(
            files,
            expected_latest_date="2026-06-26",
            expected_latest_rebalance_date="2026-06-25",
        )


def test_daily_stream_continuity_guard_detects_internal_missing_dates() -> None:
    panel_dates = pd.DatetimeIndex(pd.to_datetime(["2026-06-17", "2026-06-18", "2026-06-19"]))
    stream_dates = pd.DatetimeIndex(pd.to_datetime(["2026-06-17", "2026-06-19"]))

    with pytest.raises(RuntimeError, match="v2.0 costed.*2026-06-18"):
        v2_0.overlay_mod.validate_daily_stream_continuity(panel_dates, stream_dates, "v2.0 costed")


def test_target_versions_register_their_costed_streams_with_shared_freshness_guard() -> None:
    assert "v2_3_costed_nav" in v2_3._generate_v2_3_outputs_unlocked.__code__.co_consts
    assert "v2_5_costed_nav" in v2_5._generate_v2_5_outputs_unlocked.__code__.co_consts


def test_historical_rewrite_guard_handles_boolean_key_columns(tmp_path: Path) -> None:
    previous = pd.DataFrame(
        {
            "date": pd.to_datetime(["2026-06-17", "2026-06-18", "2026-06-19"]),
            "overheat_triggered": [False, True, False],
        }
    )
    unchanged = previous.copy()

    v2_0.base_mod.assert_no_historical_rewrite(
        previous=previous,
        candidate=unchanged,
        key_columns=["overheat_triggered"],
        allowed_tail_rows=1,
        label="bool audit",
        audit_path=tmp_path / "unchanged.csv",
    )

    changed = previous.copy()
    changed.loc[0, "overheat_triggered"] = True
    with pytest.raises(RuntimeError, match="bool audit historical rewrite detected"):
        v2_0.base_mod.assert_no_historical_rewrite(
            previous=previous,
            candidate=changed,
            key_columns=["overheat_triggered"],
            allowed_tail_rows=1,
            label="bool audit",
            audit_path=tmp_path / "changed.csv",
        )


def test_attach_proxy_source_summary_fields_marks_public_proxy_and_rescan_requirement() -> None:
    summary: dict[str, object] = {}
    lineage = {
        "source_used": "local_cache_proxy",
        "official_wind_series": False,
        "public_proxy_note": "not official Wind",
    }

    v2_0.overlay_mod.attach_proxy_source_summary_fields(
        summary,
        lineage,
        source_label="costed_v2_3",
        parameter_retest_status={"required_before_parameter_scan": True},
    )

    assert summary["microcap_series_source"] == "local_cache_proxy"
    assert summary["official_wind_series"] is False
    assert summary["proxy_warning"] == "not official Wind"
    assert summary["performance_source_label"] == "costed_v2_3_public_or_local_proxy_not_official_wind"
    assert summary["parameter_retest_status"] == {"required_before_parameter_scan": True}


def test_price_cache_refresh_preflight_counts_missing_stale_and_current(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    price_dir = tmp_path / "prices_raw"
    price_dir.mkdir()
    (price_dir / "000001.csv").write_text("date,close_raw\n2026-06-26,10\n", encoding="utf-8")
    (price_dir / "000002.csv").write_text("date,close_raw\n2026-06-17,9\n", encoding="utf-8")

    monkeypatch.setattr(v2_0.freq_mod, "PRICE_DIR", price_dir, raising=False)
    monkeypatch.setattr(v2_0.freq_mod, "SHARED_PRICE_DIR", None, raising=False)
    monkeypatch.setattr(
        v2_0.freq_mod,
        "resolve_cache_path",
        lambda local_dir, _shared_dir, symbol: local_dir / f"{symbol}.csv"
        if (local_dir / f"{symbol}.csv").exists()
        else None,
    )

    preflight = v2_0.base_mod.price_cache_refresh_preflight(
        pd.Timestamp("2026-06-26"),
        ["000001", "000002", "000003"],
        force_refresh=False,
    )

    assert preflight["symbol_count"] == 3
    assert preflight["current_count"] == 1
    assert preflight["stale_count"] == 1
    assert preflight["missing_count"] == 1
    assert preflight["stale_or_missing_count"] == 2
    assert preflight["sample_stale_or_missing_symbols"] == ["000002", "000003"]


def test_refresh_price_cache_tail_emits_preflight_and_progress(capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch) -> None:
    refreshed: list[str] = []

    monkeypatch.setattr(v2_0.freq_mod, "load_universe", lambda: ["000001", "000002", "000003"])
    monkeypatch.setattr(v2_0.fetch_mod, "ensure_dirs", lambda: None)
    monkeypatch.setattr(
        v2_0.fetch_mod,
        "fetch_price_history",
        lambda symbol, start_date, end_date, force_refresh: refreshed.append(symbol),
    )
    monkeypatch.setattr(v2_0.fetch_mod, "fetch_share_change", lambda *_args, **_kwargs: None)
    monkeypatch.setitem(
        v2_0.base_mod.refresh_price_cache_tail.__globals__,
        "price_cache_refresh_preflight",
        lambda end_date, symbols, force_refresh=False: {
            "symbol_count": len(symbols),
            "stale_or_missing_count": 3,
            "missing_count": 1,
            "stale_count": 2,
            "current_count": 0,
            "force_refresh": False,
            "sample_stale_or_missing_symbols": ["000001", "000002", "000003"],
        },
    )

    v2_0.base_mod.refresh_price_cache_tail(
        pd.Timestamp("2026-06-26"),
        max_workers=1,
        symbols=None,
        force_refresh=False,
        progress_interval=2,
    )

    captured = capsys.readouterr()
    assert refreshed == ["000001", "000002", "000003"]
    assert "price-cache refresh preflight" in captured.err
    assert "symbols=3" in captured.err
    assert "stale_or_missing=3" in captured.err
    assert "price-cache refresh progress 2/3" in captured.err
    assert "price-cache refresh complete 3/3" in captured.err


def test_v2_0_promoted_defaults_match_selected_low_drawdown_line() -> None:
    assert v2_0.TARGET_VOL == pytest.approx(0.15)
    assert v2_0.TARGET_VOL_WINDOW == 75
    assert v2_0.TARGET_VOL_MAX_LEVERAGE == pytest.approx(1.5)
    assert v2_0.TARGET_VOL_SCALE_REBALANCE_THRESHOLD == pytest.approx(0.10)
    assert v2_0.COSTED_NAV_CSV.name == "microcap_top100_mom16_targetvol15_max1p5_v2_0_costed_nav.csv"
    assert v2_0.overlay_mod.LEGACY_COSTED_NAV_CSV.name == "microcap_top100_mom16_targetvol25_max1p5_v2_0_costed_nav.csv"

    fingerprint = v2_0.current_base_fingerprint()
    assert fingerprint["overlay_type"] == "volatility_overheat_exit_then_target_volatility_scaling"
    assert fingerprint["target_vol"] == pytest.approx(0.15)
    assert fingerprint["vol_window"] == 75
    assert fingerprint["overheat_defense"] == {
        "enabled": True,
        "kind": "volatility",
        "window": 60,
        "threshold": pytest.approx(0.23),
        "metric": "microcap_minus_0p8x_hedge_realized_vol",
        "require_positive_trade_return": True,
        "require_signal_reset": True,
        "timing": "applied_to_base_state_before_target_vol_scaling",
    }


def test_v2_0_volatility_overheat_exit_blocks_until_base_signal_reset(monkeypatch: pytest.MonkeyPatch) -> None:
    trading_dates = pd.bdate_range("2020-01-01", periods=72)
    alternating_microcap = [0.02 if i % 2 == 0 else -0.02 for i in range(len(trading_dates))]
    gross = pd.DataFrame(
        {
            "return": [0.001] * len(trading_dates),
            "microcap_ret": alternating_microcap,
            "hedge_ret": [0.0] * len(trading_dates),
            "holding": ["long_microcap_short_zz1000"] * len(trading_dates),
            "next_holding": ["long_microcap_short_zz1000"] * len(trading_dates),
        },
        index=trading_dates,
    )

    monkeypatch.setattr(
        v2_0.embedded_context.base_mod.freq_mod.cost_mod,
        "map_rebalance_apply_costs",
        lambda index, _turnover_df: pd.Series(0.0, index=index),
    )

    out = v2_0.overlay_mod.apply_volatility_overheat_exit(gross, pd.DataFrame())
    trigger_dates = out.index[out["overheat_triggered"]]

    assert len(trigger_dates) >= 1
    first_trigger = trigger_dates[0]
    assert out.at[first_trigger, "holding"] == "long_microcap_short_zz1000"
    assert out.at[first_trigger, "next_holding"] == "cash"
    assert bool(out.loc[out.index > first_trigger, "blocked_until_signal_reset"].any())
    assert out.loc[out.index > first_trigger, "holding"].eq("cash").all()
    assert out["overheat_metric"].dropna().max() >= 0.23


def test_target_versions_accept_promoted_v2_0_contract() -> None:
    v2_3.validate_v2_0_contract()
    v2_5.validate_v2_0_contract()
    assert v2_5.TARGET_VOL_WINDOW == 60
