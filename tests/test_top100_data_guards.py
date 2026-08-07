from __future__ import annotations

import argparse
import inspect
import json
from pathlib import Path

import pandas as pd
import pytest

import microcap_top100_mom16_biweekly_live_v2_0 as v2_0
import microcap_top100_mom16_biweekly_live_v2_3 as v2_3
import microcap_top100_mom16_biweekly_live_v2_5 as v2_5
from scripts import run_microcap_v2_3_v2_5_combo50_comparison as combo50


COMBO50_FRESHNESS_DATE_COLUMNS = {
    "base_panel_shadow": "date",
    "base_index_csv": "date",
    "base_proxy_turnover": "rebalance_date",
    "base_costed_nav": "date",
    "v2_0_costed_nav": "date",
    "v2_3_costed_nav": "date",
    "v2_5_costed_nav": "date",
}


def _write_combo50_freshness_artifacts(
    tmp_path: Path,
) -> dict[str, tuple[Path, tuple[str, ...]]]:
    artifacts: dict[str, tuple[Path, tuple[str, ...]]] = {}
    dates = ["2026-06-26", "2026-06-29"]
    for name, date_column in COMBO50_FRESHNESS_DATE_COLUMNS.items():
        path = tmp_path / f"{name}.csv"
        pd.DataFrame({date_column: dates, "value": [1.0, 1.0]}).to_csv(path, index=False)
        artifacts[name] = (path, (date_column,))
    return artifacts


def test_full_proxy_bundle_uses_backtest_universe_when_symbols_are_omitted(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, list[str]] = {}
    trading_dates = pd.DatetimeIndex(pd.to_datetime(["2026-06-17", "2026-06-18", "2026-06-19"]))
    rebalance_dates = pd.DatetimeIndex([pd.Timestamp("2026-06-18")])

    monkeypatch.setattr(v2_0.freq_mod, "load_current_universe", lambda: ["999999"])
    monkeypatch.setattr(v2_0.freq_mod, "load_universe", lambda: ["000001", "000002"])
    build_globals = v2_0.base_mod.build_local_proxy_bundle.__globals__
    monkeypatch.setitem(build_globals, "build_biweekly_rebalance_dates", lambda _dates: rebalance_dates)
    monkeypatch.setitem(build_globals, "load_name_map", lambda: {})
    monkeypatch.setitem(
        build_globals,
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
    monkeypatch.setitem(
        build_globals,
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


def test_costed_nav_date_reads_use_utf8_sig(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    costed_path = tmp_path / "costed_nav.csv"
    costed_path.write_text("date,return_net\n2026-06-26,0\n2026-06-29,0.01\n", encoding="utf-8-sig")
    real_read_csv = pd.read_csv

    def fake_read_csv(path, *args, **kwargs):
        if Path(path) == costed_path:
            assert kwargs.get("encoding") == "utf-8-sig"
            return real_read_csv(path, *args, **kwargs)
        return real_read_csv(path, *args, **kwargs)

    monkeypatch.setattr(v2_0.overlay_mod.pd, "read_csv", fake_read_csv)
    monkeypatch.setattr(v2_0.overlay_mod, "COSTED_NAV_CSV", costed_path)
    monkeypatch.setattr(v2_0, "COSTED_NAV_CSV", costed_path)
    monkeypatch.setitem(v2_0._overlay_ns, "COSTED_NAV_CSV", costed_path)
    monkeypatch.setattr(v2_0.overlay_mod, "generate_v2_0_outputs", lambda: (_ for _ in ()).throw(AssertionError("no fallback generation")))
    monkeypatch.setitem(
        v2_0._overlay_ns,
        "generate_v2_0_outputs",
        lambda: (_ for _ in ()).throw(AssertionError("no fallback generation")),
    )

    state = v2_0.overlay_mod._file_state_fingerprint(costed_path)
    assert state["latest_date"] == "2026-06-29"
    assert state["date_column"] == "date"
    assert v2_0.overlay_mod._read_artifact_date_index(costed_path).tolist() == list(pd.to_datetime(["2026-06-26", "2026-06-29"]))

    assert v2_0.overlay_mod._load_realtime_v2_0_official_index().tolist() == list(pd.to_datetime(["2026-06-26", "2026-06-29"]))

    for module in [v2_3, v2_5]:
        monkeypatch.setattr(module, "_official_v2_0_cache_key", lambda: "unit")
        monkeypatch.setattr(module, "_OFFICIAL_V2_0_OUT_CACHE", None)
        monkeypatch.setattr(module, "_load_official_v2_0_out", lambda: (_ for _ in ()).throw(AssertionError("no fallback generation")))

        assert module._load_realtime_v2_0_official_index().tolist() == list(pd.to_datetime(["2026-06-26", "2026-06-29"]))


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


def test_combo50_freshness_proof_records_all_artifact_dates_and_row_counts(tmp_path: Path) -> None:
    proof = combo50.validate_formal_freshness(_write_combo50_freshness_artifacts(tmp_path))

    assert proof["shared_latest_close_date"] == "2026-06-29"
    assert set(proof["artifacts"]) == set(COMBO50_FRESHNESS_DATE_COLUMNS)
    assert all(state["exists"] for state in proof["artifacts"].values())
    assert all(state["latest_date"] == "2026-06-29" for state in proof["artifacts"].values())
    assert all(state["row_count"] == 2 for state in proof["artifacts"].values())


@pytest.mark.parametrize("artifact_name", COMBO50_FRESHNESS_DATE_COLUMNS)
def test_combo50_freshness_proof_rejects_each_missing_artifact(
    tmp_path: Path,
    artifact_name: str,
) -> None:
    artifacts = _write_combo50_freshness_artifacts(tmp_path)
    artifacts[artifact_name][0].unlink()

    with pytest.raises(RuntimeError, match=rf"{artifact_name}.*missing"):
        combo50.validate_formal_freshness(artifacts)


@pytest.mark.parametrize("artifact_name", COMBO50_FRESHNESS_DATE_COLUMNS)
def test_combo50_freshness_proof_rejects_each_date_misaligned_artifact(
    tmp_path: Path,
    artifact_name: str,
) -> None:
    artifacts = _write_combo50_freshness_artifacts(tmp_path)
    path, date_columns = artifacts[artifact_name]
    pd.DataFrame({date_columns[0]: ["2026-06-25", "2026-06-26"], "value": [1.0, 1.0]}).to_csv(
        path,
        index=False,
    )

    with pytest.raises(RuntimeError, match=rf"{artifact_name}.*2026-06-26.*2026-06-29"):
        combo50.validate_formal_freshness(artifacts)


def test_combo50_validates_freshness_before_creating_formal_outputs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run_folder = tmp_path / "formal-output"
    monkeypatch.setattr(combo50, "RUN_FOLDER", run_folder)
    monkeypatch.setattr(
        combo50,
        "validate_formal_freshness",
        lambda: (_ for _ in ()).throw(RuntimeError("freshness blocked")),
        raising=False,
    )
    monkeypatch.setattr(
        combo50,
        "_load_costed_nav",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("stream loaded before validation")),
    )

    with pytest.raises(RuntimeError, match="freshness blocked"):
        combo50.main()

    assert not run_folder.exists()


def test_daily_stream_continuity_guard_detects_internal_missing_dates() -> None:
    panel_dates = pd.DatetimeIndex(pd.to_datetime(["2026-06-17", "2026-06-18", "2026-06-19"]))
    stream_dates = pd.DatetimeIndex(pd.to_datetime(["2026-06-17", "2026-06-19"]))

    with pytest.raises(RuntimeError, match="v2.0 costed.*2026-06-18"):
        v2_0.overlay_mod.validate_daily_stream_continuity(panel_dates, stream_dates, "v2.0 costed")


def test_recent_extension_replacement_start_uses_trimmed_recent_index_start() -> None:
    recent_dates = pd.DatetimeIndex(pd.to_datetime(["2026-04-29", "2026-04-30", "2026-05-06"]))
    recent_index_df = pd.DataFrame({"date": pd.to_datetime(["2026-05-06"])})

    replacement_start = v2_0.base_mod.recent_extension_replacement_start(recent_dates, recent_index_df)

    assert replacement_start == pd.Timestamp("2026-05-06")


def test_target_versions_register_their_costed_streams_with_shared_freshness_guard() -> None:
    assert "v2_3_costed_nav" in inspect.getsource(v2_3._generate_v2_3_outputs_unlocked)
    assert "v2_5_costed_nav" in inspect.getsource(v2_5._generate_v2_5_outputs_unlocked)


def test_v2_3_official_params_are_lb25_overheat_without_target_vol() -> None:
    assert v2_3.LOOKBACK == 25
    assert v2_3.HALFLIFE == 2.5
    assert v2_3.R2_WINDOW == 25
    assert v2_3.R2_ENTRY_GATE == 0.08
    assert v2_3.MOMENTUM_GAP_EXIT_BUFFER == 0.08
    assert v2_3.OVERHEAT_KIND == "vol"
    assert v2_3.OVERHEAT_FEATURE_WINDOW == 10
    assert v2_3.OVERHEAT_TRIGGER_THRESHOLD == 0.26
    assert v2_3.OVERHEAT_RECOVERY_RATIO == 0.75
    assert v2_3.OVERHEAT_RECOVERY_THRESHOLD == pytest.approx(0.195)
    assert v2_3.TARGET_VOL_ENABLED is False
    assert v2_3.CASH_DAY_YIELD_ENABLED is False
    assert v2_3.FINANCING_ENABLED is False

    fingerprint = v2_3.current_base_fingerprint()

    assert fingerprint["signal_model"]["lookback"] == 25
    assert fingerprint["signal_model"]["halflife"] == 2.5
    assert fingerprint["signal_model"]["r2_entry_gate"] == 0.08
    assert fingerprint["signal_model"]["momentum_gap_exit_buffer"] == 0.08
    assert fingerprint["overheat_defense"] == {
        "enabled": True,
        "kind": "vol",
        "feature_window": 10,
        "trigger_threshold": 0.26,
        "recovery_ratio": 0.75,
        "recovery_threshold": pytest.approx(0.195),
    }
    assert fingerprint["target_volatility_scaling"] == {"enabled": False}
    assert fingerprint["cash_day_yield"] == {"enabled": False}
    assert fingerprint["financing"] == {"enabled": False}


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
    assert bool(out.at[first_trigger, "blocked_until_signal_reset"]) is True
    assert bool(out.loc[out.index > first_trigger, "blocked_until_signal_reset"].any())
    assert out.loc[out.index > first_trigger, "holding"].eq("cash").all()
    assert out["overheat_metric"].dropna().max() >= 0.23


def test_target_versions_accept_promoted_v2_0_contract() -> None:
    v2_3.validate_v2_0_contract()
    v2_5.validate_v2_0_contract()


def test_v2_5_official_params_are_selected_no_target_vol() -> None:
    assert v2_5.LOOKBACK == 17
    assert v2_5.HALFLIFE == pytest.approx(3.0)
    assert v2_5.ENTRY_THRESHOLD == pytest.approx(0.46)
    assert v2_5.EXIT_THRESHOLD == pytest.approx(0.25)
    assert v2_5.TARGET_VOL_ENABLED is False
    assert v2_5.CASH_DAY_YIELD_ENABLED is False
    assert v2_5.FINANCING_ENABLED is False
    fingerprint = v2_5.current_base_fingerprint()
    assert fingerprint["signal_model"] == "microcap_only_log_wls_exp_halflife_3p0_lb17_entry46_exit25_no_targetvol"
    assert fingerprint["target_volatility_scaling"] == {"enabled": False}


def _actionable_realtime_meta(anchor: str, quote: str) -> dict[str, object]:
    return {
        "member_count": 2,
        "member_price_count": 2,
        "member_quote_bad_symbols": [],
        "member_quote_trade_date_min": quote,
        "member_quote_trade_date_max": quote,
        "member_quote_trade_date_count": 2,
        "hedge_quote_source": "eastmoney_stock_get",
        "hedge_quote_trade_date": quote,
        "quote_trade_date": quote,
        "latest_anchor_trade_date": anchor,
        "expected_latest_completed_trade_date": anchor,
    }


def test_realtime_meta_requires_anchor_to_be_previous_completed_trade_day(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(
        v2_0._base_ns,
        "_load_realtime_v2_0_official_index",
        lambda: pd.DatetimeIndex(pd.to_datetime(["2026-06-25", "2026-06-26", "2026-06-29"])),
    )

    v2_0.base_mod.assert_realtime_meta_is_actionable(_actionable_realtime_meta("2026-06-26", "2026-06-29"))

    with pytest.raises(RuntimeError, match="latest_anchor_trade_date.*previous completed trading day"):
        v2_0.base_mod.assert_realtime_meta_is_actionable(_actionable_realtime_meta("2026-06-25", "2026-06-29"))


def test_v2_0_target_vol_costed_turnover_skips_next_session_position_exits() -> None:
    idx = pd.date_range("2026-06-24", periods=4, freq="B")
    holding = pd.Series(
        ["cash", "long_microcap_short_zz1000", "long_microcap_short_zz1000", "long_microcap_short_zz1000"],
        index=idx,
    )
    next_holding = pd.Series(
        ["long_microcap_short_zz1000", "long_microcap_short_zz1000", "long_microcap_short_zz1000", "cash"],
        index=idx,
    )
    turnover = pd.Series([1.8, 1.8, 0.2, 1.8], index=idx)

    costed = v2_0.overlay_mod.calc_target_vol_costed_turnover(
        holding,
        turnover,
        next_holding=next_holding,
    )

    assert costed.tolist() == pytest.approx([0.0, 0.0, 0.2, 0.0])


def test_v2_0_rewrite_allowlist_only_accepts_audited_scale_cost_cells(tmp_path: Path) -> None:
    audit_path = tmp_path / "rewrite_audit.csv"
    pd.DataFrame(
        [
            {"date": "2017-07-17", "column": "return_net", "change_type": "value_changed"},
            {"date": "2022-04-28", "column": "scale_change_cost", "change_type": "value_changed"},
        ]
    ).to_csv(audit_path, index=False)

    assert v2_0.overlay_mod.v2_0_rewrite_audit_matches_allowlist(audit_path)

    pd.DataFrame(
        [
            {"date": "2017-07-17", "column": "return_net", "change_type": "value_changed"},
            {"date": "2018-01-02", "column": "return_net", "change_type": "value_changed"},
        ]
    ).to_csv(audit_path, index=False)

    assert not v2_0.overlay_mod.v2_0_rewrite_audit_matches_allowlist(audit_path)


def _patch_perf_paths(monkeypatch: pytest.MonkeyPatch, module, tmp_path: Path, ns: dict[str, object] | None = None) -> None:
    paths = {
        "PERF_SUMMARY_CSV": tmp_path / "performance_summary.csv",
        "PERF_YEARLY_CSV": tmp_path / "performance_yearly.csv",
        "PERF_NAV_CSV": tmp_path / "performance_nav.csv",
        "PERF_JSON": tmp_path / "performance_summary.json",
        "PERF_PNG": tmp_path / "performance_curve.png",
    }
    for name, path in paths.items():
        if ns is not None:
            monkeypatch.setitem(ns, name, path)
        else:
            monkeypatch.setattr(module, name, path)


def test_target_versions_read_existing_costed_nav_with_utf8_sig(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    class StopAfterPreviousRead(Exception):
        pass

    idx = pd.to_datetime(["2026-06-26", "2026-06-29"])
    base_gross = pd.DataFrame(
        {
            "microcap_close": [1.0, 1.01],
            "hedge_close": [1.0, 0.99],
        },
        index=idx,
    )
    out = pd.DataFrame(
        {
            "return_net": [0.0, 0.01],
            "nav_net": [1.0, 1.01],
        },
        index=idx,
    )
    previous = out.rename_axis("date").reset_index()
    real_read_csv = pd.read_csv

    def fake_read_csv(path, *args, **kwargs):
        if Path(path) in {v2_3.COSTED_NAV_CSV, v2_5.COSTED_NAV_CSV}:
            assert kwargs.get("encoding") == "utf-8-sig"
            return previous.copy()
        return real_read_csv(path, *args, **kwargs)

    def stop_after_audit(**kwargs):
        assert "date" in kwargs["previous"].columns
        raise StopAfterPreviousRead

    monkeypatch.setattr(v2_3, "COSTED_NAV_CSV", tmp_path / "v23_costed.csv")
    monkeypatch.setattr(v2_5, "COSTED_NAV_CSV", tmp_path / "v25_costed.csv")
    v2_3.COSTED_NAV_CSV.write_text("date,return_net\n2026-06-26,0\n", encoding="utf-8-sig")
    v2_5.COSTED_NAV_CSV.write_text("date,return_net\n2026-06-26,0\n", encoding="utf-8-sig")
    monkeypatch.setattr(v2_3.pd, "read_csv", fake_read_csv)
    monkeypatch.setattr(v2_0.base_mod, "assert_no_historical_rewrite", stop_after_audit)

    for module, build_name, incompatible_name, generate_name in [
        (v2_3, "build_v2_3_result", "incompatible_v2_3_outputs", "_generate_v2_3_outputs_unlocked"),
        (v2_5, "build_v2_5_result", "incompatible_v2_5_outputs", "_generate_v2_5_outputs_unlocked"),
    ]:
        monkeypatch.setattr(module, "_load_official_v2_0_out", lambda: pd.DataFrame(index=idx))
        monkeypatch.setattr(
            v2_0.embedded_context,
            "_load_embedded_base_context",
            lambda: ({"latest_signal": {}}, base_gross.copy(), pd.DataFrame()),
        )
        monkeypatch.setattr(module, incompatible_name, lambda: [])
        monkeypatch.setattr(module, build_name, lambda *_args, **_kwargs: out.copy())

        with pytest.raises(StopAfterPreviousRead):
            getattr(module, generate_name)()


def test_v2_3_performance_query_generates_and_builds_under_one_lock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = {"locked": False, "generated": False, "built": False}
    perf_df = pd.DataFrame(
        {"return_net": [0.0, 0.01], "nav_net": [1.0, 1.01]},
        index=pd.to_datetime(["2026-06-26", "2026-06-29"]),
    )

    class GuardedLock:
        def __enter__(self):
            assert not state["locked"]
            state["locked"] = True

        def __exit__(self, exc_type, exc, tb):
            state["locked"] = False

    def fake_generate_unlocked():
        assert state["locked"]
        state["generated"] = True
        return {}, pd.DataFrame(), perf_df.copy()

    def fake_build_performance_outputs(**kwargs):
        assert state["locked"]
        state["built"] = True
        assert kwargs["perf_df"].index.name == "date"
        assert kwargs["perf_df"]["return_net"].tolist() == [0.0, 0.01]

    monkeypatch.setattr(v2_3, "v2_3_output_lock", lambda: GuardedLock())
    monkeypatch.setattr(v2_3, "generate_v2_3_outputs", lambda: (_ for _ in ()).throw(AssertionError("use unlocked generator")))
    monkeypatch.setattr(v2_3, "_generate_v2_3_outputs_unlocked", fake_generate_unlocked)
    monkeypatch.setattr(v2_3.pd, "read_csv", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("do not reread costed NAV")))
    monkeypatch.setattr(v2_0.embedded_context.base_mod, "build_performance_outputs", fake_build_performance_outputs)

    v2_3._print_performance_query("表现")

    assert state["generated"] is True
    assert state["built"] is True


@pytest.mark.parametrize(
    ("module", "namespace_name"),
    [
        (v2_0.overlay_mod, "_overlay_ns"),
        (v2_3, None),
        (v2_5, None),
    ],
)
def test_performance_payload_writes_required_window_summary(
    module,
    namespace_name: str | None,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    namespace = getattr(v2_0, namespace_name) if namespace_name else None
    _patch_perf_paths(monkeypatch, module, tmp_path, namespace)
    ret = pd.Series(0.001, index=pd.bdate_range("2011-01-03", "2026-06-29"), dtype=float)

    payload = module.build_performance_payload(ret, source_label="unit_test")
    summary = pd.read_csv(tmp_path / "performance_summary.csv")

    assert summary["window"].tolist() == ["full", "last_10y", "last_5y", "last_3y", "last_1y"]
    assert {"annual_pct", "max_drawdown_pct"}.issubset(summary.columns)
    assert payload["windows"][0]["window"] == "full"


def test_v2_5_signal_row_clears_inherited_v2_0_overlay_fields() -> None:
    idx = pd.to_datetime(["2026-06-26", "2026-06-29"])
    net_df = pd.DataFrame(
        {
            "holding": ["cash", "cash"],
            "next_holding": ["cash", "cash"],
            "current_execution_scale": [0.0, 0.0],
            "execution_scale": [0.0, 0.0],
            "next_session_target_scale": [0.0, 0.0],
            "next_session_actionable_scale": [0.0, 0.0],
            "target_vol_scale_next_session": [0.0, 0.0],
            "annualized_log_wls_score": [0.1, -0.2],
            "log_wls_r2": [0.5, 0.6],
            "microcap_nav": [1.0, 1.01],
            "cash_day_yield": [0.0, 0.0],
        },
        index=idx,
    )

    signal = v2_5._build_signal_row(net_df, {"latest_signal": {}}).iloc[0]

    assert signal["fixed_hedge_ratio"] == pytest.approx(0.0)
    assert bool(signal["overheat_enabled"]) is False
    assert signal["overheat_kind"] == "disabled"
    assert signal["overheat_window"] == 0
    assert signal["overheat_threshold"] == pytest.approx(0.0)
    assert bool(signal["cash_day_yield_enabled"]) is False
    assert float(signal["cash_day_yield_annual"]) == pytest.approx(0.0)
    assert bool(signal["financing_enabled"]) is False
    assert signal["momentum_gap_exit_buffer"] == pytest.approx(v2_5.EXIT_THRESHOLD)


def test_v2_3_signal_row_uses_fixed_exposure_entry_exit_costs() -> None:
    idx = pd.to_datetime(["2026-06-26", "2026-06-29"])
    net_df = pd.DataFrame(
        {
            "holding": ["cash", "long_microcap_short_zz1000"],
            "next_holding": ["long_microcap_short_zz1000", "cash"],
            "current_execution_scale": [0.0, 1.0],
            "execution_scale": [0.0, 1.0],
            "next_session_target_scale": [1.0, 0.0],
            "next_session_actionable_scale": [1.0, 0.0],
            "target_vol_scale_next_session": [1.0, 0.0],
            "annualized_log_wls_score": [0.1, -0.2],
            "log_wls_r2": [0.5, 0.6],
            "spread_nav": [1.0, 1.01],
            "overheat_feature_value": [0.1, 0.2],
            "actual_execution_scale": [0.0, 1.0],
        },
        index=idx,
    )

    signal = v2_3._build_signal_row(net_df, {"latest_signal": {}}).iloc[0]

    assert signal["next_session_leg_turnover"] == pytest.approx(1.0 + v2_3.EXECUTION_HEDGE_RATIO)
    assert signal["next_session_turnover"] == pytest.approx(1.0 + v2_3.EXECUTION_HEDGE_RATIO)
    assert signal["next_session_leg_cost_est_raw"] == pytest.approx(v2_0.base_mod.freq_mod.cost_mod.EXIT_COST)
    assert signal["next_session_overlay_cost_est"] == pytest.approx(0.0)
    assert signal["next_session_trade_cost_est"] == pytest.approx(v2_0.base_mod.freq_mod.cost_mod.EXIT_COST)
    assert signal["next_session_trade_cost_est_type"] == "fixed_exposure_entry_exit"


def test_v2_5_signal_row_uses_unhedged_entry_exit_costs() -> None:
    idx = pd.to_datetime(["2026-06-26", "2026-06-29"])
    net_df = pd.DataFrame(
        {
            "holding": ["cash", "long_microcap_top100"],
            "next_holding": ["long_microcap_top100", "cash"],
            "current_execution_scale": [0.0, 1.0],
            "execution_scale": [0.0, 1.0],
            "next_session_target_scale": [1.0, 0.0],
            "next_session_actionable_scale": [1.0, 0.0],
            "target_vol_scale_next_session": [1.0, 0.0],
            "annualized_log_wls_score": [0.1, -0.2],
            "log_wls_r2": [0.5, 0.6],
            "microcap_nav": [1.0, 1.01],
            "cash_day_yield": [0.0, 0.0],
        },
        index=idx,
    )

    signal = v2_5._build_signal_row(net_df, {"latest_signal": {}}).iloc[0]

    assert signal["current_holding"] == "long_microcap_top100"
    assert signal["next_holding"] == "cash"
    assert signal["trade_state"] == "close"
    assert signal["next_session_leg_turnover"] == pytest.approx(1.0)
    assert signal["next_session_turnover"] == pytest.approx(1.0)
    assert signal["next_session_leg_cost_est_raw"] == pytest.approx(v2_0.base_mod.freq_mod.cost_mod.EXIT_COST)
    assert signal["next_session_overlay_cost_est"] == pytest.approx(0.0)
    assert signal["next_session_trade_cost_est"] == pytest.approx(v2_0.base_mod.freq_mod.cost_mod.EXIT_COST)
    assert signal["next_session_trade_cost_est_type"] == "fixed_exposure_entry_exit"


def test_v2_5_signal_label_matches_native_next_holding() -> None:
    idx = pd.to_datetime(["2026-06-26", "2026-06-29"])
    net_df = pd.DataFrame(
        {
            "holding": ["cash", "cash"],
            "next_holding": ["cash", "long_microcap_top100"],
            "current_execution_scale": [0.0, 0.0],
            "execution_scale": [0.0, 0.0],
            "next_session_target_scale": [0.0, 1.0],
            "next_session_actionable_scale": [0.0, 1.0],
            "target_vol_scale_next_session": [0.0, 1.0],
            "annualized_log_wls_score": [0.1, 0.6],
            "log_wls_r2": [0.5, 0.6],
            "microcap_nav": [1.0, 1.01],
            "cash_day_yield": [0.0, 0.0],
        },
        index=idx,
    )

    signal = v2_5._build_signal_row(net_df, {"latest_signal": {}}).iloc[0]

    assert signal["current_holding"] == "cash"
    assert signal["next_holding"] == "long_microcap_top100"
    assert signal["signal_label"] == "long_microcap_top100"


def test_no_target_vol_versions_write_zero_target_vol_execution_scale_on_cash_days() -> None:
    idx = pd.to_datetime(["2026-06-26", "2026-06-29"])
    v23_out = pd.DataFrame(
        {
            "holding": ["long_microcap_short_zz1000", "cash"],
            "next_holding": ["cash", "cash"],
            "return": [0.0, 0.0],
            "microcap_ret": [0.0, 0.0],
            "hedge_ret": [0.0, 0.0],
            "spread_nav": [1.0, 1.0],
        },
        index=idx,
    )
    v23_out = v2_3.apply_overheat_defense(v23_out, pd.DataFrame())
    assert v23_out.loc[v23_out["holding"].eq("cash"), "target_vol_execution_scale"].eq(0.0).all()

    v25_out = pd.DataFrame(
        {
            "holding": ["long_microcap", "cash"],
            "next_holding": ["cash", "cash"],
            "microcap_ret": [0.0, 0.0],
            "return_net": [0.0, 0.0],
            "nav_net": [1.0, 1.0],
            "total_cost": [0.0, 0.0],
            "overlay_pre_cost_return": [0.0, 0.0],
        },
        index=idx,
    )
    v25_out = v2_5.apply_no_target_vol(v25_out)
    assert v25_out.loc[v25_out["holding"].eq("cash"), "target_vol_execution_scale"].eq(0.0).all()


def test_v2_5_legacy_retest_outputs_are_discoverable_for_cleanup(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    stale = tmp_path / "microcap_top100_mom16_biweekly_live_v2_5_retest_20260629_costed_nav.csv"
    stale.write_text("date,return_net\n2026-06-29,0\n", encoding="utf-8")
    monkeypatch.setattr(v2_5, "OUTPUT_DIR", tmp_path)

    assert v2_5.stale_v2_5_legacy_retest_outputs() == [stale]


def test_v2_3_v2_5_combo50_comparison_has_replayable_script() -> None:
    script = Path("scripts/run_microcap_v2_3_v2_5_combo50_comparison.py")

    assert script.exists()
    text = script.read_text(encoding="utf-8")
    assert "microcap_top100_mom16_lb25_hl2p5_r2w25_g0p08_eb0p08_vol10_oh_t0p26_rr0p75_exec0p8_v2_3_costed_nav.csv" in text
    assert "microcap_top100_mom16_lb17_hl3_entry46_exit25_no_targetvol_v2_5_costed_nav.csv" in text
    assert "additional_combo_rebalance_cost" in text
    assert "20260602" not in text
