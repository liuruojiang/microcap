from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

import microcap_top100_mom16_biweekly_live_v2_0 as v2_0
import microcap_top100_mom16_biweekly_live_v2_3 as v2_3
import microcap_top100_mom16_biweekly_live_v2_5 as v2_5


def test_v2_0_formal_production_identity() -> None:
    assert v2_0.OVERHEAT_WINDOW == 60
    assert v2_0.OVERHEAT_THRESHOLD == pytest.approx(0.23)
    assert v2_0.TARGET_VOL == pytest.approx(0.15)
    assert v2_0.TARGET_VOL_WINDOW == 75
    assert v2_0.TARGET_VOL_MAX_LEVERAGE == pytest.approx(1.5)


def test_v2_0_published_filter_metadata_matches_promoted_lineage() -> None:
    trade_date = pd.Timestamp("2026-06-29")
    summary = v2_0.base_mod.build_summary(
        result=pd.DataFrame(
            {"holding": ["cash"], "next_holding": ["cash"]},
            index=pd.DatetimeIndex([trade_date]),
        ),
        latest_signal=pd.DataFrame(
            [
                {
                    "signal_label": "cash",
                    "microcap_mom": 0.0,
                    "hedge_mom": 0.0,
                    "momentum_gap": 0.0,
                    "microcap_close": 1.0,
                    "hedge_close": 1.0,
                }
            ]
        ),
        latest_rebalance=trade_date,
        prev_rebalance=None,
        next_rebalance=None,
        members_df=pd.DataFrame(),
        changes_df=pd.DataFrame(),
        capital=None,
        anchor_freshness={},
    )

    assert summary["core_params"]["exclude_current_st"] is False
    assert summary["core_params"]["exclude_historical_st"] is True


def test_v2_3_formal_production_identity() -> None:
    assert v2_3.LOOKBACK == 25
    assert v2_3.HALFLIFE == pytest.approx(2.5)
    assert v2_3.R2_ENTRY_GATE == pytest.approx(0.08)
    assert v2_3.OVERHEAT_FEATURE_WINDOW == 10
    assert v2_3.OVERHEAT_TRIGGER_THRESHOLD == pytest.approx(0.26)
    assert v2_3.OVERHEAT_RECOVERY_THRESHOLD == pytest.approx(0.195)
    assert v2_3.TARGET_VOL_ENABLED is False


def test_v2_5_formal_production_identity_is_native_and_unhedged() -> None:
    assert v2_5.SIGNAL_SPREAD_HEDGE_RATIO == pytest.approx(0.0)
    assert v2_5.EXECUTION_HEDGE_RATIO == pytest.approx(0.0)


def test_close_execution_remains_the_official_proxy_timing() -> None:
    assert v2_0.base_mod.EXECUTION_TIMING == v2_0.freq_mod.EXECUTION_TIMING_CLOSE


def test_underfilled_proxy_keeps_total_capital_fully_invested() -> None:
    idx = pd.to_datetime(["2026-01-05", "2026-01-06"])
    result, _, _ = v2_0.freq_mod.simulate_rebalance_path(
        trading_dates=idx,
        returns_df=pd.DataFrame({"000001": [0.0, 0.10]}, index=idx),
        target_members_map={idx[0]: ["000001", "000002"]},
        rebalance_dates=pd.DatetimeIndex([idx[0]]),
        buyable_df=pd.DataFrame(
            {"000001": [True, True], "000002": [False, False]},
            index=idx,
        ),
        sellable_df=pd.DataFrame(
            True,
            index=idx,
            columns=["000001", "000002"],
        ),
        one_side_cost_rate=0.003,
        top_n=2,
        execution_timing=v2_0.freq_mod.EXECUTION_TIMING_CLOSE,
    )

    assert result.iloc[-1]["daily_return"] == pytest.approx(0.10)


def test_terminal_close_rebalance_records_turnover_without_future_return_date() -> None:
    dates = pd.to_datetime(["2026-08-04", "2026-08-05", "2026-08-06"])
    _, turnover, effective = v2_0.freq_mod.simulate_rebalance_path(
        trading_dates=dates,
        returns_df=pd.DataFrame(
            {"A": [0.0, 0.0, 0.01], "B": [0.0, 0.0, 0.0]},
            index=dates,
        ),
        target_members_map={dates[0]: ["A"], dates[-1]: ["B"]},
        rebalance_dates=pd.DatetimeIndex([dates[0], dates[-1]]),
        buyable_df=pd.DataFrame(True, index=dates, columns=["A", "B"]),
        sellable_df=pd.DataFrame(True, index=dates, columns=["A", "B"]),
        one_side_cost_rate=0.003,
        top_n=1,
        execution_timing=v2_0.freq_mod.EXECUTION_TIMING_CLOSE,
    )

    assert turnover["rebalance_date"].tolist() == [dates[0], dates[-1]]
    terminal = turnover.iloc[-1]
    assert terminal["execution_date"] == dates[-1]
    assert pd.isna(terminal["return_start_date"])
    assert terminal["two_side_cost_rate"] == pytest.approx(0.006)
    assert effective[dates[-1]] == ["B"]


def test_close_rebalance_with_following_day_is_not_duplicated() -> None:
    dates = pd.to_datetime(["2026-08-04", "2026-08-05", "2026-08-06", "2026-08-07"])
    _, turnover, _ = v2_0.freq_mod.simulate_rebalance_path(
        trading_dates=dates,
        returns_df=pd.DataFrame(
            {"A": [0.0, 0.0, 0.01, 0.0], "B": [0.0, 0.0, 0.0, 0.02]},
            index=dates,
        ),
        target_members_map={dates[0]: ["A"], dates[2]: ["B"]},
        rebalance_dates=pd.DatetimeIndex([dates[0], dates[2]]),
        buyable_df=pd.DataFrame(True, index=dates, columns=["A", "B"]),
        sellable_df=pd.DataFrame(True, index=dates, columns=["A", "B"]),
        one_side_cost_rate=0.003,
        top_n=1,
        execution_timing=v2_0.freq_mod.EXECUTION_TIMING_CLOSE,
    )

    final_rows = turnover.loc[turnover["rebalance_date"].eq(dates[2])]
    assert len(final_rows) == 1
    assert final_rows.iloc[0]["return_start_date"] == dates[3]


def test_recent_proxy_extension_preserves_frozen_history_and_chains_new_returns() -> None:
    existing = pd.DataFrame(
        {
            "date": pd.to_datetime(["2026-07-01", "2026-07-02"]),
            "close": [100.0, 102.0],
            "daily_return": [0.0, 0.02],
            "holding_count": [100, 100],
        }
    )
    recent = pd.DataFrame(
        {
            "date": pd.to_datetime(
                ["2026-07-01", "2026-07-02", "2026-07-03", "2026-07-06"]
            ),
            "close": [200.0, 201.0, 203.01, 199.96485],
            "daily_return": [0.0, 0.005, 0.01, -0.015],
            "holding_count": [100, 100, 100, 100],
        }
    )

    combined = v2_0.base_mod.splice_recent_proxy_extension(
        existing,
        recent,
        current_index_end=pd.Timestamp("2026-07-02"),
    )

    pd.testing.assert_frame_equal(
        combined.loc[combined["date"] <= pd.Timestamp("2026-07-02")].reset_index(drop=True),
        existing,
    )
    assert combined.loc[combined["date"].eq(pd.Timestamp("2026-07-03")), "close"].iloc[0] == pytest.approx(103.02)
    assert combined.loc[combined["date"].eq(pd.Timestamp("2026-07-06")), "close"].iloc[0] == pytest.approx(101.4747)


def test_flat_fallback_quotes_do_not_count_as_actionable_coverage() -> None:
    members = ["000001", "000002"]
    quotes = pd.DataFrame(
        [
            {
                "code": "000001",
                "rt_price": 10.1,
                "pre_close": 10.0,
                "trade_date": "2026-07-06",
                "quote_time": "14:59:00",
            }
        ]
    )
    augmented, fallback_count = v2_0.base_mod.add_last_close_flat_fallback_quotes(
        quotes,
        member_symbols=members,
        last_close_map={"000001": 10.0, "000002": 20.0},
        latest_trade_date=pd.Timestamp("2026-07-03"),
        max_missing_count=1,
        min_quoted_fraction=0.5,
    )
    stats = v2_0.base_mod.extract_member_quote_trade_date_stats(
        augmented.set_index("code"),
        members,
        pd.Timestamp("2026-07-03"),
    )
    meta = {
        "member_count": 2,
        "member_price_count": 2,
        "member_quote_flat_fallback_count": fallback_count,
        "member_quote_bad_symbols": stats["member_quote_bad_symbols"],
        "member_quote_trade_date_min": stats["member_quote_trade_date_min"],
        "member_quote_trade_date_max": stats["member_quote_trade_date_max"],
        "member_quote_trade_date_count": stats["member_quote_trade_date_count"],
        "hedge_quote_source": sorted(v2_0.base_mod.ALLOWED_ACTIONABLE_HEDGE_QUOTE_SOURCES)[0],
        "hedge_quote_trade_date": "2026-07-06",
        "quote_trade_date": "2026-07-06",
        "latest_anchor_trade_date": "2026-07-03",
        "expected_latest_completed_trade_date": "2026-07-03",
    }

    with pytest.raises(RuntimeError, match="synthetic|fallback"):
        v2_0.base_mod.assert_realtime_meta_is_actionable(meta)


def test_anchor_guard_rejects_calendar_that_does_not_reach_expected_close(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setitem(
        v2_0.base_mod.assert_realtime_anchor_precedes_quote_trade_date.__globals__,
        "_load_realtime_anchor_calendar_index",
        lambda: pd.to_datetime(["2026-07-02"]),
    )

    with pytest.raises(RuntimeError, match="2026-07-03"):
        v2_0.base_mod.assert_realtime_anchor_precedes_quote_trade_date(
            {
                "latest_anchor_trade_date": "2026-07-02",
                "quote_trade_date": "2026-07-06",
                "expected_latest_completed_trade_date": "2026-07-03",
            }
        )


def test_realtime_signal_rows_preserve_fallback_and_snapshot_provenance() -> None:
    row = pd.DataFrame([{}])
    v2_0.overlay_mod._apply_realtime_meta_columns_to_signal_row(
        row,
        {
            "fallback_warning": "stale cache",
            "member_quote_flat_fallback_count": 2,
            "snapshot_row_appended": True,
            "from_cache": True,
            "cache_age_seconds": 12.5,
        },
    )

    assert row.at[0, "fallback_warning"] == "stale cache"
    assert row.at[0, "member_quote_flat_fallback_count"] == 2
    assert bool(row.at[0, "snapshot_row_appended"]) is True


@pytest.mark.parametrize(
    ("module", "builder_name"),
    [
        (v2_3, "_build_realtime_v2_3_official_index"),
        (v2_5, "_build_realtime_v2_5_official_index"),
    ],
)
def test_realtime_version_calendar_comes_from_validated_close_history(
    module: object,
    builder_name: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    close_df = pd.DataFrame(
        {"microcap": [100.0, 101.0, 102.0], "hedge": [200.0, 201.0, 202.0]},
        index=pd.to_datetime(
            ["2026-07-01", "2026-07-02", "2026-07-03 14:59:00"],
            format="mixed",
        ),
    )
    monkeypatch.setattr(
        module,
        "_load_realtime_v2_0_official_index",
        lambda: (_ for _ in ()).throw(AssertionError("stale target-vol file was read")),
    )

    calendar = getattr(module, builder_name)(
        close_df,
        {"latest_anchor_trade_date": "2026-07-02"},
    )

    assert list(calendar) == list(close_df.index[:2])


def test_staged_output_bundle_leaves_official_files_unchanged_on_failure(tmp_path) -> None:
    nav = tmp_path / "nav.csv"
    summary = tmp_path / "summary.json"
    nav.write_text("old-nav", encoding="utf-8")
    summary.write_text("old-summary", encoding="utf-8")

    with pytest.raises(RuntimeError, match="candidate failed"):
        with v2_0.staged_output_bundle([nav, summary], summary_path=summary) as staged:
            staged[nav].write_text("new-nav", encoding="utf-8")
            staged[summary].write_text("new-summary", encoding="utf-8")
            raise RuntimeError("candidate failed")

    assert nav.read_text(encoding="utf-8") == "old-nav"
    assert summary.read_text(encoding="utf-8") == "old-summary"


def test_staged_output_bundle_promotes_summary_after_other_artifacts(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    nav = tmp_path / "nav.csv"
    summary = tmp_path / "summary.json"
    nav.write_text("old-nav", encoding="utf-8")
    summary.write_text("old-summary", encoding="utf-8")
    promotions: list[str] = []
    real_replace = v2_0.overlay_mod._replace_with_retry

    def recording_replace(source, target, *args, **kwargs):
        if target in {nav, summary}:
            promotions.append(target.name)
        return real_replace(source, target, *args, **kwargs)

    monkeypatch.setitem(
        v2_0.staged_output_bundle.__wrapped__.__globals__,
        "_replace_with_retry",
        recording_replace,
    )
    with v2_0.staged_output_bundle([summary, nav], summary_path=summary) as staged:
        staged[nav].write_text("new-nav", encoding="utf-8")
        staged[summary].write_text("new-summary", encoding="utf-8")

    assert nav.read_text(encoding="utf-8") == "new-nav"
    assert summary.read_text(encoding="utf-8") == "new-summary"
    assert promotions == ["nav.csv", "summary.json"]


def test_staged_output_bundle_rolls_back_when_post_promotion_validation_fails(tmp_path) -> None:
    nav = tmp_path / "nav.csv"
    summary = tmp_path / "summary.json"
    nav.write_text("old-nav", encoding="utf-8")
    summary.write_text("old-summary", encoding="utf-8")

    def reject_promoted_bundle() -> None:
        assert nav.read_text(encoding="utf-8") == "new-nav"
        assert summary.read_text(encoding="utf-8") == "new-summary"
        raise RuntimeError("readback failed")

    with pytest.raises(RuntimeError, match="readback failed"):
        with v2_0.staged_output_bundle(
            [nav, summary],
            summary_path=summary,
            post_promotion_validator=reject_promoted_bundle,
        ) as staged:
            staged[nav].write_text("new-nav", encoding="utf-8")
            staged[summary].write_text("new-summary", encoding="utf-8")

    assert nav.read_text(encoding="utf-8") == "old-nav"
    assert summary.read_text(encoding="utf-8") == "old-summary"


def test_generate_v2_0_outputs_keeps_atomic_stage_writable_in_long_worktree(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    overlay_globals = v2_0._generate_v2_0_outputs_unlocked.__globals__
    output_dir = tmp_path
    while len(str(output_dir)) < 125:
        output_dir /= "nested-worktree"
    output_dir.mkdir(parents=True)

    artifact_names = (
        "COSTED_NAV_CSV",
        "NAV_CSV",
        "LATEST_SIGNAL_CSV",
        "PERF_SUMMARY_CSV",
        "PERF_YEARLY_CSV",
        "PERF_NAV_CSV",
        "PERF_JSON",
        "PERF_PNG",
        "SUMMARY_JSON",
    )
    monkeypatch.setitem(overlay_globals, "OUTPUT_DIR", output_dir)
    for name in artifact_names:
        original = Path(overlay_globals[name])
        monkeypatch.setitem(overlay_globals, name, output_dir / original.name)

    staged_paths: list[Path] = []
    real_atomic_write_csv = overlay_globals["_atomic_write_csv"]
    real_atomic_write_text = overlay_globals["_atomic_write_text"]

    def record_atomic_write_csv(frame, path, **kwargs):
        staged_paths.append(Path(path))
        real_atomic_write_csv(frame, path, **kwargs)

    def record_atomic_write_text(path, text, encoding="utf-8"):
        staged_paths.append(Path(path))
        real_atomic_write_text(path, text, encoding=encoding)

    monkeypatch.setitem(overlay_globals, "_atomic_write_csv", record_atomic_write_csv)
    monkeypatch.setitem(overlay_globals, "_atomic_write_text", record_atomic_write_text)

    index = pd.DatetimeIndex([pd.Timestamp("2026-08-07")])
    strategy_frame = pd.DataFrame(
        {
            "microcap_close": [100.0],
            "hedge_close": [200.0],
            "return_net": [0.0],
        },
        index=index,
    )
    embedded_context = overlay_globals["embedded_context"]
    monkeypatch.setattr(
        embedded_context,
        "_load_embedded_base_context",
        lambda: ({}, strategy_frame, pd.DataFrame(index=index)),
    )
    monkeypatch.setattr(embedded_context.base_mod, "run_signal", lambda close_df: strategy_frame)
    monkeypatch.setattr(
        embedded_context.base_mod,
        "apply_momentum_gap_exit_buffer",
        lambda frame, buffer: frame,
    )
    monkeypatch.setitem(overlay_globals, "apply_volatility_overheat_exit", lambda frame, turnover: frame)
    monkeypatch.setitem(overlay_globals, "apply_target_vol_scaling", lambda frame: frame)
    monkeypatch.setitem(overlay_globals, "validate_close_df", lambda frame, label: None)
    monkeypatch.setitem(overlay_globals, "incompatible_v2_0_outputs", lambda: [])
    monkeypatch.setitem(
        overlay_globals,
        "assert_top100_candidate_fresh",
        lambda *args, **kwargs: {"latest_date": "2026-08-07", "row_count": 1},
    )
    monkeypatch.setitem(
        overlay_globals,
        "_build_v2_data_lineage",
        lambda: {"source_used": "test", "official_wind_series": True},
    )
    monkeypatch.setitem(
        overlay_globals,
        "_build_signal_row",
        lambda frame, summary: pd.DataFrame([{"date": index[-1], "holding": "cash"}]),
    )

    def write_performance_bundle(returns, *, source_label, output_paths):
        staged_paths.extend(
            Path(output_paths[name]) for name in ("summary", "yearly", "nav", "json", "png")
        )
        pd.DataFrame({"value": [1]}).to_csv(output_paths["summary"], index=False)
        pd.DataFrame({"value": [1]}).to_csv(output_paths["yearly"], index=False)
        pd.DataFrame({"value": [1]}).to_csv(output_paths["nav"], index=False)
        output_paths["json"].write_text("{}", encoding="utf-8")
        output_paths["png"].write_bytes(b"png")
        return {"summary": {"source_label": source_label}}

    monkeypatch.setitem(overlay_globals, "build_performance_payload", write_performance_bundle)
    monkeypatch.setitem(overlay_globals, "current_base_fingerprint", lambda: {})
    monkeypatch.setitem(overlay_globals, "assert_top100_outputs_fresh", lambda **kwargs: None)
    monkeypatch.setitem(overlay_globals, "_stale_outputs_to_remove_after_generate", lambda *args: [])

    summary, signal, result = v2_0._generate_v2_0_outputs_unlocked()

    assert summary["latest_nav_date"] == "2026-08-07"
    assert signal.at[0, "holding"] == "cash"
    assert result.index.max() == index[-1]
    expected_suffixes = [Path(overlay_globals[name]).suffix for name in artifact_names]
    expected_stage_names = [f"{position:02d}{suffix}" for position, suffix in enumerate(expected_suffixes)]
    actual_stage_names = [path.name for path in staged_paths]
    assert actual_stage_names == expected_stage_names
    assert len(set(actual_stage_names)) == len(artifact_names)
    assert max(map(len, actual_stage_names)) <= len("00.json")
    assert [path.suffix for path in staged_paths] == expected_suffixes
    assert len({path.parent for path in staged_paths}) == 1
    for name in artifact_names:
        assert Path(overlay_globals[name]).exists()


@pytest.mark.parametrize(
    ("module", "prefix_flag", "custom_prefix"),
    [
        (v2_3, "--v23-output-prefix", "audit23"),
        (v2_5, "--v25-output-prefix", "audit25"),
    ],
)
def test_version_main_restores_output_paths_after_custom_prefix(
    module,
    prefix_flag: str,
    custom_prefix: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_prefix = module.OUTPUT_PREFIX
    original_paths = (module.SUMMARY_JSON, module.LATEST_SIGNAL_CSV, module.COSTED_NAV_CSV)
    monkeypatch.setattr(module, "_handle_query", lambda query: None)

    module.main([prefix_flag, custom_prefix, "signal"])

    assert module.OUTPUT_PREFIX == original_prefix
    assert (module.SUMMARY_JSON, module.LATEST_SIGNAL_CSV, module.COSTED_NAV_CSV) == original_paths


def test_custom_v2_output_prefix_isolates_overlay_paths() -> None:
    original_prefix = v2_0.OUTPUT_PREFIX
    try:
        v2_0.configure_output_paths(output_prefix="audit_v20")
        assert v2_0.OUTPUT_PREFIX == "audit_v20"
        assert v2_0.SUMMARY_JSON.name == "audit_v20_summary.json"
        assert v2_0.COSTED_NAV_CSV.name == "audit_v20_costed_nav.csv"
    finally:
        v2_0.configure_output_paths(output_prefix=original_prefix)


def test_clean_rewrite_audit_removes_stale_failure_rows(tmp_path) -> None:
    audit = tmp_path / "audit.csv"
    audit.write_text("date,column\n2020-01-01,return_net\n", encoding="utf-8")

    v2_0.base_mod.clear_rewrite_audit_after_clean_result(audit)

    assert not audit.exists()


def test_dead_pid_lock_is_recovered_immediately(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(v2_0, "OUTPUT_DIR", tmp_path)
    monkeypatch.setattr(v2_0, "_pid_is_alive", lambda _pid: False)
    lock = tmp_path / "dead.lock"
    lock.write_text("999999", encoding="ascii")

    with v2_0._v2_file_lock(
        "dead.lock",
        wait_timeout_seconds=0.2,
        stale_lock_seconds=600.0,
    ):
        assert lock.exists()

    assert not lock.exists()


def test_malformed_symbol_price_cache_raises_with_symbol(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    price_path = tmp_path / "bad-price.csv"
    share_path = tmp_path / "shares.csv"
    price_path.write_text("wrong,close_raw\n2026-01-05,10\n", encoding="utf-8")
    share_path.write_text(
        "change_date,total_shares_10k\n2020-01-01,1000\n",
        encoding="utf-8",
    )
    fn_globals = v2_0.freq_mod.load_symbol_cache.__globals__

    def fake_resolve(local_dir, _shared_dir, _symbol):
        if local_dir == fn_globals["PRICE_DIR"]:
            return price_path
        if local_dir == fn_globals["SHARE_DIR"]:
            return share_path
        return None

    monkeypatch.setitem(fn_globals, "resolve_cache_path", fake_resolve)

    with pytest.raises(RuntimeError, match="000001"):
        v2_0.freq_mod.load_symbol_cache(
            "000001",
            pd.to_datetime(["2026-01-05", "2026-01-06"]),
            pd.to_datetime(["2026-01-05"]),
        )


def test_valid_prelisting_cache_is_legitimately_unavailable(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    price_path = tmp_path / "future-price.csv"
    share_path = tmp_path / "shares.csv"
    price_path.write_text("date,close_raw\n2027-01-05,10\n", encoding="utf-8")
    share_path.write_text(
        "change_date,total_shares_10k\n2020-01-01,1000\n",
        encoding="utf-8",
    )
    fn_globals = v2_0.freq_mod.load_symbol_cache.__globals__

    def fake_resolve(local_dir, _shared_dir, _symbol):
        if local_dir == fn_globals["PRICE_DIR"]:
            return price_path
        if local_dir == fn_globals["SHARE_DIR"]:
            return share_path
        return None

    monkeypatch.setitem(fn_globals, "resolve_cache_path", fake_resolve)

    result = v2_0.freq_mod.load_symbol_cache(
        "000001",
        pd.to_datetime(["2026-01-05", "2026-01-06"]),
        pd.to_datetime(["2026-01-05"]),
    )

    assert result is None


def test_cache_panel_loader_records_legitimate_unavailability(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dates = pd.to_datetime(["2026-01-05", "2026-01-06"])
    cap_dates = pd.to_datetime(["2026-01-05"])
    fn_globals = v2_0.freq_mod.load_cache_panels.__globals__

    def fake_load(symbol, *_args, **_kwargs):
        if symbol == "000002":
            return None
        returns = pd.Series([0.0, 0.01], index=dates)
        caps = pd.Series([1.0], index=cap_dates)
        flags = pd.Series(True, index=dates)
        return symbol, returns, caps, flags, flags

    monkeypatch.setitem(fn_globals, "load_symbol_cache", fake_load)
    returns, _, _, _ = v2_0.freq_mod.load_cache_panels(
        symbols=["000001", "000002"],
        trading_dates=dates,
        cap_dates=cap_dates,
        max_workers=1,
    )

    stats = returns.attrs["symbol_load_stats"]
    assert stats["loaded_count"] == 1
    assert stats["legitimately_unavailable"] == ["000002"]


def test_v2_3_component_momentum_diagnostics_are_not_signal_score_aliases() -> None:
    idx = pd.bdate_range("2025-01-02", periods=80)
    close_df = pd.DataFrame(
        {
            "microcap": 100.0 * (1.002 ** pd.Series(range(len(idx)), index=idx)),
            "hedge": 200.0 * (1.0005 ** pd.Series(range(len(idx)), index=idx)),
        },
        index=idx,
    )
    gross = v2_3.build_spread_log_wls_gross(close_df)

    pd.testing.assert_series_equal(
        gross["momentum_gap"],
        gross["annualized_log_wls_score"],
        check_names=False,
    )
    assert not gross["microcap_mom"].equals(gross["annualized_log_wls_score"])
    assert gross["hedge_mom"].abs().gt(0).any()


def test_v2_3_signal_schema_overwrites_inherited_v2_0_overlay_fields() -> None:
    idx = pd.to_datetime(["2026-06-26", "2026-06-29"])
    frame = pd.DataFrame(
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
    row = v2_3._build_signal_row(frame, {}).iloc[0]

    assert row["overheat_enabled"] is True or bool(row["overheat_enabled"]) is True
    assert int(row["overheat_window"]) == v2_3.OVERHEAT_FEATURE_WINDOW
    assert float(row["overheat_threshold"]) == pytest.approx(v2_3.OVERHEAT_TRIGGER_THRESHOLD)
    assert bool(row["overheat_require_positive_trade_return"]) is False
    assert bool(row["overheat_require_signal_reset"]) is False
    assert bool(row["target_vol_enabled"]) is False
    assert int(row["target_vol_window"]) == 0
    assert float(row["target_vol_max_leverage"]) == pytest.approx(1.0)
