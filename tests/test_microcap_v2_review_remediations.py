from __future__ import annotations

import pandas as pd
import pytest

import microcap_top100_mom16_biweekly_live_v2_0 as v2_0
import microcap_top100_mom16_biweekly_live_v2_3 as v2_3
import microcap_top100_mom16_biweekly_live_v2_5 as v2_5


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
    frame = pd.read_csv(v2_3.COSTED_NAV_CSV, parse_dates=["date"]).set_index("date")
    row = v2_3._build_signal_row(frame, {}).iloc[0]

    assert row["overheat_enabled"] is True or bool(row["overheat_enabled"]) is True
    assert int(row["overheat_window"]) == v2_3.OVERHEAT_FEATURE_WINDOW
    assert float(row["overheat_threshold"]) == pytest.approx(v2_3.OVERHEAT_TRIGGER_THRESHOLD)
    assert bool(row["overheat_require_positive_trade_return"]) is False
    assert bool(row["overheat_require_signal_reset"]) is False
    assert bool(row["target_vol_enabled"]) is False
    assert int(row["target_vol_window"]) == 0
    assert float(row["target_vol_max_leverage"]) == pytest.approx(1.0)
