from __future__ import annotations

import time
import json
import hashlib
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

import microcap_top100_mom16_biweekly_live_v2_0 as v2_0
import microcap_top100_mom16_biweekly_live_v2_3 as v2_3
import microcap_top100_mom16_biweekly_live_v2_5 as v2_5


def _current_realtime_refresh_proof_fields() -> dict[str, str]:
    return {
        "expected_latest_completed_trade_date_source": v2_0.base_mod.REALTIME_REFRESH_PROOF_SOURCE,
        "expected_latest_completed_trade_date_verified_on": str(v2_0.base_mod._cn_local_day().date()),
    }


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
                **_current_realtime_refresh_proof_fields(),
            }
        )


def test_anchor_guard_rejects_calendar_self_validation_without_refresh_proof(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setitem(
        v2_0.base_mod.assert_realtime_anchor_precedes_quote_trade_date.__globals__,
        "_load_realtime_anchor_calendar_index",
        lambda: pd.to_datetime(["2026-08-10"]),
    )

    with pytest.raises(RuntimeError, match="independent refresh proof"):
        v2_0.base_mod.assert_realtime_anchor_precedes_quote_trade_date(
            {
                "latest_anchor_trade_date": "2026-08-10",
                "quote_trade_date": "2026-08-12",
                "expected_latest_completed_trade_date": "2026-08-10",
            }
        )


def test_refresh_state_fails_closed_when_independent_target_is_unavailable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from scripts import realtime_state_bundle
    import run_top100_v1_6_v1_8_realtime_signals as realtime_runner

    monkeypatch.setattr(realtime_runner, "ensure_static_realtime_inputs", lambda **_kwargs: None)
    monkeypatch.setattr(v2_0, "_sync_embedded_base_config", lambda: None)
    monkeypatch.setattr(v2_0, "_build_base_args", lambda max_workers: SimpleNamespace())
    monkeypatch.setattr(v2_0.base_mod, "build_output_paths", lambda _prefix: {})
    monkeypatch.setattr(
        v2_0.base_mod,
        "build_refreshed_panel_shadow",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("independent refresh failed")),
    )
    monkeypatch.setattr(
        realtime_state_bundle,
        "validate_state",
        lambda *_args, **_kwargs: {"ok": True, "errors": [], "warnings": []},
    )

    with pytest.raises(RuntimeError, match="no independent refresh target"):
        realtime_state_bundle.refresh_state(tmp_path, max_workers=1)


def test_validate_state_accepts_only_current_matching_refresh_proof(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from scripts import realtime_state_bundle

    target = pd.Timestamp("2026-08-12").date()
    proof = {
        "version": realtime_state_bundle.REFRESH_PROOF_VERSION,
        "source": realtime_state_bundle.REFRESH_PROOF_SOURCE,
        "target_end_date": target.isoformat(),
        "verified_on": target.isoformat(),
    }
    monkeypatch.setattr(realtime_state_bundle, "REQUIRED_FILES", ())
    monkeypatch.setattr(realtime_state_bundle, "_csv_last_date", lambda *_args, **_kwargs: target)
    monkeypatch.setattr(realtime_state_bundle, "_current_member_symbols", lambda _root: ["000001"])
    monkeypatch.setattr(realtime_state_bundle, "_has_current_v2_static_member_context", lambda _root: True)
    monkeypatch.setattr(realtime_state_bundle, "_load_refresh_proof", lambda _root: dict(proof))

    current = realtime_state_bundle.validate_state(tmp_path, today=target)
    assert current["ok"] is True

    proof["verified_on"] = "2026-08-11"
    stale = realtime_state_bundle.validate_state(tmp_path, today=target)
    assert stale["ok"] is False
    assert any("refresh proof is stale" in error for error in stale["errors"])

    monkeypatch.setattr(realtime_state_bundle, "_load_refresh_proof", lambda _root: None)
    offline_restore_seed = realtime_state_bundle.validate_state(
        tmp_path,
        today=target,
        require_current_refresh_proof=False,
    )
    assert offline_restore_seed["ok"] is True


def test_refresh_proof_is_packaged_with_realtime_state(tmp_path: Path) -> None:
    from scripts import realtime_state_bundle

    proof_path = tmp_path / realtime_state_bundle.REFRESH_PROOF_REL
    proof_path.parent.mkdir(parents=True, exist_ok=True)
    proof_path.write_text("{}", encoding="utf-8")

    assert realtime_state_bundle.REFRESH_PROOF_REL in realtime_state_bundle._iter_bundle_files(tmp_path)


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
    ("degraded", "expected_source", "expects_warning"),
    [
        (False, "validated_refreshed_state", False),
        (True, "cached_proxy_fallback", True),
    ],
)
def test_cached_proxy_context_distinguishes_validated_state_from_fallback(
    monkeypatch: pytest.MonkeyPatch,
    degraded: bool,
    expected_source: str,
    expects_warning: bool,
) -> None:
    cache_end = pd.Timestamp("2026-08-21")
    close_df = pd.DataFrame(
        {"microcap": [1.0], "hedge": [1.0]},
        index=pd.DatetimeIndex([cache_end]),
    )
    builder_globals = v2_0.base_mod.build_realtime_context_from_cached_proxy.__globals__
    monkeypatch.setitem(
        builder_globals,
        "reusable_cached_proxy_end_for_realtime",
        lambda *_args, **_kwargs: cache_end,
    )
    monkeypatch.setitem(builder_globals, "load_close_df", lambda *_args, **_kwargs: close_df)
    monkeypatch.setitem(builder_globals, "build_base_signal_context", lambda *_args, **_kwargs: {})

    context = v2_0.base_mod.build_realtime_context_from_cached_proxy(
        SimpleNamespace(index_csv=Path("proxy.csv")),
        {},
        Path("panel.csv"),
        cache_end,
        "production state-only mode avoids implicit cache rebuilds",
        degraded=degraded,
    )

    assert context is not None
    assert context["realtime_base_source"] == expected_source
    assert context["allow_quote_pre_close_after_anchor"] is True
    assert ("fallback_warning" in context) is expects_warning


def test_realtime_query_refresh_uses_base_stale_anchor_default(monkeypatch: pytest.MonkeyPatch) -> None:
    from scripts import realtime_state_bundle

    calls: dict[str, object] = {}

    def fake_refresh_state(root, max_workers: int, max_anchor_age_days: int) -> dict[str, object]:
        calls["root"] = root
        calls["max_workers"] = max_workers
        calls["max_anchor_age_days"] = max_anchor_age_days
        return {"ok": True}

    monkeypatch.delenv("TOP100_REALTIME_REQUIRE_STATE", raising=False)
    monkeypatch.setattr(realtime_state_bundle, "refresh_state", fake_refresh_state)

    result = v2_0.run_realtime_query_with_fresh_state(lambda: "refreshed")

    assert result == "refreshed"
    assert calls["max_anchor_age_days"] == v2_0.base_mod.DEFAULT_MAX_STALE_ANCHOR_DAYS


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
    output_dir = tmp_path / "long-worktree"
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
        lambda: (
            {},
            strategy_frame,
            pd.DataFrame({"rebalance_date": [pd.Timestamp("2026-08-06")]}),
        ),
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


def _exercise_versioned_formal_stage_in_long_worktree(
    module,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    rollback_after_validation: bool = False,
) -> None:
    output_dir = tmp_path.parent / f"stage-v{module.VERSION.replace('.', '')}"
    minimum_path_length = 122 if rollback_after_validation else 108
    while len(str(output_dir)) < minimum_path_length:
        output_dir /= "x"
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
    monkeypatch.setattr(module, "OUTPUT_DIR", output_dir)
    for name in artifact_names:
        original = Path(getattr(module, name))
        monkeypatch.setattr(module, name, output_dir / original.name)

    official_paths = {Path(getattr(module, name)) for name in artifact_names}
    original_payloads: dict[Path, bytes] = {}
    if rollback_after_validation:
        for name in artifact_names:
            path = Path(getattr(module, name))
            payload = f"old-{name}".encode("utf-8")
            path.write_bytes(payload)
            original_payloads[path] = payload

    staged_paths: list[Path] = []
    real_atomic_write_csv = module._atomic_write_csv
    real_atomic_write_text = module._atomic_write_text

    def record_atomic_write_csv(frame, path, **kwargs):
        staged_paths.append(Path(path))
        real_atomic_write_csv(frame, path, **kwargs)

    def record_atomic_write_text(path, text, encoding="utf-8"):
        staged_paths.append(Path(path))
        real_atomic_write_text(path, text, encoding=encoding)

    monkeypatch.setattr(module, "_atomic_write_csv", record_atomic_write_csv)
    monkeypatch.setattr(module, "_atomic_write_text", record_atomic_write_text)

    copy_records: list[tuple[Path, Path]] = []
    shared_bundle_globals = v2_0.staged_output_bundle.__wrapped__.__globals__
    shared_shutil = shared_bundle_globals["shutil"]
    real_copy2 = shared_shutil.copy2

    def record_copy2(source, destination, *args, **kwargs):
        copy_records.append((Path(source), Path(destination)))
        return real_copy2(source, destination, *args, **kwargs)

    monkeypatch.setattr(shared_shutil, "copy2", record_copy2)

    index = pd.DatetimeIndex([pd.Timestamp("2026-08-07")])
    strategy_frame = pd.DataFrame(
        {
            "microcap_close": [100.0],
            "hedge_close": [200.0],
            "return_net": [0.0],
        },
        index=index,
    )
    monkeypatch.setattr(module, "_load_official_v2_0_out", lambda: strategy_frame)
    monkeypatch.setattr(
        v2_0.embedded_context,
        "_load_embedded_base_context",
        lambda: (
            {},
            strategy_frame,
            pd.DataFrame({"rebalance_date": [pd.Timestamp("2026-08-06")]}),
        ),
    )
    incompatible_outputs_name = (
        "incompatible_v2_3_outputs" if module is v2_3 else "incompatible_v2_5_outputs"
    )
    monkeypatch.setattr(module, incompatible_outputs_name, lambda: [])
    monkeypatch.setattr(module, "_close_df_from_base", lambda frame: frame)
    monkeypatch.setattr(
        module,
        "build_v2_3_common_index" if module is v2_3 else "build_v2_5_common_index",
        lambda *args: index,
    )
    monkeypatch.setattr(
        module,
        "build_v2_3_result" if module is v2_3 else "build_v2_5_result",
        lambda *args: strategy_frame,
    )
    monkeypatch.setattr(
        v2_0,
        "assert_top100_candidate_fresh",
        lambda *args, **kwargs: {"latest_date": "2026-08-07", "row_count": 1},
    )
    monkeypatch.setattr(
        v2_0.overlay_mod,
        "_build_v2_data_lineage",
        lambda: {"source_used": "test", "official_wind_series": True},
    )
    monkeypatch.setattr(
        v2_0.overlay_mod,
        "proxy_aware_performance_source_label",
        lambda data_lineage, label: label,
    )
    monkeypatch.setattr(
        module,
        "_build_signal_row",
        lambda frame, summary: pd.DataFrame([{"date": index[-1], "holding": "cash"}]),
    )

    if module is v2_3:
        monkeypatch.setattr(module, "build_signal_execution_mismatch_diagnostics", lambda *args: {})
        monkeypatch.setattr(module, "apply_signal_execution_mismatch_columns", lambda *args: None)
    else:
        monkeypatch.setattr(module, "_valid_log_wls_index", lambda frame: index)
        monkeypatch.setattr(module, "_common_index_gap_summary", lambda *args: {})
        monkeypatch.setattr(module, "COMPATIBILITY_AUDIT_JSON", output_dir / "compatibility_audit.json")

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

    monkeypatch.setattr(module, "build_performance_payload", write_performance_bundle)
    monkeypatch.setattr(v2_0.overlay_mod, "attach_proxy_source_summary_fields", lambda *args, **kwargs: None)
    monkeypatch.setattr(module, "current_base_fingerprint", lambda: {})
    monkeypatch.setattr(module, "_stale_outputs_to_remove_after_generate", lambda *args: [])
    if rollback_after_validation:
        monkeypatch.setattr(module, "_read_costed_nav_csv", lambda *args, **kwargs: strategy_frame)
        monkeypatch.setattr(v2_0.base_mod, "assert_no_historical_rewrite", lambda *args, **kwargs: None)

        def reject_promoted_outputs(**kwargs) -> None:
            assert all(path.read_bytes() != original_payloads[path] for path in official_paths)
            raise RuntimeError("forced post-promotion validation failure")

        monkeypatch.setattr(v2_0, "assert_top100_outputs_fresh", reject_promoted_outputs)
    else:
        monkeypatch.setattr(v2_0, "assert_top100_outputs_fresh", lambda **kwargs: None)

    generator = (
        module._generate_v2_3_outputs_unlocked
        if module is v2_3
        else module._generate_v2_5_outputs_unlocked
    )
    if rollback_after_validation:
        with pytest.raises(RuntimeError, match="forced post-promotion validation failure"):
            generator()

        promotion_paths = [destination for source, destination in copy_records if source not in official_paths]
        rollback_paths = [destination for source, destination in copy_records if source in official_paths]
        assert len(promotion_paths) == len(artifact_names)
        assert len(rollback_paths) == len(artifact_names)
        for internal_paths in (promotion_paths, rollback_paths):
            internal_names = [path.name for path in internal_paths]
            assert len(set(internal_names)) == len(artifact_names)
            assert max(map(len, internal_names)) <= 64
            assert all(path.parent == output_dir for path in internal_paths)
            assert all(
                not any(target.name in internal.name for target in official_paths)
                for internal in internal_paths
            )
        for path, payload in original_payloads.items():
            assert path.read_bytes() == payload
        assert {path for path in output_dir.iterdir() if path.is_file()} == official_paths
        assert not [path for path in output_dir.iterdir() if path.is_dir()]
        return

    summary, signal, result = generator()

    assert summary["latest_nav_date"] == "2026-08-07"
    assert signal.at[0, "holding"] == "cash"
    assert result.index.max() == index[-1]
    expected_suffixes = [Path(getattr(module, name)).suffix for name in artifact_names]
    expected_stage_names = [f"{position:02d}{suffix}" for position, suffix in enumerate(expected_suffixes)]
    actual_stage_names = [path.name for path in staged_paths]
    assert actual_stage_names == expected_stage_names
    assert len(set(actual_stage_names)) == len(artifact_names)
    assert max(map(len, actual_stage_names)) <= len("00.json")
    assert [path.suffix for path in staged_paths] == expected_suffixes
    assert len({path.parent for path in staged_paths}) == 1
    for name in artifact_names:
        assert Path(getattr(module, name)).exists()


def test_generate_v2_3_outputs_keeps_atomic_stage_writable_in_long_worktree(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _exercise_versioned_formal_stage_in_long_worktree(v2_3, tmp_path, monkeypatch)


def test_generate_v2_5_outputs_keeps_atomic_stage_writable_in_long_worktree(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _exercise_versioned_formal_stage_in_long_worktree(v2_5, tmp_path, monkeypatch)


def test_generate_v2_3_outputs_rolls_back_existing_bundle_in_long_worktree(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _exercise_versioned_formal_stage_in_long_worktree(
        v2_3,
        tmp_path,
        monkeypatch,
        rollback_after_validation=True,
    )


def test_generate_v2_5_outputs_rolls_back_existing_bundle_in_long_worktree(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _exercise_versioned_formal_stage_in_long_worktree(
        v2_5,
        tmp_path,
        monkeypatch,
        rollback_after_validation=True,
    )


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


def test_stale_turnover_cannot_hide_scheduled_cost_rebalance(tmp_path: Path) -> None:
    turnover = tmp_path / "turnover.csv"
    pd.DataFrame({"rebalance_date": ["2026-07-23"]}).to_csv(turnover, index=False)
    trading_dates = pd.bdate_range("2026-07-20", "2026-08-12")

    missing = v2_0.base_mod.find_missing_cost_rebalances(
        gross_index=trading_dates,
        current_costed_end=pd.Timestamp("2026-07-31"),
        target_end_date=pd.Timestamp("2026-08-12"),
        proxy_turnover_path=turnover,
        trading_dates=trading_dates,
    )

    assert list(missing) == [pd.Timestamp("2026-08-06")]


def test_costed_tail_extension_rejects_anchor_state_mismatch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    anchor = pd.Timestamp("2026-08-10")
    next_day = pd.Timestamp("2026-08-11")
    fingerprint = {
        "return": 0.01,
        "microcap_close": 101.0,
        "hedge_close": 202.0,
        "microcap_mom": 0.03,
        "hedge_mom": 0.01,
        "momentum_gap": 0.02,
        "signal_on": True,
    }
    costed = pd.DataFrame(
        [
            {
                "date": anchor,
                "holding": "long_microcap_short_zz1000",
                "next_holding": "long_microcap_short_zz1000",
                "nav_net": 1.2,
                **fingerprint,
            }
        ]
    )
    gross = pd.DataFrame(
        [
            {
                "holding": "cash",
                "next_holding": "cash",
                **fingerprint,
            },
            {
                "holding": "cash",
                "next_holding": "cash",
                **{**fingerprint, "return": 0.0},
            },
        ],
        index=pd.DatetimeIndex([anchor, next_day]),
    )
    costed_path = tmp_path / "costed.csv"
    index_path = tmp_path / "index.csv"
    costed.to_csv(costed_path, index=False)
    index_path.write_text("date\n", encoding="utf-8")
    before = costed_path.read_bytes()
    extend = v2_0.base_mod.try_extend_costed_nav_without_turnover
    monkeypatch.setitem(extend.__globals__, "load_close_df", lambda *_args, **_kwargs: pd.DataFrame())
    monkeypatch.setitem(extend.__globals__, "run_signal", lambda _close: gross)

    extended = extend(
        SimpleNamespace(index_csv=index_path, costed_nav_csv=costed_path),
        panel_path=tmp_path / "panel.csv",
        target_end_date=next_day,
    )

    assert extended is False
    assert costed_path.read_bytes() == before


def test_staged_output_bundle_continues_rollback_and_retains_failed_backup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    nav = tmp_path / "nav.csv"
    signal = tmp_path / "signal.csv"
    summary = tmp_path / "summary.json"
    for path in (nav, signal, summary):
        path.write_text(f"old-{path.stem}", encoding="utf-8")
    real_replace = v2_0.overlay_mod._replace_with_retry

    def fail_one_rollback(source: Path, target: Path, *args, **kwargs) -> None:
        if target == summary and ".rollback" in source.name:
            raise OSError("injected rollback failure")
        real_replace(source, target, *args, **kwargs)

    monkeypatch.setitem(
        v2_0.staged_output_bundle.__wrapped__.__globals__,
        "_replace_with_retry",
        fail_one_rollback,
    )

    with pytest.raises(RuntimeError, match="rollback was incomplete"):
        with v2_0.staged_output_bundle(
            [nav, signal, summary],
            summary_path=summary,
            post_promotion_validator=lambda: (_ for _ in ()).throw(RuntimeError("readback failed")),
        ) as staged:
            for path in (nav, signal, summary):
                staged[path].write_text(f"new-{path.stem}", encoding="utf-8")

    assert nav.read_text(encoding="utf-8") == "old-nav"
    assert signal.read_text(encoding="utf-8") == "old-signal"
    assert summary.read_text(encoding="utf-8") == "new-summary"
    retained = list(tmp_path.glob(".bundle.*.rollback*"))
    assert len(retained) == 1
    assert retained[0].read_text(encoding="utf-8") == "old-summary"


@pytest.mark.parametrize(
    ("column", "bad_value"),
    [
        ("microcap_ret", float("nan")),
        ("hedge_ret", float("inf")),
        ("return", float("-inf")),
    ],
)
def test_v2_0_overheat_rejects_nonfinite_inputs(
    column: str,
    bad_value: float,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dates = pd.bdate_range("2026-01-05", periods=70)
    gross = pd.DataFrame(
        {
            "return": [0.001] * len(dates),
            "microcap_ret": [0.02 if i % 2 == 0 else -0.02 for i in range(len(dates))],
            "hedge_ret": [0.0] * len(dates),
            "holding": ["long_microcap_short_zz1000"] * len(dates),
            "next_holding": ["long_microcap_short_zz1000"] * len(dates),
        },
        index=dates,
    )
    gross.loc[dates[20], column] = bad_value
    monkeypatch.setattr(
        v2_0.embedded_context.base_mod.freq_mod.cost_mod,
        "map_rebalance_apply_costs",
        lambda index, _turnover_df: pd.Series(0.0, index=index),
    )

    with pytest.raises(ValueError, match=f"{column}.*non-finite"):
        v2_0.overlay_mod.apply_volatility_overheat_exit(gross, pd.DataFrame())


@pytest.mark.parametrize(
    "kwargs",
    [
        {"target_vol": float("nan")},
        {"target_vol": float("inf")},
        {"target_vol": -0.01},
        {"scale_rebalance_threshold": float("nan")},
        {"scale_rebalance_threshold": -0.01},
    ],
)
def test_v2_0_target_vol_parameters_must_be_finite_and_nonnegative(kwargs: dict[str, float]) -> None:
    base = pd.DataFrame(
        {
            "return_net": [0.0],
            "holding": ["cash"],
            "next_holding": ["cash"],
        },
        index=pd.to_datetime(["2026-08-10"]),
    )

    with pytest.raises(ValueError, match="finite and non-negative"):
        v2_0.overlay_mod.apply_target_vol_scaling(base, **kwargs)


def test_v2_0_target_vol_enabled_matches_applied_scaling() -> None:
    dates = pd.bdate_range("2026-08-07", periods=3)
    base = pd.DataFrame(
        {
            "return_net": [0.0, 0.01, -0.005],
            "holding": ["cash", "long_microcap_short_zz1000", "long_microcap_short_zz1000"],
            "next_holding": [
                "long_microcap_short_zz1000",
                "long_microcap_short_zz1000",
                "long_microcap_short_zz1000",
            ],
            "total_cost": [0.003, 0.0, 0.0],
            "overlay_pre_cost_return": [0.0, 0.01, -0.005],
        },
        index=dates,
    )

    scaled = v2_0.overlay_mod.apply_target_vol_scaling(base)
    signal = v2_0.overlay_mod._build_signal_row(scaled, {})

    assert scaled["target_vol_enabled"].eq(True).all()
    assert bool(signal.iloc[0]["target_vol_enabled"]) is True


def test_cache_write_lock_honors_timeout_when_windows_unlink_is_denied(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_lock = tmp_path / "cache.lock"
    real_lock.write_text("held", encoding="ascii")

    class UnlinkDeniedPath:
        parent = real_lock.parent

        def __str__(self) -> str:
            return str(real_lock)

        def stat(self):
            return real_lock.stat()

        def unlink(self) -> None:
            raise PermissionError("simulated WinError 32")

    lock_fn = v2_0.base_mod._cache_write_lock
    monkeypatch.setitem(lock_fn.__wrapped__.__globals__, "REALTIME_CACHE_STALE_LOCK_SECONDS", 0.0)
    monkeypatch.setitem(lock_fn.__wrapped__.__globals__, "REALTIME_CACHE_LOCK_TIMEOUT_SECONDS", 0.02)
    started = time.monotonic()

    with pytest.raises(TimeoutError, match="Timed out waiting"):
        with lock_fn(UnlinkDeniedPath()):
            pytest.fail("contender must not acquire the live lock")

    assert time.monotonic() - started < 0.5


def test_empty_name_reject_policy_excludes_unnamed_low_cap_candidate() -> None:
    rebalance = pd.Timestamp("2026-08-06")
    members = v2_0.base_mod.build_live_target_members_map(
        caps_by_date={rebalance: {"000001": 1.0, "000002": 2.0}},
        rebalance_dates=pd.DatetimeIndex([rebalance]),
        name_map={"000001": "", "000002": "valid-name"},
        top_n=1,
    )

    assert members[rebalance] == ["000002"]


@pytest.mark.parametrize("name", ["ST龙韵", "*ST东珠", "PT水仙", " ST 麦趣 "])
def test_live_member_name_policy_rejects_all_st_prefixes(name: str) -> None:
    assert v2_0.base_mod.is_live_tradable_name(name) is False


def test_live_member_ranking_backfills_after_st_exclusion() -> None:
    rebalance = pd.Timestamp("2026-08-20")
    members = v2_0.base_mod.build_live_target_members_map(
        caps_by_date={rebalance: {"000001": 1.0, "000002": 2.0, "000003": 3.0}},
        rebalance_dates=pd.DatetimeIndex([rebalance]),
        name_map={"000001": "*ST示例", "000002": "正常股票A", "000003": "正常股票B"},
        top_n=2,
        exclude_current_st_names=True,
    )

    assert members[rebalance] == ["000002", "000003"]


def test_live_member_output_guard_blocks_st_rows() -> None:
    members = pd.DataFrame({"symbol": ["603359"], "name": ["*ST东珠"]})

    with pytest.raises(RuntimeError, match="forbidden ST"):
        v2_0.base_mod.assert_no_st_members(members, "test members")


def test_current_universe_uses_security_master_name_when_st_code_cache_misses(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    active = tmp_path / "active_universe.csv"
    current_st = tmp_path / "current_st.csv"
    pd.DataFrame(
        {
            "symbol": ["sh603359", "sz000001"],
            "name": ["东珠生态", "平安银行"],
            "code": ["603359", "000001"],
        }
    ).to_csv(active, index=False)
    pd.DataFrame({"code": [], "name": []}).to_csv(current_st, index=False)
    master = pd.DataFrame(
        {
            "symbol": ["603359", "000001"],
            "name": ["*ST东珠", "平安银行"],
        }
    )

    function_globals = v2_0.freq_mod.load_current_universe.__globals__
    monkeypatch.setitem(function_globals, "ACTIVE_UNIVERSE", active)
    monkeypatch.setitem(function_globals, "CURRENT_ST", current_st)
    monkeypatch.setitem(function_globals, "load_security_master", lambda: master)
    monkeypatch.setitem(function_globals, "resolve_cache_path", lambda *_args, **_kwargs: tmp_path / "exists")

    assert v2_0.freq_mod.load_current_universe() == ["000001"]


def test_live_member_cache_version_requires_st_name_guard() -> None:
    assert v2_0.base_mod.LIVE_MEMBER_FILTER_POLICY_VERSION == "exclude-current-st-name-v1"
    assert "live-st-name-guard" in v2_0.base_mod.STATIC_CONTEXT_CACHE_VERSION


def test_realtime_member_fetch_falls_back_to_backup_market_data_host(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    function = v2_0.base_mod.fetch_realtime_smallcap_members_fast
    function_globals = function.__globals__
    requests_module = function_globals["requests"]
    calls: list[str] = []

    class FakeResponse:
        def __init__(self, payload: dict[str, object] | None = None, fail: bool = False) -> None:
            self.payload = payload or {}
            self.fail = fail

        def raise_for_status(self) -> None:
            if self.fail:
                raise requests_module.HTTPError("temporary primary-host failure")

        def json(self) -> dict[str, object]:
            return self.payload

    def fake_get(url: str, **_kwargs: object) -> FakeResponse:
        calls.append(url)
        if "pn=1" in url and "https://push2.eastmoney.com" in url:
            return FakeResponse(fail=True)
        if "pn=1" in url and "https://82.push2.eastmoney.com" in url:
            return FakeResponse(
                {
                    "data": {
                        "diff": [
                            {
                                "f12": "000001",
                                "f14": "平安银行",
                                "f2": 10.0,
                                "f20": 100.0,
                            }
                        ]
                    }
                }
            )
        return FakeResponse({"data": {"diff": []}})

    monkeypatch.setitem(function_globals, "load_realtime_eligible_codes", lambda: {"000001"})
    monkeypatch.setattr(requests_module, "get", fake_get)

    members, source = function(None, None, target_size=1)

    assert source == "eastmoney_clist_f20_sorted"
    assert members["symbol"].tolist() == ["000001"]
    assert any("82.push2.eastmoney.com" in url for url in calls)


def test_tencent_realtime_member_fallback_parses_and_strips_current_name(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    function = v2_0.base_mod.fetch_tencent_realtime_smallcap_members
    requests_module = function.__globals__["requests"]
    fields = [""] * 46
    fields[1] = " 平安银行 "
    fields[2] = "000001"
    fields[3] = "10.00"
    fields[4] = "9.90"
    fields[5] = "9.95"
    fields[33] = "10.10"
    fields[34] = "9.80"
    fields[37] = "12345"
    fields[45] = "20.50"
    response_text = 'v_sz000001="' + "~".join(fields) + '";\n'

    class FakeResponse:
        content = response_text.encode("gbk")

        @staticmethod
        def raise_for_status() -> None:
            return None

    monkeypatch.setattr(requests_module, "get", lambda *_args, **_kwargs: FakeResponse())

    members, source = function({"000001"}, None, None, target_size=1)

    assert source == "tencent_qt_total_market_cap"
    assert members.loc[0, "name"] == "平安银行"
    assert members.loc[0, "market_cap"] == pytest.approx(2_050_000_000.0)


def test_empty_name_reject_policy_invalidates_static_member_context(tmp_path: Path) -> None:
    from scripts import realtime_state_bundle

    members = pd.DataFrame(
        {
            "symbol": [f"{value:06d}" for value in range(1, 101)],
            "name": [""] + [f"member-{value}" for value in range(2, 101)],
        }
    )
    path = tmp_path / "members.csv"
    members.to_csv(path, index=False)

    assert realtime_state_bundle._csv_has_valid_named_symbols(path) is False


def test_empty_name_reject_policy_requires_matching_proxy_metadata() -> None:
    core_params = {
        "research_stack_version": v2_0.base_mod.RESEARCH_STACK_VERSION,
        "execution_timing": v2_0.base_mod.EXECUTION_TIMING,
        "trade_constraint_mode": v2_0.base_mod.TRADE_CONSTRAINT_MODE,
        "exclude_current_st": False,
        "exclude_historical_st": True,
        "rebalance_phase_anchor_date": v2_0.base_mod.REBALANCE_ANCHOR_DATE,
        "realtime_quote_policy_version": v2_0.base_mod.REALTIME_QUOTE_POLICY_VERSION,
        "proxy_rebalance_policy_version": v2_0.base_mod.PROXY_REBALANCE_POLICY_VERSION,
        "st_notice_policy_version": v2_0.freq_mod.ST_NOTICE_POLICY_VERSION,
    }

    assert v2_0.base_mod.proxy_meta_matches_execution_model({"core_params": core_params}) is False
    core_params["member_filter_policy_version"] = v2_0.base_mod.MEMBER_FILTER_POLICY_VERSION
    assert v2_0.base_mod.proxy_meta_matches_execution_model({"core_params": core_params}) is False
    core_params["security_meta_cache_fingerprint"] = v2_0.base_mod.security_meta_cache_fingerprint()
    assert v2_0.base_mod.proxy_meta_matches_execution_model({"core_params": core_params}) is True


def test_existing_base_bundle_is_rebuilt_when_proxy_metadata_is_incompatible(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    paths = {
        "proxy_meta": tmp_path / "proxy_meta.json",
        "proxy_members": tmp_path / "proxy_members.csv",
        "proxy_turnover": tmp_path / "proxy_turnover.csv",
    }
    index_csv = tmp_path / "index.csv"
    costed_nav_csv = tmp_path / "costed_nav.csv"
    for path in [*paths.values(), index_csv, costed_nav_csv]:
        path.write_text("placeholder", encoding="utf-8")
    args = SimpleNamespace(index_csv=index_csv, costed_nav_csv=costed_nav_csv)
    resolved = SimpleNamespace(
        output_paths=paths,
        index_csv=index_csv,
        costed_nav_csv=costed_nav_csv,
    )
    calls: list[tuple[Path, pd.Timestamp]] = []

    monkeypatch.setattr(v2_0, "_sync_embedded_base_config", lambda: None)
    monkeypatch.setattr(v2_0, "_build_base_args", lambda: args)
    monkeypatch.setattr(v2_0, "_resolve_base_paths", lambda _args: resolved)
    monkeypatch.setattr(v2_0, "_proxy_meta_matches_execution_model", lambda _path: False)
    monkeypatch.setattr(v2_0, "_base_costed_nav_matches_current_hedge_ratio", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(
        v2_0.base_mod,
        "build_refreshed_panel_shadow",
        lambda _args, _paths: (tmp_path / "panel.csv", pd.Timestamp("2026-08-20")),
    )
    monkeypatch.setattr(
        v2_0.base_mod,
        "ensure_strategy_files",
        lambda _args, _paths, panel_path, target_end_date: calls.append((panel_path, target_end_date)),
    )

    v2_0._ensure_base_outputs_unlocked()

    assert calls == [(tmp_path / "panel.csv", pd.Timestamp("2026-08-20"))]


def _valid_proxy_core_params() -> dict[str, object]:
    return {
        "research_stack_version": v2_0.base_mod.RESEARCH_STACK_VERSION,
        "execution_timing": v2_0.base_mod.EXECUTION_TIMING,
        "trade_constraint_mode": v2_0.base_mod.TRADE_CONSTRAINT_MODE,
        "exclude_current_st": False,
        "exclude_historical_st": True,
        "rebalance_phase_anchor_date": v2_0.base_mod.REBALANCE_ANCHOR_DATE,
        "member_filter_policy_version": v2_0.base_mod.MEMBER_FILTER_POLICY_VERSION,
        "realtime_quote_policy_version": v2_0.base_mod.REALTIME_QUOTE_POLICY_VERSION,
        "proxy_rebalance_policy_version": v2_0.base_mod.PROXY_REBALANCE_POLICY_VERSION,
        "st_notice_policy_version": v2_0.freq_mod.ST_NOTICE_POLICY_VERSION,
        "security_meta_cache_fingerprint": {
            "present_count": 4975,
            "missing_count": 0,
            "sha256": "audited-fingerprint",
        },
    }


def test_frozen_seed_hash_is_stable_across_git_newline_materialization(tmp_path: Path) -> None:
    lf_path = tmp_path / "seed_lf.csv"
    crlf_path = tmp_path / "seed_crlf.csv"
    lf_path.write_bytes(b"date,value\n2026-08-20,1\n")
    crlf_path.write_bytes(b"date,value\r\n2026-08-20,1\r\n")

    assert v2_0.base_mod._file_sha256(lf_path) == v2_0.base_mod._file_sha256(crlf_path)


def test_frozen_tail_authority_requires_exact_seed_hashes_and_current_st_gate(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    paths = {
        "proxy_meta": tmp_path / "proxy_meta.json",
        "proxy_members": tmp_path / "proxy_members.csv",
        "proxy_turnover": tmp_path / "proxy_turnover.csv",
        "proxy_effective_members": tmp_path / "proxy_effective_members.csv",
    }
    args = SimpleNamespace(
        index_csv=tmp_path / "proxy_index.csv",
        costed_nav_csv=tmp_path / "costed_nav.csv",
    )
    meta = {"core_params": _valid_proxy_core_params(), "end_date": "2026-08-20"}
    paths["proxy_meta"].write_text(json.dumps(meta), encoding="utf-8")
    for path in (args.index_csv, args.costed_nav_csv, paths["proxy_members"], paths["proxy_turnover"]):
        path.write_text("seed\n", encoding="utf-8")
    pd.DataFrame(
        {
            "as_of_date": ["2026-08-20"] * v2_0.base_mod.TOP_N,
            "rank": range(1, v2_0.base_mod.TOP_N + 1),
            "symbol": [f"{value:06d}" for value in range(1, v2_0.base_mod.TOP_N + 1)],
        }
    ).to_csv(paths["proxy_effective_members"], index=False)
    required = {
        "proxy_index": args.index_csv,
        "costed_nav": args.costed_nav_csv,
        "proxy_meta": paths["proxy_meta"],
        "proxy_members": paths["proxy_members"],
        "proxy_turnover": paths["proxy_turnover"],
        "proxy_effective_members": paths["proxy_effective_members"],
    }
    authority = {
        "version": v2_0.base_mod.FROZEN_TAIL_AUTHORITY_VERSION,
        "seed_end_date": "2026-08-20",
        "security_meta_cache_fingerprint": meta["core_params"]["security_meta_cache_fingerprint"],
        "seed_file_sha256": {
            label: v2_0.base_mod._file_sha256(path) for label, path in required.items()
        },
    }
    authority_path = tmp_path / "authority.json"
    authority_path.write_text(json.dumps(authority), encoding="utf-8")
    current_st_path = tmp_path / "current_st.csv"
    current_st_path.write_text("code,name\n999999,*ST test\n", encoding="utf-8")

    authority_globals = v2_0.base_mod.frozen_tail_authority_matches_seed.__globals__
    monkeypatch.setitem(authority_globals, "FROZEN_TAIL_AUTHORITY_PATH", authority_path)
    monkeypatch.setattr(v2_0.freq_mod, "list_backtest_universe_symbols", lambda: [])
    monkeypatch.setattr(v2_0.freq_mod, "CURRENT_ST", current_st_path)
    monkeypatch.setattr(v2_0.freq_mod, "load_current_st_name_map", lambda: {"999999": "*ST test"})

    assert v2_0.base_mod.frozen_tail_authority_matches_seed(
        args,
        paths,
        meta,
        pd.Timestamp("2026-08-20"),
        pd.Timestamp("2026-08-20"),
    )

    monkeypatch.setattr(
        v2_0.freq_mod,
        "list_backtest_universe_symbols",
        lambda: [f"{value:06d}" for value in range(100)],
    )
    assert v2_0.base_mod.frozen_tail_authority_matches_seed(
        args,
        paths,
        meta,
        pd.Timestamp("2026-08-20"),
        pd.Timestamp("2026-08-20"),
    )

    monkeypatch.setattr(
        v2_0.freq_mod,
        "list_backtest_universe_symbols",
        lambda: [f"{value:06d}" for value in range(4975)],
    )
    assert not v2_0.base_mod.frozen_tail_authority_matches_seed(
        args,
        paths,
        meta,
        pd.Timestamp("2026-08-20"),
        pd.Timestamp("2026-08-20"),
    )
    monkeypatch.setattr(v2_0.freq_mod, "list_backtest_universe_symbols", lambda: [])

    args.index_csv.write_text("tampered\n", encoding="utf-8")
    assert not v2_0.base_mod.frozen_tail_authority_matches_seed(
        args,
        paths,
        meta,
        pd.Timestamp("2026-08-20"),
        pd.Timestamp("2026-08-20"),
    )


def test_ensure_strategy_files_routes_exact_frozen_seed_only_to_short_tail(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    index_csv = tmp_path / "index.csv"
    costed_nav_csv = tmp_path / "costed.csv"
    proxy_meta = tmp_path / "meta.json"
    proxy_turnover = tmp_path / "turnover.csv"
    for path in (index_csv, costed_nav_csv, proxy_meta, proxy_turnover):
        path.write_text("seed", encoding="utf-8")
    proxy_meta.write_text("{}", encoding="utf-8")
    paths = {
        "proxy_meta": proxy_meta,
        "proxy_turnover": proxy_turnover,
        "proxy_members": tmp_path / "members.csv",
        "proxy_effective_members": tmp_path / "effective.csv",
    }
    args = SimpleNamespace(
        index_csv=index_csv,
        costed_nav_csv=costed_nav_csv,
        rebuild_index_if_missing=True,
    )
    calls: list[str] = []

    ensure_globals = v2_0.base_mod.ensure_strategy_files.__globals__
    monkeypatch.setitem(
        ensure_globals,
        "read_csv_last_date",
        lambda path: pd.Timestamp("2026-08-20") if path in (index_csv, costed_nav_csv) else None,
    )
    monkeypatch.setitem(ensure_globals, "proxy_meta_matches_execution_model", lambda _meta: False)
    monkeypatch.setitem(ensure_globals, "frozen_tail_authority_matches_seed", lambda *_args: True)
    monkeypatch.setitem(
        ensure_globals,
        "try_extend_proxy_index_without_rebalance",
        lambda *_args: calls.append("proxy_tail") or True,
    )
    monkeypatch.setitem(
        ensure_globals,
        "try_extend_costed_nav_without_turnover",
        lambda *_args: calls.append("costed_tail") or True,
    )
    monkeypatch.setitem(
        ensure_globals,
        "refresh_price_cache_tail",
        lambda *_args, **_kwargs: pytest.fail("full historical refresh must remain blocked"),
    )

    v2_0.base_mod.ensure_strategy_files(
        args,
        paths,
        tmp_path / "panel.csv",
        pd.Timestamp("2026-08-21"),
    )

    assert calls == ["proxy_tail", "costed_tail"]


def test_frozen_tail_extension_is_reusable_only_with_validated_written_rows(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    paths = {
        "proxy_meta": tmp_path / "meta.json",
        "proxy_members": tmp_path / "members.csv",
        "proxy_turnover": tmp_path / "turnover.csv",
        "proxy_effective_members": tmp_path / "effective.csv",
    }
    args = SimpleNamespace(index_csv=tmp_path / "index.csv", costed_nav_csv=tmp_path / "costed.csv")
    paths["proxy_members"].write_text("members\n", encoding="utf-8")
    paths["proxy_turnover"].write_text("turnover\n", encoding="utf-8")
    pd.DataFrame(
        {
            "as_of_date": ["2026-08-20"] * v2_0.base_mod.TOP_N,
            "rank": range(1, v2_0.base_mod.TOP_N + 1),
            "symbol": [f"{value:06d}" for value in range(1, v2_0.base_mod.TOP_N + 1)],
        }
    ).to_csv(paths["proxy_effective_members"], index=False)
    pd.DataFrame(
        {
            "date": ["2026-08-20", "2026-08-21"],
            "close": [100.0, 101.0],
            "daily_return": [0.0, 0.01],
        }
    ).to_csv(args.index_csv, index=False)
    pd.DataFrame(
        {
            "date": ["2026-08-20", "2026-08-21"],
            "nav_net": [2.0, 2.01],
            "return_net": [0.0, 0.005],
        }
    ).to_csv(args.costed_nav_csv, index=False)
    authority = {
        "version": v2_0.base_mod.FROZEN_TAIL_AUTHORITY_VERSION,
        "seed_end_date": "2026-08-20",
        "seed_file_rows": {"proxy_index": 1, "costed_nav": 1},
        "seed_file_sha256": {
            label: v2_0.base_mod._file_sha256(path)
            for label, path in {
                "proxy_members": paths["proxy_members"],
                "proxy_turnover": paths["proxy_turnover"],
                "proxy_effective_members": paths["proxy_effective_members"],
            }.items()
        },
    }
    authority_path = tmp_path / "authority.json"
    authority_path.write_text(json.dumps(authority), encoding="utf-8")
    meta = {
        "core_params": _valid_proxy_core_params(),
        "tail_extension_method": "no_new_rebalance_saved_target_replay",
        "tail_extension_start": "2026-08-20",
        "tail_extension_end": "2026-08-21",
        "tail_extension_rows": 1,
        "tail_extension_effective_member_count": 100,
        "tail_extension_return_source_counts": {"raw": 100, "adjusted": 0},
        "tail_extension_authority_sha256": hashlib.sha256(authority_path.read_bytes()).hexdigest(),
    }
    current_st_path = tmp_path / "current_st.csv"
    current_st_path.write_text("code,name\n999999,*ST test\n", encoding="utf-8")
    extension_globals = v2_0.base_mod.frozen_tail_extension_matches_authority.__globals__
    monkeypatch.setitem(extension_globals, "FROZEN_TAIL_AUTHORITY_PATH", authority_path)
    monkeypatch.setattr(v2_0.freq_mod, "CURRENT_ST", current_st_path)
    monkeypatch.setattr(v2_0.freq_mod, "load_current_st_name_map", lambda: {"999999": "*ST test"})

    assert v2_0.base_mod.frozen_tail_extension_matches_authority(
        args,
        paths,
        meta,
        pd.Timestamp("2026-08-21"),
        pd.Timestamp("2026-08-21"),
    )

    tampered = pd.read_csv(args.costed_nav_csv)
    tampered.loc[1, "nav_net"] = -1.0
    tampered.to_csv(args.costed_nav_csv, index=False)
    assert not v2_0.base_mod.frozen_tail_extension_matches_authority(
        args,
        paths,
        meta,
        pd.Timestamp("2026-08-21"),
        pd.Timestamp("2026-08-21"),
    )


def test_realtime_cached_proxy_accepts_validated_frozen_tail_extension(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    index_csv = tmp_path / "index.csv"
    costed_nav_csv = tmp_path / "costed.csv"
    proxy_meta = tmp_path / "meta.json"
    proxy_turnover = tmp_path / "turnover.csv"
    for path in (index_csv, costed_nav_csv, proxy_turnover):
        path.write_text("seed", encoding="utf-8")
    proxy_meta.write_text("{}", encoding="utf-8")
    args = SimpleNamespace(
        index_csv=index_csv,
        costed_nav_csv=costed_nav_csv,
        max_stale_anchor_days=5,
        allow_stale_realtime=False,
    )
    paths = {"proxy_meta": proxy_meta, "proxy_turnover": proxy_turnover}
    function = v2_0.base_mod.reusable_cached_proxy_end_for_realtime
    function_globals = function.__globals__
    monkeypatch.setitem(function_globals, "read_csv_last_date", lambda _path: pd.Timestamp("2026-08-24"))
    monkeypatch.setitem(function_globals, "proxy_meta_matches_execution_model", lambda _meta: False)
    monkeypatch.setitem(function_globals, "frozen_tail_extension_matches_authority", lambda *_args: True)
    monkeypatch.setitem(
        function_globals,
        "assess_history_anchor_freshness",
        lambda *_args, **_kwargs: {"is_stale": False},
    )

    assert function(args, paths, pd.Timestamp("2026-08-24")) == pd.Timestamp("2026-08-24")
