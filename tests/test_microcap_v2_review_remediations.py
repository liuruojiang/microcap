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
