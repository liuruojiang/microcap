"""Realtime gate tests use an explicit independent calendar oracle, never NAV dates."""
import sys
from datetime import date
from types import SimpleNamespace

import pandas as pd
import pytest

import microcap_top100_mom16_biweekly_live_v2_0 as v2
import microcap_top100_mom16_biweekly_live_v2_3 as v23
import microcap_top100_mom16_biweekly_live_v2_5 as v25


@pytest.fixture
def oracle(monkeypatch):
    calls = []
    def completed(now):
        stamp = pd.Timestamp(now)
        calls.append(stamp)
        return date(2026, 10, 8) if stamp.hour * 60 + stamp.minute >= 930 else date(2026, 9, 30)
    module = SimpleNamespace(latest_completed_session=completed, is_trading_day=lambda day: day == date(2026, 10, 8))
    monkeypatch.setitem(sys.modules, "scripts.exchange_calendar", module)
    original = v2.base_mod.assess_realtime_anchor_freshness.__globals__["_cn_timestamp"]
    clock = pd.Timestamp("2026-10-08 14:30", tz="Asia/Shanghai")
    namespace = v2.base_mod.assess_realtime_anchor_freshness.__globals__
    monkeypatch.setitem(namespace, "_cn_timestamp", lambda now=None: clock if now is None else original(now))
    monkeypatch.setitem(namespace, "_cn_local_day", lambda now=None: pd.Timestamp((clock if now is None else original(now)).date()))
    return module, calls


def meta(**updates):
    return {"quote_trade_date": "2026-10-08", "latest_anchor_trade_date": "2026-09-30",
            "snapshot_time": "2026-10-08 14:30:00",
            "expected_latest_completed_trade_date": "2026-09-30",
            "expected_latest_completed_trade_date_source": v2.base_mod.REALTIME_REFRESH_PROOF_SOURCE,
            "expected_latest_completed_trade_date_verified_on": "2026-10-08", **updates}


def test_holiday_realtime_passes_but_historical_rule_is_unchanged(oracle):
    now = pd.Timestamp("2026-10-08 14:30")
    truncated_history = pd.DatetimeIndex(["2026-09-30"])
    original = v2.base_mod.assess_history_anchor_freshness(pd.Timestamp("2026-09-30"), 5, now, truncated_history)
    result = v2.base_mod.assess_realtime_anchor_freshness(pd.Timestamp("2026-09-30"), 5, now, truncated_history)
    assert original["is_stale"] and original["stale_calendar_days"] == 8
    assert not result["is_stale"] and result["stale_calendar_days"] == 8
    assert result["freshness_source"] == "official_exchange_calendar"
    assert oracle[1][-1].tzinfo is not None


@pytest.mark.parametrize("anchor", ["2026-09-29", "2026-10-01", "2026-10-08", "2026-10-09"])
def test_missing_or_future_completed_session_is_not_relaxed(oracle, anchor):
    result = v2.base_mod.assess_realtime_anchor_freshness(pd.Timestamp(anchor), 999,
        pd.Timestamp("2026-10-08 14:30"), pd.DatetimeIndex([anchor]))
    assert result["is_stale"]


def test_calendar_unavailability_is_not_treated_as_holiday(oracle):
    def unavailable(now):
        raise RuntimeError("independent calendar coverage unavailable")
    oracle[0].latest_completed_session = unavailable
    with pytest.raises(RuntimeError, match="coverage"):
        v2.base_mod.assess_realtime_anchor_freshness(pd.Timestamp("2026-09-30"), 5)


def test_final_guard_accepts_holiday_and_ignores_local_truncated_calendar(oracle, monkeypatch):
    monkeypatch.setitem(v2.base_mod.assert_realtime_anchor_precedes_quote_trade_date.__globals__,
        "_load_realtime_anchor_calendar_index", lambda: pytest.fail("local NAV is not a calendar"))
    v2.base_mod.assert_realtime_anchor_precedes_quote_trade_date(meta())


def test_forged_matching_local_proof_cannot_hide_a_missing_session(oracle):
    with pytest.raises(RuntimeError, match="official previous"):
        v2.base_mod.assert_realtime_anchor_precedes_quote_trade_date(meta(
            latest_anchor_trade_date="2026-09-29", expected_latest_completed_trade_date="2026-09-29"))


@pytest.mark.parametrize("update", [
    {"snapshot_time": ""}, {"snapshot_time": "2026-10-07 14:30"},
    {"snapshot_time": "2026-10-08 15:31"}, {"quote_trade_date": "2026-10-07"},
    {"expected_latest_completed_trade_date_verified_on": "2026-10-07"},
])
def test_final_guard_keeps_same_day_snapshot_and_proof_requirements(oracle, update):
    with pytest.raises(RuntimeError):
        v2.base_mod.assert_realtime_anchor_precedes_quote_trade_date(meta(**update))


def test_fake_weekend_quote_is_rejected(oracle):
    oracle[0].is_trading_day = lambda day: False
    with pytest.raises(RuntimeError, match="official exchange"):
        v2.base_mod.assert_realtime_anchor_precedes_quote_trade_date(meta())


def test_three_versions_share_the_guarded_realtime_core():
    assert v23.v2_0.realtime_core is v2.realtime_core and v25.v2_0.realtime_core is v2.realtime_core
    assert v23.v2_0.realtime_core.load_realtime_base is v25.v2_0.realtime_core.load_realtime_base
    assert v2.REALTIME_CALENDAR_GUARD_REVISION >= 5


def test_cached_context_receives_realtime_freshness_not_historical_age(oracle, monkeypatch):
    fn = v2.base_mod.build_realtime_context_from_cached_proxy
    namespace = fn.__globals__
    monkeypatch.setitem(namespace, "reusable_cached_proxy_end_for_realtime", lambda *a: pd.Timestamp("2026-09-30"))
    monkeypatch.setitem(namespace, "load_close_df", lambda *a, **kw: pd.DataFrame(index=pd.DatetimeIndex(["2026-09-30"])))
    monkeypatch.setitem(namespace, "build_base_signal_context", lambda *a: {"anchor_freshness": {"is_stale": True}})
    result = fn(SimpleNamespace(index_csv=None, max_stale_anchor_days=5), {}, None,
                pd.Timestamp("2026-09-30"), "diagnostic fixture", degraded=False)
    assert not result["anchor_freshness"]["is_stale"]
    assert result["realtime_base_source"] == "validated_refreshed_state"


@pytest.mark.parametrize("entrypoint", ["generic", "legacy", "production"])
def test_all_realtime_fallback_entrypoints_replace_historical_age(oracle, monkeypatch, tmp_path, entrypoint):
    from contextlib import nullcontext
    turnover = tmp_path / "turnover.csv"
    turnover.write_text("rebalance_date\n2026-09-24\n", encoding="utf-8")
    paths = {"proxy_turnover": turnover}
    args = SimpleNamespace(output_prefix="fixture", max_stale_anchor_days=5)
    close = pd.DataFrame(index=pd.DatetimeIndex(["2026-09-30"]))
    context = {"close_df": close, "anchor_freshness": {"is_stale": True, "stale_calendar_days": 8}}
    target = pd.Timestamp("2026-09-30")
    def missing(*a, **kw):
        raise ValueError("injected recoverable context miss")
    def fallback(*a, **kw):
        return {**context}
    def members(*a):
        return a[-1]
    if entrypoint == "generic":
        namespace = v2.base_mod.execute_query.__globals__
        monkeypatch.setitem(namespace, "normalize_query_text", lambda value: value)
        monkeypatch.setitem(namespace, "classify_query_kind", lambda value: "realtime_signal")
        monkeypatch.setitem(namespace, "build_output_paths", lambda value: paths)
        monkeypatch.setitem(namespace, "refresh_history_anchor", lambda *a: (tmp_path / "panel.csv", target))
        monkeypatch.setitem(namespace, "ensure_realtime_query_base_context", missing)
        monkeypatch.setitem(namespace, "ensure_base_signal_fresh", fallback)
        monkeypatch.setitem(namespace, "ensure_static_members_fresh", members)
        observed = []
        monkeypatch.setitem(namespace, "handle_query", lambda result, *a: observed.append(result))
        v2.base_mod.execute_query(args, "realtime")
        result = observed[0]
    else:
        fn = v2._load_realtime_embedded_base_context if entrypoint == "legacy" else v2.load_realtime_context
        namespace = fn.__globals__
        monkeypatch.setitem(namespace, "_v2_base_build_lock", nullcontext)
        monkeypatch.setitem(namespace, "_ensure_base_outputs_unlocked", lambda: None)
        monkeypatch.setitem(namespace, "_build_base_args", lambda: args)
        monkeypatch.setitem(namespace, "_resolve_base_paths", lambda args: SimpleNamespace(output_paths=paths))
        monkeypatch.setitem(namespace, "_load_reference_summary_unlocked", lambda *a: {})
        monkeypatch.setitem(namespace, "realtime_state_required", lambda: False)
        monkeypatch.setattr(v2.base_mod, "refresh_history_anchor", lambda *a: (tmp_path / "panel.csv", target))
        monkeypatch.setattr(v2.base_mod, "ensure_realtime_query_base_context", missing)
        monkeypatch.setattr(v2.base_mod, "ensure_base_signal_fresh", fallback)
        monkeypatch.setattr(v2.base_mod, "ensure_static_members_fresh", members)
        result, _, _ = fn()
    assert result["anchor_freshness"]["freshness_source"] == "official_exchange_calendar"
    assert not result["anchor_freshness"]["is_stale"]
    assert result["anchor_freshness"]["stale_calendar_days"] == 8
