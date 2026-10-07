"""The local publication deadline is stricter than close-confirmed data time."""
from datetime import date

import pandas as pd
import pytest

import microcap_top100_mom16_biweekly_live as shared
import microcap_top100_mom16_biweekly_live_v2_0 as v20
import microcap_top100_mom16_biweekly_live_v2_3 as v23
import microcap_top100_mom16_biweekly_live_v2_5 as v25
from scripts import exchange_calendar as calendar
from scripts.top100_data_contracts import realtime_row_is_consistent


@pytest.fixture
def sessions(monkeypatch):
    dates = (date(2026, 9, 30), date(2026, 10, 8), date(2026, 10, 9))
    monkeypatch.setattr(calendar, "sessions_for_day", lambda day: dates)


def set_clock(monkeypatch, guard, stamp):
    namespace = guard.__globals__
    original = namespace["_cn_timestamp"]
    now = pd.Timestamp(stamp).tz_convert("Asia/Shanghai")
    monkeypatch.setitem(namespace, "_cn_timestamp", lambda value=None: now if value is None else original(value))
    monkeypatch.setitem(namespace, "_cn_local_day", lambda value=None: pd.Timestamp(now.date()))


def meta(stamp):
    return {
        "member_count": 100, "member_price_count": 100,
        "member_quote_bad_symbols": [], "member_quote_trade_date_count": 100,
        "member_quote_trade_date_min": "2026-10-08", "member_quote_trade_date_max": "2026-10-08",
        "hedge_quote_source": "tencent_batch_free", "hedge_quote_trade_date": "2026-10-08",
        "quote_trade_date": "2026-10-08", "latest_anchor_trade_date": "2026-09-30",
        "snapshot_time": stamp, "expected_latest_completed_trade_date": "2026-09-30",
        "expected_latest_completed_trade_date_source": v20.base_mod.REALTIME_REFRESH_PROOF_SOURCE,
        "expected_latest_completed_trade_date_verified_on": "2026-10-08",
    }


def written_row(stamp):
    return {
        **meta(stamp), "date": "2026-10-08",
        "signal_timing": "intraday_hypothetical_if_now_close",
        "official_close_confirmed_signal": False,
        "current_holding": "cash", "next_holding": "cash",
        "current_execution_scale": 0, "next_session_actionable_scale": 0,
        "trade_state": "hold", "signal_label": "cash", "member_rebalance_actionable": False,
    }


@pytest.fixture(params=[v20, v23.v2_0, v25.v2_0], ids=["v2.0", "v2.3", "v2.5"])
def published_guard(request):
    guard = request.param.realtime_core.base_mod.assert_realtime_meta_is_actionable
    assert guard is v20.base_mod.assert_realtime_meta_is_actionable
    return guard


@pytest.mark.parametrize("stamp", ["2026-10-08T14:59:59+08:00", "2026-10-08T06:59:59+00:00"])
def test_last_intraday_second_remains_valid(sessions, monkeypatch, published_guard, stamp):
    set_clock(monkeypatch, published_guard, stamp)
    published_guard(meta(stamp))
    assert realtime_row_is_consistent(written_row(stamp), "long_microcap_short_zz1000")


@pytest.mark.parametrize("stamp", [
    "2026-10-08T15:00:00+08:00", "2026-10-08T15:15:00+08:00",
    "2026-10-08T15:29:59+08:00", "2026-10-08T07:00:00+00:00",
])
def test_close_snapshot_cannot_be_labeled_intraday(sessions, monkeypatch, published_guard, stamp):
    set_clock(monkeypatch, published_guard, stamp)
    with pytest.raises(RuntimeError, match="publication deadline") as exc:
        published_guard(meta(stamp))
    assert v20.base_mod.is_realtime_actionability_error(exc.value)
    assert not realtime_row_is_consistent(written_row(stamp), "long_microcap_short_zz1000")


@pytest.mark.parametrize("now", ["2026-10-08T15:00:00+08:00", "2026-10-08T07:15:00+00:00"])
def test_pre_close_snapshot_cannot_be_published_after_deadline(sessions, monkeypatch, published_guard, now):
    set_clock(monkeypatch, published_guard, now)
    with pytest.raises(RuntimeError, match="publication deadline"):
        published_guard(meta("2026-10-08T14:59:59+08:00"))


def test_archived_valid_snapshot_survives_evening_inspection(sessions, monkeypatch):
    set_clock(monkeypatch, v20.base_mod.assert_realtime_meta_is_actionable, "2026-10-08T22:00:00+08:00")
    assert realtime_row_is_consistent(written_row("2026-10-08T14:59:59+08:00"), "long_microcap_short_zz1000")


@pytest.mark.parametrize("now", ["2026-10-08T15:00:00+08:00", "2026-10-08T07:15:00+00:00"])
def test_shared_main_rejects_late_publication(monkeypatch, now):
    shared._load_runtime_modules()
    set_clock(monkeypatch, shared.assert_realtime_meta_is_actionable, now)
    with pytest.raises(RuntimeError, match="publication deadline"):
        shared.assert_realtime_meta_is_actionable(meta("2026-10-08T14:59:59+08:00"))


def test_close_confirmed_path_remains_available_in_evening(monkeypatch):
    set_clock(monkeypatch, v20.base_mod.assert_realtime_meta_is_actionable, "2026-10-08T22:00:00+08:00")
    result = pd.DataFrame([{"holding": "cash", "next_holding": "cash"}])
    signal = v20.base_mod.enrich_signal_frame(pd.DataFrame([{"date": "2026-10-08"}]), result)
    assert signal.iloc[0]["signal_timing"] == "close_confirmed"
    assert bool(signal.iloc[0]["official_close_confirmed_signal"])


@pytest.mark.parametrize("version", ["v2.0", "v2.3", "v2.5"])
@pytest.mark.parametrize("finish", ["2026-10-08T14:59:59+08:00", "2026-10-08T15:00:00+08:00"])
def test_calculation_crossing_deadline_never_writes_live_csv(sessions, monkeypatch, tmp_path, version, finish):
    from test_realtime_member_action_contract import _fake_realtime_base, _patch_native_realtime_builder

    base = _fake_realtime_base(
        latest_anchor_trade_date="2026-09-30", quote_trade_date="2026-10-08", official_rebalance=False,
    )
    base.meta.update(meta("2026-10-08T14:59:59+08:00"))
    base.context["latest_rebalance"] = pd.Timestamp("2026-09-17")
    builder = _patch_native_realtime_builder(monkeypatch, version, base)
    if version == "v2.0":
        builder = v20._build_realtime_v2_0_outputs_unlocked
    namespace = builder.__globals__
    monkeypatch.setitem(namespace, "ensure_output_dir", lambda: None)
    monkeypatch.setitem(namespace, "REALTIME_SIGNAL_CSV", tmp_path / "realtime.csv")
    writes = []
    monkeypatch.setitem(namespace, "_atomic_write_text", lambda *args, **kwargs: writes.append(args))
    clock = [pd.Timestamp("2026-10-08T14:59:59+08:00")]
    guard_ns = v20.base_mod.assert_realtime_anchor_precedes_quote_trade_date.__globals__
    original = guard_ns["_cn_timestamp"]
    monkeypatch.setitem(guard_ns, "_cn_timestamp", lambda value=None: clock[0] if value is None else original(value))
    monkeypatch.setitem(guard_ns, "_cn_local_day", lambda value=None: pd.Timestamp(clock[0].date()))

    def load_before_deadline():
        v20.base_mod.assert_realtime_meta_is_actionable(base.meta)
        return base

    def finish_calculation(*args, **kwargs):
        clock[0] = pd.Timestamp(finish)
        return pd.DataFrame([{"trade_state": "hold", "current_holding": "cash", "next_holding": "cash"}])

    monkeypatch.setattr(v20.realtime_core, "load_realtime_base", load_before_deadline)
    monkeypatch.setitem(namespace, "_build_signal_row", finish_calculation)
    if pd.Timestamp(finish).hour >= 15:
        with pytest.raises(RuntimeError, match="publication deadline"):
            builder()
        assert writes == []
    else:
        signal, _, _ = builder()
        assert len(writes) == 1
        assert signal.iloc[0]["signal_timing"] == "intraday_hypothetical_if_now_close"
