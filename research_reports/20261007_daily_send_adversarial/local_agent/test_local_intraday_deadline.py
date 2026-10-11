"""Diagnostic counterexamples for the local 15:00:00 publication deadline."""
from datetime import date
from pathlib import Path
import sys

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
import microcap_top100_mom16_biweekly_live_v2_0 as v20
from scripts import exchange_calendar as calendar
from scripts.top100_data_contracts import realtime_row_is_consistent


@pytest.fixture
def calendar_oracle(monkeypatch):
    sessions = (date(2026, 9, 30), date(2026, 10, 8), date(2026, 10, 9))
    monkeypatch.setattr(calendar, "sessions_for_day", lambda day: sessions)


def meta(stamp):
    return {
        "quote_trade_date": "2026-10-08",
        "latest_anchor_trade_date": "2026-09-30",
        "snapshot_time": stamp,
        "expected_latest_completed_trade_date": "2026-09-30",
        "expected_latest_completed_trade_date_source": v20.base_mod.REALTIME_REFRESH_PROOF_SOURCE,
        "expected_latest_completed_trade_date_verified_on": "2026-10-08",
    }


def row(stamp):
    return {
        **meta(stamp), "date": "2026-10-08",
        "signal_timing": "intraday_hypothetical_if_now_close",
        "official_close_confirmed_signal": False,
        "current_holding": "cash", "next_holding": "cash",
        "current_execution_scale": 0, "next_session_actionable_scale": 0,
        "trade_state": "hold", "signal_label": "cash",
        "member_rebalance_actionable": False,
    }


@pytest.mark.parametrize("stamp", [
    "2026-10-08T14:59:59+08:00",
    "2026-10-08T06:59:59+00:00",
])
def test_before_deadline_is_accepted(calendar_oracle, monkeypatch, stamp):
    clock = pd.Timestamp(stamp).tz_convert("Asia/Shanghai")
    namespace = v20.base_mod.assert_realtime_anchor_precedes_quote_trade_date.__globals__
    original = namespace["_cn_timestamp"]
    monkeypatch.setitem(namespace, "_cn_timestamp", lambda now=None: clock if now is None else original(now))
    monkeypatch.setitem(namespace, "_cn_local_day", lambda now=None: pd.Timestamp(clock.date()))
    v20.base_mod.assert_realtime_anchor_precedes_quote_trade_date(meta(stamp))
    assert realtime_row_is_consistent(row(stamp), "long_microcap_short_zz1000")


@pytest.mark.parametrize("stamp", [
    "2026-10-08T15:00:00+08:00",
    "2026-10-08T15:15:00+08:00",
    "2026-10-08T15:29:59+08:00",
    "2026-10-08T07:00:00+00:00",
])
def test_at_or_after_deadline_final_guard_rejects(calendar_oracle, monkeypatch, stamp):
    clock = pd.Timestamp(stamp).tz_convert("Asia/Shanghai")
    namespace = v20.base_mod.assert_realtime_anchor_precedes_quote_trade_date.__globals__
    original = namespace["_cn_timestamp"]
    monkeypatch.setitem(namespace, "_cn_timestamp", lambda now=None: clock if now is None else original(now))
    monkeypatch.setitem(namespace, "_cn_local_day", lambda now=None: pd.Timestamp(clock.date()))
    with pytest.raises(RuntimeError):
        v20.base_mod.assert_realtime_anchor_precedes_quote_trade_date(meta(stamp))


@pytest.mark.parametrize("stamp", [
    "2026-10-08T15:00:00+08:00",
    "2026-10-08T15:15:00+08:00",
    "2026-10-08T15:29:59+08:00",
    "2026-10-08T07:00:00+00:00",
])
def test_at_or_after_deadline_written_row_rejects(calendar_oracle, stamp):
    assert not realtime_row_is_consistent(row(stamp), "long_microcap_short_zz1000")
