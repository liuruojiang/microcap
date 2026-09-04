"""Offline failure injection; synthetic calendars are not market-data claims."""
import json
import subprocess
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from hypothesis import given, settings, strategies as st
from scripts import exchange_calendar as calendar


@pytest.fixture(autouse=True)
def clear_calendar_cache():
    calendar.sessions_for_day.cache_clear()
    yield
    calendar.sessions_for_day.cache_clear()


def provider(monkeypatch, days):
    def run(*args, **kwargs):
        assert kwargs['timeout'] == 30
        assert kwargs['check'] is True
        return SimpleNamespace(stdout=json.dumps(days))
    monkeypatch.setattr(calendar.subprocess, 'run', run)


@pytest.mark.parametrize('stamp,expected', [
    ('2026-10-09T14:30:00+08:00', '2026-09-30'),
    ('2026-10-09T15:29:59+08:00', '2026-09-30'),
    ('2026-10-09T15:30:00+08:00', '2026-10-09'),
    ('2026-10-09T06:30:00+00:00', '2026-09-30'),
    ('2026-10-10T14:30:00+08:00', '2026-10-09'),
])
def test_holiday_boundary_and_timezone(monkeypatch, stamp, expected):
    provider(monkeypatch, ['2026-09-30','2026-10-09','2026-10-12'])
    assert calendar.latest_completed_session(datetime.fromisoformat(stamp)) == date.fromisoformat(expected)


@pytest.mark.parametrize('days', [[], ['2026-09-30'], ['bad'], {'trade_date': []}])
def test_truncated_or_invalid_calendar_cannot_prove_holiday(monkeypatch, days):
    provider(monkeypatch, days)
    with pytest.raises(RuntimeError, match='calendar unavailable'):
        calendar.latest_completed_session(datetime(2026,10,9,14,30))


def test_subprocess_timeout_fails_closed(monkeypatch):
    def fail(*args, **kwargs):
        raise subprocess.TimeoutExpired('calendar', 30)
    monkeypatch.setattr(calendar.subprocess, 'run', fail)
    with pytest.raises(RuntimeError, match='TimeoutExpired'):
        calendar.latest_completed_session(datetime(2026,10,9,14,30))


@given(gap=st.integers(min_value=1,max_value=40))
@settings(max_examples=40)
def test_any_holiday_length_preserves_previous_session(gap):
    day = date(2026,10,9)
    anchor = day-timedelta(days=gap)
    # The independent calendar contains no completed session inside the gap.
    from unittest.mock import patch
    with patch.object(calendar, 'sessions_for_day', return_value=(anchor,day,day+timedelta(days=3))):
        assert calendar.latest_completed_session(datetime(2026,10,9,6,30,tzinfo=timezone.utc)) == anchor


def test_holiday_is_not_a_trading_day(monkeypatch):
    provider(monkeypatch, ['2026-09-30','2026-10-09','2026-10-12'])
    assert not calendar.is_trading_day(date(2026,10,8))
    assert calendar.is_trading_day(date(2026,10,9))


def test_refreshed_holiday_state_requires_independent_session_and_preflight(monkeypatch, tmp_path):
    from scripts import realtime_state_bundle as state
    anchor = date(2026,9,30)
    monkeypatch.setattr(state, '_cn_today', lambda: date(2026,10,8))
    monkeypatch.setattr(calendar, 'latest_completed_session', lambda: anchor)
    calls = []
    monkeypatch.setattr(state, 'preflight_state', lambda root, age, expected_date: (
        calls.append((root,age,expected_date)) or {'ok':True}))
    assert state.validate_refreshed_state(tmp_path, anchor, 3)['ok']
    assert calls == [(tmp_path,3,anchor)]
    with pytest.raises(RuntimeError, match='misses completed exchange session'):
        state.validate_refreshed_state(tmp_path, date(2026,9,29), 3)
    assert len(calls) == 1
