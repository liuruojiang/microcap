"""Independent recorded sessions for offline regression, never a live proof."""
from datetime import date
from functools import cache
import hashlib
import json
from pathlib import Path

import pytest

from scripts import exchange_calendar


CALENDAR_SHA256 = "f6c5474c46aeae51259a415a708dc71101aee47cdccee0809af451411e804b32"


@cache
def _load_sessions():
    path = Path(__file__).with_name("fixtures") / "exchange_sessions_20261015.json"
    evidence = json.loads(path.read_text(encoding="utf-8"))
    assert evidence["scope"] == "frozen_regression_calendar_only_not_current_freshness"
    assert evidence["source"] == "akshare.tool_trade_date_hist_sina"
    values = evidence["sessions"]
    payload = json.dumps(values, separators=(",", ":")).encode("utf-8")
    assert hashlib.sha256(payload).hexdigest() == evidence["calendar_sha256"] == CALENDAR_SHA256
    sessions = tuple(date.fromisoformat(value) for value in values)
    assert sessions and sessions == tuple(sorted(set(sessions)))
    assert sessions[0].isoformat() == evidence["first_session"]
    assert sessions[-1].isoformat() == evidence["last_session"]
    return sessions


@pytest.fixture
def frozen_exchange_sessions(monkeypatch):
    """Replace only calendar acquisition; retain all native session checks."""
    sessions = _load_sessions()

    def recorded_sessions(day):
        if not sessions[0] <= day <= sessions[-1]:
            raise RuntimeError(f"Recorded independent exchange calendar does not cover {day}")
        return sessions

    monkeypatch.setattr(exchange_calendar, "sessions_for_day", recorded_sessions)
    return sessions
