"""Independent exchange-session evidence; never infer holidays from NAV gaps.

Uses the existing pinned AkShare Sina calendar parser in a bounded subprocess.
No market prices, strategy state, or credentials are written by this module.
"""
from __future__ import annotations

import json
import subprocess
import sys
from datetime import date, datetime, time
from functools import lru_cache
from contextlib import contextmanager
from threading import RLock
import uuid
from zoneinfo import ZoneInfo

BJ = ZoneInfo("Asia/Shanghai")
CLOSE_CONFIRMED = time(15, 0)  # Existing Top100 CN_CLOSE_CONFIRM_TIME.
CALENDAR_TIMEOUT_SECONDS = 30
SOURCE = "akshare.tool_trade_date_hist_sina"
_PREFLIGHT_REQUEST_GUARDS = {}
_PREFLIGHT_REQUEST_GUARDS_LOCK = RLock()


@contextmanager
def preflight_request_window(before_request):
    """Process-wide admission scope, including original validator workers.

    Each owner removes only its own token. All active intraday scopes apply;
    nesting, parallel scopes and an explicit request can only tighten a limit.
    Close-only readiness adds no process-wide intraday scope.
    """
    if getattr(before_request, "intraday_deadline", None) is None:
        yield
        return
    token = uuid.uuid4().hex
    with _PREFLIGHT_REQUEST_GUARDS_LOCK:
        _PREFLIGHT_REQUEST_GUARDS[token] = before_request
    try:
        yield
    finally:
        with _PREFLIGHT_REQUEST_GUARDS_LOCK:
            del _PREFLIGHT_REQUEST_GUARDS[token]


def _preflight_request_limit(explicit):
    with _PREFLIGHT_REQUEST_GUARDS_LOCK:
        guards = list(_PREFLIGHT_REQUEST_GUARDS.values())
    if explicit is not None:
        guards.append(explicit)
    deadlines = []
    for guard in guards:
        guard()
        deadline = getattr(guard, "intraday_deadline", None)
        if deadline is not None:
            if not isinstance(deadline, datetime) or deadline.tzinfo is None:
                raise ValueError("Calendar request deadline lacks timezone")
            deadlines.append(deadline)
    return min(deadlines) if deadlines else None


def before_network_request(explicit=None):
    """Apply every active native intraday admission scope to a transport."""
    _preflight_request_limit(explicit)


def intraday_request_window_active():
    """Native entrypoint capability, independent of removable report fields."""
    with _PREFLIGHT_REQUEST_GUARDS_LOCK:
        return bool(_PREFLIGHT_REQUEST_GUARDS)


@lru_cache(maxsize=32)
def sessions_for_day(day: date, *, before_request=None) -> tuple[date, ...]:
    code = ("import json,akshare as ak; "
            "print(json.dumps([str(d) for d in ak.tool_trade_date_hist_sina()['trade_date']]))")
    deadline = _preflight_request_limit(before_request)
    if deadline is not None:
        if not isinstance(deadline, datetime) or deadline.tzinfo is None:
            raise ValueError("Calendar request deadline lacks timezone")
        code = ("import json,akshare as ak\nfrom datetime import datetime,timezone\n"
                f"deadline=datetime.fromisoformat({deadline.isoformat()!r})\n"
                "if datetime.now(timezone.utc)>=deadline:\n"
                " raise RuntimeError('Preflight intraday request cutoff before calendar HTTP')\n"
                "print(json.dumps([str(d) for d in ak.tool_trade_date_hist_sina()['trade_date']]))")
    try:
        process = subprocess.run([sys.executable, "-X", "utf8", "-c", code],
                                 capture_output=True, text=True, encoding="utf-8",
                                 timeout=CALENDAR_TIMEOUT_SECONDS, check=True)
        values = json.loads(process.stdout)
        if not isinstance(values, list) or not values:
            raise ValueError("empty calendar")
        sessions = tuple(sorted({date.fromisoformat(value) for value in values}))
        if not sessions[0] <= day <= sessions[-1]:
            raise ValueError(f"calendar does not cover {day}")
        return sessions
    except (ValueError, TypeError, subprocess.SubprocessError, OSError) as exc:
        raise RuntimeError(f"Independent exchange calendar unavailable for {day}: {type(exc).__name__}") from exc


def latest_completed_session(now: datetime | None = None, *, before_request=None) -> date:
    current = datetime.now(BJ) if now is None else now
    current = current.replace(tzinfo=BJ) if current.tzinfo is None else current.astimezone(BJ)
    day = current.date()
    options = {"before_request": before_request} if before_request is not None else {}
    sessions = sessions_for_day(day, **options)
    completed = [value for value in sessions if value < day or (
        value == day and current.time().replace(tzinfo=None) >= CLOSE_CONFIRMED)]
    if not completed:
        raise RuntimeError(f"No completed exchange session before {current.isoformat()}")
    return completed[-1]


def is_trading_day(day: date) -> bool:
    return day in sessions_for_day(day)
