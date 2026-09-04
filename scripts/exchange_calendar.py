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
from zoneinfo import ZoneInfo

BJ = ZoneInfo("Asia/Shanghai")
CLOSE_CONFIRMED = time(15, 30)  # Existing Top100 CN_CLOSE_CONFIRM_TIME.
CALENDAR_TIMEOUT_SECONDS = 30
SOURCE = "akshare.tool_trade_date_hist_sina"


@lru_cache(maxsize=4)
def sessions_for_day(day: date) -> tuple[date, ...]:
    code = ("import json,akshare as ak; "
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


def latest_completed_session(now: datetime | None = None) -> date:
    current = datetime.now(BJ) if now is None else now
    current = current.replace(tzinfo=BJ) if current.tzinfo is None else current.astimezone(BJ)
    day = current.date()
    sessions = sessions_for_day(day)
    completed = [value for value in sessions if value < day or (
        value == day and current.time().replace(tzinfo=None) >= CLOSE_CONFIRMED)]
    if not completed:
        raise RuntimeError(f"No completed exchange session before {current.isoformat()}")
    return completed[-1]


def is_trading_day(day: date) -> bool:
    return day in sessions_for_day(day)
