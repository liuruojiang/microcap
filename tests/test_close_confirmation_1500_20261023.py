"""The authorized close boundary must agree across independent and price paths."""
from datetime import datetime, date
import pandas as pd
import pytest
from scripts import exchange_calendar
import microcap_top100_mom16_biweekly_live_v2_0 as v20

@pytest.mark.parametrize("clock,expected", [
    ("2026-10-23T14:59:59+08:00", "2026-10-22"),
    ("2026-10-23T15:00:00+08:00", "2026-10-23"),
    ("2026-10-23T15:00:01+08:00", "2026-10-23"),
    ("2026-10-23T06:59:59+00:00", "2026-10-22"),
    ("2026-10-23T07:00:01+00:00", "2026-10-23"),
])
def test_calendar_and_real_loader_close_boundary_agree(monkeypatch, clock, expected):
    days=(date(2026,10,22),date(2026,10,23),date(2026,10,26))
    monkeypatch.setattr(exchange_calendar, "sessions_for_day", lambda day: days)
    assert exchange_calendar.latest_completed_session(datetime.fromisoformat(clock)).isoformat()==expected
    history=pd.DataFrame({"date":pd.to_datetime(["2026-10-22","2026-10-23","2026-10-26"]), "close":[100.,101.,102.]})
    assert v20.base_mod.latest_closed_history_date(history,pd.Timestamp(clock)).date().isoformat()==expected

def test_after_close_does_not_invent_missing_today_price():
    history=pd.DataFrame({"date":pd.to_datetime(["2026-10-22"]), "close":[100.]})
    assert v20.base_mod.latest_closed_history_date(history,pd.Timestamp("2026-10-23T15:01:00+08:00")).date()==date(2026,10,22)
