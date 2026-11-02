from datetime import date
import pytest
from scripts import exchange_calendar as calendar
from scripts import realtime_state_bundle as state

@pytest.fixture
def proof(monkeypatch):
    monkeypatch.setattr(calendar, 'latest_completed_session', lambda: date(2026, 11, 2))
    return dict(target_end_date='2026-11-02', verified_on='2026-11-02',
                verified_at_utc='2026-11-02T08:00:00+00:00')

def test_midnight_preserves_actual_proof_date(proof):
    before = proof.copy()
    assert state._close_confirmed_proof_day(proof, date(2026,11,2), date(2026,11,3)) == '2026-11-02'
    assert proof == before

def test_new_completed_session_blocks_old_proof(proof, monkeypatch):
    monkeypatch.setattr(calendar, 'latest_completed_session', lambda: date(2026,11,3))
    with pytest.raises(ValueError):
        state._close_confirmed_proof_day(proof, date(2026,11,2), date(2026,11,3))

@pytest.mark.parametrize('change', [
    {'target_end_date':'2026-10-30'}, {'verified_on':'2026-10-30'},
    {'verified_on':'2026-11-04'}, {'verified_at_utc':'2026-11-02T08:00:00'},
    {'verified_at_utc':'2026-11-01T08:00:00+00:00'},
    {'verified_at_utc':'2026-11-02T06:59:59+00:00'},
])
def test_invalid_proof_rejected(proof, change):
    proof.update(change)
    with pytest.raises(ValueError):
        state._close_confirmed_proof_day(proof, date(2026,11,2), date(2026,11,3))
