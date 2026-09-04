from datetime import date
from copy import deepcopy
import pytest
from scripts import realtime_state_bundle as state


def report(day):
    return {"ok": True, "errors": [], "files": [{"sha256": "a"}],
            "price_cache_files": [], "anchor_dates": {
                name: day for name in ("proxy_index", "costed_nav", "panel_shadow")}}


@pytest.mark.parametrize("target", [date(2026,9,3), date(2026,9,4)])
def test_preflight_certifies_only_independent_matching_close(monkeypatch, tmp_path, target):
    current = report("2026-09-03")
    writes = []
    monkeypatch.setattr(state, "validate_state", lambda *a, **kw: deepcopy(current))
    monkeypatch.setattr(state, "_write_refresh_proof", lambda root, day: writes.append(day))
    result = state.certify_existing_state(tmp_path, current, target)
    assert result["ok"] == (target == date(2026,9,3))
    assert writes == ([target] if result["ok"] else [])


def test_concurrent_state_change_cannot_receive_proof(monkeypatch, tmp_path):
    before = report("2026-09-03")
    after = deepcopy(before)
    after["files"][0]["sha256"] = "changed"
    monkeypatch.setattr(state, "validate_state", lambda *a, **kw: after)
    monkeypatch.setattr(state, "_write_refresh_proof", lambda *a: pytest.fail("must not write"))
    assert not state.certify_existing_state(tmp_path, before, date(2026,9,3))["ok"]


@pytest.mark.parametrize("day,name,passes", [("2026-09-04", "normal", True),
                                           ("2026-09-03", "normal", False),
                                           ("2026-09-04", "*ST bad", False)])
def test_current_names_require_today_complete_non_st(monkeypatch, day, name, passes):
    import pandas as pd
    from types import SimpleNamespace
    symbols = [str(i).zfill(6) for i in range(100)]
    quotes = pd.DataFrame({"code": symbols, "name": [name]*100, "trade_date": [day]*100})
    def guard(frame, label):
        if frame["name"].str.startswith("*ST").any():
            raise RuntimeError("ST member")
    base = SimpleNamespace(fetch_member_realtime_quotes=lambda codes: quotes, assert_no_st_members=guard)
    monkeypatch.setattr(state, "_cn_today", lambda: date(2026,9,4))
    if passes:
        assert state.verify_live_member_names(base, symbols)["current_st_name_intersection"] == 0
    else:
        with pytest.raises(RuntimeError):
            state.verify_live_member_names(base, symbols)
