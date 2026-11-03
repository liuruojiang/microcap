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


def _request_clock(monkeypatch, hour=14, minute=59, second=59):
    from datetime import datetime
    from zoneinfo import ZoneInfo
    current = [datetime(2026, 11, 3, hour, minute, second, tzinfo=ZoneInfo("Asia/Shanghai"))]
    monkeypatch.setattr(state, "_preflight_now", lambda: current[0], raising=False)
    return current


def test_preflight_validation_crossing_cutoff_starts_no_history_request(monkeypatch, tmp_path):
    import sys
    from types import SimpleNamespace
    from scripts import index_history_preflight as history
    current = _request_clock(monkeypatch)
    def validated(*args, **kwargs):
        current[0] = current[0].replace(hour=15, minute=0, second=0)
        value = report("2026-11-02")
        value["current_member_symbols"] = []
        return value
    monkeypatch.setattr(state, "validate_state", validated)
    monkeypatch.setattr(state, "validate_reference_summary", lambda *args: [])
    monkeypatch.setitem(sys.modules, "microcap_top100_mom16_biweekly_live_v2_0",
                        SimpleNamespace(_sync_embedded_base_config=lambda: None, base_mod=object()))
    calls = []
    def transport(*args, **kwargs):
        calls.append("request")
        raise RuntimeError("unexpected history request after cutoff")
    monkeypatch.setattr(history, "fetch_preflight_history", transport)
    with pytest.raises(RuntimeError, match="intraday request cutoff"):
        state.preflight_state(tmp_path, expected_date=date(2026, 11, 2))
    assert calls == []


def test_preflight_history_finishing_after_cutoff_starts_no_members_or_proof(monkeypatch, tmp_path):
    import pandas as pd
    import sys
    from types import SimpleNamespace
    from scripts import index_history_preflight as history
    current = _request_clock(monkeypatch)
    value = report("2026-11-02"); value["current_member_symbols"] = []
    monkeypatch.setattr(state, "validate_state", lambda *a, **k: deepcopy(value))
    monkeypatch.setattr(state, "validate_reference_summary", lambda *a: [])
    base = SimpleNamespace(latest_closed_history_date=lambda rows: pd.Timestamp("2026-11-02"))
    monkeypatch.setitem(sys.modules, "microcap_top100_mom16_biweekly_live_v2_0",
                        SimpleNamespace(_sync_embedded_base_config=lambda: None, base_mod=base))
    def fetched(*a, **k):
        current[0] = current[0].replace(hour=15, minute=0, second=0)
        return pd.DataFrame({"date": ["2026-11-02"]})
    monkeypatch.setattr(history, "fetch_preflight_history", fetched)
    monkeypatch.setattr(state, "verify_live_member_names", lambda *a, **k: pytest.fail("late member request"))
    monkeypatch.setattr(state, "_write_refresh_proof", lambda *a, **k: pytest.fail("late proof"))
    with pytest.raises(RuntimeError, match="intraday request cutoff"):
        state.preflight_state(tmp_path, expected_date=date(2026, 11, 2))


def test_preflight_last_member_response_after_cutoff_cannot_certify(monkeypatch):
    import pandas as pd
    from types import SimpleNamespace
    current = _request_clock(monkeypatch)
    guard = state._preflight_request_guard("intraday")
    symbols = [str(i).zfill(6) for i in range(100)]
    def fetched(*a, **k):
        current[0] = current[0].replace(hour=15, minute=0, second=0)
        return pd.DataFrame({"code": symbols, "trade_date": ["2026-11-03"]*100})
    base = SimpleNamespace(fetch_member_realtime_quotes=fetched,
                           assert_no_st_members=lambda *a: pytest.fail("late data cannot pass preflight"))
    with pytest.raises(RuntimeError, match="intraday request cutoff"):
        state.verify_live_member_names(base, symbols, before_request=guard)


def test_preflight_final_validation_crossing_cutoff_cannot_write_proof(monkeypatch, tmp_path):
    current = _request_clock(monkeypatch)
    guard = state._preflight_request_guard("intraday")
    before = report("2026-11-02")
    def validated(*a, **k):
        current[0] = current[0].replace(hour=15, minute=0, second=0)
        return deepcopy(before)
    monkeypatch.setattr(state, "validate_state", validated)
    monkeypatch.setattr(state, "_write_refresh_proof", lambda *a, **k: pytest.fail("late proof write"))
    with pytest.raises(RuntimeError, match="intraday request cutoff"):
        state.certify_existing_state(tmp_path, before, date(2026, 11, 2), before_commit=guard)


def test_preflight_proof_serialization_crossing_cutoff_preserves_previous_proof(monkeypatch, tmp_path):
    current = _request_clock(monkeypatch)
    guard = state._preflight_request_guard("intraday")
    path = tmp_path/state.REFRESH_PROOF_REL; path.parent.mkdir(parents=True)
    path.write_bytes(b"previous exact proof")
    original = state.json.dump
    def delayed(*a, **k):
        result = original(*a, **k)
        current[0] = current[0].replace(hour=15, minute=0, second=0)
        return result
    monkeypatch.setattr(state.json, "dump", delayed)
    with pytest.raises(RuntimeError, match="intraday request cutoff"):
        state._write_refresh_proof(tmp_path, date(2026, 11, 2), before_commit=guard)
    assert path.read_bytes() == b"previous exact proof"
    assert not list(path.parent.glob(".*.tmp"))


def test_preflight_guard_reaches_actual_calendar_subprocess(monkeypatch):
    from scripts import exchange_calendar as calendar
    current = _request_clock(monkeypatch)
    guard = state._preflight_request_guard("intraday")
    current[0] = current[0].replace(hour=15, minute=0, second=0)
    calls = []
    monkeypatch.setattr(calendar.subprocess, "run", lambda *a, **k: calls.append(a))
    calendar.sessions_for_day.cache_clear()
    with pytest.raises(RuntimeError, match="intraday request cutoff"):
        calendar.sessions_for_day(date(2026, 11, 3), before_request=guard)
    assert calls == []


def test_calendar_child_import_delay_cannot_start_http_after_deadline(monkeypatch, tmp_path):
    from datetime import datetime, timedelta, timezone
    from scripts import exchange_calendar as calendar
    marker = tmp_path/"request_started.txt"
    (tmp_path/"akshare.py").write_text(
        "def tool_trade_date_hist_sina():\n"
        f" from pathlib import Path; Path({str(marker)!r}).write_text('HTTP')\n"
        " return {'trade_date':['2026-11-03']}\n", encoding="utf8")
    monkeypatch.setenv("PYTHONPATH", str(tmp_path))
    # Parent permission was acquired earlier; the child must still use its
    # own real wall clock after imports, before calling the transport library.
    def guard(): pass
    guard.intraday_deadline = datetime.now(timezone.utc)-timedelta(seconds=1)
    calendar.sessions_for_day.cache_clear()
    with pytest.raises(RuntimeError, match="Independent exchange calendar unavailable"):
        calendar.sessions_for_day(date(2026, 11, 3), before_request=guard)
    assert not marker.exists()


def test_preflight_history_fallback_crossing_cutoff_starts_no_second_http(monkeypatch):
    from scripts import index_history_preflight as history
    from types import SimpleNamespace
    current = _request_clock(monkeypatch)
    guard = state._preflight_request_guard("intraday")
    monkeypatch.setattr(history.exchange_calendar, "latest_completed_session", lambda **k: date(2026, 11, 2))
    monkeypatch.setattr(history.exchange_calendar, "sessions_for_day", lambda *a, **k: (date(2026, 11, 2),))
    calls = []
    def get(url, **kwargs):
        calls.append(url)
        current[0] = current[0].replace(hour=15, minute=0, second=0)
        return SimpleNamespace(raise_for_status=lambda: None)
    monkeypatch.setattr(history.requests, "get", get)
    monkeypatch.setattr(history, "sina_static", lambda *a, **k: history._get("https://primary", **k))
    monkeypatch.setattr(history, "tencent", lambda *a, **k: history._get("https://backup", **k))
    with pytest.raises(RuntimeError, match="intraday request cutoff"):
        history.fetch_preflight_history(date(2026, 11, 2), date(2026, 11, 2), before_request=guard)
    assert calls == ["https://primary"]


def _quote_functions(embedded):
    """Compile actual transport functions, without running a strategy import."""
    import ast
    from pathlib import Path
    import pandas as pd
    root = Path(__file__).resolve().parents[1]
    if embedded:
        tree = ast.parse((root/"microcap_top100_mom16_biweekly_live_v2_0.py").read_text(encoding="utf-8-sig"))
        source = next(node.value.value for node in tree.body if isinstance(node, ast.Assign)
                      and any(isinstance(t, ast.Name) and t.id == "BASE_SOURCE" for t in node.targets))
    else:
        source = (root/"microcap_top100_mom16_biweekly_live.py").read_text(encoding="utf-8-sig")
    wanted = {"fetch_eastmoney_batch_realtime_quotes", "fetch_tencent_realtime_quotes",
              "fetch_eastmoney_stock_spot", "fetch_member_realtime_quotes"}
    nodes = [node for node in ast.parse(source).body if isinstance(node, ast.FunctionDef) and node.name in wanted]
    assert {node.name for node in nodes} == wanted
    namespace = {"pd": pd, "eastmoney_secid": lambda symbol: symbol,
                 "tencent_quote_symbol": lambda symbol: symbol,
                 "_normalize_realtime_quote_frame": lambda frame: frame,
                 "_cn_timestamp": lambda: pd.Timestamp("2026-11-03T15:00:00+08:00")}
    exec(compile(ast.fix_missing_locations(ast.Module(body=nodes, type_ignores=[])), "<actual-quote-transports>", "exec"), namespace)
    return namespace


@pytest.mark.parametrize("embedded", [False, True])
@pytest.mark.parametrize("function", ["fetch_eastmoney_batch_realtime_quotes", "fetch_tencent_realtime_quotes"])
def test_preflight_quote_batches_recheck_each_http(monkeypatch, embedded, function):
    from types import SimpleNamespace
    current = _request_clock(monkeypatch)
    guard = state._preflight_request_guard("intraday")
    namespace = _quote_functions(embedded)
    calls = []
    def get(url, **kwargs):
        calls.append(url)
        current[0] = current[0].replace(hour=15, minute=0, second=0)
        return SimpleNamespace(raise_for_status=lambda: None, json=lambda: {"data": {}}, text="", encoding="gbk")
    namespace["requests"] = SimpleNamespace(get=get)
    with pytest.raises(RuntimeError, match="intraday request cutoff"):
        namespace[function](["000001", "000002"], batch_size=1, before_request=guard)
    assert len(calls) == 1


@pytest.mark.parametrize("embedded", [False, True])
def test_preflight_per_symbol_http_checks_inside_worker(monkeypatch, embedded):
    from types import SimpleNamespace
    current = _request_clock(monkeypatch)
    guard = state._preflight_request_guard("intraday")
    current[0] = current[0].replace(hour=15, minute=0, second=0)
    namespace = _quote_functions(embedded)
    calls = []
    namespace["requests"] = SimpleNamespace(get=lambda *a, **k: calls.append(a))
    with pytest.raises(RuntimeError, match="intraday request cutoff"):
        namespace["fetch_eastmoney_stock_spot"]("000001", before_request=guard)
    assert calls == []


@pytest.mark.parametrize("embedded", [False, True])
def test_preflight_member_fallback_propagates_guard_to_queued_workers(monkeypatch, embedded):
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from types import SimpleNamespace
    import pandas as pd
    current = _request_clock(monkeypatch)
    guard = state._preflight_request_guard("intraday")
    namespace = _quote_functions(embedded)
    empty = lambda: pd.DataFrame(columns=["code"])
    namespace.update(_empty_realtime_quote_frame=empty, _valid_quote_code_set=lambda value: set(),
                     ThreadPoolExecutor=ThreadPoolExecutor, as_completed=as_completed,
                     fetch_eastmoney_batch_realtime_quotes=lambda *a, **k: empty(),
                     fetch_tencent_realtime_quotes=lambda *a, **k: empty())
    calls = []
    def get(url, **kwargs):
        calls.append(url)
        current[0] = current[0].replace(hour=15, minute=0, second=0)
        return SimpleNamespace(raise_for_status=lambda: None, json=lambda: {"data": {}})
    namespace["requests"] = SimpleNamespace(get=get)
    with pytest.raises(RuntimeError, match="intraday request cutoff"):
        namespace["fetch_member_realtime_quotes"](["000001", "000002"], max_workers=1, before_request=guard)
    assert len(calls) == 1


def test_explicit_close_preflight_allows_after_close_transport(monkeypatch):
    from types import SimpleNamespace
    from scripts import index_history_preflight as history
    _request_clock(monkeypatch, hour=20, minute=0, second=0)
    guard = state._preflight_request_guard("close-confirmed-check")
    calls = []
    monkeypatch.setattr(history.requests, "get", lambda *a, **k: calls.append(a) or SimpleNamespace(raise_for_status=lambda: None))
    history._get("https://close-proof", before_request=guard)
    assert len(calls) == 1


def test_close_preflight_mode_cannot_start_before_close(monkeypatch):
    _request_clock(monkeypatch)
    guard = state._preflight_request_guard("close-confirmed-check")
    with pytest.raises(RuntimeError, match="close-confirmed-check"):
        guard()
