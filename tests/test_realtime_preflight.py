from datetime import date
from copy import deepcopy
import pytest
from scripts import realtime_state_bundle as state
from frozen_exchange_calendar import frozen_exchange_sessions


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


@pytest.mark.parametrize("scenario", ["already_closed", "validation_crossed"])
def test_native_initial_validation_calendar_is_inside_request_scope(tmp_path, monkeypatch, scenario):
    from scripts import exchange_calendar
    import json
    output = tmp_path/"outputs"; output.mkdir()
    for name in ("wind_microcap_top_100_biweekly_thursday_16y_cached.csv",
                 "microcap_top100_mom16_hedge_zz1000_0p8x_biweekly_thursday_16y_v2_0_base_costed_nav.csv",
                 "microcap_top100_mom16_biweekly_live_v2_0_base_panel_refreshed.csv"):
        (output/name).write_text("date,close\n2026-11-02,1\n")
    (output/"microcap_top100_mom16_biweekly_live_v2_0_base_proxy_meta.json").write_text(
        json.dumps({"core_params":{"stock_return_basis_revision":"sina_ratio_ex_reference_v1"}}))
    current = _request_clock(monkeypatch, hour=21 if scenario=="already_closed" else 14)
    original = state._sha256
    hashes = []
    def actual_hash(path):
        value = original(path); hashes.append(path)
        if scenario=="validation_crossed":
            current[0] = current[0].replace(hour=15,minute=0,second=0)
        return value
    monkeypatch.setattr(state,"_sha256",actual_hash)
    requests = []
    def forbidden(*args,**kwargs):
        requests.append(args)
        raise RuntimeError("uncaught calendar transport")
    monkeypatch.setattr(exchange_calendar.subprocess,"run",forbidden)
    exchange_calendar.sessions_for_day.cache_clear()
    previous = dict(exchange_calendar._PREFLIGHT_REQUEST_GUARDS)
    try:
        result = state.preflight_state(tmp_path,expected_date=date(2026,11,2))
    except RuntimeError as error:
        assert "cutoff" in str(error)
    else:
        assert result["ok"] is False
    assert requests==[]
    assert exchange_calendar._PREFLIGHT_REQUEST_GUARDS==previous
    assert bool(hashes) == (scenario=="validation_crossed")


def test_native_request_scope_reaches_worker_threads_and_does_not_relax_explicit_limit(monkeypatch):
    from concurrent.futures import ThreadPoolExecutor
    from scripts import exchange_calendar
    current = _request_clock(monkeypatch)
    strict = state._preflight_request_guard("intraday")
    def wider(): pass
    wider.intraday_deadline = current[0].replace(hour=16)
    previous = dict(exchange_calendar._PREFLIGHT_REQUEST_GUARDS)
    with exchange_calendar.preflight_request_window(strict):
        current[0] = current[0].replace(hour=15,minute=0,second=0)
        with exchange_calendar.preflight_request_window(wider):
            with ThreadPoolExecutor(max_workers=1) as pool:
                with pytest.raises(RuntimeError,match="cutoff"):
                    pool.submit(exchange_calendar.before_network_request,wider).result()
    assert exchange_calendar._PREFLIGHT_REQUEST_GUARDS==previous


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
              "fetch_eastmoney_stock_spot", "fetch_member_realtime_quotes", "fetch_hedge_realtime_quote_fast",
              "fetch_tencent_realtime_smallcap_members", "fetch_realtime_smallcap_members_fast"}
    nodes = [node for node in ast.parse(source).body if isinstance(node, ast.FunctionDef) and node.name in wanted]
    assert {node.name for node in nodes} == wanted
    namespace = {"pd": pd, "TOP_N": 100, "eastmoney_secid": lambda symbol: symbol,
                 "tencent_quote_symbol": lambda symbol: symbol,
                 "_normalize_realtime_quote_frame": lambda frame: frame,
                 "_cn_timestamp": lambda: pd.Timestamp("2026-11-03T15:00:00+08:00")}
    exec(compile(ast.fix_missing_locations(ast.Module(body=nodes, type_ignores=[])), "<actual-quote-transports>", "exec"), namespace)
    return namespace


@pytest.mark.parametrize("embedded", [False, True])
def test_active_native_scope_blocks_implicit_hedge_fallback_after_cutoff(monkeypatch, embedded):
    from scripts import exchange_calendar
    from types import SimpleNamespace
    current = _request_clock(monkeypatch)
    guard = state._preflight_request_guard("intraday")
    namespace = _quote_functions(embedded)
    calls = []
    def get(url, **kwargs):
        calls.append(url)
        current[0] = current[0].replace(hour=15,minute=0,second=0)
        raise RuntimeError("failed primary completed at cutoff")
    namespace["requests"] = SimpleNamespace(get=get)
    with exchange_calendar.preflight_request_window(guard):
        with pytest.raises(RuntimeError, match="intraday request cutoff"):
            namespace["fetch_hedge_realtime_quote_fast"]()
    assert len(calls) == 1


@pytest.mark.parametrize("embedded", [False, True])
@pytest.mark.parametrize("function", ["fetch_tencent_realtime_smallcap_members","fetch_realtime_smallcap_members_fast"])
def test_active_native_scope_blocks_each_member_preview_transport(monkeypatch, embedded, function):
    from scripts import exchange_calendar
    from types import SimpleNamespace
    current = _request_clock(monkeypatch)
    guard = state._preflight_request_guard("intraday")
    current[0] = current[0].replace(hour=15,minute=0,second=0)
    namespace = _quote_functions(embedded)
    namespace["load_realtime_eligible_codes"] = lambda: {"000001"}
    calls = []
    namespace["requests"] = SimpleNamespace(get=lambda *a, **k:calls.append(a))
    kwargs = dict(effective_date=None,capital=None,target_size=1)
    if function.startswith("fetch_tencent"):
        kwargs["eligible_codes"] = {"000001"}
    with exchange_calendar.preflight_request_window(guard):
        with pytest.raises(RuntimeError,match="intraday request cutoff"):
            namespace[function](**kwargs)
    assert calls == []


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


def test_explicit_close_preflight_allows_after_close_transport(monkeypatch, frozen_exchange_sessions):
    from types import SimpleNamespace
    from scripts import index_history_preflight as history
    _request_clock(monkeypatch, hour=20, minute=0, second=0)
    guard = state._preflight_request_guard("close-confirmed-check", expected_date=date(2026, 11, 3))
    calls = []
    monkeypatch.setattr(history.requests, "get", lambda *a, **k: calls.append(a) or SimpleNamespace(raise_for_status=lambda: None))
    history._get("https://close-proof", before_request=guard)
    assert len(calls) == 1


def test_close_preflight_mode_cannot_start_before_close(monkeypatch, frozen_exchange_sessions):
    _request_clock(monkeypatch)
    guard = state._preflight_request_guard("close-confirmed-check", expected_date=date(2026, 11, 3))
    with pytest.raises(RuntimeError, match="close-confirmed-check"):
        guard()


def test_close_guard_accepts_actual_previous_completed_session_after_midnight(monkeypatch, frozen_exchange_sessions):
    from datetime import datetime
    from zoneinfo import ZoneInfo
    monkeypatch.setattr(state, "_preflight_now", lambda: datetime(2026, 11, 4, 0, 30, tzinfo=ZoneInfo("Asia/Shanghai")))
    guard = state._preflight_request_guard("close-confirmed-check", expected_date=date(2026, 11, 3))
    guard()


@pytest.mark.parametrize("expected", [date(2026, 11, 2), date(2026, 11, 4), date(2026, 11, 5)])
def test_close_guard_rejects_old_unclosed_or_future_target(monkeypatch, frozen_exchange_sessions, expected):
    from datetime import datetime
    from zoneinfo import ZoneInfo
    monkeypatch.setattr(state, "_preflight_now", lambda: datetime(2026, 11, 4, 0, 30, tzinfo=ZoneInfo("Asia/Shanghai")))
    guard = state._preflight_request_guard("close-confirmed-check", expected_date=expected)
    with pytest.raises(RuntimeError, match="completed session"):
        guard()


def test_close_guard_rechecks_new_close_before_next_request(monkeypatch, frozen_exchange_sessions):
    from datetime import datetime
    from zoneinfo import ZoneInfo
    now = [datetime(2026, 11, 4, 14, 59, 59, tzinfo=ZoneInfo("Asia/Shanghai"))]
    monkeypatch.setattr(state, "_preflight_now", lambda: now[0])
    guard = state._preflight_request_guard("close-confirmed-check", expected_date=date(2026, 11, 3))
    guard()
    now[0] = now[0].replace(hour=15, minute=0, second=0)
    with pytest.raises(RuntimeError, match="completed session"):
        guard()


def test_close_guard_missing_expected_target_is_not_implicit_today(monkeypatch):
    _request_clock(monkeypatch, hour=20)
    with pytest.raises(ValueError, match="expected"):
        state._preflight_request_guard("close-confirmed-check")


def test_close_guard_independent_calendar_failure_closes(monkeypatch):
    from scripts import exchange_calendar
    _request_clock(monkeypatch, hour=20)
    def unavailable(*a, **k):
        raise RuntimeError("independent calendar unavailable")
    monkeypatch.setattr(exchange_calendar, "latest_completed_session", unavailable)
    guard = state._preflight_request_guard("close-confirmed-check", expected_date=date(2026, 11, 3))
    with pytest.raises(RuntimeError, match="calendar"):
        guard()


@pytest.fixture
def close_quote_fixture(monkeypatch, frozen_exchange_sessions):
    """Synthetic quote faults against real parsers/ST checks, not source acceptance."""
    import importlib
    import pandas as pd
    from datetime import datetime
    from zoneinfo import ZoneInfo
    base = importlib.import_module("microcap_top100_mom16_biweekly_live_v2_0").base_mod
    now = [datetime(2026, 11, 4, 0, 30, tzinfo=ZoneInfo("Asia/Shanghai"))]
    monkeypatch.setattr(state, "_preflight_now", lambda: now[0])
    namespace = base.fetch_member_realtime_quotes.__globals__
    original_clock = namespace["_cn_timestamp"]
    monkeypatch.setitem(namespace, "_cn_timestamp", lambda value=None: pd.Timestamp(now[0]) if value is None else original_clock(value))
    symbols = [f"{i:06d}" for i in range(1, 101)]
    frame = pd.DataFrame({"code": symbols, "name": ["SYNTHETIC"] * 100, "trade_date": ["2026-11-03"] * 100,
                          "rt_price": [10.] * 100, "price_origin": ["provider_latest"] * 100,
                          "quote_source": ["synthetic_quote_fault_fixture"] * 100,
                          "quoted_at": ["2026-11-03T15:00:00+08:00"] * 100,
                          "fetched_at": ["2026-11-04T00:30:00+08:00"] * 100})
    originals = {name: getattr(base, name) for name in ("fetch_eastmoney_batch_realtime_quotes", "fetch_tencent_realtime_quotes", "fetch_eastmoney_stock_spot")}
    monkeypatch.setattr(base, "fetch_eastmoney_batch_realtime_quotes", lambda *a, **k: frame.copy())
    monkeypatch.setattr(base, "fetch_tencent_realtime_quotes", lambda *a, **k: pd.DataFrame())
    monkeypatch.setattr(base, "fetch_eastmoney_stock_spot", lambda *a, **k: None)
    guard = state._preflight_request_guard("close-confirmed-check", expected_date=date(2026, 11, 3))
    return base, symbols, frame, now, guard, originals


def test_close_member_names_cross_midnight_without_relaxing_intraday(close_quote_fixture):
    base, symbols, frame, _, guard, _ = close_quote_fixture
    assert base.realtime_quote_row_is_genuine(frame.iloc[0]) is False
    result = state.verify_close_member_names(base, symbols, date(2026, 11, 3), before_request=guard)
    assert result["member_name_count"] == 100 and result["member_name_quote_day"] == "2026-11-03"
    assert result["member_name_audit_mode"] == "close_state_latest_completed_session"
    assert base.realtime_quote_row_is_genuine(frame.iloc[0]) is False


@pytest.mark.parametrize("field,value", [
    ("trade_date", "2026-11-04"), ("quoted_at", "2026-11-04T00:00:00+08:00"),
    ("quoted_at", "2026-11-03T14:59:59+08:00"), ("quoted_at", ""),
    ("fetched_at", "2026-11-03T14:00:00+08:00"), ("fetched_at", "2026-11-04T00:30:01+08:00"),
    ("rt_price", float("nan")), ("rt_price", 0), ("rt_price", True), ("price_origin", "previous_close"),
    ("name", ""), ("quote_source", ""),
])
def test_close_member_names_reject_wrong_day_unclosed_future_or_synthetic_fields(close_quote_fixture, field, value):
    base, symbols, frame, _, guard, _ = close_quote_fixture
    if isinstance(value, bool):
        frame[field] = frame[field].astype(object)
    frame.loc[0, field] = value
    with pytest.raises(RuntimeError, match="100/100"):
        state.verify_close_member_names(base, symbols, date(2026, 11, 3), before_request=guard)


@pytest.mark.parametrize("fault", ["duplicate", "ST"])
def test_close_member_names_uniqueness_and_st_are_not_relaxed(close_quote_fixture, fault):
    base, symbols, frame, _, guard, _ = close_quote_fixture
    frame.loc[0, "code" if fault == "duplicate" else "name"] = symbols[1] if fault == "duplicate" else "*ST SYNTHETIC"
    with pytest.raises((RuntimeError, ValueError), match="duplicate|ST"):
        state.verify_close_member_names(base, symbols, date(2026, 11, 3), before_request=guard)


def test_close_names_use_real_provider_transport_after_midnight(close_quote_fixture, monkeypatch):
    import pandas as pd
    import test_realtime_quote_integrity_20261024 as quotes
    base, symbols, _, _, guard, originals = close_quote_fixture
    monkeypatch.setattr(base, "fetch_eastmoney_batch_realtime_quotes", originals["fetch_eastmoney_batch_realtime_quotes"])
    monkeypatch.setattr(quotes, "DAY", "2026-11-03")
    provider = quotes.Quotes(times={"em": pd.Timestamp("2026-11-03T15:00:00+08:00")})
    monkeypatch.setattr(base.requests, "get", provider)
    result = state.verify_close_member_names(base, symbols, date(2026, 11, 3), before_request=guard)
    assert result["member_name_count"] == 100 and len(provider.calls) == 2


def test_close_names_new_session_close_stops_second_actual_batch(close_quote_fixture, monkeypatch):
    import pandas as pd
    import test_realtime_quote_integrity_20261024 as quotes
    base, symbols, _, now, guard, originals = close_quote_fixture
    now[0] = now[0].replace(hour=14, minute=59, second=59)
    monkeypatch.setattr(base, "fetch_eastmoney_batch_realtime_quotes", originals["fetch_eastmoney_batch_realtime_quotes"])
    monkeypatch.setattr(quotes, "DAY", "2026-11-03")
    provider = quotes.Quotes(times={"em": pd.Timestamp("2026-11-03T15:00:00+08:00")})
    def cross_close(*a, **k):
        response = provider(*a, **k)
        now[0] = now[0].replace(hour=15, minute=0, second=0)
        return response
    monkeypatch.setattr(base.requests, "get", cross_close)
    with pytest.raises(RuntimeError, match="completed session"):
        state.verify_close_member_names(base, symbols, date(2026, 11, 3), before_request=guard)
    assert len(provider.calls) == 1


@pytest.fixture
def persisted_close_case(close_quote_fixture):
    base, symbols, _, now, guard, _ = close_quote_fixture
    evidence = state.verify_close_member_names(base, symbols, date(2026, 11, 3), before_request=guard)
    files = [{"path": "outputs/control_fixture.csv", "sha256": "a" * 64}]
    evidence.update(preflight_publication_mode="close-confirmed-check", expected_calendar_day="2026-11-03",
                    independent_completed_day="2026-11-03", close_state_input_identity=state._close_state_input_identity(files, []))
    proof = {"version": state.REFRESH_PROOF_VERSION, "source": state.REFRESH_PROOF_SOURCE,
             "verified_on": "2026-11-04", "verified_at_utc": now[0].isoformat(),
             "target_end_date": "2026-11-03", "preflight_evidence": evidence}
    anchors = {name: date(2026, 11, 3) for name in ("proxy_index", "costed_nav", "panel_shadow")}
    return proof, anchors, symbols, files, now


def validate_persisted_close(case):
    proof, anchors, symbols, files, now = case
    return state._validate_close_refresh_proof(proof, anchors, symbols, files, [], now[0].date())


def test_persisted_close_proof_rechecks_all_actual_quote_fields(persisted_close_case):
    assert len(persisted_close_case[0]["preflight_evidence"]["member_name_close_quotes"]) == 100
    validate_persisted_close(persisted_close_case)


@pytest.mark.parametrize("fault", ["old_proof", "future_generated", "generated_before_fetch", "wrong_mode", "missing_mode",
                                  "missing_rows", "duplicate", "ST", "future_fetch", "preclose_quote", "input_hash",
                                  "aggregate", "count", "st_count", "wrong_audit_mode"])
def test_persisted_close_proof_faults_never_waive_state_age(persisted_close_case, fault):
    from datetime import timedelta
    proof, _, _, _, now = persisted_close_case
    evidence = proof["preflight_evidence"]
    if fault == "old_proof": proof["verified_on"] = "2026-11-03"
    if fault == "future_generated": proof["verified_at_utc"] = (now[0] + timedelta(seconds=1)).isoformat()
    if fault == "generated_before_fetch": proof["verified_at_utc"] = (now[0] - timedelta(seconds=1)).isoformat()
    if fault == "wrong_mode": evidence["preflight_publication_mode"] = "intraday"
    if fault == "missing_mode": evidence.pop("preflight_publication_mode")
    if fault == "missing_rows": evidence["member_name_close_quotes"].pop()
    if fault == "duplicate": evidence["member_name_close_quotes"][0]["code"] = evidence["member_name_close_quotes"][1]["code"]
    if fault == "ST": evidence["member_name_close_quotes"][0]["name"] = "*ST SYNTHETIC"
    if fault == "future_fetch": evidence["member_name_close_quotes"][0]["fetched_at"] = (now[0] + timedelta(seconds=1)).isoformat()
    if fault == "preclose_quote": evidence["member_name_close_quotes"][0]["quoted_at"] = "2026-11-03T14:59:59+08:00"
    if fault == "input_hash": evidence["close_state_input_identity"]["sha256"] = "0" * 64
    if fault == "aggregate": evidence["member_name_quoted_at_max"] = "2026-11-03T15:00:01+08:00"
    if fault == "count": evidence["member_name_count"] = 99
    if fault == "st_count": evidence["current_st_name_intersection"] = 1
    if fault == "wrong_audit_mode": evidence["member_name_audit_mode"] = "intraday"
    with pytest.raises((ValueError, RuntimeError)):
        validate_persisted_close(persisted_close_case)


def test_long_holiday_current_close_proof_uses_actual_latest_session(persisted_close_case):
    from datetime import datetime
    from zoneinfo import ZoneInfo
    proof, anchors, _, _, now = persisted_close_case
    now[0] = datetime(2026, 10, 8, 10, 0, tzinfo=ZoneInfo("Asia/Shanghai"))
    proof.update(target_end_date="2026-09-30", verified_on="2026-10-08", verified_at_utc=now[0].isoformat())
    evidence = proof["preflight_evidence"]
    evidence.update(expected_calendar_day="2026-09-30", independent_completed_day="2026-09-30", member_name_quote_day="2026-09-30",
                    member_name_quoted_at_min="2026-09-30T15:00:00+08:00", member_name_quoted_at_max="2026-09-30T15:00:00+08:00",
                    member_name_fetched_at_min=now[0].isoformat(), member_name_fetched_at_max=now[0].isoformat())
    for row in evidence["member_name_close_quotes"]:
        row.update(trade_date="2026-09-30", quoted_at="2026-09-30T15:00:00+08:00", fetched_at=now[0].isoformat())
    for key in anchors: anchors[key] = date(2026, 9, 30)
    assert (now[0].date() - anchors["proxy_index"]).days == 8
    validate_persisted_close(persisted_close_case)


def test_close_proof_validation_crossing_new_close_writes_nothing(monkeypatch, tmp_path, frozen_exchange_sessions):
    now = _request_clock(monkeypatch)
    now[0] = now[0].replace(day=4)
    before = report("2026-11-03")
    def validated(*a, **k):
        now[0] = now[0].replace(hour=15, minute=0, second=0)
        return deepcopy(before)
    monkeypatch.setattr(state, "validate_state", validated)
    monkeypatch.setattr(state, "_write_refresh_proof", lambda *a, **k: pytest.fail("new session cannot receive old close proof"))
    guard = state._preflight_request_guard("close-confirmed-check", expected_date=date(2026, 11, 3))
    with pytest.raises(RuntimeError, match="completed session"):
        state.certify_existing_state(tmp_path, before, date(2026, 11, 3), before_commit=guard)


def test_close_guard_rechecks_clock_after_actual_calendar_selection(monkeypatch, frozen_exchange_sessions):
    from scripts import exchange_calendar
    now = _request_clock(monkeypatch)
    now[0] = now[0].replace(day=4)
    original = exchange_calendar.latest_completed_session
    calls = []
    def slow_calendar(*a, **k):
        value = original(*a, **k)
        calls.append(k["now"])
        now[0] = now[0].replace(hour=15, minute=0, second=0)
        return value
    monkeypatch.setattr(exchange_calendar, "latest_completed_session", slow_calendar)
    guard = state._preflight_request_guard("close-confirmed-check", expected_date=date(2026, 11, 3))
    with pytest.raises(RuntimeError, match="completed session"):
        guard()
    assert len(calls) == 2 and calls[-1].hour == 15


def test_close_guard_calendar_crossing_midnight_rechecks_new_calendar_day(monkeypatch, frozen_exchange_sessions):
    from scripts import exchange_calendar
    now = _request_clock(monkeypatch, hour=23, minute=59, second=59)
    original = exchange_calendar.latest_completed_session
    calls = []
    def slow_calendar(*a, **k):
        value = original(*a, **k)
        calls.append(k["now"])
        now[0] = now[0].replace(day=4, hour=0, minute=0, second=1)
        return value
    monkeypatch.setattr(exchange_calendar, "latest_completed_session", slow_calendar)
    state._preflight_request_guard("close-confirmed-check", expected_date=date(2026, 11, 3))()
    assert len(calls) == 2 and [stamp.day for stamp in calls] == [3, 4]


def test_typed_close_proof_final_calendar_crossing_cannot_publish_old_session(persisted_close_case, monkeypatch):
    from scripts import exchange_calendar
    proof, _, _, _, now = persisted_close_case
    original = exchange_calendar.latest_completed_session
    def slow_calendar(*a, **k):
        value = original(*a, **k)
        now[0] = now[0].replace(hour=15, minute=0, second=0)
        return value
    monkeypatch.setattr(exchange_calendar, "latest_completed_session", slow_calendar)
    with pytest.raises(RuntimeError, match="completed session"):
        validate_persisted_close(persisted_close_case)


def test_archive_dated_close_proof_is_not_todays_live_proof(persisted_close_case):
    from datetime import timedelta
    proof, anchors, symbols, files, now = persisted_close_case
    proof.update(verified_on="2026-11-03", verified_at_utc="2026-11-03T20:00:00+08:00")
    evidence = proof["preflight_evidence"]
    for row in evidence["member_name_close_quotes"]:
        row["fetched_at"] = "2026-11-03T20:00:00+08:00"
    evidence.update(member_name_fetched_at_min="2026-11-03T20:00:00+08:00", member_name_fetched_at_max="2026-11-03T20:00:00+08:00")
    with pytest.raises(ValueError):
        validate_persisted_close(persisted_close_case)
    state._validate_close_refresh_proof(proof, anchors, symbols, files, [], now[0].date(), allow_dated_archive=True)


@pytest.mark.parametrize("version", ["2_0","2_3","2_5"])
def test_official_realtime_entry_cannot_fetch_hedge_after_members_complete_at_cutoff(monkeypatch, capsys, version):
    """Actual entry wrapper and quote code; no new policy gate is mocked as PASS."""
    import importlib
    import pandas as pd
    import requests
    import test_realtime_quote_integrity_20261024 as quotes
    module = importlib.import_module("microcap_top100_mom16_biweekly_live_v"+version)
    v20 = module if version == "2_0" else module.v2_0
    base = v20.base_mod
    clock = [pd.Timestamp(quotes.DAY+"T14:59:59+08:00")]
    monkeypatch.setattr(quotes,"NOW",clock[0])
    monkeypatch.setattr(state,"_preflight_now",lambda:clock[0].to_pydatetime())
    namespace = base.build_realtime_signal_fast.__globals__
    original = namespace["_cn_timestamp"]
    monkeypatch.setitem(namespace,"_cn_timestamp",lambda value=None:clock[0] if value is None else original(value))
    monkeypatch.setitem(namespace,"_cn_local_day",lambda value=None:pd.Timestamp(clock[0].date()))
    monkeypatch.setitem(namespace,"REALTIME_QUOTE_FETCH_ATTEMPTS",1)
    monkeypatch.setitem(namespace,"ensure_realtime_last_close_map",lambda *a,**k:dict.fromkeys(quotes.SYMBOLS,10.))
    provider = quotes.Quotes()
    hedge_calls = []
    member_calls = []
    def get(url,**kwargs):
        response = provider(url,**kwargs)
        if "ulist.np" in url:
            member_calls.append(url)
            if len(member_calls)==2:
                clock[0]=clock[0].replace(hour=15,minute=0,second=0)
        if "/stock/get" in url or "sh000852" in url:
            hedge_calls.append(url)
        return response
    monkeypatch.setattr(requests,"get",get)
    monkeypatch.setenv("TOP100_REALTIME_REQUIRE_STATE","1")
    entry = (v20.overlay_mod._print_realtime_signal_query if version=="2_0" else module._print_realtime_signal_query)
    key = "build_realtime_v"+version+"_outputs"
    def quote_builder():
        signal, metadata, close, _result = base.build_realtime_signal_fast(quotes.context())
        base.assert_realtime_meta_is_actionable(metadata)
        return signal, metadata, close
    monkeypatch.setitem(entry.__globals__,key,quote_builder)
    entry()
    assert len(member_calls)==2
    assert hedge_calls==[]
    assert "blocked" in capsys.readouterr().out.lower()
