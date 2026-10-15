"""F1 fault injections use real metadata builders and temporary local inputs.

These are failure-path regressions, not strategy returns or a freshness proof.
"""

import json

import pandas as pd
import pytest

import microcap_top100_mom16_biweekly_live_v2_0 as v20


@pytest.fixture
def metadata_runtime(tmp_path, monkeypatch):
    ns = v20.freq_mod.build_security_meta.__globals__
    price = tmp_path / "prices.csv"
    price.write_text("date,close_raw\n2026-09-02,10.0\n2026-09-03,10.01\n", encoding="utf-8")
    shares = tmp_path / "shares.csv"
    shares.write_text("change_date,total_shares_10k\n2026-09-02,100.0\n", encoding="utf-8")
    metadata = tmp_path / "security_meta"

    def cache_path(base_dir, _shared_dir, _symbol):
        if base_dir == ns["PRICE_DIR"]:
            return price
        if base_dir == ns["SHARE_DIR"]:
            return shares
        return None

    monkeypatch.setitem(ns, "resolve_cache_path", cache_path)
    monkeypatch.setitem(ns, "SECURITY_META_DIR", metadata)
    monkeypatch.setitem(ns, "SHARED_SECURITY_META_DIR", None)
    monkeypatch.setitem(ns, "load_security_master", lambda: pd.DataFrame())
    monkeypatch.setitem(ns, "load_current_st_name_map", lambda: {})
    monkeypatch.setitem(ns, "fetch_sz_name_change_history", lambda: pd.DataFrame(
        columns=["symbol", "change_date", "old_name", "new_name"]))
    monkeypatch.setitem(ns, "fetch_cninfo_st_notices", lambda **_kwargs: pd.DataFrame(
        columns=["notice_date", "title"]))
    monkeypatch.setitem(ns, "read_ohlc_cache", lambda *_args: pd.DataFrame())
    return ns, metadata


def _source_outage(*_args, **_kwargs):
    raise RuntimeError("FAULT_INJECTION: historical source unavailable")


def _metadata_payload(ns, symbol="600000"):
    return {
        "symbol": symbol,
        "meta_version": ns["SECURITY_META_VERSION"],
        "st_notice_policy_version": ns["ST_NOTICE_POLICY_VERSION"],
        "first_trade_date": "2026-09-02",
        "last_trade_date": "2026-09-03",
        "current_st_snapshot_name": None,
        "current_st_history_resolved": True,
        "name_history_status": "not_applicable" if symbol.startswith("6") else "ok",
        "notice_query_status": "ok",
        "st_intervals": [],
    }


def _write_payload(directory, payload):
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / (payload["symbol"] + ".json")
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return path


def _load_caps(ns, symbol="600000"):
    days = pd.DatetimeIndex(["2026-09-02", "2026-09-03"])
    return v20.freq_mod.load_cache_panels(
        [symbol], trading_dates=days, cap_dates=days, max_workers=1,
        trade_constraint_mode=ns["TRADE_CONSTRAINT_MODE_CLOSE"],
    )


@pytest.mark.parametrize("symbol,source", [
    ("600000", "fetch_cninfo_st_notices"),
    ("000001", "fetch_cninfo_st_notices"),
    ("000001", "fetch_sz_name_change_history"),
])
def test_new_non_st_source_failure_blocks_before_cache_write(metadata_runtime, monkeypatch, symbol, source):
    ns, directory = metadata_runtime
    monkeypatch.setitem(ns, source, _source_outage)
    with pytest.raises(ns["SecurityMetaHistoryEvidenceUnresolved"], match="historical source unavailable"):
        ns["build_security_meta"](symbol)
    assert not (directory / (symbol + ".json")).exists()


def test_source_failure_cannot_enter_official_cap_panel(metadata_runtime, monkeypatch):
    ns, directory = metadata_runtime
    monkeypatch.setitem(ns, "fetch_cninfo_st_notices", _source_outage)
    with pytest.raises(RuntimeError, match="symbol cache panel load failed") as exc:
        _load_caps(ns)
    assert "600000" in str(exc.value)
    assert "SecurityMetaHistoryEvidenceUnresolved" in str(exc.value)
    assert not (directory / "600000.json").exists()


@pytest.mark.parametrize("field,source", [
    ("notice_query_status", "fetch_cninfo_st_notices"),
    ("name_history_status", "fetch_sz_name_change_history"),
])
def test_explicit_non_st_error_cache_is_not_reused(metadata_runtime, monkeypatch, field, source):
    ns, directory = metadata_runtime
    payload = _metadata_payload(ns, "000001")
    payload[field] = "error:RuntimeError:previous failed source"
    path = _write_payload(directory, payload)
    before = path.read_bytes()
    monkeypatch.setitem(ns, source, _source_outage)
    with pytest.raises(ns["SecurityMetaHistoryEvidenceUnresolved"], match="000001"):
        ns["load_security_meta"]("000001")
    assert path.read_bytes() == before


def test_failed_empty_cache_can_be_rebuilt_from_successful_sources(metadata_runtime):
    ns, directory = metadata_runtime
    payload = _metadata_payload(ns)
    payload["notice_query_status"] = "error:RuntimeError:previous failed source"
    path = _write_payload(directory, payload)
    result = ns["load_security_meta"]("600000")
    assert result["notice_query_status"] == "ok"
    assert result["st_intervals"] == []
    assert json.loads(path.read_text(encoding="utf-8")) == result


def _known_st_history(ns, monkeypatch):
    monkeypatch.setitem(ns, "load_current_st_name_map", lambda: {"600000": "*ST故障注入"})
    notices = pd.DataFrame([{
        "notice_date": "2026-09-02", "title": "关于公司股票实施其他风险警示的公告",
    }])
    monkeypatch.setitem(ns, "fetch_cninfo_st_notices", lambda **_kwargs: notices.copy())


@pytest.mark.parametrize("entry", ["metadata", "cap_panel", "historical_fetch"])
def test_real_metadata_write_failure_propagates_through_consumers(metadata_runtime, monkeypatch, entry):
    ns, directory = metadata_runtime
    _known_st_history(ns, monkeypatch)
    marker = b"FAULT_INJECTION: metadata directory is a file"
    directory.write_bytes(marker)
    days = pd.DatetimeIndex(["2026-09-02", "2026-09-03"])
    with pytest.raises(RuntimeError, match="600000") as exc:
        if entry == "metadata":
            ns["load_security_meta"]("600000")
        elif entry == "cap_panel":
            _load_caps(ns)
        else:
            v20.fetch_mod.build_historical_st_status_series("600000", days)
    assert "FileExistsError" in str(exc.value)
    assert directory.read_bytes() == marker


def test_healthy_known_st_history_is_still_excluded_from_caps(metadata_runtime, monkeypatch):
    ns, directory = metadata_runtime
    _known_st_history(ns, monkeypatch)
    _returns, caps, _buyable, _sellable = _load_caps(ns)
    assert not any("600000" in members for members in caps.values())
    result = ns["load_security_meta"]("600000")
    assert result["current_st_history_resolved"] is True
    assert result["st_intervals"][0]["start"] == "2026-09-02"
    assert (directory / "600000.json").is_file()


@pytest.mark.parametrize("symbol", ["600000", "000001"])
def test_healthy_non_st_metadata_keeps_normal_cap_membership(metadata_runtime, symbol):
    ns, directory = metadata_runtime
    _returns, caps, _buyable, _sellable = _load_caps(ns, symbol)
    assert len(caps) == 2
    assert all(symbol in members for members in caps.values())
    result = ns["load_security_meta"](symbol)
    assert result["notice_query_status"] == "ok"
    assert result["st_intervals"] == []
    assert json.loads((directory / (symbol + ".json")).read_text(encoding="utf-8")) == result


@pytest.mark.parametrize("corruption", [
    "invalid_json", "non_object", "invalid_intervals", "unreadable_path",
    "interval_not_object", "missing_start", "invalid_start", "invalid_end",
    "end_before_start", "numeric_start", "intraday_start", "timezone_end",
])
@pytest.mark.parametrize("entry", ["metadata", "builder"])
def test_existing_unreadable_metadata_never_disappears_into_non_st(metadata_runtime, corruption, entry):
    ns, directory = metadata_runtime
    directory.mkdir()
    path = directory / "600000.json"
    if corruption == "unreadable_path":
        path.mkdir()
        before = None
    else:
        bad_intervals = {
            "interval_not_object": ["2026-09-02"],
            "missing_start": [{"end": None}],
            "invalid_start": [{"start": "not-a-date", "end": None}],
            "invalid_end": [{"start": "2026-09-02", "end": "not-a-date"}],
            "end_before_start": [{"start": "2026-09-02", "end": "2026-09-01"}],
            "numeric_start": [{"start": 0, "end": None}],
            "intraday_start": [{"start": "2026-09-02 12:00:00", "end": None}],
            "timezone_end": [{"start": "2026-09-02", "end": "2026-09-03T00:00:00+08:00"}],
        }
        bad_text = {
            "invalid_json": '{"symbol":',
            "non_object": "[]",
            "invalid_intervals": json.dumps({**_metadata_payload(ns), "st_intervals": None}),
            **{key: json.dumps({**_metadata_payload(ns), "st_intervals": intervals})
               for key, intervals in bad_intervals.items()},
        }[corruption]
        path.write_text(bad_text, encoding="utf-8")
        before = path.read_bytes()
    function = ns["load_security_meta"] if entry == "metadata" else ns["build_security_meta"]
    with pytest.raises(ns["SecurityMetaHistoryEvidenceUnresolved"], match="600000"):
        function("600000")
    assert path.is_dir() if before is None else path.read_bytes() == before


@pytest.mark.parametrize("field", ["list_date", "delist_date", "first_trade_date", "last_trade_date"])
def test_malformed_security_lifetime_date_cannot_be_coerced_away(metadata_runtime, field):
    ns, directory = metadata_runtime
    path = _write_payload(directory, {**_metadata_payload(ns), field: "FAULT_INJECTION: invalid date"})
    before = path.read_bytes()
    with pytest.raises(ns["SecurityMetaHistoryEvidenceUnresolved"], match="600000"):
        ns["load_security_meta"]("600000")
    assert path.read_bytes() == before


@pytest.mark.parametrize("unknown", [None, ""])
def test_legacy_unknown_lifetime_dates_keep_existing_semantics(metadata_runtime, unknown):
    ns, directory = metadata_runtime
    payload = _metadata_payload(ns)
    payload.update({field: unknown for field in ("list_date", "delist_date", "first_trade_date", "last_trade_date")})
    path = _write_payload(directory, payload)
    before = path.read_bytes()
    assert ns["load_security_meta"]("600000") == payload
    assert path.read_bytes() == before


@pytest.mark.parametrize("missing_price", ["absent", "empty"])
def test_loader_rejects_real_builder_without_usable_price_dates(metadata_runtime, monkeypatch, tmp_path, missing_price):
    ns, directory = metadata_runtime
    if missing_price == "absent":
        monkeypatch.setitem(ns, "resolve_cache_path", lambda *_args: None)
    else:
        price = tmp_path / "empty-prices.csv"
        price.write_text("date\n", encoding="utf-8")
        monkeypatch.setitem(ns, "resolve_cache_path", lambda *_args: price)
    with pytest.raises(ns["SecurityMetaHistoryEvidenceUnresolved"], match="unavailable.*600000"):
        ns["load_security_meta"]("600000")
    assert not (directory / "600000.json").exists()


def test_approved_legacy_non_st_metadata_keeps_exact_bytes_and_no_refresh(metadata_runtime, monkeypatch):
    ns, directory = metadata_runtime
    legacy = {
        "meta_version": ns["SECURITY_META_VERSION"], "symbol": "600000",
        "first_trade_date": "2010-01-04", "last_trade_date": "2026-04-30",
        "list_date": "1999-11-10", "delist_date": None,
        "exchange": "SSE", "board": "主板", "st_intervals": [],
    }
    path = _write_payload(directory, legacy)
    before = path.read_bytes()
    calls = []

    def unexpected_rebuild(symbol):
        calls.append(symbol)
        raise AssertionError("Approved legacy metadata must remain a cache hit")

    monkeypatch.setitem(ns, "build_security_meta", unexpected_rebuild)
    assert ns["load_security_meta"]("600000") == legacy
    assert calls == []
    assert path.read_bytes() == before


@pytest.mark.parametrize("current_st", [False, True])
def test_failed_refresh_preserves_certified_interval_and_exact_bytes(metadata_runtime, monkeypatch, current_st):
    ns, directory = metadata_runtime
    payload = _metadata_payload(ns, "000001")
    payload["st_intervals"] = [{
        "start": "2026-09-02", "end": None if current_st else "2026-09-02",
        "source": "name_change",
    }]
    if current_st:
        payload["current_st_snapshot_name"] = "ST已认证"
        payload["notice_query_status"] = "error:HTTPError:403"
        monkeypatch.setitem(ns, "load_current_st_name_map", lambda: {"000001": "ST已认证"})
    path = _write_payload(directory, payload)
    before = path.read_bytes()
    monkeypatch.setitem(ns, "fetch_sz_name_change_history", _source_outage)
    monkeypatch.setitem(ns, "fetch_cninfo_st_notices", _source_outage)
    result = ns["build_security_meta"]("000001")
    assert result == payload
    assert ns["load_security_meta"]("000001") == payload
    assert path.read_bytes() == before


def test_historical_st_wrapper_requires_metadata_runtime(monkeypatch):
    ns = v20.fetch_mod.build_historical_st_status_series.__globals__
    monkeypatch.setitem(ns, "_FREQ_MOD_FOR_ST", None)
    with pytest.raises(RuntimeError, match="runtime is unavailable"):
        v20.fetch_mod.build_historical_st_status_series("600000", pd.DatetimeIndex(["2026-09-02"]))
