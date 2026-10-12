"""Unresolved current-ST cache entries must not enter PIT membership builds."""

import json

import pandas as pd
import pytest

import microcap_top100_mom16_biweekly_live_v2_0 as v2


@pytest.fixture
def unresolved_688496(tmp_path, monkeypatch):
    ns = v2.base_mod.freq_mod.build_security_meta.__globals__
    cache = tmp_path / "688496.json"
    old = {
        "meta_version": ns["SECURITY_META_VERSION"],
        "st_notice_policy_version": ns["ST_NOTICE_POLICY_VERSION"],
        "symbol": "688496",
        "first_trade_date": "2022-12-28",
        "last_trade_date": "2026-09-10",
        "current_st_snapshot_name": "*ST清越",
        "current_st_history_resolved": False,
        "name_history_status": "not_applicable",
        "notice_query_status": "error:HTTPError:403",
        "st_intervals": [
            {"start": "2026-09-10", "end": None, "source": "current_name_snapshot"}
        ],
    }
    cache.write_text(json.dumps(old, ensure_ascii=False, indent=2), encoding="utf-8")
    monkeypatch.setitem(ns, "SECURITY_META_DIR", tmp_path)
    monkeypatch.setitem(ns, "resolve_security_meta_path", lambda symbol: cache)
    monkeypatch.setitem(ns, "load_current_st_name_map", lambda: {"688496": "*ST清越"})
    monkeypatch.setitem(ns, "latest_symbol_price_date", lambda code: pd.Timestamp("2026-09-10"))
    return ns, cache, old


def test_unresolved_same_date_st_cache_is_not_reused(unresolved_688496, monkeypatch):
    ns, cache, old = unresolved_688496
    before = cache.read_bytes()
    calls = []

    def unresolved_builder(symbol):
        calls.append(symbol)
        return old

    monkeypatch.setitem(ns, "build_security_meta", unresolved_builder)
    with pytest.raises(ns["SecurityMetaHistoryEvidenceUnresolved"], match="688496"):
        ns["load_security_meta"]("688496")
    assert calls == ["688496"]
    assert cache.read_bytes() == before


def test_resolved_official_history_replaces_snapshot_only_cache(unresolved_688496, monkeypatch):
    ns, cache, old = unresolved_688496
    before = cache.read_bytes()
    calls = []
    verified = {
        **old,
        "current_st_history_resolved": True,
        "notice_query_status": "ok:independent_official_evidence",
        "st_intervals": [
            {"start": "2026-04-29", "end": None, "source": "cninfo_notice"}
        ],
    }

    def resolved_builder(symbol):
        calls.append(symbol)
        return verified

    monkeypatch.setitem(ns, "build_security_meta", resolved_builder)
    result = ns["load_security_meta"]("688496")
    assert calls == ["688496"]
    assert result["st_intervals"][0] == {
        "start": "2026-04-29",
        "end": None,
        "source": "cninfo_notice",
    }
    assert cache.read_bytes() == before


def test_official_builder_does_not_overwrite_unresolved_history_in_place(unresolved_688496, tmp_path, monkeypatch):
    ns, cache, _old = unresolved_688496
    before = cache.read_bytes()
    price = tmp_path / "688496-prices.csv"
    price.write_text("date\n2022-12-28\n2026-09-10\n", encoding="utf-8")
    monkeypatch.setitem(ns, "resolve_cache_path", lambda *_args: price)
    monkeypatch.setitem(ns, "load_security_master", lambda: pd.DataFrame())
    notices = pd.DataFrame(
        [
            {"notice_date": "2026-04-29", "title": "清越科技关于实施其他风险警示暨股票停牌的公告"},
            {"notice_date": "2026-05-09", "title": "清越科技关于实施退市风险警示并继续实施其他风险警示暨停牌的公告"},
            {"notice_date": "2026-09-02", "title": "清越科技关于公司股票可能被实施重大违法强制退市暨继续实施退市风险警示的公告"},
        ]
    )
    calls = []

    def official_notices(symbol, start_date, end_date):
        calls.append((symbol, start_date, end_date))
        return notices

    monkeypatch.setitem(ns, "fetch_cninfo_st_notices", official_notices)
    with pytest.raises(ns["SecurityMetaHistoryCorrectionRequired"], match="exact-hash lineage review"):
        ns["load_security_meta"]("688496")
    assert calls == [("688496", "20221228", "20260910")]
    assert cache.read_bytes() == before


def test_new_unresolved_current_st_meta_is_rejected_before_cache_write(tmp_path, monkeypatch):
    ns = v2.base_mod.freq_mod.build_security_meta.__globals__
    price = tmp_path / "688496-prices.csv"
    price.write_text("date\n2022-12-28\n2026-09-10\n", encoding="utf-8")
    meta_dir = tmp_path / "security_meta"
    monkeypatch.setitem(ns, "resolve_cache_path", lambda *_args: price)
    monkeypatch.setitem(ns, "SECURITY_META_DIR", meta_dir)
    monkeypatch.setitem(ns, "SHARED_SECURITY_META_DIR", None)
    monkeypatch.setitem(ns, "resolve_security_meta_path", lambda _symbol: None)
    monkeypatch.setitem(ns, "load_security_master", lambda: pd.DataFrame())
    monkeypatch.setitem(ns, "load_current_st_name_map", lambda: {"688496": "*ST清越"})

    def unavailable_notices(**_kwargs):
        raise RuntimeError("HTTP403")

    monkeypatch.setitem(ns, "fetch_cninfo_st_notices", unavailable_notices)
    with pytest.raises(ns["SecurityMetaHistoryEvidenceUnresolved"], match="HTTP403"):
        ns["build_security_meta"]("688496")
    assert not (meta_dir / "688496.json").exists()


def test_official_cache_panel_loader_fails_closed_on_unresolved_history(tmp_path, monkeypatch):
    freq = v2.base_mod.freq_mod
    ns = freq.load_symbol_cache.__globals__
    price = tmp_path / "688496-price.csv"
    share = tmp_path / "688496-share.csv"
    price.write_text("date,close_raw\n2026-09-10,1.0\n", encoding="utf-8")
    share.write_text("change_date,total_shares_10k\n2026-09-10,100.0\n", encoding="utf-8")

    def resolve_cache_path(base_dir, _shared_dir, _symbol):
        if base_dir == ns["PRICE_DIR"]:
            return price
        if base_dir == ns["SHARE_DIR"]:
            return share
        return None

    def unresolved_meta(_symbol):
        raise ns["SecurityMetaHistoryEvidenceUnresolved"]("688496 history remains unresolved")

    monkeypatch.setitem(ns, "resolve_cache_path", resolve_cache_path)
    monkeypatch.setitem(ns, "load_security_meta", unresolved_meta)
    dates = pd.DatetimeIndex([pd.Timestamp("2026-09-10")])
    with pytest.raises(RuntimeError, match="symbol cache panel load failed for 1 symbols") as exc:
        freq.load_cache_panels(
            ["688496"],
            trading_dates=dates,
            cap_dates=dates,
            max_workers=1,
        )
    assert "688496" in str(exc.value)
    assert "SecurityMetaHistoryEvidenceUnresolved" in str(exc.value)
