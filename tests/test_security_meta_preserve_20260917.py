"""Source outages cannot erase already certified ST intervals."""
import copy
import json
from pathlib import Path

import pandas as pd
import pytest
import microcap_top100_mom16_biweekly_live_v2_0 as v2


@pytest.fixture
def meta_runtime(tmp_path, monkeypatch):
    ns = v2.base_mod.freq_mod.build_security_meta.__globals__
    price = tmp_path / "price.csv"
    price.write_text("date\n2026-01-05\n2026-09-17\n", encoding="utf-8")
    meta = tmp_path / "002198.json"
    previous = {
        "meta_version": ns["SECURITY_META_VERSION"],
        "st_notice_policy_version": ns["ST_NOTICE_POLICY_VERSION"],
        "symbol": "002198", "first_trade_date": "2026-01-05",
        "last_trade_date": "2026-09-16", "current_st_snapshot_name": "ST测试",
        "current_st_history_resolved": True,
        "name_history_status": "ok", "notice_query_status": "error:HTTPError:403",
        "st_intervals": [{"start": "2026-05-06", "end": None, "source": "name_change|current_name_snapshot"}],
    }
    meta.write_text(json.dumps(previous, ensure_ascii=False, indent=2), encoding="utf-8")
    monkeypatch.setitem(ns, "resolve_cache_path", lambda *args: price)
    monkeypatch.setitem(ns, "resolve_security_meta_path", lambda *args: meta)
    monkeypatch.setitem(ns, "SECURITY_META_DIR", tmp_path)
    monkeypatch.setitem(ns, "load_security_master", lambda: pd.DataFrame())
    monkeypatch.setitem(ns, "load_current_st_name_map", lambda: {"002198": "ST测试"})
    monkeypatch.setitem(ns, "latest_symbol_price_date", lambda code: pd.Timestamp("2026-09-17"))
    def failed(*args, **kwargs):
        raise RuntimeError("source outage")
    monkeypatch.setitem(ns, "fetch_sz_name_change_history", failed)
    monkeypatch.setitem(ns, "fetch_cninfo_st_notices", failed)
    return ns, meta, previous


def test_actual_metadata_builder_retains_certified_interval_and_exact_bytes(meta_runtime):
    ns, path, previous = meta_runtime
    before = path.read_bytes()
    result = ns["build_security_meta"]("002198")
    assert result == previous
    assert result["current_st_history_resolved"] is True
    assert result["st_intervals"][0]["start"] == "2026-05-06"
    assert path.read_bytes() == before


def test_actual_loader_does_not_relabel_old_snapshot_on_source_failure(meta_runtime):
    ns, path, previous = meta_runtime
    previous["current_st_history_resolved"] = False
    previous["st_intervals"] = [{"start": "2026-09-16", "end": None, "source": "current_name_snapshot"}]
    path.write_text(json.dumps(previous), encoding="utf-8")
    before = path.read_bytes()
    result = ns["load_security_meta"]("002198")
    assert result == previous
    assert result["st_intervals"][0]["start"] == "2026-09-16"
    assert path.read_bytes() == before


@pytest.mark.parametrize("field,value", [("meta_version", -1), ("st_notice_policy_version", "different-policy"), ("current_st_snapshot_name", "ST另一名称"), ("symbol", "000001")])
def test_preservation_never_crosses_identity_or_policy(meta_runtime, field, value):
    ns, path, previous = meta_runtime
    candidate = copy.deepcopy(previous)
    candidate[field] = value
    candidate["notice_query_status"] = "error:outage"
    assert ns["_preserve_security_meta_evidence"](previous, candidate) is candidate


def test_successful_source_correction_fails_before_overwriting_existing_history(meta_runtime, monkeypatch):
    ns, path, previous = meta_runtime
    before = path.read_bytes()
    changes = pd.DataFrame([{"symbol": "002198", "change_date": pd.Timestamp("2026-06-01"), "old_name": "测试", "new_name": "ST测试"}])
    monkeypatch.setitem(ns, "fetch_sz_name_change_history", lambda: changes)
    monkeypatch.setitem(ns, "fetch_cninfo_st_notices", lambda **kwargs: pd.DataFrame())
    with pytest.raises(RuntimeError, match="exact-hash lineage review"):
        ns["load_security_meta"]("002198")
    assert path.read_bytes() == before

def test_cninfo_official_https_transport_preserves_all_notice_queries(monkeypatch):
    import microcap_top100_mom16_biweekly_live_v2_0 as strategy
    freq = strategy.base_mod.freq_mod
    fn = freq.fetch_cninfo_st_notices
    calls = []
    monkeypatch.setitem(fn.__globals__, 'fetch_cninfo_org_map', lambda: {'300876': '990003'} )
    class Response:
        def raise_for_status(self): pass
        def json(self): return {'totalAnnouncement': 0, 'announcements': []}
    def post(url, **kwargs):
        calls.append((url, kwargs))
        return Response()
    monkeypatch.setattr(fn.__globals__['requests'], 'post', post)
    assert fn('300876', '20200824', '20260917').empty
    assert len(calls) == 6
    assert all(url == 'https://www.cninfo.com.cn/new/hisAnnouncement/query' for url, _ in calls)
    assert all(k['headers']['User-Agent'] == 'Mozilla/5.0' and k['headers']['Referer'] == 'https://www.cninfo.com.cn/new/index' for _, k in calls)
    assert {k['data']['searchkey'] for _, k in calls} == {'', '风险警示', '特别处理', '撤销风险警示', '撤销特别处理', '摘帽'}
    assert calls[0][1]['data']['category'] == 'category_tbclts_szsh'
