"""Real fetcher/builder regressions for invalid CNINFO history responses.

HTTP payloads are isolated fault injections. No network or formal data writes.
"""

import json

import pandas as pd
import pytest

import microcap_top100_mom16_biweekly_live_v2_0 as v20


NOTICE_MS = int(pd.Timestamp("2026-09-02", tz="UTC").timestamp() * 1000)
NOTICE = {"announcementTime": NOTICE_MS, "announcementTitle": "关于公司股票实施其他风险警示的公告"}


@pytest.fixture
def cninfo_runtime(tmp_path, monkeypatch):
    ns = v20.freq_mod.fetch_cninfo_st_notices.__globals__
    metadata = tmp_path / "security_meta"
    price = tmp_path / "prices.csv"
    price.write_text("date\n2026-09-02\n2026-09-03\n", encoding="utf-8")
    state = {"body": {"totalAnnouncement": 0, "announcements": None}, "calls": []}

    class Response:
        def __init__(self, body):
            self.body = body

        def raise_for_status(self):
            pass

        def json(self):
            return self.body

    def post(url, **kwargs):
        assert url == "https://www.cninfo.com.cn/new/hisAnnouncement/query"
        payload = dict(kwargs["data"])
        state["calls"].append(payload)
        body = state["body"](payload) if callable(state["body"]) else state["body"]
        return Response(body)

    monkeypatch.setattr(ns["requests"], "post", post)
    monkeypatch.setitem(ns, "fetch_cninfo_org_map", lambda: {"600000": "gssh0600000"})
    monkeypatch.setitem(ns, "resolve_cache_path", lambda *_args: price)
    monkeypatch.setitem(ns, "SECURITY_META_DIR", metadata)
    monkeypatch.setitem(ns, "SHARED_SECURITY_META_DIR", None)
    monkeypatch.setitem(ns, "load_security_master", lambda: pd.DataFrame())
    monkeypatch.setitem(ns, "load_current_st_name_map", lambda: {})
    return ns, state, metadata


def _call(ns, entry):
    if entry == "fetcher":
        return ns["fetch_cninfo_st_notices"]("600000", "20260902", "20260903")
    return ns["build_security_meta"]("600000")


BAD_PAGES = [
    [],
    {},
    {"error": "FAULT_INJECTION: upstream failure"},
    {"totalAnnouncement": None, "announcements": []},
    {"totalAnnouncement": "invalid", "announcements": []},
    {"totalAnnouncement": True, "announcements": [NOTICE]},
    {"totalAnnouncement": -1, "announcements": []},
    {"totalAnnouncement": 1.5, "announcements": [NOTICE]},
    {"totalAnnouncement": 0, "announcements": [NOTICE]},
    {"totalAnnouncement": 1, "announcements": None},
    {"totalAnnouncement": 1, "announcements": []},
    {"totalAnnouncement": 1},
    {"totalAnnouncement": 1, "announcements": {}},
    {"totalAnnouncement": 1, "announcements": [None]},
    {"totalAnnouncement": 1, "announcements": [{**NOTICE, "announcementTitle": None}]},
    {"totalAnnouncement": 1, "announcements": [{**NOTICE, "announcementTitle": "<em></em>"}]},
    {"totalAnnouncement": 1, "announcements": [{"announcementTitle": NOTICE["announcementTitle"]}]},
    {"totalAnnouncement": 1, "announcements": [{**NOTICE, "announcementTime": None}]},
    {"totalAnnouncement": 1, "announcements": [{**NOTICE, "announcementTime": "bad-date"}]},
    {"totalAnnouncement": 1, "announcements": [{**NOTICE, "announcementTime": float("nan")}]},
]


@pytest.mark.parametrize("body", BAD_PAGES)
@pytest.mark.parametrize("entry", ["fetcher", "builder"])
def test_invalid_http_200_history_cannot_become_zero_notices(cninfo_runtime, body, entry):
    ns, state, metadata = cninfo_runtime
    state["body"] = body
    error = ValueError if entry == "fetcher" else ns["SecurityMetaHistoryEvidenceUnresolved"]
    with pytest.raises(error):
        _call(ns, entry)
    assert not (metadata / "600000.json").exists()


@pytest.mark.parametrize("org_id", [None, "", "  ", float("nan"), 0])
@pytest.mark.parametrize("entry", ["fetcher", "builder"])
def test_missing_organization_identity_is_unknown_not_zero(cninfo_runtime, monkeypatch, org_id, entry):
    ns, state, metadata = cninfo_runtime
    monkeypatch.setitem(ns, "fetch_cninfo_org_map", lambda: {"600000": org_id})
    error = ValueError if entry == "fetcher" else ns["SecurityMetaHistoryEvidenceUnresolved"]
    with pytest.raises(error, match="organization ID"):
        _call(ns, entry)
    assert state["calls"] == []
    assert not (metadata / "600000.json").exists()


@pytest.mark.parametrize("body", [
    {"totalAnnouncement": 0, "announcements": None},
    {"totalAnnouncement": 0, "announcements": []},
    {"totalAnnouncement": 0},
])
def test_explicit_zero_is_a_valid_result_for_all_six_queries(cninfo_runtime, body):
    ns, state, _metadata = cninfo_runtime
    state["body"] = body
    result = _call(ns, "fetcher")
    assert result.empty
    assert list(result.columns) == ["notice_date", "title", "announcement_id", "issuer_symbol", "source_url"]
    assert len(state["calls"]) == 6
    assert {call["searchkey"] for call in state["calls"]} == {
        "", "风险警示", "特别处理", "撤销风险警示", "撤销特别处理", "摘帽",
    }


def test_explicit_zero_keeps_healthy_non_st_builder_behavior(cninfo_runtime):
    ns, state, metadata = cninfo_runtime
    result = _call(ns, "builder")
    assert result["notice_query_status"] == "ok"
    assert result["st_intervals"] == []
    assert len(state["calls"]) == 6
    assert json.loads((metadata / "600000.json").read_text(encoding="utf-8")) == result


def _notice_rows(count):
    return [{**NOTICE, "announcementTime": NOTICE_MS + index * 60_000,
             "announcementTitle": NOTICE["announcementTitle"] + str(index)}
            for index in range(count)]


@pytest.mark.parametrize("fault", ["empty_second_page", "missing_rows", "excess_rows", "changed_total", "missing_total"])
@pytest.mark.parametrize("entry", ["fetcher", "builder"])
def test_incomplete_or_contradictory_pagination_blocks_history(cninfo_runtime, fault, entry):
    ns, state, metadata = cninfo_runtime
    rows = _notice_rows(32)

    def body(payload):
        page = int(payload["pageNum"])
        if page == 1:
            return {"totalAnnouncement": 31, "announcements": rows[:29 if fault == "missing_rows" else 30]}
        total = 32 if fault == "changed_total" else 31
        items = [] if fault == "empty_second_page" else rows[30:32 if fault == "excess_rows" else 31]
        result = {"totalAnnouncement": total, "announcements": items}
        if fault == "missing_total":
            result.pop("totalAnnouncement")
        return result

    state["body"] = body
    error = ValueError if entry == "fetcher" else ns["SecurityMetaHistoryEvidenceUnresolved"]
    with pytest.raises(error):
        _call(ns, entry)
    assert len(state["calls"]) == 2
    assert not (metadata / "600000.json").exists()


def test_valid_short_last_page_and_cross_query_duplicates_are_preserved(cninfo_runtime):
    ns, state, _metadata = cninfo_runtime
    rows = _notice_rows(31)

    def body(payload):
        items = rows[:30] if payload["pageNum"] == "1" else rows[30:]
        # Real CNINFO totalpages is not a reliable ceil(total / pageSize).
        return {"totalAnnouncement": 31, "totalpages": 1, "announcements": items}

    state["body"] = body
    result = _call(ns, "fetcher")
    assert len(result) == 31
    assert result["notice_date"].notna().all()
    assert len(state["calls"]) == 12
    assert [call["pageNum"] for call in state["calls"]] == ["1", "2"] * 6


def test_valid_single_notice_is_retained_by_the_real_builder(cninfo_runtime):
    ns, state, metadata = cninfo_runtime
    state["body"] = {"totalAnnouncement": "1", "announcements": [NOTICE], "totalpages": 0}
    result = _call(ns, "builder")
    assert result["notice_query_status"] == "ok"
    assert result["notice_count"] == 1
    assert result["st_intervals"] == [{"start": "2026-09-02", "end": None, "source": "cninfo_notice"}]
    assert json.loads((metadata / "600000.json").read_text(encoding="utf-8")) == result


def test_protocol_failure_cannot_erase_certified_cached_history(cninfo_runtime):
    ns, state, metadata = cninfo_runtime
    previous = {
        "symbol": "600000", "meta_version": ns["SECURITY_META_VERSION"],
        "st_notice_policy_version": ns["ST_NOTICE_POLICY_VERSION"],
        "current_st_snapshot_name": None, "current_st_history_resolved": True,
        "name_history_status": "not_applicable", "notice_query_status": "ok",
        "st_intervals": [{"start": "2026-09-02", "end": "2026-09-02", "source": "cninfo_notice"}],
    }
    metadata.mkdir()
    path = metadata / "600000.json"
    path.write_text(json.dumps(previous, ensure_ascii=False, indent=2), encoding="utf-8")
    before = path.read_bytes()
    state["body"] = {}
    assert _call(ns, "builder") == previous
    assert path.read_bytes() == before
