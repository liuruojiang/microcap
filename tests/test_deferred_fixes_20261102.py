"""Recorded real notice/basis evidence and explicit synthetic transport fault injection."""
import copy
import hashlib
import json
import os
from pathlib import Path
import time

import pandas as pd
import pytest

from scripts import bounded_market_data as bounded
from scripts import st_notice_boundary_review as review
from scripts import st_notice_refresh as notice
from scripts.stock_return_basis import build_ratio_return_basis, validate_cached_candidate
from test_stock_return_basis import inputs


def reviewed_row():
    return json.loads((review.DIRECTORY / "registry.json").read_bytes())["reviews"]["AN202611011831295457"]["native_row"]


def test_real_reviewed_regulatory_decision_is_native_and_legacy_readable():
    row = reviewed_row(); current = {row["art_code"]: row}
    result = notice._boundary_precision_drift({}, current)
    assert result["policy"] == "document_reviewed_non_st_boundary_additions_v1"
    assert result["changed_event_count"] == 1
    legacy = review.legacy_summary({}, current, notice._precision_drift_only, result)
    assert notice._boundary_summary_matches(legacy, result, {}, current)
    legacy["certificate_sha256"] = "0" * 64
    assert not notice._boundary_summary_matches(legacy, result, {}, current)


@pytest.mark.parametrize("fault", ["unknown", "deletion", "issuer", "title", "time", "extra"])
def test_review_is_not_a_generic_history_waiver(fault):
    row = copy.deepcopy(reviewed_row()); identity = row["art_code"]
    previous, current = {}, {identity: row}
    if fault == "unknown": current = {"unregistered": row}
    elif fault == "deletion": previous, current = current, {}
    elif fault == "issuer": row["codes"][0]["stock_code"] = "600000"
    elif fault == "title": row["title"] = "关于实施其他风险警示的公告"
    elif fault == "time": row["display_time"] = "2026-11-02 17:16:27:255"
    else: current["another"] = row
    with pytest.raises(notice.StNoticeEvidenceError): notice._boundary_precision_drift(previous, current)


def test_changed_review_document_fails(tmp_path, monkeypatch):
    import shutil
    for p in review.DIRECTORY.iterdir():
        if p.is_file(): shutil.copy2(p, tmp_path / p.name)
    (tmp_path / "document.pdf").write_bytes(b"not the reviewed decision")
    monkeypatch.setattr(review, "DIRECTORY", tmp_path)
    row = reviewed_row()
    with pytest.raises(notice.StNoticeEvidenceError, match="document identity"):
        notice._boundary_precision_drift({}, {row["art_code"]: row})


def test_real_basis_hash_is_independent_of_host_newlines(monkeypatch):
    manifests = []
    for newline in ("\n", "\r\n"):
        monkeypatch.setattr(os, "linesep", newline)
        frame, manifest = build_ratio_return_basis(**inputs())
        raw = frame.to_csv(index=False, lineterminator="\r\n").encode()
        assert hashlib.sha256(raw).hexdigest() == manifest["candidate_sha256"]
        validate_cached_candidate(raw, manifest, expected_manifest=manifest)
        with pytest.raises(ValueError):
            validate_cached_candidate(raw.replace(b"\r\n", b"\n"), manifest, expected_manifest=manifest)
        manifests.append(manifest)
    assert manifests[0] == manifests[1]


def test_worker_has_total_deadline_and_preserves_explicit_http_timeout(tmp_path, monkeypatch):
    # Fake SDK is local transport test data, never market evidence.
    (tmp_path / "akshare.py").write_text('''
import time
def stock_zh_a_spot(): time.sleep(5)
def stock_zh_a_spot_em():
    import requests,pandas as pd
    seen=[]
    def send(self,request,**kwargs):
        seen.append(kwargs['timeout'])
        r=requests.Response();r.status_code=200;r._content=b'{}';return r
    requests.adapters.HTTPAdapter.send=send
    requests.get('https://fixture.invalid')
    requests.get('https://fixture.invalid',timeout=2)
    return pd.DataFrame({'code':['000001'],'default':[str(seen[0])],'explicit':[str(seen[1])]})
''', encoding="utf-8")
    monkeypatch.setenv("PYTHONPATH", str(tmp_path))
    started = time.monotonic()
    with pytest.raises(TimeoutError, match="total deadline"):
        bounded.fetch_akshare_frame("stock_zh_a_spot", timeout=.3)
    assert time.monotonic() - started < 5
    frame = bounded.fetch_akshare_frame("stock_zh_a_spot_em", timeout=15)
    assert frame.iloc[0].to_dict() == {"code": "000001", "default": "(5, 15)", "explicit": "2"}


def test_universe_falls_back_and_failure_preserves_existing_cache(tmp_path, monkeypatch):
    import fetch_wind_microcap_index as market
    path = tmp_path / "universe.csv"; monkeypatch.setattr(market, "UNIVERSE_CACHE", path)
    calls=[]
    def fetch(provider):
        calls.append(provider)
        if provider == "stock_zh_a_spot": raise TimeoutError("fixture timeout")
        return pd.DataFrame({market.COL_CODE: ["000001", "600000", "830001"], market.COL_NAME: ["fixture A", "fixture B", "fixture BJ"]})
    monkeypatch.setattr(bounded, "fetch_akshare_frame", fetch)
    frame=market.fetch_active_universe(force_refresh=True)
    assert frame.code.tolist() == ["000001", "600000"] and len(calls)==2
    before=path.read_bytes()
    def fail(provider): raise TimeoutError("all fixture providers unavailable")
    monkeypatch.setattr(bounded, "fetch_akshare_frame", fail)
    with pytest.raises(RuntimeError): market.fetch_active_universe(force_refresh=True)
    assert path.read_bytes()==before
