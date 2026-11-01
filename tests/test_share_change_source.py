import hashlib
import json
from pathlib import Path

import pandas as pd
import pytest

from scripts import share_change_source as source

FIXTURE = Path(__file__).parent / "fixtures/share_change_real"


def real_history():
    return source.parse_source((FIXTURE / "response.json").read_bytes(), symbol="000422",
                               start="2025-01-01", end="2026-10-29")


def test_original_share_units_and_registered_events():
    frame = real_history().set_index("change_date")
    assert frame.loc[pd.Timestamp("2026-09-24"), "total_shares_10k"] == 108694.9102
    assert frame.loc[pd.Timestamp("2025-10-20"), "total_shares_10k"] == 108824.8412


@pytest.mark.parametrize("change", ["issuer", "count", "date", "quantity"])
def test_source_identity_completeness_and_values(change):
    obj = json.loads((FIXTURE / "response.json").read_bytes())
    if change == "issuer": obj["records"][0]["SECCODE"] = "000001"
    if change == "count": obj["records"].pop()
    if change == "date": obj["records"][0]["VARYDATE"] = "2026-10-30"
    if change == "quantity": obj["records"][0]["F003N"] = -1
    with pytest.raises(ValueError):
        source.parse_source(json.dumps(obj).encode(), symbol="000422", start="2025-01-01", end="2026-10-29")


def test_new_events_append_without_rewriting_old_counts():
    frame = real_history()
    old = frame.loc[frame.change_date.le("2025-10-20")].copy()
    out = source.merge_preserving_history(old, frame)
    pd.testing.assert_frame_equal(out.reset_index(drop=True), frame.reset_index(drop=True))
    bad = frame.copy()
    bad.loc[bad.change_date.eq("2025-10-20"), "total_shares_10k"] += 1
    with pytest.raises(ValueError, match="history changed"):
        source.merge_preserving_history(old, bad)


def test_numeric_dtype_change_is_not_a_historical_share_change():
    old = pd.DataFrame({"change_date": pd.to_datetime(["2026-10-28"]), "total_shares_10k": [10000], "reason": ["fixture"]})
    incoming = pd.DataFrame({"change_date": pd.to_datetime(["2026-10-28", "2026-10-29"]),
                             "total_shares_10k": [10000.0, 10001.0], "reason": ["fixture", "fixture"]})
    assert len(source.merge_preserving_history(old, incoming)) == 2


def install_fixture(root):
    cache = root / ".microcap_index_cache/share_change/000422.csv"
    cache.parent.mkdir(parents=True)
    cache.write_bytes(real_history().to_csv(index=False).encode())
    directory = root / ".microcap_index_cache/share_sources"
    directory.mkdir()
    raw = (FIXTURE / "response.json").read_bytes()
    (directory / "source.json").write_bytes(raw)
    original = json.loads((FIXTURE / "receipt.json").read_bytes())
    state = dict(schema_version=1, symbol="000422", url=original["url"], params=original["params"],
                 status=200, retrieved_at_utc=original["retrieved_at_utc"], query_end="2026-10-29",
                 response_path="source.json", response_sha256=hashlib.sha256(raw).hexdigest(),
                 candidate_sha256=hashlib.sha256(cache.read_bytes()).hexdigest())
    path = directory / "000422.json"
    path.write_text(json.dumps(state), encoding="utf-8")
    return cache, path, state


def test_complete_same_day_cache_never_requeries(tmp_path, monkeypatch):
    cache, _, _ = install_fixture(tmp_path)
    def forbidden(*args, **kwargs): raise AssertionError("Unexpected network")
    monkeypatch.setattr(source.requests, "post", forbidden)
    old = cache.read_bytes()
    result = source.refresh_share_change(root=tmp_path, symbol="000422", cache_path=cache,
                                         as_of="2026-10-29", allow_network=False)
    assert len(result) == 10 and cache.read_bytes() == old


@pytest.mark.parametrize("change", ["future", "subject", "hash", "escape"])
def test_cached_source_receipts_fail_closed(tmp_path, change):
    cache, path, state = install_fixture(tmp_path)
    if change == "future": state["retrieved_at_utc"] = "2999-01-01T00:00:00+00:00"
    if change == "subject": state["params"]["scode"] = "000001"
    if change == "hash": state["response_sha256"] = "0" * 64
    if change == "escape": state["response_path"] = "../../other.json"
    path.write_text(json.dumps(state), encoding="utf-8")
    with pytest.raises(ValueError):
        source.refresh_share_change(root=tmp_path, symbol="000422", cache_path=cache,
                                     as_of="2026-10-29", allow_network=False)

def empty_provider(monkeypatch, calls):
    import sys
    from types import SimpleNamespace
    class Encoder:
        def eval(self, value): pass
        def call(self, value): return 'not-a-real-token'
    function = lambda: None
    function.__globals__['py_mini_racer'] = SimpleNamespace(MiniRacer=Encoder)
    function.__globals__['_get_file_content_cninfo'] = lambda name: ''
    monkeypatch.setitem(sys.modules, 'akshare', SimpleNamespace(stock_share_change_cninfo=function))
    class Response:
        content = b'{"resultcode":200,"total":0,"count":0,"records":[]}'
        status_code = 200
        def raise_for_status(self): pass
    def post(url, **kwargs):
        calls.append(kwargs['params'].copy())
        return Response()
    monkeypatch.setattr(source.requests, 'post', post)


def test_contiguous_empty_updates_advance_cutoff_without_changing_counts(tmp_path, monkeypatch):
    cache, path, _ = install_fixture(tmp_path)
    before = real_history()
    calls = []; empty_provider(monkeypatch, calls)
    source.refresh_share_change(root=tmp_path, symbol='000422', cache_path=cache, as_of='2026-10-30', allow_network=True)
    assert calls == [dict(scode='000422', sdate='2026-10-30', edate='2026-10-30')]
    state = json.loads(path.read_text())
    assert state['query_end'] == '2026-10-30'
    assert source.verify_share_cache(root=tmp_path, symbol='000422', as_of='2026-10-30')['source_files']
    pd.testing.assert_frame_equal(pd.read_csv(cache,parse_dates=['change_date']),before,check_dtype=False)
    # Same day is cache-only, including an explicitly empty successful response.
    source.refresh_share_change(root=tmp_path, symbol='000422', cache_path=cache, as_of='2026-10-30', allow_network=False)
    assert len(calls) == 1


def test_gap_cannot_be_hidden_by_rehashing_latest_receipt(tmp_path, monkeypatch):
    cache,path,_=install_fixture(tmp_path);calls=[];empty_provider(monkeypatch,calls)
    source.refresh_share_change(root=tmp_path,symbol='000422',cache_path=cache,as_of='2026-10-30',allow_network=True)
    state=json.loads(path.read_text());state['params']['sdate']='2026-10-31';state['query_start']='2026-10-31';path.write_text(json.dumps(state))
    with pytest.raises(ValueError,match='gap/overlap'):
        source.verify_share_cache(root=tmp_path,symbol='000422',as_of='2026-10-30')


def test_existing_cache_without_authority_is_not_self_certified(tmp_path):
    cache,path,_=install_fixture(tmp_path);path.unlink()
    with pytest.raises(ValueError,match='approved exact-hash baseline'):
        source.refresh_share_change(root=tmp_path,symbol='000422',cache_path=cache,as_of='2026-10-30',allow_network=False)


def test_approved_baseline_requires_exact_bytes_and_all_sources(tmp_path):
    cache,path,_=install_fixture(tmp_path);path.unlink()
    authority=tmp_path/'authority';authority.mkdir();raw=(FIXTURE/'response.json').read_bytes();(authority/'source.json').write_bytes(raw)
    record=dict(symbol='000422',candidate_sha256=hashlib.sha256(cache.read_bytes()).hexdigest(),certified_cutoff='2026-10-29',source_files=[dict(path='source.json',sha256=hashlib.sha256(raw).hexdigest())])
    report=dict(schema='share_baseline_approval_v1',approved=False,records=[record]);approval=authority/'approval.json';approval.write_text(json.dumps(report))
    with pytest.raises(ValueError,match='approval required'):
        source.install_approved_share_baseline(root=tmp_path,symbol='000422',cache_path=cache,approval_path=approval,approval_sha256=hashlib.sha256(approval.read_bytes()).hexdigest())
    report['approved']=True;approval.write_text(json.dumps(report));digest=hashlib.sha256(approval.read_bytes()).hexdigest()
    source.install_approved_share_baseline(root=tmp_path,symbol='000422',cache_path=cache,approval_path=approval,approval_sha256=digest)
    result=source.verify_share_cache(root=tmp_path,symbol='000422',as_of='2026-10-29')
    assert len(result['source_files'])==3
    copied=tmp_path/'.microcap_index_cache/share_sources/baselines'/digest/'source.json';source._extended(copied).write_text('{}')
    with pytest.raises(ValueError,match='source hash mismatch'):
        source.verify_share_cache(root=tmp_path,symbol='000422',as_of='2026-10-29')

def test_skipped_days_query_from_last_certified_cutoff(tmp_path, monkeypatch):
    cache,path,state=install_fixture(tmp_path)
    state['query_end']='2026-10-27';state['params']['edate']='2026-10-27';path.write_text(json.dumps(state))
    calls=[];empty_provider(monkeypatch,calls)
    source.refresh_share_change(root=tmp_path,symbol='000422',cache_path=cache,as_of='2026-10-30',allow_network=True)
    assert calls == [dict(scode='000422',sdate='2026-10-28',edate='2026-10-30')]
    source.verify_share_cache(root=tmp_path,symbol='000422',as_of='2026-10-30')


def test_concurrent_raw_change_during_share_query_is_not_overwritten(tmp_path,monkeypatch):
    cache,path,_=install_fixture(tmp_path);calls=[];empty_provider(monkeypatch,calls)
    post=source.requests.post
    changed=cache.read_bytes()+b'\n'
    def concurrent(*a,**kw):
        response=post(*a,**kw);cache.write_bytes(changed);return response
    monkeypatch.setattr(source.requests,'post',concurrent)
    with pytest.raises(ValueError,match='changed during source query'):
        source.refresh_share_change(root=tmp_path,symbol='000422',cache_path=cache,as_of='2026-10-30',allow_network=True)
    assert cache.read_bytes()==changed
