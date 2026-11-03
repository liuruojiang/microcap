import hashlib
import json
from pathlib import Path

import pandas as pd
import pytest

from scripts import share_change_source as source

FIXTURE = Path(__file__).parent / "fixtures/share_change_real"


def _protocol_empty_tail(root, monkeypatch, field, value):
    """Mutate only an isolated native source chain, including its authentic hashes."""
    cache, path, _ = install_fixture(root)
    empty_provider(monkeypatch, [])
    source.refresh_share_change(root=root, symbol='000422', cache_path=cache,
                               as_of='2026-10-30', allow_network=True)
    current=json.loads(path.read_bytes())
    response=path.parent/current['response_path']
    payload=json.loads(response.read_bytes());payload[field]=value
    response.write_bytes(json.dumps(payload).encode())
    current['response_sha256']=hashlib.sha256(response.read_bytes()).hexdigest()
    path.write_bytes(json.dumps(current).encode())
    return cache,path,current


@pytest.mark.parametrize('field',['total','count'])
def test_delivery_rejects_boolean_empty_native_counts_after_original_chain_validation(tmp_path,monkeypatch,field):
    from scripts import realtime_state_bundle as state
    _protocol_empty_tail(tmp_path,monkeypatch,field,False)
    # Retain the original verifier; the regression is that False equals integer zero.
    assert source.verify_share_cache(root=tmp_path,symbol='000422',as_of='2026-10-30')['source_files']
    with pytest.raises(ValueError,match='share source JSON protocol'):
        state._iter_bundle_files(tmp_path)


def test_delivery_rejects_boolean_native_share_quantity_after_original_chain_validation(tmp_path):
    from scripts import realtime_state_bundle as state
    cache,path,current=install_fixture(tmp_path)
    response=path.parent/current['response_path'];payload=json.loads(response.read_bytes())
    payload['records'][0]['F003N']=True
    response.write_bytes(json.dumps(payload).encode())
    parsed=source.parse_source(response.read_bytes(),symbol='000422',start=current['params']['sdate'],end=current['query_end'])
    cache.write_bytes(parsed.to_csv(index=False).encode())
    current.update(response_sha256=hashlib.sha256(response.read_bytes()).hexdigest(),candidate_sha256=hashlib.sha256(cache.read_bytes()).hexdigest())
    path.write_bytes(json.dumps(current).encode())
    assert source.verify_share_cache(root=tmp_path,symbol='000422',as_of='2026-10-29')['source_files']
    with pytest.raises(ValueError,match='share source JSON protocol'):
        state._iter_bundle_files(tmp_path)


@pytest.mark.parametrize('field,value',[('resultcode',200.0),('total',0.0),('count',0.0)])
def test_delivery_requires_json_integer_counts_without_native_float_coercion(tmp_path,monkeypatch,field,value):
    from scripts import realtime_state_bundle as state
    _protocol_empty_tail(tmp_path,monkeypatch,field,value)
    assert source.verify_share_cache(root=tmp_path,symbol='000422',as_of='2026-10-30')['source_files']
    with pytest.raises(ValueError,match='share source JSON protocol'):
        state._iter_bundle_files(tmp_path)


def test_delivery_keeps_real_native_frame_and_integer_empty_query_compatible(tmp_path,monkeypatch):
    from scripts import realtime_state_bundle as state
    cache,path,current=install_fixture(tmp_path)
    assert state._iter_bundle_files(tmp_path)
    empty_provider(monkeypatch,[])
    source.refresh_share_change(root=tmp_path,symbol='000422',cache_path=cache,as_of='2026-10-30',allow_network=True)
    assert state._iter_bundle_files(tmp_path)


def test_delivery_does_not_expand_unknown_json_key_or_numeric_string_contract(tmp_path):
    from scripts import realtime_state_bundle as state
    cache,path,current=install_fixture(tmp_path)
    response=path.parent/current['response_path'];payload=json.loads(response.read_bytes())
    payload['provider_extension']={'unreviewed_server_detail':'retained, not a source of policy'}
    payload['records'][0]['F003N']=str(payload['records'][0]['F003N'])
    response.write_bytes(json.dumps(payload).encode())
    current['response_sha256']=hashlib.sha256(response.read_bytes()).hexdigest()
    path.write_bytes(json.dumps(current).encode())
    assert source.verify_share_cache(root=tmp_path,symbol='000422',as_of='2026-10-29')['source_files']
    assert state._iter_bundle_files(tmp_path)


def test_delivery_checks_old_native_response_beneath_later_valid_chain(tmp_path,monkeypatch):
    from scripts import realtime_state_bundle as state
    cache,path,current=_protocol_empty_tail(tmp_path,monkeypatch,'count',False)
    empty_provider(monkeypatch,[])
    source.refresh_share_change(root=tmp_path,symbol='000422',cache_path=cache,as_of='2026-11-02',allow_network=True)
    latest=json.loads(path.read_bytes())
    assert json.loads((path.parent/latest['response_path']).read_bytes())['count']==0
    proof=source.verify_share_cache(root=tmp_path,symbol='000422',as_of='2026-11-02')
    with pytest.raises(ValueError,match='share source JSON protocol'):
        state._verify_share_source_json_protocol(tmp_path,'000422',proof)


def test_delivery_guard_rejects_raw_changed_after_original_native_verification(tmp_path):
    from scripts import realtime_state_bundle as state
    _,path,current=install_fixture(tmp_path)
    proof=source.verify_share_cache(root=tmp_path,symbol='000422',as_of='2026-10-29')
    response=path.parent/current['response_path'];response.write_bytes(response.read_bytes()+b'\n')
    with pytest.raises(ValueError,match='original verified closure changed'):
        state._verify_share_source_json_protocol(tmp_path,'000422',proof)


def test_delivery_guard_rejects_ancestor_snapshot_change_after_original_native_verification(tmp_path,monkeypatch):
    from scripts import realtime_state_bundle as state
    _protocol_empty_tail(tmp_path,monkeypatch,'count',0)
    proof=source.verify_share_cache(root=tmp_path,symbol='000422',as_of='2026-10-30')
    snapshots=[name for name in proof['source_files'] if name.startswith('snapshots/') and name.endswith('.csv')]
    assert snapshots
    path=tmp_path/'.microcap_index_cache/share_sources'/snapshots[0]
    path.write_bytes(path.read_bytes()+b'\n')
    with pytest.raises(ValueError,match='original verified closure changed'):
        state._verify_share_source_json_protocol(tmp_path,'000422',proof)


def test_delivery_guard_requires_response_present_in_original_verified_closure(tmp_path):
    from scripts import realtime_state_bundle as state
    _,_,current=install_fixture(tmp_path)
    proof=source.verify_share_cache(root=tmp_path,symbol='000422',as_of='2026-10-29')
    proof['source_files'].pop(current['response_path'])
    with pytest.raises(ValueError,match='original verified closure changed'):
        state._verify_share_source_json_protocol(tmp_path,'000422',proof)


def test_validate_state_keeps_native_protocol_failure_visible(tmp_path,monkeypatch):
    from scripts import realtime_state_bundle as state
    from scripts.stock_return_basis import RETURN_BASIS_REVISION
    _protocol_empty_tail(tmp_path,monkeypatch,'total',False)
    outputs=tmp_path/'outputs';outputs.mkdir()
    (outputs/'wind_microcap_top_100_biweekly_thursday_16y_cached.csv').write_text('date,close\n2026-10-30,100\n')
    (outputs/'microcap_top100_mom16_biweekly_live_v2_0_base_proxy_meta.json').write_text(json.dumps({'core_params':{'stock_return_basis_revision':RETURN_BASIS_REVISION}}))
    # This partial fixture tests the original share validation branch, not a
    # successful whole state or replacement economic-validation certificate.
    monkeypatch.setattr(state,'_verify_return_basis_transport_memoized',lambda *args:None)
    monkeypatch.setattr(state,'_transport_member_symbols',lambda *args,**kwargs:{'000422'})
    result=state.validate_state(tmp_path,require_current_refresh_proof=False)
    assert result['ok'] is False
    assert any('stock return basis transport: share source JSON protocol' in error for error in result['errors'])


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


@pytest.mark.parametrize('permanent', [False, True])
def test_network_retries_preserve_query_and_bound_failure(tmp_path, monkeypatch, permanent):
    cache, path, _ = install_fixture(tmp_path)
    before = cache.read_bytes(), path.read_bytes()
    successful = []
    empty_provider(monkeypatch, successful)
    original = source.requests.post
    attempts = []
    def flaky(url, **kwargs):
        attempts.append(kwargs['params'].copy())
        if permanent or len(attempts) < 3:
            raise source.requests.ConnectionError('temporary DNS failure')
        return original(url, **kwargs)
    monkeypatch.setattr(source.requests, 'post', flaky)
    monkeypatch.setattr(source.time, 'sleep', lambda seconds: None)
    if permanent:
        with pytest.raises(source.requests.ConnectionError):
            source.refresh_share_change(root=tmp_path, symbol='000422', cache_path=cache,
                                        as_of='2026-10-30', allow_network=True)
        assert (cache.read_bytes(), path.read_bytes()) == before
    else:
        source.refresh_share_change(root=tmp_path, symbol='000422', cache_path=cache,
                                    as_of='2026-10-30', allow_network=True)
        source.verify_share_cache(root=tmp_path, symbol='000422', as_of='2026-10-30')
        assert cache.read_bytes() == before[0]
    assert len(attempts) == 3 and attempts[0] == attempts[1] == attempts[2]


def test_native_encoder_initialization_is_serialized(monkeypatch):
    import sys
    import threading
    import time
    from concurrent.futures import ThreadPoolExecutor
    from types import SimpleNamespace
    empty_provider(monkeypatch, [])
    guard = threading.Lock()
    active = 0
    peak = 0
    class Encoder:
        def __init__(self):
            nonlocal active, peak
            with guard:
                active += 1
                peak = max(peak, active)
        def eval(self, text):
            time.sleep(0.01)
        def call(self, name):
            nonlocal active
            with guard:
                active -= 1
            return 'fixture-key'
    function = sys.modules['akshare'].stock_share_change_cninfo
    function.__globals__['py_mini_racer'] = SimpleNamespace(MiniRacer=Encoder)
    with ThreadPoolExecutor(max_workers=4) as pool:
        assert list(pool.map(lambda _: source._cninfo_request_key(), range(12))) == ['fixture-key'] * 12
    assert peak == 1 and active == 0


@pytest.mark.parametrize('tls_failure', [False, True])
@pytest.mark.parametrize('empty_mode', ['none', 'first', 'all'])
def test_dns_fallback_preserves_hostname_and_tls_validation(monkeypatch, tls_failure, empty_mode):
    from types import SimpleNamespace
    monkeypatch.setattr(source, '_DNS_CACHE', {})
    def unresolved(*args, **kwargs):
        raise source.requests.ConnectionError('getaddrinfo failed')
    monkeypatch.setattr(source.requests, 'post', unresolved)
    dns = {'Status': 0, 'Question': [{'name': 'webapi.cninfo.com.cn.', 'type': 1}],
           'Answer': [{'name': 'webapi.cninfo.com.cn.', 'type': 1, 'data': '8.8.8.8', 'TTL': 6}]}
    lookups = []
    def lookup(url, **kwargs):
        lookups.append(url)
        body = dict(dns, Answer=[]) if empty_mode == 'all' or (empty_mode == 'first' and len(lookups) == 1) else dns
        return SimpleNamespace(json=lambda: body, raise_for_status=lambda: None)
    monkeypatch.setattr(source.requests, 'get', lookup)
    def curl(command, **kwargs):
        assert '--insecure' not in command and '-k' not in command
        address = source._KNOWN_CNINFO_IPV4 if empty_mode == 'all' else '8.8.8.8'
        assert command[command.index('--resolve') + 1] == 'webapi.cninfo.com.cn:443:' + address
        assert command[-1] == 'https://webapi.cninfo.com.cn/api/stock/p_stock2215?scode=000001'
        return SimpleNamespace(returncode=60 if tls_failure else 0, stdout=b'{"resultcode":200}\n200', stderr=b'certificate failure')
    monkeypatch.setattr(source.subprocess, 'run', curl)
    args = dict(params={'scode': '000001'}, headers={'Accept-Enckey': 'fixture'}, timeout=15)
    if tls_failure:
        with pytest.raises(source.requests.ConnectionError, match='certificate failure'):
            source._post_with_dns_fallback('https://webapi.cninfo.com.cn/api/stock/p_stock2215', **args)
    else:
        response = source._post_with_dns_fallback('https://webapi.cninfo.com.cn/api/stock/p_stock2215', **args)
        assert response.status_code == 200 and response.content == b'{"resultcode":200}'
        assert response._microcap_transport['https_certificate_verification'] is True
        assert response._microcap_transport['kind'] == ('previously_observed_route_not_fresh_dns' if empty_mode == 'all' else 'https_dns_response')
    assert len(lookups) == (1 if empty_mode == 'none' else 2)
