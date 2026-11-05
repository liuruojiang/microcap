"""Real preserved provider bodies plus independently rebound failure probes."""
import base64
import copy
import ast
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import zlib

import pytest

from scripts import share_unchanged_tail as source
from scripts import share_change_source as native
from frozen_exchange_calendar import frozen_exchange_sessions


def approved_301288_fixture():
    """Exact approved real observations; contains no successful quote fixture."""
    import io
    import zipfile
    root = Path(source.__file__).resolve().parents[1]
    registry = json.loads((root / "data/share_source_future_plan_reviews.json").read_bytes())
    raw = base64.b64decode(registry["unchanged_event_approval"]["package_base64"])
    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
        bodies = {name: archive.read(name) for name in archive.namelist()}
    now = datetime(2026, 11, 7, tzinfo=timezone.utc)
    candidate = bodies["prior_share_history.csv"]
    return root, registry, bodies, now, candidate


def test_exact_approved_301288_real_inventory_and_unchanged_history():
    root, registry, bodies, now, candidate = approved_301288_fixture()
    event = source._approved_unchanged_event(root=root, symbol="301288", previous_day="2026-11-04",
                                           end="2026-11-05", candidate=candidate, now=now)
    def bound(path, digest):
        assert source.sha(bodies[path]) == digest
        return bodies[path]
    for bounded, name in ((True, "bounded"), (False, "unfiltered")):
        ref = {"raw_path": name+".raw", "raw_sha256": source.sha(bodies[name+".raw"]),
               "receipt_path": name+".receipt.json", "receipt_sha256": source.sha(bodies[name+".receipt.json"])}
        with pytest.raises(ValueError, match="future announcement"):
            source._em_inventory([ref], bound, "301288", "2026-11-05", bounded, now)
        rows = source._em_inventory([ref], bound, "301288", "2026-11-05", bounded, now, event)
        assert len(rows) == 9 and source._matches_approved_event(rows[0], event)
    assert event["supplemental_verification_day"] == "2026-11-06"
    assert source.sha(candidate) == "abf7572f5abcb80bdaf73c83d6e6851356e2d3bdd7ed1707d68b7e8b0a83a5f4"


@pytest.mark.parametrize("field,value", [("TOTAL_SHARES",108010001), ("NOTICE_DATE","2026-11-07 00:00:00"),
    ("END_DATE","2026-11-06 00:00:00"),("LISTING_DATE","2026-11-04 00:00:00"),
    ("SECURITY_CODE","301289"),("INFO_CODE","AN000000000000000000"),("TOTAL_SHARES_CHANGE",1)])
def test_exact_event_cannot_approve_changed_row(field, value):
    root, _, _, now, candidate = approved_301288_fixture()
    event = source._approved_unchanged_event(root=root,symbol="301288",previous_day="2026-11-04",
                                           end="2026-11-05",candidate=candidate,now=now)
    changed = {**event["row"], field:value}
    assert not source._matches_approved_event(changed,event)


@pytest.mark.parametrize("failure", ["approval", "package", "candidate", "clock"])
def test_exact_event_rejects_missing_approval_or_tampering(tmp_path, failure):
    _, registry, _, now, candidate = approved_301288_fixture()
    if failure == "approval": registry["unchanged_event_approval"]["approval"]["decision"] = "pending"
    if failure == "package": registry["unchanged_event_approval"]["package_base64"] = base64.b64encode(b"bad").decode()
    if failure == "candidate": candidate += b"\n"
    if failure == "clock": now = datetime(2026,11,5,7,tzinfo=timezone.utc)
    (tmp_path/"data").mkdir()
    (tmp_path/"data/share_source_future_plan_reviews.json").write_text(json.dumps(registry),encoding="utf8")
    with pytest.raises(ValueError):
        source._approved_unchanged_event(root=tmp_path,symbol="301288",previous_day="2026-11-04",
                                        end="2026-11-05",candidate=candidate,now=now)


@pytest.mark.parametrize("identity",[("301289","2026-11-04","2026-11-05"),
    ("301288","2026-11-05","2026-11-06"),("301288","2026-11-03","2026-11-05")])
def test_exact_event_does_not_cover_other_symbols_or_sessions(identity):
    root, _, _, now, candidate = approved_301288_fixture()
    assert source._approved_unchanged_event(root=root,symbol=identity[0],previous_day=identity[1],
        end=identity[2],candidate=candidate,now=now) is None


@pytest.fixture(autouse=True)
def frozen_real_regression_sessions(frozen_exchange_sessions):
    """Preserved real calendar for offline regression, never live freshness."""
    return frozen_exchange_sessions


@pytest.fixture
def evidence():
    return load_evidence("share_unchanged_tail_002144.json")


@pytest.fixture
def future_evidence():
    return load_evidence("share_unchanged_tail_603028_reviewed_plan.json")


def load_evidence(name):
    data = json.loads((Path(__file__).parent / "fixtures" / name).read_bytes())
    manifest = copy.deepcopy(data["manifest"])
    bodies = {p: base64.b64decode(raw) for p, raw in data["artifacts_b64"].items()}
    candidate = base64.b64decode(data["candidate_b64"])
    assert source.sha(candidate) == data["candidate_sha256"]
    def bound(path, digest):
        raw = bodies[path]
        if source.sha(raw) != digest:
            raise ValueError("source hash mismatch")
        return raw
    return data, manifest, bodies, candidate, bound


def verify(evidence, root=None):
    data, manifest, _, candidate, bound = evidence
    return source.verify_manifest(manifest, symbol=data["symbol"], previous_day=data["previous"], end=data["target"],
                                  candidate=candidate, bound=bound, now=datetime(2026, 11, 3, 18, tzinfo=timezone.utc), root=root)


def entry(evidence, name):
    found = evidence[1]["files"][name]
    return found[0] if isinstance(found, list) else found


def rewrite(evidence, name, mutate, *, receipt=False):
    item = entry(evidence, name)
    bodies = evidence[2]
    key = "receipt_path" if receipt else "raw_path"
    body = json.loads(bodies[item[key]])
    mutate(body)
    bodies[item[key]] = json.dumps(body).encode()
    item["receipt_sha256" if receipt else "raw_sha256"] = source.sha(bodies[item[key]])
    if not receipt:
        rec = json.loads(bodies[item["receipt_path"]])
        rec["raw_sha256"] = item["raw_sha256"]
        rec["bytes"] = len(bodies[item["raw_path"]])
        bodies[item["receipt_path"]] = json.dumps(rec).encode()
        item["receipt_sha256"] = source.sha(bodies[item["receipt_path"]])


def test_real_three_provider_observations_preserve_exact_candidate(evidence):
    result = verify(evidence)
    assert result["total_shares_10k"] == "17676.2528"
    assert result["unchanged_observations"] is True
    assert result["f84_independent_asof_guarantee_proven"] is False
    assert result["provider_latency_guaranteed"] is False


@pytest.mark.parametrize("field", source.DATE_FIELDS)
def test_any_em_tail_date_blocks_without_selecting_effective_date(evidence, field):
    rewrite(evidence, "unfiltered", lambda d: d["result"]["data"][0].update({field: "2026-11-03 00:00:00"}))
    rewrite(evidence, "bounded", lambda d: d["result"]["data"][0].update({field: "2026-11-03 00:00:00"}))
    with pytest.raises(ValueError, match="tail event|cross"):
        verify(evidence)


@pytest.mark.parametrize("mutation", [
    lambda d: d.update(success=False, code=9201, result=None),
    lambda d: d["result"].update(count=0),
    lambda d: d["result"].update(pages=2),
    lambda d: d["result"].update(pages=True),
    lambda d: d["result"]["data"][0].update(SECUCODE="000001.SZ"),
    lambda d: d["result"]["data"][0].update(TOTAL_SHARES=1),
    lambda d: d["result"]["data"][0].update(NOTICE_DATE=None),
    lambda d: d["result"]["data"][0].update(LISTING_DATE="2026-11-04 00:00:00"),
])
def test_rebound_provider_invalid_data_is_not_hash_only_protection(evidence, mutation):
    rewrite(evidence, "unfiltered", mutation)
    with pytest.raises(ValueError):
        verify(evidence)


@pytest.mark.parametrize("field,value", [
    ("f57", "000001"), ("f84", 1), ("f84", float("nan")),
    ("f86", 0), ("f86", 1793777679), ("f86", True),
    ("f124", 1793777679), ("f124", float("nan")), ("f124", True),
])
def test_quote_clock_identity_total_conflicts(evidence, field, value):
    rewrite(evidence, "quote", lambda d: d["data"].update({field: value}))
    with pytest.raises((ValueError, OverflowError)):
        verify(evidence)


@pytest.mark.parametrize("value", [None, 0, 1793691279])
def test_missing_zero_or_valid_same_target_f124(evidence, value):
    rewrite(evidence, "quote", lambda d: d["data"].update(f124=value))
    assert verify(evidence)["unchanged_observations"]


@pytest.mark.parametrize("mutate", [
    lambda d: d.update(actual_url=d["actual_url"].replace("eastmoney.com", "invalid.example")),
    lambda d: d.update(http_start="2026-11-03T14:59:59+08:00"),
    lambda d: d.update(http_complete="2026-11-04T12:00:00+08:00"),
    lambda d: d.update(tls_verification=False),
    lambda d: d["params"].update(filter='(SECUCODE="000001.SZ")'),
])
def test_rebound_receipt_constraints(evidence, mutate):
    rewrite(evidence, "unfiltered", mutate, receipt=True)
    with pytest.raises(ValueError):
        verify(evidence)


@pytest.mark.parametrize('boolean_native_ancestor', [False, True])
def test_original_legacy_and_new_mixed_chain_native_roundtrip(evidence, tmp_path, boolean_native_ancestor):
    data, manifest, bodies, candidate, _ = evidence
    tmp_path = native._extended(tmp_path)
    directory = tmp_path / ".microcap_index_cache/share_sources"
    for name, compressed in data["old_source_files_zlib_b64"].items():
        raw = zlib.decompress(base64.b64decode(compressed))
        assert source.sha(raw) == data["provenance"]["old_source_files_sha256"][name]
        path = directory / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(raw)
    csv = tmp_path / ".microcap_index_cache/share_change/002144.csv"
    csv.parent.mkdir(parents=True)
    csv.write_bytes(candidate)
    assert native.verify_share_cache(root=tmp_path, symbol="002144", as_of="2026-11-02")
    if boolean_native_ancestor:
        old = json.loads((directory / "002144.json").read_bytes())
        path = directory / old['response_path']
        payload = json.loads(path.read_bytes())
        assert payload['count'] == 0 and payload['records'] == []
        payload['count'] = False
        path.write_bytes(json.dumps(payload).encode())
        old['response_sha256'] = source.sha(path.read_bytes())
        (directory / "002144.json").write_bytes(json.dumps(old).encode())
    old_state = (directory / "002144.json").read_bytes()
    previous = "states/" + source.sha(old_state) + ".json"
    (directory / previous).write_bytes(old_state)
    snapshot = directory / "snapshots/002144" / (source.sha(candidate) + ".csv")
    snapshot.parent.mkdir(parents=True, exist_ok=True)
    snapshot.write_bytes(candidate)
    for name, raw in bodies.items():
        p = directory / name
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(raw)
    raw = json.dumps(manifest).encode()
    (directory / "manifest.json").write_bytes(raw)
    state = dict(schema_version=2, kind=source.KIND, symbol="002144", query_start="2026-11-03", query_end="2026-11-03",
                 candidate_sha256=source.sha(candidate), evidence_manifest=dict(path="manifest.json", sha256=source.sha(raw)),
                 previous_state=dict(path=previous, sha256=source.sha(old_state)))
    (directory / "002144.json").write_text(json.dumps(state), encoding="utf8")
    proof = native.verify_share_cache(root=tmp_path, symbol="002144", as_of="2026-11-03")
    assert csv.read_bytes() == candidate
    assert len(proof["source_files"]) >= len(bodies) + 13
    assert (directory / previous).read_bytes() == old_state
    from scripts import realtime_state_bundle as delivery
    if boolean_native_ancestor:
        with pytest.raises(ValueError, match='share source JSON protocol'):
            delivery._verify_share_source_json_protocol(tmp_path, '002144', proof)
    else:
        delivery._verify_share_source_json_protocol(tmp_path, '002144', proof)
    state["query_start"] = "2026-11-04"
    (directory / "002144.json").write_text(json.dumps(state), encoding="utf8")
    with pytest.raises(ValueError, match="gap"):
        native.verify_share_cache(root=tmp_path, symbol="002144", as_of="2026-11-03")


@pytest.mark.parametrize("header", ["Set-Cookie", "Cookie", "Authorization", "X-Api-Key"])
def test_private_headers_never_form_public_source_objects(evidence, header):
    rewrite(evidence, "unfiltered", lambda d: d.setdefault("response_headers", {}).update({header: "private"}), receipt=True)
    with pytest.raises(ValueError, match="unsafe"):
        verify(evidence)


def test_denied_primary_routes_to_fallback_and_preserves_original_stop(evidence, tmp_path, monkeypatch):
    data, _, _, candidate, _ = evidence
    tmp_path = native._extended(tmp_path)
    directory = tmp_path / ".microcap_index_cache/share_sources"
    for name, compressed in data["old_source_files_zlib_b64"].items():
        path = directory / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(zlib.decompress(base64.b64decode(compressed)))
    cache = tmp_path / ".microcap_index_cache/share_change/002144.csv"
    cache.parent.mkdir(parents=True)
    cache.write_bytes(candidate)
    state = directory / "002144.json"
    before = state.read_bytes()
    stop = directory / "RATE_LIMIT_STOP.json"
    stop.write_bytes(b'{"status":403,"private":"preserved-original-control"}')
    def forbidden(*args, **kwargs):
        raise AssertionError("denied primary must not be queried")
    monkeypatch.setattr(native, "_cninfo_request_key", forbidden)
    monkeypatch.setattr(native.requests, "post", forbidden)
    calls = []
    def incomplete(**kwargs):
        calls.append(kwargs)
        raise ValueError("independent fallback qualification failed")
    monkeypatch.setattr(source, "collect_manifest", incomplete)
    with pytest.raises(ValueError, match="qualification failed"):
        native.refresh_share_change(root=tmp_path, symbol="002144", cache_path=cache,
                                    as_of="2026-11-03", allow_network=True)
    assert len(calls) == 1 and calls[0]["candidate"] == candidate
    assert stop.read_bytes() == b'{"status":403,"private":"preserved-original-control"}'
    assert cache.read_bytes() == candidate and state.read_bytes() == before


def test_real_reviewed_future_plan_preserved_without_consuming_or_rewriting(future_evidence):
    original = future_evidence[3]
    result = verify(future_evidence)
    assert result["total_shares_10k"] == "28514.52"
    assert result["future_plans_preserved_not_consumed"] == [{"date": "2026-11-04", "total_shares_10k": "28483.52"}]
    assert future_evidence[3] == original
    raw = future_evidence[2][entry(future_evidence, "sina")["raw_path"]]
    with pytest.raises(ValueError, match="future"):
        source.parse_sina_total_history(raw, symbol="603028", observed_session="2026-11-03")


@pytest.mark.parametrize("field,value", [
    ("END_DATE", "2026-11-05 00:00:00"),
    ("LISTING_DATE", "2026-11-05 00:00:00"),
    ("NOTICE_DATE", "2026-11-04 00:00:00"),
    ("INFO_CODE", "AN000000"), ("INFO_CODE", None), ("TOTAL_SHARES", 284835201),
])
def test_future_plan_rebound_date_quantity_or_identity_conflict(future_evidence, field, value):
    rewrite(future_evidence, "unfiltered", lambda d: d["result"]["data"][0].update({field: value}))
    with pytest.raises(ValueError):
        verify(future_evidence)


@pytest.mark.parametrize("field,value", [
    ("planned_date", "2026-11-05"), ("status", "executed"),
    ("before_total_shares", 285145201), ("pdf_sha256", "0" * 64),
    ("provider_notice_date", "2026-11-03"), ("document_signature_date", "2026-11-01"),
    ("unknown_permission", True),
])
def test_review_registry_mismatch_never_self_promotes(future_evidence, tmp_path, field, value):
    registry = json.loads((Path(source.__file__).resolve().parents[1] / "data/share_source_future_plan_reviews.json").read_bytes())
    registry["records"][0][field] = value
    target = tmp_path / "data/share_source_future_plan_reviews.json"
    target.parent.mkdir()
    target.write_text(json.dumps(registry), encoding="utf8")
    with pytest.raises(ValueError):
        verify(future_evidence, root=tmp_path)


def test_missing_exact_pdf_or_unknown_manifest_approval_cannot_authorize(future_evidence):
    future_evidence[1]["future_plan_documents"] = []
    future_evidence[1]["approved"] = True
    with pytest.raises(ValueError, match="missing/extra"):
        verify(future_evidence)


def test_native_fingerprint_excludes_only_exact_control_marker(tmp_path):
    root = Path(source.__file__).resolve().parents[1]
    tree = ast.parse((root / "microcap_top100_mom16_biweekly_live_v2_0.py").read_text(encoding="utf8"))
    embedded = next(n.value.value for n in tree.body if isinstance(n, ast.Assign)
                    and any(isinstance(t, ast.Name) and t.id == "BASE_SOURCE" for t in n.targets))
    function = next(n for n in ast.parse(embedded).body if isinstance(n, ast.FunctionDef)
                    and n.name == "stock_input_evidence_fingerprint")
    from scripts.stock_input_contract import SOURCE_FILES
    assert {"scripts/share_unchanged_tail.py", "scripts/sina_share_history.py", "data/share_source_future_plan_reviews.json"} <= set(SOURCE_FILES)
    for name in (*SOURCE_FILES, ".microcap_index_cache/security_master.csv", ".microcap_index_cache/share_sources/002144.json"):
        p = tmp_path / name
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(b"unchanged fixture identity")
    ns = dict(ROOT=tmp_path, hashlib=hashlib)
    module = ast.Module(body=[function], type_ignores=[])
    exec(compile(ast.fix_missing_locations(module), "actual_native_fingerprint", "exec"), ns)
    fingerprint = ns[function.name]
    before = fingerprint()
    stop = tmp_path / ".microcap_index_cache/share_sources/RATE_LIMIT_STOP.json"
    stop.write_bytes(b"private diagnostic original bytes")
    assert fingerprint() == before
    assert stop.read_bytes() == b"private diagnostic original bytes"
    (stop.parent / "UNKNOWN_CONTROL.json").write_bytes(b"must remain identity bound")
    assert fingerprint() != before
    from scripts import top100_delivery
    assert set(SOURCE_FILES).issubset(top100_delivery.RELEASE_FILES)


@pytest.mark.parametrize("previous,end", [("2026-10-30", "2026-11-03"), ("2026-11-01", "2026-11-03"),
                                           ("2026-10-30", "2026-11-01")])
def test_skipped_session_or_holiday_not_promoted(previous, end):
    with pytest.raises(ValueError, match="adjacent"):
        source.require_adjacent(previous, end)


def test_long_weekend_adjacent_sessions_not_calendar_day_age():
    source.require_adjacent("2026-10-30", "2026-11-02")


@pytest.mark.parametrize("mutation", [
    lambda d: d.update(tls_verify_configuration=False),
    lambda d: d.pop("tls_verify_configuration"),
    lambda d: d.update(final_url="https://invalid.example/other.pdf"),
    lambda d: d.update(final_url=d["final_url"] + "?token=not-allowed"),
    lambda d: d.update(final_url=None),
    lambda d: d.update(redirects_allowed=True),
    lambda d: d.update(response_headers={"Set-Cookie": "private"}),
    lambda d: d.update(response_headers={"Content-Type": "application/pdf\r\nSet-Cookie: private"}),
])
def test_reviewed_pdf_actual_tls_redirect_and_header_constraints(future_evidence, mutation):
    item = future_evidence[1]["future_plan_documents"][0]
    bodies = future_evidence[2]
    receipt = json.loads(bodies[item["receipt_path"]])
    mutation(receipt)
    raw = json.dumps(receipt).encode()
    bodies[item["receipt_path"]] = raw
    item["receipt_sha256"] = source.sha(raw)
    with pytest.raises(ValueError):
        verify(future_evidence)


def prepare_real_legacy(evidence, tmp_path):
    data, _, _, candidate, _ = evidence
    root = native._extended(tmp_path)
    directory = root / ".microcap_index_cache/share_sources"
    for name, compressed in data["old_source_files_zlib_b64"].items():
        path = directory / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(zlib.decompress(base64.b64decode(compressed)))
    csv = root / ".microcap_index_cache/share_change/002144.csv"
    csv.parent.mkdir(parents=True)
    csv.write_bytes(candidate)
    return root, csv, directory


def denied_primary(monkeypatch, status):
    calls = []
    monkeypatch.setattr(native, "_cninfo_request_key", lambda: "unit-control-flow-not-a-token")
    def post(url, **kwargs):
        calls.append(kwargs["params"])
        response = native.requests.Response()
        response.status_code = status
        response.url = url
        response._content = b"Synthetic denied response for routing regression only"
        return response
    monkeypatch.setattr(native.requests, "post", post)
    return calls


@pytest.mark.parametrize("status", [403, 429])
def test_first_denial_same_call_fallback_native_verifies_and_preserves_stop(evidence, tmp_path, monkeypatch, status):
    root, csv, directory = prepare_real_legacy(evidence, tmp_path)
    old_state = (directory / "002144.json").read_bytes()
    before = csv.read_bytes()
    calls = denied_primary(monkeypatch, status)
    collected = []
    def existing_real_evidence(**kwargs):
        collected.append(kwargs)
        for name, raw in evidence[2].items():
            p = directory / name
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_bytes(raw)
        raw = json.dumps(evidence[1]).encode()
        (directory / "manifest.json").write_bytes(raw)
        return dict(path="manifest.json", sha256=source.sha(raw))
    monkeypatch.setattr(source, "collect_manifest", existing_real_evidence)
    native.refresh_share_change(root=root, symbol="002144", cache_path=csv, as_of="2026-11-03", allow_network=True)
    assert len(calls) == len(collected) == 1
    proof = native.verify_share_cache(root=root, symbol="002144", as_of="2026-11-03")
    assert proof["candidate_sha256"] == source.sha(before) and csv.read_bytes() == before
    assert (directory / "states" / (source.sha(old_state) + ".json")).read_bytes() == old_state
    stop_bytes = (directory / "RATE_LIMIT_STOP.json").read_bytes()
    stop = json.loads(stop_bytes)
    assert stop["status"] == status
    assert source.sha((directory / stop["response_path"]).read_bytes()) == stop["response_sha256"]
    native.refresh_share_change(root=root, symbol="002144", cache_path=csv, as_of="2026-11-03", allow_network=False)
    assert len(calls) == 1 and (directory / "RATE_LIMIT_STOP.json").read_bytes() == stop_bytes


@pytest.mark.parametrize("status", [403, 429])
def test_first_denial_fallback_failure_keeps_primary_cause_and_old_bytes(evidence, tmp_path, monkeypatch, status):
    root, csv, directory = prepare_real_legacy(evidence, tmp_path)
    old = csv.read_bytes(), (directory / "002144.json").read_bytes()
    calls = denied_primary(monkeypatch, status)
    def failed(**kwargs):
        raise ValueError("qualified fallback rejected the real evidence")
    monkeypatch.setattr(source, "collect_manifest", failed)
    with pytest.raises(native.requests.HTTPError) as error:
        native.refresh_share_change(root=root, symbol="002144", cache_path=csv, as_of="2026-11-03", allow_network=True)
    assert isinstance(error.value.__cause__, ValueError) and len(calls) == 1
    assert old == (csv.read_bytes(), (directory / "002144.json").read_bytes())
    diagnostic = [json.loads(p.read_bytes()) for p in (directory / "failures/002144").glob("*.json")]
    assert len(diagnostic) == 1 and diagnostic[0]["primary_status"] == status
    assert diagnostic[0]["fallback_error_type"] == "ValueError"
    assert diagnostic[0]["stop_sha256"] == source.sha((directory / "RATE_LIMIT_STOP.json").read_bytes())


@pytest.mark.parametrize("interrupt", [KeyboardInterrupt, SystemExit])
def test_fallback_does_not_swallow_process_interruption(evidence, tmp_path, monkeypatch, interrupt):
    root, csv, directory = prepare_real_legacy(evidence, tmp_path)
    old = csv.read_bytes(), (directory / "002144.json").read_bytes()
    calls = denied_primary(monkeypatch, 403)
    def interrupted(**kwargs):
        raise interrupt("unit-interruption")
    monkeypatch.setattr(source, "collect_manifest", interrupted)
    with pytest.raises(interrupt):
        native.refresh_share_change(root=root, symbol="002144", cache_path=csv, as_of="2026-11-03", allow_network=True)
    assert len(calls) == 1 and old == (csv.read_bytes(), (directory / "002144.json").read_bytes())
    assert (directory / "RATE_LIMIT_STOP.json").exists()


@pytest.mark.parametrize("status", [403, 429])
def test_denied_source_without_previous_authority_still_fails_closed(tmp_path, monkeypatch, status):
    calls = denied_primary(monkeypatch, status)
    def forbidden(**kwargs):
        raise AssertionError("empty deployment cannot acquire a fallback baseline")
    monkeypatch.setattr(source, "collect_manifest", forbidden)
    csv = tmp_path / ".microcap_index_cache/share_change/002144.csv"
    with pytest.raises(native.requests.HTTPError):
        native.refresh_share_change(root=tmp_path, symbol="002144", cache_path=csv, as_of="2026-11-03", allow_network=True)
    assert len(calls) == 1 and not csv.exists()
    assert not (tmp_path / ".microcap_index_cache/share_sources/002144.json").exists()


def public_response(status, body):
    response = source.requests.Response()
    response.status_code = status
    response.url = source.QUOTE_URL + "?secid=0.003042"
    response._content = body
    response._content_consumed = True
    response.headers = {"Content-Type": "application/json", "Set-Cookie": "private"}
    return response


def save_public_probe(tmp_path):
    return source._save_public_observation(directory=tmp_path, source=tmp_path,
        url=source.QUOTE_URL, params={"secid": "0.003042"}, purpose="quote", end="2026-11-03")


@pytest.mark.parametrize("status", [500, 502, 503, 504])
def test_public_share_transient_http_retries_only_failed_request_and_keeps_raw(tmp_path, monkeypatch, status):
    replies = [public_response(status, b"unit transient response"), public_response(200, b"unit successful response")]
    calls = []
    def get(url, **kwargs):
        calls.append((url, kwargs))
        return replies.pop(0)
    monkeypatch.setattr(source.requests, "get", get)
    monkeypatch.setattr(source.time, "sleep", lambda _: None)
    entry, raw = save_public_probe(tmp_path)
    assert len(calls) == 2 and calls[0] == calls[1]
    assert calls[0][1]["verify"] is True and calls[0][1]["timeout"] == 20
    assert raw == b"unit successful response" and (tmp_path / entry["raw_path"]).read_bytes() == raw
    failed = json.loads((tmp_path / "quote_attempt1.receipt.json").read_bytes())
    assert failed["http_status"] == status
    assert source.sha((tmp_path / "quote_attempt1.raw").read_bytes()) == failed["raw_sha256"]
    assert "Set-Cookie" not in failed["response_headers"]
    assert json.loads((tmp_path / entry["receipt_path"]).read_bytes())["http_status"] == 200


@pytest.mark.parametrize("status", [400, 401, 403, 404, 429, 501])
def test_public_share_denial_or_permanent_http_is_not_retried(tmp_path, monkeypatch, status):
    calls = []
    def get(*args, **kwargs):
        calls.append(1)
        return public_response(status, b"unit non-retryable response")
    monkeypatch.setattr(source.requests, "get", get)
    with pytest.raises(source.requests.HTTPError):
        save_public_probe(tmp_path)
    assert len(calls) == 1 and (tmp_path / "quote_attempt1.raw").is_file()
    assert not (tmp_path / "quote.raw").exists()


def test_public_share_exhausted_502_is_terminal_and_keeps_all_bodies(tmp_path, monkeypatch):
    calls = []
    def get(*args, **kwargs):
        calls.append(1)
        return public_response(502, f"unit failure {len(calls)}".encode())
    monkeypatch.setattr(source.requests, "get", get)
    monkeypatch.setattr(source.time, "sleep", lambda _: None)
    with pytest.raises(RuntimeError, match="SHARE_REQUEST_RETRY_EXHAUSTED") as caught:
        save_public_probe(tmp_path)
    assert isinstance(caught.value.__cause__, source.requests.HTTPError)
    assert len(calls) == 4 and len(list(tmp_path.glob('quote_attempt*.raw'))) == 4
    assert (tmp_path / "quote_attempt1.raw").read_bytes() != (tmp_path / "quote_attempt2.raw").read_bytes()
    assert not (tmp_path / "quote.raw").exists()


@pytest.mark.parametrize("error", [source.requests.Timeout, source.requests.ConnectionError])
def test_public_share_connection_failure_has_bounded_request_budget_and_diagnostic(tmp_path, monkeypatch, error):
    calls = []
    def get(*args, **kwargs):
        calls.append(1)
        raise error("unit connection failure")
    monkeypatch.setattr(source.requests, "get", get)
    monkeypatch.setattr(source.time, "sleep", lambda _: None)
    with pytest.raises(RuntimeError, match="SHARE_REQUEST_RETRY_EXHAUSTED") as caught:
        save_public_probe(tmp_path)
    assert isinstance(caught.value.__cause__, error)
    assert len(calls) == 4 and len(list(tmp_path.glob("*.transport.json"))) == 4


def test_public_share_recovers_third_request_without_repeating_successful_request(tmp_path, monkeypatch):
    calls = []
    def get(url, **kwargs):
        calls.append(url)
        if url.endswith('/failed') and calls.count(url) <= 2:
            raise source.requests.ConnectTimeout('actual incident control: two connection timeouts')
        return public_response(200, b'unit successful response')
    monkeypatch.setattr(source.requests, 'get', get)
    monkeypatch.setattr(source.time, 'sleep', lambda _: None)
    def save(name):
        return source._save_public_observation(directory=tmp_path, source=tmp_path,
            url='https://example.invalid/' + name, params={}, purpose=name, end='2026-11-03')
    first, _ = save('completed')
    original = (tmp_path / first['receipt_path']).read_bytes()
    save('failed')
    assert calls == ['https://example.invalid/completed'] + ['https://example.invalid/failed'] * 3
    assert (tmp_path / first['receipt_path']).read_bytes() == original
    assert len(list(tmp_path.glob('failed_attempt*.transport.json'))) == 2


def test_public_share_certificate_failure_is_not_retried(tmp_path, monkeypatch):
    calls = []
    def get(*args, **kwargs):
        calls.append(1)
        raise source.requests.exceptions.SSLError("unit certificate failure")
    monkeypatch.setattr(source.requests, "get", get)
    with pytest.raises(source.requests.exceptions.SSLError):
        save_public_probe(tmp_path)
    assert len(calls) == 1


def test_public_share_successful_http_with_invalid_data_is_not_retried_or_published(tmp_path, monkeypatch):
    calls = []
    def get(*args, **kwargs):
        calls.append(1)
        return public_response(200, b"unit invalid provider JSON")
    monkeypatch.setattr(source.requests, "get", get)
    with pytest.raises(json.JSONDecodeError):
        source.collect_manifest(root=tmp_path, symbol="003042", previous_day="2026-11-02",
                                end="2026-11-03", candidate=b"unit unchanged candidate")
    assert len(calls) == 1
    assert not list(tmp_path.rglob("manifest.json"))
    assert not (tmp_path / ".microcap_index_cache/share_sources/003042.json").exists()
