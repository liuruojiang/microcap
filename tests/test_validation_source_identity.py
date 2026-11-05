"""Independent tests of the exact D4 validation-source compatibility boundary."""
import copy
import gzip
import hashlib
import json
from pathlib import Path

import pytest

from scripts import realtime_state_bundle as state
from scripts.stock_input_contract import SOURCE_FILES, source_fingerprint

ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / "tests/fixtures/validation_source_identity_20261104.json"
REVIEWED_V20 = ROOT / "tests/fixtures/validation_source_identity/v20_reviewed_b657.py.gz"


def reviewed_v20_source():
    """Exact published Git bytes, never today's changed producer implementation."""
    raw = gzip.decompress(REVIEWED_V20.read_bytes())
    expected = state.VALIDATION_SOURCE_REVIEW["changes"]["microcap_top100_mom16_biweekly_live_v2_0.py"][1]
    assert hashlib.sha256(raw).hexdigest() == expected
    return raw


@pytest.fixture
def identities():
    evidence = json.loads(FIXTURE.read_text(encoding="utf8"))
    old = evidence["stock_input_evidence_fingerprint"]
    current = copy.deepcopy(old)
    current["code"] = source_fingerprint(ROOT)
    return old, current


def test_real_published_identity_and_actual_current_source_match_exact_review(identities, reviewed_root):
    old, current = identities
    assert len(SOURCE_FILES) == len(old["code"]) == len(current["code"]) == 18
    assert set(old["code"]) == set(current["code"]) == set(SOURCE_FILES)
    serialized = json.dumps(old, sort_keys=True, separators=(",", ":")).encode()
    assert hashlib.sha256(serialized).hexdigest() == state.VALIDATION_SOURCE_REVIEW["stock_fingerprint_sha256"]
    changed = {name for name in old["code"] if old["code"][name] != current["code"][name]}
    assert changed == {"scripts/st_history_scope.py", "scripts/st_notice_incremental.py", "scripts/st_notice_refresh.py"}
    for name in changed:
        assert (old["code"][name], current["code"][name]) == tuple(state.VALIDATION_SOURCE_REVIEW["changes"][name])
    assert state.stock_input_evidence_matches_validated_source(old, current)
    assert state.validation_source_review(reviewed_root) == state.VALIDATION_SOURCE_REVIEW


@pytest.mark.parametrize("side", ["recorded", "current"])
@pytest.mark.parametrize("change", ["data", "count", "code-key-missing", "code-key-extra", "outer-key-missing", "outer-key-extra", "unknown-code"])
def test_unreviewed_identity_change_rejected(identities, side, change):
    old, current = identities
    target = old if side == "recorded" else current
    if change == "data": target["sha256"] = "0" * 64
    elif change == "count": target["file_count"] += 1
    elif change == "code-key-missing": target["code"].pop(SOURCE_FILES[0])
    elif change == "code-key-extra": target["code"]["unreviewed.py"] = "0" * 64
    elif change == "outer-key-missing": target.pop("sha256")
    elif change == "outer-key-extra": target["other"] = True
    else: target["code"]["scripts/share_change_source.py"] = "0" * 64
    assert not state.stock_input_evidence_matches_validated_source(old, current)


@pytest.mark.parametrize("name", ["scripts/st_history_scope.py", "scripts/st_notice_incremental.py", "scripts/st_notice_refresh.py"])
def test_partial_upgrade_or_unknown_target_cannot_use_full_transition(identities, name):
    old, current = identities
    current["code"][name] = old["code"][name]
    assert not state.stock_input_evidence_matches_validated_source(old, current)
    current["code"][name] = "0" * 64
    assert not state.stock_input_evidence_matches_validated_source(old, current)


def test_reverse_transitive_and_same_data_changed_at_both_ends_rejected(identities):
    old, current = identities
    assert not state.stock_input_evidence_matches_validated_source(current, old)
    intermediate = copy.deepcopy(current)
    intermediate["code"]["scripts/st_history_scope.py"] = "0" * 64
    assert not state.stock_input_evidence_matches_validated_source(current, intermediate)
    assert not state.stock_input_evidence_matches_validated_source(old, intermediate)
    old["sha256"] = current["sha256"] = "1" * 64
    assert not state.stock_input_evidence_matches_validated_source(old, current)


def test_equal_input_original_semantics_and_no_mutation(identities):
    old, current = identities
    before = copy.deepcopy((old, current, state.VALIDATION_SOURCE_REVIEW))
    assert state.stock_input_evidence_matches_validated_source(old, copy.deepcopy(old))
    assert state.stock_input_evidence_matches_validated_source(current, copy.deepcopy(current))
    assert state.stock_input_evidence_matches_validated_source(old, current)
    assert (old, current, state.VALIDATION_SOURCE_REVIEW) == before


@pytest.mark.parametrize("kind", ["none", "empty", "missing-code", "missing-code-key", "extra-code-key", "bool-count", "float-count", "zero-count", "negative-count", "invalid-data-sha", "invalid-code-sha", "nonstring-code-sha"])
def test_equal_but_malformed_identities_are_not_evidence(identities, kind):
    value = copy.deepcopy(identities[0])
    if kind == "none": value = None
    elif kind == "empty": value = {}
    elif kind == "missing-code": value.pop("code")
    elif kind == "missing-code-key": value["code"].pop(SOURCE_FILES[0])
    elif kind == "extra-code-key": value["code"]["unknown.py"] = "0" * 64
    elif kind == "bool-count": value["file_count"] = True
    elif kind == "float-count": value["file_count"] = float(value["file_count"])
    elif kind == "zero-count": value["file_count"] = 0
    elif kind == "negative-count": value["file_count"] = -1
    elif kind == "invalid-data-sha": value["sha256"] = "x" * 64
    elif kind == "invalid-code-sha": value["code"][SOURCE_FILES[0]] = "0" * 63
    else: value["code"][SOURCE_FILES[0]] = None
    assert not state.stock_input_evidence_matches_validated_source(value, copy.deepcopy(value))


def test_review_result_is_deep_copy_and_not_authorization_mutable_by_caller(reviewed_root):
    review = state.validation_source_review(reviewed_root)
    review["changes"]["scripts/st_history_scope.py"] = ("0" * 64, "0" * 64)
    review["stock_fingerprint_sha256"] = "0" * 64
    assert state.validation_source_review(reviewed_root) == state.VALIDATION_SOURCE_REVIEW
    assert state.validation_source_review(reviewed_root) != review


@pytest.fixture
def reviewed_root(tmp_path):
    for name in state.VALIDATION_SOURCE_REVIEW["changes"]:
        target = tmp_path / name
        target.parent.mkdir(parents=True, exist_ok=True)
        raw = reviewed_v20_source() if name == "microcap_top100_mom16_biweekly_live_v2_0.py" else (ROOT / name).read_bytes()
        assert hashlib.sha256(raw.replace(b"\r\n", b"\n")).hexdigest() == state.VALIDATION_SOURCE_REVIEW["changes"][name][1]
        target.write_bytes(raw)
    return tmp_path


@pytest.mark.parametrize("name", list(state.VALIDATION_SOURCE_REVIEW["changes"]))
@pytest.mark.parametrize("kind", ["missing", "tampered"])
def test_actual_root_source_readback_rejects_every_missing_or_changed_file(reviewed_root, name, kind):
    path = reviewed_root / name
    if kind == "missing": path.unlink()
    else: path.write_bytes(path.read_bytes() + b"\n# unreviewed edit\n")
    with pytest.raises((FileNotFoundError, ValueError)):
        state.validation_source_review(reviewed_root)


def test_actual_root_source_readback_preserves_existing_lf_contract(reviewed_root):
    for name in state.VALIDATION_SOURCE_REVIEW["changes"]:
        path = reviewed_root / name
        path.write_bytes(path.read_bytes().replace(b"\r\n", b"\n").replace(b"\n", b"\r\n"))
    assert state.validation_source_review(reviewed_root) == state.VALIDATION_SOURCE_REVIEW


def test_proxy_consumer_retains_economic_metadata_and_shared_exact_rule(identities, monkeypatch):
    import microcap_top100_mom16_biweekly_live_v2_0 as v20
    base = v20.base_mod
    old, current = identities
    economic = {"present_count": 1, "missing_count": 0, "sha256": "e" * 64}
    core = {
        "research_stack_version": next(iter(base.COMPATIBLE_PROXY_RESEARCH_STACK_VERSIONS)),
        "stock_return_basis_revision": "sina_ratio_ex_reference_v1",
        "execution_timing": base.EXECUTION_TIMING,
        "trade_constraint_mode": base.TRADE_CONSTRAINT_MODE,
        "exclude_current_st": False, "exclude_historical_st": True,
        "rebalance_phase_anchor_date": base.REBALANCE_ANCHOR_DATE,
        "member_filter_policy_version": base.MEMBER_FILTER_POLICY_VERSION,
        "realtime_quote_policy_version": base.REALTIME_QUOTE_POLICY_VERSION,
        "proxy_rebalance_policy_version": base.PROXY_REBALANCE_POLICY_VERSION,
        "delisting_valuation_policy": base.freq_mod.DELISTING_VALUATION_POLICY,
        "st_notice_policy_version": base.freq_mod.ST_NOTICE_POLICY_VERSION,
        "security_meta_cache_fingerprint": copy.deepcopy(economic),
        "stock_input_evidence_fingerprint": old,
    }
    namespace = base.proxy_meta_matches_execution_model.__globals__
    monkeypatch.setitem(namespace, "security_meta_cache_fingerprint", lambda: economic)
    monkeypatch.setitem(namespace, "stock_input_evidence_fingerprint", lambda: current)
    assert base.proxy_meta_matches_execution_model({"core_params": core})
    core["security_meta_cache_fingerprint"]["sha256"] = "0" * 64
    assert not base.proxy_meta_matches_execution_model({"core_params": core})
    core["security_meta_cache_fingerprint"] = copy.deepcopy(economic)
    core["stock_input_evidence_fingerprint"] = copy.deepcopy(old)
    core["stock_input_evidence_fingerprint"]["sha256"] = "0" * 64
    assert not base.proxy_meta_matches_execution_model({"core_params": core})


def test_state_validator_reads_real_data_and_calls_same_identity_predicate(tmp_path, monkeypatch):
    # A tiny structural fixture exercises the caller, not D4 production acceptance.
    for name in SOURCE_FILES:
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes((ROOT / name).read_bytes())
    cache = tmp_path / ".microcap_index_cache"
    cache.mkdir()
    master = cache / "security_master.csv"
    master.write_bytes(b"symbol\n000001\n")
    stock = {"file_count": 1, "sha256": hashlib.sha256(b".microcap_index_cache/security_master.csv\0" + master.read_bytes() + b"\0").hexdigest(), "code": source_fingerprint(tmp_path)}
    economic = {"present_count": 0, "missing_count": 0, "sha256": hashlib.sha256(b"").hexdigest()}
    meta_path = tmp_path / "outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_meta.json"
    meta_path.parent.mkdir()
    meta = {"core_params": {"security_meta_cache_fingerprint": economic,
            "stock_return_basis_revision": "sina_ratio_ex_reference_v1", "stock_input_evidence_fingerprint": stock}}
    meta_path.write_text(json.dumps(meta), encoding="utf8")
    real = state.stock_input_evidence_matches_validated_source
    calls = []
    def observed(recorded, actual):
        calls.append((copy.deepcopy(recorded), copy.deepcopy(actual)))
        return real(recorded, actual)
    monkeypatch.setattr(state, "stock_input_evidence_matches_validated_source", observed)
    assert state.validate_security_metadata(tmp_path) == []
    assert calls == [(stock, stock)]
    master.write_bytes(master.read_bytes() + b"000002\n")
    with pytest.raises(ValueError, match="stock return/trading-state"):
        state.validate_security_metadata(tmp_path)
    assert len(calls) == 2 and calls[-1][0] != calls[-1][1]


def test_current_runtime_does_not_inherit_historical_source_review_authority():
    current = (ROOT / "microcap_top100_mom16_biweekly_live_v2_0.py").read_bytes()
    assert hashlib.sha256(current.replace(b"\r\n", b"\n")).hexdigest() != hashlib.sha256(reviewed_v20_source()).hexdigest()
    with pytest.raises(ValueError, match="Unreviewed validation source: microcap_top100_mom16_biweekly_live_v2_0.py"):
        state.validation_source_review(ROOT)
