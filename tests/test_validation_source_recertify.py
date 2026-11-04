"""Exact source-only adoption; diagnostic fixtures never certify production data."""
import hashlib
import json

import pytest

from scripts import top100_delivery as delivery
from scripts import realtime_state_bundle as state
from test_top100_delivery import workspace


def raw_sha(raw):
    return hashlib.sha256(raw).hexdigest()


@pytest.fixture
def adoption(tmp_path, monkeypatch):
    day = "2026-11-04"
    changed = tuple(state.VALIDATION_SOURCE_REVIEW["changes"])
    pairs = {}
    for index, name in enumerate(changed):
        old, new = f"old{index}\n".encode(), f"new{index}\n".encode()
        path = tmp_path/name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(new)
        pairs[name] = (raw_sha(old), raw_sha(new))
    shared = ("outputs/panel.csv", "outputs/proxy_meta.json", "outputs/authority.json")
    (tmp_path/"outputs").mkdir()
    for name in shared:
        (tmp_path/name).write_bytes(b"unchanged\n")
    artifact = "outputs/final_nav.csv"
    (tmp_path/artifact).write_bytes(b"date,nav\n2026-11-04,1\n")
    names = (*changed, *shared)
    def report():
        return {"ok": True, "errors": [], "scope": "whole_workspace_delivery", "expected_date": day,
                "inputs": {name: delivery.sha(tmp_path/name) for name in names},
                "artifacts": {artifact: delivery.sha(tmp_path/artifact)},
                "streams": {"final_nav.csv": {"rows": 1, "latest_date": day,
                             "sha256": delivery.sha(tmp_path/artifact)}}}
    original = report()
    original.update(status="complete", release_sha="b"*40, verified_at="2026-11-04T09:00:00+00:00")
    for name, pair in pairs.items():
        original["inputs"][name] = pair[0]
    raw = (json.dumps(original, ensure_ascii=False, indent=2) + "\n").encode()
    manifest = tmp_path/delivery.MANIFEST
    manifest.write_bytes(raw)
    review = {"revision": "diagnostic_exact_source_review", "expected_date": day,
              "manifest_sha256": raw_sha(raw), "stock_fingerprint_sha256": "c"*64, "changes": pairs}
    monkeypatch.setattr(state, "VALIDATION_SOURCE_REVIEW", review)
    monkeypatch.setattr(delivery, "RELEASE_FILES", list(names))
    monkeypatch.setattr(delivery, "verify_release", lambda root: "a"*40)
    monkeypatch.setattr(delivery, "independent_target", lambda root: day)
    counters = {"whole": 0, "base": 0}
    def inspect(root, expected):
        counters["whole"] += 1
        result = report()
        result["expected_date"] = expected
        saved = json.loads(manifest.read_bytes())
        if "source_validation_compatibility" in saved:
            result["source_validation_compatibility"] = delivery.validate_source_compatibility(root, result, saved)
        return result
    def base(root, expected):
        counters["base"] += 1
        return {"ok": True, "errors": []}
    monkeypatch.setattr(delivery, "inspect_outputs", inspect)
    monkeypatch.setattr(delivery, "validate_base_state_for_session", base)
    return {"root": tmp_path, "day": day, "original": original, "raw": raw, "manifest": manifest,
            "review": review, "report": report, "inspect": inspect, "base": base,
            "counters": counters, "changed": changed, "shared": shared, "artifact": artifact}


def test_recertify_writes_portable_receipt_preserves_data_and_revalidates_idempotently(adoption):
    case = adoption
    before = {path: path.read_bytes() for path in case["root"].rglob("*") if path.is_file()}
    result = delivery.recertify_validation_source(case["root"], case["day"])
    saved = json.loads(case["manifest"].read_bytes())
    receipt = saved["source_validation_compatibility"]
    assert result["recertification_action"] == "completed"
    assert raw_sha(receipt["original_manifest_utf8"].encode()) == raw_sha(case["raw"])
    assert json.loads(receipt["original_manifest_utf8"])["release_sha"] == "b"*40
    assert saved["release_sha"] == receipt["validation_release_sha"] == "a"*40
    assert all(path.read_bytes() == raw for path, raw in before.items() if path != case["manifest"])
    first = case["manifest"].read_bytes()
    again = delivery.recertify_validation_source(case["root"], case["day"])
    assert again["recertification_action"] == "already_complete_revalidated"
    assert case["manifest"].read_bytes() == first
    assert case["counters"] == {"whole": 4, "base": 2}
    assert not (case["root"]/delivery.LOCK).exists()


@pytest.mark.parametrize("kind", ["panel", "metadata", "authority", "artifact", "source", "rollback"])
def test_recertify_rejects_unreviewed_data_or_code_without_manifest_write(adoption, kind):
    case = adoption
    name = {"panel": case["shared"][0], "metadata": case["shared"][1], "authority": case["shared"][2],
            "artifact": case["artifact"], "source": case["changed"][0], "rollback": case["changed"][0]}[kind]
    (case["root"]/name).write_bytes(b"old0\n" if kind == "rollback" else b"unreviewed\n")
    with pytest.raises(ValueError):
        delivery.recertify_validation_source(case["root"], case["day"])
    assert case["manifest"].read_bytes() == case["raw"]
    assert not (case["root"]/delivery.LOCK).exists()


@pytest.mark.parametrize("kind", ["unknown_manifest", "wrong_requested_day", "new_completed_day", "lock", "missing_key"])
def test_recertify_rejects_identity_date_lock_and_incomplete_inputs(adoption, monkeypatch, kind):
    case = adoption
    expected = case["day"]
    if kind == "unknown_manifest":
        case["manifest"].write_bytes(case["raw"] + b" ")
    elif kind == "wrong_requested_day":
        expected = "2026-11-03"
    elif kind == "new_completed_day":
        monkeypatch.setattr(delivery, "independent_target", lambda root: "2026-11-05")
    elif kind == "lock":
        (case["root"]/delivery.LOCK).write_bytes(b"another-owner")
    else:
        def incomplete(root, expected):
            result = case["inspect"](root, expected)
            result["inputs"].pop(case["shared"][0])
            return result
        monkeypatch.setattr(delivery, "inspect_outputs", incomplete)
    before = case["manifest"].read_bytes()
    with pytest.raises((ValueError, RuntimeError)):
        delivery.recertify_validation_source(case["root"], expected)
    assert case["manifest"].read_bytes() == before
    if kind == "lock":
        assert (case["root"]/delivery.LOCK).read_bytes() == b"another-owner"


@pytest.mark.parametrize("kind", ["whole", "base", "source_during", "data_during", "raw_crlf_during", "manifest_during", "date_during", "owner_during"])
def test_recertify_failure_or_concurrent_change_does_not_replace_manifest(adoption, monkeypatch, kind):
    case = adoption
    expected_manifest = case["raw"]
    if kind == "whole":
        monkeypatch.setattr(delivery, "inspect_outputs", lambda *args: {"ok": False, "errors": ["failed"]})
    else:
        def base(root, expected):
            nonlocal expected_manifest
            if kind == "base":
                return {"ok": False, "errors": ["failed"]}
            if kind == "source_during":
                (root/case["changed"][0]).write_bytes(b"unreviewed\n")
            elif kind == "data_during":
                (root/case["shared"][0]).write_bytes(b"modified\n")
            elif kind == "raw_crlf_during":
                # Normalized input hashes alone would miss this byte mutation.
                (root/case["shared"][0]).write_bytes(b"unchanged\r\n")
            elif kind == "manifest_during":
                expected_manifest = case["raw"] + b" "
                case["manifest"].write_bytes(expected_manifest)
            elif kind == "date_during":
                monkeypatch.setattr(delivery, "independent_target", lambda root: "2026-11-05")
            elif kind == "owner_during":
                (root/delivery.LOCK).write_bytes(b"replacement-owner")
            return {"ok": True, "errors": []}
        monkeypatch.setattr(delivery, "validate_base_state_for_session", base)
    with pytest.raises(ValueError):
        delivery.recertify_validation_source(case["root"], case["day"])
    assert case["manifest"].read_bytes() == expected_manifest
    if kind == "owner_during":
        assert (case["root"]/delivery.LOCK).read_bytes() == b"replacement-owner"


@pytest.mark.parametrize("kind", ["missing_key", "old_bytes", "revision", "source_hash", "stock_hash", "naive", "future", "data_after"])
def test_receipt_tampering_or_changed_idempotent_input_is_rejected(adoption, monkeypatch, kind):
    case = adoption
    delivery.recertify_validation_source(case["root"], case["day"])
    saved = json.loads(case["manifest"].read_bytes())
    receipt = saved["source_validation_compatibility"]
    if kind == "missing_key":
        receipt.pop("original_manifest_sha256")
    elif kind == "old_bytes":
        receipt["original_manifest_utf8"] += " "
    elif kind == "revision":
        receipt["revision"] = "unknown"
    elif kind == "source_hash":
        receipt["source_changes"][case["changed"][0]][1] = "0"*64
    elif kind == "stock_hash":
        receipt["stock_fingerprint_sha256"] = "0"*64
    elif kind == "naive":
        receipt["validated_at"] = "2026-11-05T01:00:00"
    elif kind == "future":
        receipt["validated_at"] = "2099-11-05T01:00:00+00:00"
    else:
        (case["root"]/case["shared"][0]).write_bytes(b"changed after certification\n")
    case["manifest"].write_text(json.dumps(saved), encoding="utf8")
    raw = case["manifest"].read_bytes()
    with pytest.raises(ValueError):
        delivery.recertify_validation_source(case["root"], case["day"])
    assert case["manifest"].read_bytes() == raw


def test_native_inspect_carries_validated_provenance_and_rejects_changed_receipt(workspace, monkeypatch):
    root = workspace
    day = "2026-09-03"
    original = delivery.inspect_outputs(root, day)
    assert original["ok"], original["errors"]
    original.update(status="complete", release_sha="b"*40)
    raw = json.dumps(original, indent=2).encode()
    (root/delivery.MANIFEST).write_bytes(raw)
    changes = {}
    for name in state.VALIDATION_SOURCE_REVIEW["changes"]:
        before = delivery.sha(root/name)
        (root/name).write_bytes((root/name).read_bytes() + b"\n# diagnostic validator change\n")
        changes[name] = (before, delivery.sha(root/name))
    review = {"revision": "diagnostic_native_inspect", "expected_date": day,
              "manifest_sha256": raw_sha(raw), "stock_fingerprint_sha256": "c"*64, "changes": changes}
    monkeypatch.setattr(state, "VALIDATION_SOURCE_REVIEW", review)
    monkeypatch.setattr(delivery, "RELEASE_FILES", list(original["inputs"]))
    monkeypatch.setattr(delivery, "verify_release", lambda root: "a"*40)
    monkeypatch.setattr(delivery, "independent_target", lambda root: day)
    monkeypatch.setattr(delivery, "validate_base_state_for_session", lambda *args: {"ok": True, "errors": []})
    delivery.recertify_validation_source(root, day)
    saved = json.loads((root/delivery.MANIFEST).read_bytes())
    fresh = delivery.inspect_outputs(root, day)
    assert fresh["ok"], fresh["errors"]
    assert fresh["source_validation_compatibility"] == saved["source_validation_compatibility"]
    assert delivery.validate_manifest(root, fresh)["ok"]
    saved["source_validation_compatibility"]["original_manifest_utf8"] += " "
    (root/delivery.MANIFEST).write_text(json.dumps(saved), encoding="utf8")
    blocked = delivery.inspect_outputs(root, day)
    assert not blocked["ok"]
    assert any("compatibility" in error for error in blocked["errors"])


def test_original_validate_manifest_does_not_accept_source_only_difference(adoption):
    report = delivery.validate_manifest(adoption["root"], adoption["report"]())
    assert not report["ok"]
    assert any("manifest mismatch: inputs" in error for error in report["errors"])


def test_recertification_write_failure_preserves_original_and_releases_own_lock(adoption, monkeypatch):
    def unavailable(*args):
        raise PermissionError("diagnostic atomic commit failure")
    monkeypatch.setattr(delivery, "write_manifest", unavailable)
    with pytest.raises(PermissionError):
        delivery.recertify_validation_source(adoption["root"], adoption["day"])
    assert adoption["manifest"].read_bytes() == adoption["raw"]
    assert not (adoption["root"]/delivery.LOCK).exists()


@pytest.mark.parametrize("arguments", [
    ["recertify-validation-source"],
    ["check", "--expected-date", "2026-11-04"],
    ["refresh-all", "--expected-date", "2026-11-04"],
])
def test_recertification_cli_requires_explicit_separate_mode(arguments):
    with pytest.raises(SystemExit) as error:
        delivery.main(arguments)
    assert error.value.code == 2
