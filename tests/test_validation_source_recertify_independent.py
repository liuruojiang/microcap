"""Independent source-adoption transition probes; synthetic, never production proof."""
import hashlib
import json

import pytest

from scripts import top100_delivery as delivery
from scripts import realtime_state_bundle as state
from test_validation_source_recertify import adoption
from test_top100_delivery import workspace


@pytest.mark.parametrize("relative", [
    ".microcap_index_cache/prices_raw/000001.csv",
    ".microcap_index_cache/shares/000001.csv",
    ".microcap_index_cache/security_meta/000001.json",
])
def test_authenticated_cache_change_after_base_validation_cannot_be_recertified(adoption, monkeypatch, relative):
    """Inject a leaf mutation after its successful validation, before commit.

    The synthetic whole report binds the proxy/authority/artifacts, as production
    does, but leaf hashes live in the base validator's evidence. This is a
    transition-unit test, not a claim that synthetic files pass production gates.
    """
    case = adoption
    path = case["root"] / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    initial = b'{"value":1}\n' if path.suffix == ".json" else b"date,value\n2026-11-04,1\n"
    path.write_bytes(initial)
    validated = False

    def base(root, expected):
        nonlocal validated
        assert path.read_bytes() == initial
        validated = True
        return {"ok": True, "errors": [], "files": [{"path": relative,
                "bytes": len(initial), "sha256": hashlib.sha256(initial).hexdigest()}],
                "price_cache_files": []}

    def inspect(root, expected, **_options):
        if validated:
            changed = initial.replace(b":1}", b":2}") if path.suffix == ".json" else initial.replace(b",1\n", b",2\n")
            path.write_bytes(changed)
        return case["inspect"](root, expected)

    monkeypatch.setattr(delivery, "validate_base_state_for_session", base)
    monkeypatch.setattr(delivery, "inspect_outputs", inspect)
    with pytest.raises(ValueError):
        delivery.recertify_validation_source(case["root"], case["day"])
    assert case["manifest"].read_bytes() == case["raw"]


def test_native_next_session_refresh_expires_source_only_receipt(adoption, monkeypatch):
    """Keep native manifest/receipt lifecycle; substitute only expensive work."""
    case = adoption
    delivery.recertify_validation_source(case["root"], case["day"])
    assert "source_validation_compatibility" in json.loads(case["manifest"].read_bytes())
    next_day = "2026-11-05"
    calls = []

    def refresh(root, **kwargs):
        saved = json.loads(case["manifest"].read_bytes())
        assert saved["status"] == "refreshing"
        assert "source_validation_compatibility" not in saved
        calls.append("refresh")
        return {"ok": True, "errors": []}

    def inspect(root, expected, **_options):
        result = case["report"]()
        result["expected_date"] = expected
        for stream in result["streams"].values():
            stream["latest_date"] = expected
        saved = json.loads(case["manifest"].read_bytes())
        if "source_validation_compatibility" in saved:
            delivery.validate_source_compatibility(root, result, saved)
        return result

    monkeypatch.setattr(state, "refresh_state", refresh)
    monkeypatch.setattr(delivery, "independent_target", lambda root: next_day)
    monkeypatch.setattr(delivery, "input_hashes", lambda root: case["report"]()["inputs"])
    monkeypatch.setattr(delivery, "inspect_outputs", inspect)
    monkeypatch.setattr(delivery.subprocess, "run", lambda *args, **kwargs: calls.append("variant"))
    result = delivery.refresh_all(case["root"])
    assert result["ok"] and result["expected_date"] == next_day
    assert calls == ["refresh", "variant", "variant", "variant"]
    saved = json.loads(case["manifest"].read_bytes())
    assert saved["status"] == "complete"
    assert "source_validation_compatibility" not in saved
    assert not (case["root"] / delivery.LOCK).exists()


def test_native_commit_identity_covers_nonmember_raw_even_when_whole_inputs_do_not(workspace, monkeypatch):
    """The repaired commit guard sees the leaf absent from whole-report inputs."""
    root = workspace
    day = "2026-09-03"
    raw = root / state.PRICE_CACHE_DIR / "999998.csv"
    raw.write_bytes(b"date,close_raw\n2026-09-03,1\n")
    # The native forward reader parses this leaf even in a metadata-free
    # software case. Use a valid empty object instead of the old literal seed.
    (root / "outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_meta.json").write_bytes(b"{}")
    first = delivery.inspect_outputs(root, day)
    assert first["ok"], first["errors"]
    # Only RELEASE_FILES fixture completeness is substituted; inputs/artifacts
    # and both native fingerprint implementations remain real.
    monkeypatch.setattr(delivery, "RELEASE_FILES", list(first["inputs"]))
    before = delivery._recertification_raw_identity(root, first)
    fingerprint, unlocked = state._return_basis_input_fingerprint(root, day, ("000001",))
    assert unlocked and fingerprint is not None
    raw.write_bytes(b"date,close_raw\n2026-09-03,2\n")
    after = delivery.inspect_outputs(root, day)
    assert after["ok"], after["errors"]
    assert after["inputs"] == first["inputs"]
    assert delivery._recertification_raw_identity(root, after) != before
    changed, still_unlocked = state._return_basis_input_fingerprint(root, day, ("000001",))
    assert still_unlocked and changed != fingerprint


def test_readback_failure_after_atomic_commit_rolls_back_exact_original(adoption, monkeypatch):
    """A single failed readback must restore the byte-identical previous state."""
    case = adoption
    writer = delivery.write_manifest
    path_type = type(case["manifest"])
    original_read = path_type.read_bytes
    committed = False
    failed = False

    def write(root, report):
        nonlocal committed
        writer(root, report)
        committed = True

    def read(path):
        nonlocal failed
        if committed and not failed and path == case["manifest"]:
            failed = True
            raise OSError("diagnostic readback I/O interruption after atomic replacement")
        return original_read(path)

    monkeypatch.setattr(delivery, "write_manifest", write)
    monkeypatch.setattr(path_type, "read_bytes", read)
    with pytest.raises(OSError, match="readback I/O interruption"):
        delivery.recertify_validation_source(case["root"], case["day"])
    assert original_read(case["manifest"]) == case["raw"]
    backups = list((case["root"] / "outputs").glob("source_recertification_*.original"))
    assert len(backups) == 1 and original_read(backups[0]) == case["raw"]
    failures = list((case["root"] / "outputs").glob("*.failure.json"))
    assert len(failures) == 1 and json.loads(original_read(failures[0]))["status"] == "rolled_back"
    assert not (case["root"] / delivery.LOCK).exists()


@pytest.mark.parametrize("mutation", ["add", "delete"])
def test_cache_inventory_change_during_validation_is_rejected(adoption, monkeypatch, mutation):
    case = adoption
    path = case["root"] / ".microcap_index_cache" / "shares" / "999998.csv"
    path.parent.mkdir(parents=True)
    if mutation == "delete":
        path.write_bytes(b"date,value\n2026-11-04,1\n")
    def base(root, expected):
        if mutation == "delete":
            path.unlink()
        else:
            path.write_bytes(b"date,value\n2026-11-04,1\n")
        return {"ok": True, "errors": []}
    monkeypatch.setattr(delivery, "validate_base_state_for_session", base)
    with pytest.raises(ValueError, match="changed before commit"):
        delivery.recertify_validation_source(case["root"], case["day"])
    assert case["manifest"].read_bytes() == case["raw"]


@pytest.mark.parametrize("fault", ["persistent_read", "rollback_replace", "owner_changed", "manifest_changed"])
def test_unconfirmed_commit_retains_backup_and_blocks_pack_without_overwrite(adoption, monkeypatch, fault):
    case = adoption
    writer = delivery.write_manifest
    path_type = type(case["manifest"])
    original_read, original_replace = path_type.read_bytes, path_type.replace
    committed = False
    replacement = b"another-writer"
    def write(root, report):
        nonlocal committed
        writer(root, report)
        committed = True
        if fault == "owner_changed":
            (root / delivery.LOCK).write_bytes(replacement)
        elif fault == "manifest_changed":
            case["manifest"].write_bytes(replacement)
        raise OSError("diagnostic interrupted commit completion")
    def read(path):
        if committed and fault == "persistent_read" and path == case["manifest"]:
            raise OSError("diagnostic persistent read failure")
        return original_read(path)
    def replace(path, target):
        if fault == "rollback_replace" and path.name.endswith(".rollback.tmp"):
            raise OSError("diagnostic rollback replacement failure")
        return original_replace(path, target)
    monkeypatch.setattr(delivery, "write_manifest", write)
    monkeypatch.setattr(path_type, "read_bytes", read)
    monkeypatch.setattr(path_type, "replace", replace)
    with pytest.raises(RuntimeError, match="COMMIT_STATE_UNKNOWN"):
        delivery.recertify_validation_source(case["root"], case["day"])
    assert (case["root"] / delivery.LOCK).exists()
    if fault == "owner_changed":
        assert original_read(case["root"] / delivery.LOCK) == replacement
    if fault == "manifest_changed":
        assert original_read(case["manifest"]) == replacement
    backups = list((case["root"] / "outputs").glob("source_recertification_*.original"))
    assert len(backups) == 1 and original_read(backups[0]) == case["raw"]
    failure = next((case["root"] / "outputs").glob("*.failure.json"))
    assert json.loads(original_read(failure))["status"] == "unknown"
    # The existing shared gate rejects retained/replacement review locks.
    assert not delivery.validate_manifest(case["root"], {**case["report"](), "errors": []})["ok"]
