import json
import shutil
import zipfile
from pathlib import Path
from unittest.mock import patch

import pytest
from hypothesis import given, settings, strategies as st

from scripts import top100_cloud_delivery as cloud
from scripts import top100_delivery as delivery
from scripts import realtime_state_bundle as state
from test_top100_delivery import workspace, certify  # noqa: F401


def prepare(workspace, monkeypatch):
    monkeypatch.setattr(state, "validate_state", lambda *a, **kw: {
        "ok": True, "errors": [], "anchor_dates": {
            "proxy_index": "2026-09-03", "costed_nav": "2026-09-03"}})
    monkeypatch.setattr(cloud.subprocess, "check_output", lambda *a, **kw: "a" * 40)
    bundle = workspace.parent / (workspace.name + "-delivery.zip")
    cloud.pack({v: workspace for v in delivery.COSTED}, bundle, "2026-09-03")
    target = workspace.parent / (workspace.name + "-target")
    target.mkdir()
    for name in cloud.SOURCE_FILES:
        cloud.copy_file(workspace / name, target / name)
    return bundle, target


def test_bundle_roundtrip_preserves_all_final_streams(workspace, monkeypatch):
    bundle, target = prepare(workspace, monkeypatch)
    result = cloud.restore(target, bundle, "2026-09-03")
    assert result["ok"] and result["signal_ready"] is False
    assert Path(result["backup"]).is_dir()
    require = delivery.validate_manifest(target, delivery.inspect_outputs(target, "2026-09-03"))
    assert require["ok"], require["errors"]
    for version in delivery.COSTED:
        for name in cloud.final_files(version):
            assert (target / name).read_bytes() == (workspace / name).read_bytes()


@pytest.mark.parametrize("failure", ["date", "code", "newer", "lock"])
def test_restore_rejects_before_replacing_local_files(workspace, monkeypatch, failure):
    bundle, target = prepare(workspace, monkeypatch)
    if failure == "code":
        (target / cloud.SOURCE_FILES[1]).write_text("# different strategy")
    if failure == "newer":
        cloud.copy_file(workspace / "outputs" / delivery.BASE_PANEL,
                        target / "outputs" / delivery.BASE_PANEL)
        path = target / "outputs" / delivery.BASE_PANEL
        path.write_text("date\n2026-09-04\n")
    if failure == "lock":
        (target / "outputs").mkdir(exist_ok=True)
        (target / delivery.LOCK).write_text("active")
    before = {p.relative_to(target): p.read_bytes() for p in target.rglob("*") if p.is_file()}
    with pytest.raises((RuntimeError, ValueError, FileExistsError)):
        cloud.restore(target, bundle, "2026-09-04" if failure == "date" else "2026-09-03")
    after = {p.relative_to(target): p.read_bytes() for p in target.rglob("*") if p.is_file()}
    assert before == after


def test_pack_rejects_different_inputs_across_versions(workspace, monkeypatch):
    prepare(workspace, monkeypatch)
    sibling = workspace.parent / "sibling"
    shutil.copytree(workspace, sibling)
    (sibling / cloud.SOURCE_FILES[1]).write_text("# changed")
    with pytest.raises(ValueError, match="identical"):
        cloud.pack({"0": workspace, "3": sibling, "5": workspace},
                   workspace.parent / "bad.zip", "2026-09-03")


@settings(max_examples=20, deadline=None)
@given(st.binary(min_size=1, max_size=128))
def test_manifest_rejects_any_changed_payload(payload):
    """Changing bytes without the matching manifest must always be rejected."""
    import hashlib
    import io
    original = b"accepted"
    changed = original + payload
    buffer = io.BytesIO()
    manifest = {"files": [{"path": "outputs/data.csv", "bytes": len(original),
                            "sha256": hashlib.sha256(original).hexdigest()}]}
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr(state.MANIFEST_NAME, json.dumps(manifest))
        archive.writestr("outputs/data.csv", changed)
    with zipfile.ZipFile(buffer) as archive, pytest.raises(ValueError):
        state._verify_bundle_manifest(archive)


def test_prior_day_proof_is_not_rewritten_during_transport(workspace, monkeypatch):
    bundle, target = prepare(workspace, monkeypatch)
    calls = []
    def validate(root, **kw):
        calls.append(kw.get("require_current_refresh_proof"))
        return {"ok": True}
    with patch.object(state, "validate_state", side_effect=validate):
        assert cloud.restore(target, bundle, "2026-09-03")["signal_ready"] is False
    assert calls and all(value is False for value in calls)
