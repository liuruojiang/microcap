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

# One collection point in the existing daily core suite.
from archive_pack_cases_20261105 import (
    archive_workspace, archive_clock, archive_transport,
    test_normal_d4_close_proof_still_fails_after_d5 as test_archive_normal_d4_guard,
    test_archive_d4_accepts_only_restore_grade_integrity,
    test_archive_future_non_session_missing_date_rejected_early,
    test_archive_state_failure_is_preserved,
    test_archive_and_current_modes_are_mutually_exclusive,
    test_archive_native_pack_labels_zip_and_keeps_original_files,
    test_normal_pack_does_not_enter_archive_mode as test_archive_normal_mode_unchanged,
    test_archive_native_pack_keeps_data_and_writer_guards,
)


def prepare(workspace, monkeypatch):
    monkeypatch.setattr(state, "validate_state", lambda *a, **kw: {
        "ok": True, "errors": [], "anchor_dates": {
            "proxy_index": "2026-09-03", "costed_nav": "2026-09-03",
            "panel_shadow": "2026-09-03"}})
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


def test_same_session_metadata_damage_is_repaired_from_identical_bundle(workspace, monkeypatch):
    name = ".microcap_index_cache/security_meta/000001.json"
    path = workspace / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b'{"st_intervals": []}')
    bundle, target = prepare(workspace, monkeypatch)
    cloud.restore(target, bundle, "2026-09-03")
    (target / name).write_bytes(b"damaged")
    def validate(root, *a, **kw):
        ok = (root / name).read_bytes() == path.read_bytes()
        return {"ok": ok, "errors": [] if ok else ["metadata mismatch"]}
    monkeypatch.setattr(state, "validate_state", validate)
    result = cloud.restore(target, bundle, "2026-09-03")
    assert result["ok"]
    assert (target / name).read_bytes() == path.read_bytes()
    assert (Path(result["backup"]) / name).read_bytes() == b"damaged"


@pytest.mark.parametrize("failure", ["date", "code", "evidence", "newer", "lock"])
def test_restore_rejects_before_replacing_local_files(workspace, monkeypatch, failure):
    bundle, target = prepare(workspace, monkeypatch)
    if failure == "code":
        (target / cloud.SOURCE_FILES[1]).write_text("# different strategy")
    if failure == "evidence":
        pdf_name = next(name for name in cloud.SOURCE_FILES if name.endswith(".pdf"))
        with (target / pdf_name).open("ab") as handle:
            handle.write(b"tampered source evidence")
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


@pytest.mark.parametrize("completed_text,today_text", [
    ("2026-09-30", "2026-10-08"),  # Eight-calendar-day closure.
    ("2027-01-29", "2027-02-12"),  # Fourteen-day synthetic Spring Festival closure.
])
def test_pack_accepts_long_holiday_when_anchors_match_completed_session(
    workspace, monkeypatch, completed_text, today_text
):
    from datetime import date
    from scripts import exchange_calendar

    completed = date.fromisoformat(completed_text)
    previous = completed - date.resolution
    monkeypatch.setattr(exchange_calendar, "sessions_for_day",
                        lambda _day: (previous, completed))
    # Shift only synthetic strategy outputs, never immutable issuer evidence.
    synthetic_paths = list((workspace / "outputs").rglob("*"))
    synthetic_paths += list((workspace / state.PRICE_CACHE_DIR).glob("*.csv"))
    synthetic_paths += list((workspace / state.SHARE_CACHE_DIR).glob("*.csv"))
    for path in synthetic_paths:
        if path.is_file():
            payload = path.read_bytes()
            payload = payload.replace(b"2026-09-03", completed_text.encode("ascii"))
            payload = payload.replace(b"2026-09-02", previous.isoformat().encode("ascii"))
            path.write_bytes(payload)
    calls = []

    def validate(root, **kwargs):
        calls.append(kwargs)
        return {"ok": True, "errors": [], "anchor_dates": {
            "proxy_index": completed_text, "costed_nav": completed_text,
            "panel_shadow": completed_text}}

    monkeypatch.setattr(state, "validate_state", validate)
    monkeypatch.setattr(cloud.subprocess, "check_output", lambda *a, **kw: "a" * 40)
    today = date.fromisoformat(today_text)
    assert (today - completed).days > 5

    result = cloud.pack({v: workspace for v in delivery.COSTED},
                        workspace.parent / f"long-holiday-{completed_text}.zip", completed.isoformat())

    assert result["ok"]
    assert calls and all(call.get("max_anchor_age_days") is None for call in calls)


def test_pack_rejects_anchor_that_differs_from_completed_session(workspace, monkeypatch):
    monkeypatch.setattr(cloud.subprocess, "check_output", lambda *a, **kw: "a" * 40)
    monkeypatch.setattr(state, "validate_state", lambda *a, **kw: {
        "ok": True, "errors": [], "anchor_dates": {
            "proxy_index": "2026-09-03", "costed_nav": "2026-09-03",
            "panel_shadow": "2026-09-02"}})
    with pytest.raises(RuntimeError, match="panel_shadow is not aligned with latest completed session"):
        cloud.pack({v: workspace for v in delivery.COSTED},
                   workspace.parent / "stale-anchor.zip", "2026-09-03")


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


@pytest.mark.parametrize('distinct', [False, True])
def test_pack_validates_each_resolved_root_once_and_hashes_before_and_after(workspace, monkeypatch, distinct):
    from collections import Counter
    monkeypatch.setattr(state, 'validate_state', lambda *a, **kw: {
        'ok':True,'errors':[],'anchor_dates':{'proxy_index':'2026-09-03','costed_nav':'2026-09-03','panel_shadow':'2026-09-03'}})
    monkeypatch.setattr(cloud.subprocess,'check_output',lambda *a,**kw:'a'*40)
    roots={version:workspace for version in delivery.COSTED}
    if distinct:
        for version in ('3','5'):
            other=workspace.parent/('pack-root-'+version)
            shutil.copytree(workspace,other);roots[version]=other
    else:
        roots['3']=workspace/'outputs'/'..'
        roots['5']=workspace/'.'
    unique={root.resolve() for root in roots.values()};hash_calls=Counter();validation_calls=Counter()
    original_hashes=delivery.input_hashes;original_validate=delivery.validate_base_state_for_session
    def counted_hashes(root):
        if root.resolve() in unique:hash_calls[root.resolve()]+=1
        return original_hashes(root)
    def counted_validate(root,expected):
        if root.resolve() in unique:validation_calls[root.resolve()]+=1
        return original_validate(root,expected)
    monkeypatch.setattr(delivery,'input_hashes',counted_hashes)
    monkeypatch.setattr(delivery,'validate_base_state_for_session',counted_validate)
    assert cloud.pack(roots,workspace.parent/('unique-'+str(distinct)+'.zip'),'2026-09-03')['ok']
    # Direct streaming also inspects the three finals before and after writing.
    assert hash_calls==Counter({root:(2 if distinct else 4) for root in unique})
    assert validation_calls==Counter({root:1 for root in unique})


def test_shared_pack_streams_without_copying_or_mutating_source(workspace, monkeypatch):
    bundle, _ = prepare(workspace, monkeypatch)
    manifest = workspace / delivery.MANIFEST
    before = manifest.read_bytes() if manifest.exists() else None
    validations = []
    original = delivery.validate_base_state_for_session
    def validate(root, expected):
        validations.append(root.resolve())
        return original(root, expected)
    def forbidden_copy(*args, **kwargs):
        raise AssertionError('Shared delivery must not copy a historical workspace')
    monkeypatch.setattr(delivery, 'validate_base_state_for_session', validate)
    monkeypatch.setattr(cloud, 'copy_file', forbidden_copy)
    result = cloud.pack({v:workspace for v in delivery.COSTED}, bundle, '2026-09-03')
    assert result['shared_base'] is True
    assert validations == [workspace.resolve()]
    assert (manifest.read_bytes() if manifest.exists() else None) == before
    assert not (workspace / delivery.LOCK).exists()


def test_shared_pack_rejects_transient_final_change_and_preserves_published_bundle(workspace, monkeypatch):
    bundle, _ = prepare(workspace, monkeypatch)
    original_bundle = bundle.read_bytes()
    original_pack = state.pack_state
    final = workspace / 'outputs/microcap_top100_mom16_biweekly_live_v2_3_latest_signal.csv'
    original_final = final.read_bytes()
    def transient_change(*args, **kwargs):
        final.write_bytes(original_final + b'changed_during_pack\n')
        try:
            return original_pack(*args, **kwargs)
        finally:
            final.write_bytes(original_final)
    monkeypatch.setattr(state, 'pack_state', transient_change)
    with pytest.raises(RuntimeError, match='Packed content differs'):
        cloud.pack({v:workspace for v in delivery.COSTED}, bundle, '2026-09-03')
    assert final.read_bytes() == original_final
    assert bundle.read_bytes() == original_bundle
    assert not (workspace / delivery.LOCK).exists()


@pytest.mark.parametrize("directory", [state.PRICE_CACHE_DIR, state.SHARE_CACHE_DIR])
def test_shared_pack_rejects_transient_cache_change_after_validation(workspace, monkeypatch, directory):
    """Cached inputs outside report.inputs must share the validated ZIP identity."""
    bundle, _ = prepare(workspace, monkeypatch)
    original_bundle = bundle.read_bytes()
    cache = workspace / directory / "000001.csv"
    original_cache = cache.read_bytes()
    original_validate = state.validate_universe_cache_transport
    original_pack = state.pack_state

    def change_after_validation(root):
        errors = original_validate(root)
        assert not errors
        # Same-size replacement also requires a hash comparison, not just size.
        cache.write_bytes(original_cache.replace(b"2026-09", b"2020-09"))
        return errors

    def restore_after_archiving(*args, **kwargs):
        try:
            return original_pack(*args, **kwargs)
        finally:
            cache.write_bytes(original_cache)

    monkeypatch.setattr(state, "validate_universe_cache_transport", change_after_validation)
    monkeypatch.setattr(state, "pack_state", restore_after_archiving)
    with pytest.raises(RuntimeError, match="Packed content differs from validated delivery"):
        cloud.pack({v: workspace for v in delivery.COSTED}, bundle, "2026-09-03")
    assert cache.read_bytes() == original_cache
    assert bundle.read_bytes() == original_bundle
    assert not (workspace / delivery.LOCK).exists()


@pytest.mark.parametrize("directory", [state.PRICE_CACHE_DIR, state.SHARE_CACHE_DIR])
@pytest.mark.parametrize("symbol", ["000001", "999999"])
def test_shared_pack_rejects_cache_created_between_inventory_and_validation(workspace, monkeypatch, directory, symbol):
    """An initially missing payload cannot escape ZIP completeness checks."""
    if symbol == "999999":
        # Ranking-universe security outside all 100 current/historical positions.
        meta = workspace / ".microcap_index_cache/security_meta/999999.json"
        meta.parent.mkdir(parents=True, exist_ok=True)
        meta.write_text("{}", encoding="utf-8")
        for cache_dir in (state.PRICE_CACHE_DIR, state.SHARE_CACHE_DIR):
            target = workspace / cache_dir / f"{symbol}.csv"
            target.write_bytes((workspace / cache_dir / "000001.csv").read_bytes())
    bundle, _ = prepare(workspace, monkeypatch)
    original_bundle = bundle.read_bytes()
    cache = workspace / directory / f"{symbol}.csv"
    original_cache = cache.read_bytes()
    original_validate = state.validate_state
    original_pack = state.pack_state

    def create_before_validation(*args, **kwargs):
        cache.write_bytes(original_cache)
        return original_validate(*args, **kwargs)

    def remove_before_inventory(*args, **kwargs):
        cache.unlink()
        return original_pack(*args, **kwargs)

    monkeypatch.setattr(state, "validate_state", create_before_validation)
    monkeypatch.setattr(state, "pack_state", remove_before_inventory)
    with pytest.raises(RuntimeError, match="inventory changed or is missing from archive"):
        cloud.pack({v: workspace for v in delivery.COSTED}, bundle, "2026-09-03")
    assert cache.read_bytes() == original_cache
    assert bundle.read_bytes() == original_bundle
    assert not (workspace / delivery.LOCK).exists()


@pytest.mark.parametrize("damage", [False, True])
def test_restore_stages_calendar_for_native_st_source_closure(workspace, monkeypatch, damage):
    """Exercise the native byte-bound closure, not an unconditional validator mock."""
    import hashlib
    from scripts.st_history_scope import source_closure

    calendar = "scripts/exchange_calendar.py"
    reference = ".microcap_index_cache/realtime/st_restore_dependency.json"
    record = ".microcap_index_cache/realtime/st_restore_record.json"
    (workspace / record).parent.mkdir(parents=True, exist_ok=True)
    (workspace / record).write_text("{}", encoding="utf-8")
    def bound(name):
        return {"path": name, "sha256": hashlib.sha256((workspace / name).read_bytes()).hexdigest()}
    raw = json.dumps({"schema_version": 2, "symbols": {
        "000001": {"records": bound(record), "source_files": [bound(calendar)]}}}).encode()
    (workspace / reference).write_bytes(raw)
    expected = hashlib.sha256(raw).hexdigest()
    bundle, target = prepare(workspace, monkeypatch)
    if damage:
        (target / calendar).write_bytes(b"# changed after bundle creation")
    def validate(root, *args, **kwargs):
        if (root / reference).exists():
            source_closure(root, reference, expected)
        return {"ok": True, "errors": []}
    monkeypatch.setattr(state, "validate_state", validate)
    if damage:
        before = {p.relative_to(target): p.read_bytes() for p in target.rglob("*") if p.is_file()}
        with pytest.raises(Exception, match="ST scope source hash mismatch"):
            cloud.restore(target, bundle, "2026-09-03")
        after = {p.relative_to(target): p.read_bytes() for p in target.rglob("*") if p.is_file()}
        assert before == after
        assert not (target / ".codex_backups").exists()
    else:
        result = cloud.restore(target, bundle, "2026-09-03")
        assert result["ok"]
        assert (target / calendar).read_bytes() == (workspace / calendar).read_bytes()
        assert calendar in source_closure(target, reference, expected)
