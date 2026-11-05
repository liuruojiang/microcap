"""Explicit dated archives never become current signals or relax normal pack."""
from datetime import date
import json
from pathlib import Path
import zipfile

import pytest

from scripts import realtime_state_bundle as state
from scripts import top100_cloud_delivery as cloud
from scripts import top100_delivery as delivery
from scripts import exchange_calendar
from test_top100_delivery import workspace as archive_workspace


@pytest.fixture
def archive_clock(monkeypatch):
    monkeypatch.setattr(exchange_calendar, "latest_completed_session", lambda **kwargs: date(2026, 11, 5))
    monkeypatch.setattr(exchange_calendar, "sessions_for_day", lambda day: (date(2026, 11, 4), date(2026, 11, 5)))


def test_normal_d4_close_proof_still_fails_after_d5(archive_clock):
    with pytest.raises(ValueError, match="misses latest"):
        state._close_confirmed_proof_day({}, date(2026, 11, 4), date(2026, 11, 5))


def test_archive_d4_accepts_only_restore_grade_integrity(archive_clock, tmp_path, monkeypatch):
    calls = []
    def validate(root, **kwargs):
        calls.append(kwargs)
        return {"ok": True, "errors": [], "anchor_dates": {
            "proxy_index": "2026-11-04", "costed_nav": "2026-11-04",
            "panel_shadow": "2026-11-04", "proxy_turnover": "2026-10-29"}}
    monkeypatch.setattr(state, "validate_state", validate)
    result = state.validate_archive_state(tmp_path, date(2026, 11, 4))
    assert result["ok"] and result["signal_ready"] is False and result["current_signal_ready"] is False
    assert result["freshness"] == "verified_archive_only_requires_current_refresh"
    assert calls == [{"max_anchor_age_days": None, "require_current_refresh_proof": False}]


@pytest.mark.parametrize("target", [date(2026, 11, 6), date(2026, 11, 3), None, "2026-11-04"])
def test_archive_future_non_session_missing_date_rejected_early(archive_clock, tmp_path, monkeypatch, target):
    monkeypatch.setattr(state, "validate_state", lambda *a, **kw: pytest.fail("invalid target reached state scan"))
    with pytest.raises(ValueError):
        state.validate_archive_state(tmp_path, target)


@pytest.mark.parametrize("fault", ["anchor", "no_anchors", "integrity"])
def test_archive_state_failure_is_preserved(archive_clock, tmp_path, monkeypatch, fault):
    report = {"ok": True, "errors": [], "anchor_dates": dict.fromkeys(
        ("proxy_index", "costed_nav", "panel_shadow"), "2026-11-04")}
    if fault == "anchor": report["anchor_dates"]["panel_shadow"] = "2026-11-03"
    elif fault == "no_anchors": del report["anchor_dates"]
    else: report.update(ok=False, errors=["raw/share integrity failed"])
    monkeypatch.setattr(state, "validate_state", lambda *a, **kw: report)
    assert not state.validate_archive_state(tmp_path, date(2026, 11, 4))["ok"]


def test_archive_and_current_modes_are_mutually_exclusive(tmp_path):
    with pytest.raises(ValueError, match="mutually exclusive"):
        state.pack_state(tmp_path, tmp_path / "never.zip", None,
                         archive_session=date(2026, 11, 4), close_confirmed_session=date(2026, 11, 4))
    assert not (tmp_path / "never.zip").exists()


@pytest.fixture
def archive_transport(archive_workspace, monkeypatch):
    root = archive_workspace
    monkeypatch.setattr(exchange_calendar, "latest_completed_session", lambda **kwargs: date(2026, 9, 4))
    monkeypatch.setattr(cloud.subprocess, "check_output", lambda *a, **kw: "a" * 40)
    calls = []
    def validated(root, **kwargs):
        calls.append(kwargs)
        if kwargs.get("require_current_refresh_proof", True):
            state._close_confirmed_proof_day({}, date(2026, 9, 3), date(2026, 9, 4))
        return {"ok": True, "errors": [], "anchor_dates": dict.fromkeys(
            ("proxy_index", "costed_nav", "panel_shadow"), "2026-09-03")}
    monkeypatch.setattr(state, "validate_state", validated)
    return root, calls


def test_archive_native_pack_labels_zip_and_keeps_original_files(archive_transport):
    root, calls = archive_transport
    before = {p.relative_to(root): p.read_bytes() for p in root.rglob("*") if p.is_file()}
    bundle = root.parent / "archive.zip"
    report = cloud.pack({v: root for v in delivery.COSTED}, bundle, "2026-09-03", archive_only=True)
    assert report["ok"] and report["signal_ready"] is False and report["current_signal_ready"] is False
    assert report["archive_session"] == "2026-09-03"
    assert calls and all(call.get("require_current_refresh_proof") is False for call in calls)
    with zipfile.ZipFile(bundle) as archive:
        integrity = state._verify_bundle_manifest(archive)
        whole = json.loads(archive.read(delivery.MANIFEST))
        assert integrity["validation"]["current_signal_ready"] is False
        assert whole["current_signal_ready"] is False and whole["expected_date"] == "2026-09-03"
        assert whole["freshness"] == "verified_archive_only_requires_current_refresh"
    assert before == {p.relative_to(root): p.read_bytes() for p in root.rglob("*") if p.is_file()}


def test_normal_pack_does_not_enter_archive_mode(archive_transport):
    root, calls = archive_transport
    with pytest.raises(ValueError, match="misses latest"):
        cloud.pack({v: root for v in delivery.COSTED}, root.parent / "normal.zip", "2026-09-03")
    assert calls and all("require_current_refresh_proof" not in c for c in calls)


@pytest.mark.parametrize("fault", ["missing_raw", "wrong_raw_day", "bad_final_stream", "lock"])
def test_archive_native_pack_keeps_data_and_writer_guards(archive_transport, fault):
    root, _ = archive_transport
    if fault == "missing_raw": (root / state.PRICE_CACHE_DIR / "000001.csv").unlink()
    elif fault == "wrong_raw_day":
        path = root / state.PRICE_CACHE_DIR / "000001.csv"
        path.write_bytes(path.read_bytes().replace(b"2026-09-03", b"2026-09-02"))
    elif fault == "bad_final_stream":
        (root / cloud.final_files("5")[0]).write_text("date\n2026-09-02\n")
    else: (root / delivery.LOCK).write_text("another-writer")
    bundle = root.parent / "bad-archive.zip"
    with pytest.raises((ValueError, RuntimeError)):
        cloud.pack({v: root for v in delivery.COSTED}, bundle, "2026-09-03", archive_only=True)
    assert not bundle.exists()
