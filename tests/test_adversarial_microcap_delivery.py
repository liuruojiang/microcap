"""Offline fault injection; fixtures are diagnostic, never strategy results."""
import hashlib
import io
import json
import shutil
import subprocess
import sys
import zipfile
from datetime import date
from types import SimpleNamespace

import pytest

from scripts import realtime_state_bundle as state
from scripts import top100_cloud_delivery as cloud
from scripts import top100_delivery as delivery
from test_top100_delivery import workspace, certify  # noqa: F401
from test_top100_cloud_delivery import prepare
from test_realtime_member_snapshot_fallback import (
    _write_minimal_required_state, _write_current_v2_static_context, _proxy_member_rows,
)


def valid_state(root, monkeypatch, today=date(2026, 8, 10)):
    _write_minimal_required_state(root)
    _write_current_v2_static_context(root, _proxy_member_rows("2026-08-06"))
    monkeypatch.setattr(state, "_cn_today", lambda: today)
    state._write_refresh_proof(root, date(2026, 8, 7))


def test_future_state_is_error_not_warning(tmp_path, monkeypatch):
    valid_state(tmp_path, monkeypatch, date(2026, 8, 6))
    result = state.validate_state(tmp_path, max_anchor_age_days=5)
    assert not result["ok"], "A future price/NAV anchor must never be accepted"


def test_transport_long_holiday_uses_exact_session_not_five_calendar_days(workspace, monkeypatch):
    bundle, target = prepare(workspace, monkeypatch)
    calls = []
    def validate(root, **kw):
        calls.append(kw)
        return {"ok": kw.get("max_anchor_age_days") is None,
                "errors": ["current completed session is over five calendar days old"]}
    monkeypatch.setattr(state, "validate_state", validate)
    assert cloud.restore(target, bundle, "2026-09-03")["ok"]
    assert calls and all(c["max_anchor_age_days"] is None for c in calls)


def test_same_day_rollback(workspace, monkeypatch):
    bundle, target = prepare(workspace, monkeypatch)
    cloud.restore(target, bundle, "2026-09-03")
    path = target / cloud.final_files("5")[4]
    path.write_text(path.read_text() + "\n", encoding="utf-8")
    certify(target)
    before = path.read_bytes()
    with pytest.raises(ValueError, match="same-session"):
        cloud.restore(target, bundle, "2026-09-03")
    assert path.read_bytes() == before


def test_interrupted_restore(workspace, monkeypatch):
    bundle, target = prepare(workspace, monkeypatch)
    cloud.restore(target, bundle, "2026-09-03")
    # An incomplete prior run has no valid same-session success to reuse.
    (target / delivery.MANIFEST).unlink()
    def interrupted(*a, **kw):
        delivery.write_manifest(target, {"status": "complete"})
        raise KeyboardInterrupt("injected interruption after first replacement")
    monkeypatch.setattr(state, "restore_state", interrupted)
    with pytest.raises(KeyboardInterrupt):
        cloud.restore(target, bundle, "2026-09-03")
    assert json.loads((target / delivery.MANIFEST).read_text())["status"] == "blocked"
    assert not (target / delivery.LOCK).exists()


def test_windows_case_aliases_rejected_before_any_extract():
    buffer = io.BytesIO()
    entries = []
    with zipfile.ZipFile(buffer, "w") as archive:
        for name, payload in [("outputs/data.csv", b"one"), ("outputs/DATA.csv", b"two")]:
            entries.append({"path": name, "bytes": len(payload), "sha256": hashlib.sha256(payload).hexdigest()})
            archive.writestr(name, payload)
        archive.writestr(state.MANIFEST_NAME, json.dumps({"files": entries}))
    with zipfile.ZipFile(buffer) as archive, pytest.raises(ValueError, match="alias|collision"):
        state._verify_bundle_manifest(archive)


def test_verify_release_fetches_remote_object_not_present_locally(workspace, monkeypatch):
    fetched = []
    sha = "b" * 40
    def check_output(args, **kw):
        if args[1] == "ls-remote":
            return sha + "\trefs/heads/main\n"
        if args[1] == "cat-file":
            if not fetched:
                raise subprocess.CalledProcessError(1, args)
            return b""
        if args[1] == "show":
            if not fetched:
                raise subprocess.CalledProcessError(128, args)
            return (workspace / args[2].split(":", 1)[1]).read_bytes()
        raise AssertionError(args)
    monkeypatch.setattr(delivery.subprocess, "check_output", check_output)
    monkeypatch.setattr(delivery.subprocess, "run", lambda args, **kw: fetched.append(args))
    assert delivery.verify_release(workspace) == sha
    assert fetched and "fetch" in fetched[0]


@pytest.mark.parametrize("corruption", ["missing", "json", "incomplete", "hash"])
def test_invalid_whole_manifest_always_blocks(workspace, corruption):
    certify(workspace)
    path = workspace / delivery.MANIFEST
    if corruption == "missing":
        path.unlink()
    elif corruption == "json":
        path.write_text("{", encoding="utf-8")
    else:
        value = json.loads(path.read_text())
        if corruption == "incomplete":
            value["status"] = "refreshing"
        else:
            value["inputs"] = {}
        path.write_text(json.dumps(value), encoding="utf-8")
    assert not delivery.validate_manifest(workspace, delivery.inspect_outputs(workspace, "2026-09-03"))["ok"]


@pytest.mark.parametrize("missing", [None, "expected_calendar_day", "independent_completed_day",
                                    "member_name_quote_day", "member_name_count"])
def test_holiday_requires_full_independent_proof(tmp_path, monkeypatch, missing):
    valid_state(tmp_path, monkeypatch, date(2026, 8, 17))
    evidence = {"expected_calendar_day": "2026-08-07", "independent_completed_day": "2026-08-07",
                "member_name_quote_day": "2026-08-17", "member_name_count": 100,
                "current_st_name_intersection": 0}
    if missing:
        evidence.pop(missing)
    state._write_refresh_proof(tmp_path, date(2026, 8, 7), evidence)
    assert state.validate_state(tmp_path, max_anchor_age_days=5)["ok"] == (missing is None)


@pytest.mark.parametrize("expected", [date(2026, 8, 7), date(2026, 8, 14)])
def test_preflight_uses_external_calendar_and_independent_history(tmp_path, monkeypatch, expected):
    import pandas as pd
    valid_state(tmp_path, monkeypatch, date(2026, 8, 17))
    base = SimpleNamespace(
        pd=pd, fetch_eastmoney_index_history=lambda *a: ["history"],
        latest_closed_history_date=lambda history: pd.Timestamp("2026-08-07"),
    )
    module = SimpleNamespace(_sync_embedded_base_config=lambda: None, base_mod=base)
    monkeypatch.setitem(sys.modules, "microcap_top100_mom16_biweekly_live_v2_0", module)
    monkeypatch.setattr(state, "verify_live_member_names", lambda *a: {
        "member_name_quote_day": "2026-08-17", "member_name_count": 100,
        "current_st_name_intersection": 0})
    proof_before = (tmp_path / state.REFRESH_PROOF_REL).read_bytes()
    if expected == date(2026, 8, 7):
        assert state.preflight_state(tmp_path, 5, expected_date=expected)["ok"]
    else:
        with pytest.raises(RuntimeError, match="calendar"):
            state.preflight_state(tmp_path, 5, expected_date=expected)
        assert (tmp_path / state.REFRESH_PROOF_REL).read_bytes() == proof_before


@pytest.mark.parametrize("execution", ["", "2026-09-02", "2026-09-03"])
def test_actionable_close_contract_requires_future_execution(workspace, execution):
    path = workspace / "outputs/microcap_top100_mom16_biweekly_live_v2_0_latest_signal.csv"
    path.write_text("date,version,member_rebalance_actionable,member_rebalance_execution_date\n"
                    f"2026-09-03,2.0,True,{execution}\n", encoding="utf-8")
    assert not delivery.inspect_outputs(workspace, "2026-09-03")["ok"]


def test_real_core_next_session_member_contract_is_accepted(workspace):
    import pandas as pd
    import microcap_top100_mom16_biweekly_live_v2_0 as v2
    signal = pd.DataFrame([{"date": "2026-09-03", "version": "2.0", "member_rebalance_required": True}])
    row = v2.augment_close_confirmed_signal_with_member_contract(
        signal, pd.DataFrame({"rebalance_date": ["2026-09-03"]}),
        pd.DatetimeIndex(["2026-09-03", "2026-09-04"]))
    assert bool(row.iloc[0]["member_rebalance_actionable"])
    assert row.iloc[0]["member_rebalance_execution_date"] == "2026-09-04"
    row.to_csv(workspace / "outputs/microcap_top100_mom16_biweekly_live_v2_0_latest_signal.csv", index=False)
    result = delivery.inspect_outputs(workspace, "2026-09-03")
    assert result["ok"], result["errors"]


def test_windows_long_backup_path_copy(tmp_path):
    source = tmp_path / "source.txt"
    source.write_bytes(b"backup must survive a long workspace path")
    target = tmp_path / ("nested" * 25) / ("file" * 25 + ".txt")
    cloud.copy_file(source, target)
    returned = tmp_path / "roundtrip.txt"
    cloud.copy_file(target, returned)
    assert returned.read_bytes() == source.read_bytes()


def test_frozen_core_still_blocks_long_calendar_age():
    # Policy characterization, not a signal/backtest and not a silent relaxation.
    import pandas as pd
    import microcap_top100_mom16_biweekly_live_v2_0 as v2
    result = v2.base_mod.assess_history_anchor_freshness(
        pd.Timestamp("2026-09-30"), 5, now=pd.Timestamp("2026-10-08 14:30"),
        trading_dates=pd.DatetimeIndex(["2026-09-30"]))
    assert result["is_stale"] and result["stale_calendar_days"] == 8


def test_security_metadata_content_changes_fingerprint(tmp_path, monkeypatch):
    import microcap_top100_mom16_biweekly_live_v2_0 as v2
    meta = tmp_path / "000001.json"
    meta.write_text('{"name":"normal"}', encoding="utf-8")
    monkeypatch.setattr(v2.freq_mod, "list_backtest_universe_symbols", lambda: ["000001"])
    monkeypatch.setattr(v2.freq_mod, "resolve_security_meta_path", lambda code: meta)
    before = v2.base_mod.security_meta_cache_fingerprint()
    meta.write_text('{"name":"*ST flagged"}', encoding="utf-8")
    after = v2.base_mod.security_meta_cache_fingerprint()
    assert before["present_count"] == after["present_count"] == 1
    assert before["sha256"] != after["sha256"]
