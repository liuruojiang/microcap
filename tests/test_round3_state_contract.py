"""Third-round diagnostic attacks on actual final CSVs and native entrypoints.

Production inputs remain read only. Upstream write-capable loaders are replaced
with contexts independently rebuilt from the freshness-proven real inputs.
Delivery's remote/base gates are explicitly isolated; these tests certify only
the written-file contract, never deployment, delivery, PIT history or fills.
"""
from __future__ import annotations

import copy
import csv
import hashlib
import json
import os
import shutil
from contextlib import contextmanager
from pathlib import Path

import pandas as pd
import pytest

import microcap_top100_mom16_biweekly_live_v2_0 as v20
import microcap_top100_mom16_biweekly_live_v2_3 as v23
import microcap_top100_mom16_biweekly_live_v2_5 as v25
from scripts import top100_delivery as delivery

ROOT = Path(__file__).resolve().parents[1]
AUDIT = ROOT / "research_reports" / "20261007_audit_round3"
TAG = os.environ.get("ROUND3_STATE_LOG_TAG", "probe")


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def record(case: str, **details: object) -> None:
    AUDIT.mkdir(parents=True, exist_ok=True)
    with (AUDIT / f"state_{TAG}_probes.jsonl").open("a", encoding="utf-8") as handle:
        handle.write(json.dumps({"scope": "diagnostic_only", "case": case, **details},
                                ensure_ascii=False, default=str) + "\n")


def read_csv(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    with path.open(encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        return list(reader.fieldnames or []), list(reader)


def write_csv(path: Path, fields: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def mutate(path: Path, field: str, value: str) -> None:
    fields, rows = read_csv(path)
    assert len(rows) == 1 and field in fields, (field, fields)
    if value == "__DROP__":
        fields.remove(field)
        rows[0].pop(field)
    else:
        assert rows[0][field] != value
        rows[0][field] = value
    write_csv(path, fields, rows)


@pytest.fixture(scope="module")
def real_context():
    proof = json.loads((AUDIT / "baseline_and_freshness.json").read_text(encoding="utf-8"))
    assert proof["scope"] == "round3_diagnostic_only"
    assert proof["formal_delivery_manifest_left_blocked"] is True
    for item in proof["artifacts"].values():
        assert item["matches_same_day_refreshed_input"] is True
        assert digest(Path(item["path"])) == item["raw_sha256"]
    target = proof["latest_completed_session"]
    report = delivery.inspect_outputs(ROOT, target)
    assert report["ok"], report["errors"]
    all_real_paths = set(Path(item["path"]) for item in proof["artifacts"].values())
    all_real_paths.update(ROOT / name for name in report["inputs"] if not name.endswith(".py"))
    all_real_paths.update(ROOT / name for name in report["artifacts"])
    all_real_paths.add(ROOT / delivery.state.REQUIRED_FILES[1])
    all_real_paths.update(ROOT / "outputs" / f"microcap_top100_mom16_biweekly_live_v2_{v}_realtime_signal.csv"
                         for v in ("0", "3", "5"))
    before = {str(path): digest(path) for path in all_real_paths}
    paths = v20._resolve_base_paths()
    close = v20.base_mod.load_close_df(paths.output_paths["panel_shadow"], paths.index_csv,
                                     max_date=pd.Timestamp(target))
    base_gross = v20.base_mod.run_signal(close).sort_index()
    turnover = pd.read_csv(paths.output_paths["proxy_turnover"], encoding="utf-8-sig")
    for field in ("rebalance_date", "execution_date", "effective_date", "return_start_date"):
        turnover[field] = pd.to_datetime(turnover[field], format="ISO8601", errors="raise")
    official = pd.read_csv(ROOT / "outputs" / "microcap_top100_mom16_biweekly_live_v2_0_nav.csv",
                           encoding="utf-8-sig", parse_dates=["date"]).set_index("date")
    reference = json.loads((ROOT / delivery.state.REQUIRED_FILES[1]).read_text(encoding="utf-8"))
    code = {str(Path(module.__file__).relative_to(ROOT)): digest(Path(module.__file__))
            for module in (v20, v23, v25, delivery)}
    contract_helper = ROOT / "scripts" / "top100_data_contracts.py"
    if contract_helper.is_file():
        code["scripts/top100_data_contracts.py"] = digest(contract_helper)
    for source_name in ("microcap_top100_mom16_biweekly_live.py", "scripts/top100_cloud_delivery.py"):
        code[source_name] = digest(ROOT / source_name)
    code["tests/test_round3_state_contract.py"] = digest(Path(__file__))
    record("real_input_proof", latest_completed_session=target, sources=before,
           code_sha256=code, formal_refresh=proof["formal_refresh"])
    yield {"target": target, "report": report, "base_gross": base_gross,
           "turnover": turnover, "official": official, "reference": reference}
    for path, expected in before.items():
        assert digest(Path(path)) == expected, f"Production input changed: {path}"
    for source_name, expected in code.items():
        assert digest(ROOT / source_name) == expected, f"Source changed during validation: {source_name}"
    record("real_inputs_and_source_unchanged_after_validation", real_file_count=len(before),
           code_sha256=code, sources=before)


def native_generation(tmp_path, real_context, monkeypatch, capsys, version, attack):
    module = {"0": v20, "3": v23, "5": v25}[version]
    output_dir = tmp_path / f"state_native_v2{version}"
    output_dir.mkdir()
    prefix = v20.DEFAULT_OUTPUT_PREFIX if version == "0" else f"state_v2{version}"
    costed_name = delivery.COSTED[version] if version == "0" else f"{prefix}_costed_nav.csv"
    realtime = output_dir / f"{prefix}_realtime_signal.csv"
    real_realtime = ROOT / "outputs" / f"microcap_top100_mom16_biweekly_live_v2_{version}_realtime_signal.csv"
    shutil.copy2(real_realtime, realtime)
    legal_bytes = realtime.read_bytes()
    replacement_events = []
    replacement_bytes = []
    if attack in {"wrong_version", "concurrent_valid_replacement"}:
        mutate(realtime, "version", "9.9")
    elif attack == "missing_anchor":
        mutate(realtime, "latest_anchor_trade_date", "__DROP__")
    elif attack == "scale_conflict":
        mutate(realtime, "current_execution_scale", "9.0")
    elif attack != "legal_dated_artifact":
        raise AssertionError(attack)
    shutil.copy2(ROOT / "outputs" / delivery.COSTED[version], output_dir / costed_name)
    shutil.copy2(ROOT / "outputs" / f"microcap_top100_mom16_biweekly_live_v2_{version}_summary.json",
                 output_dir / f"{prefix}_summary.json")
    corrupted = realtime.read_bytes()
    old_v20_prefix = v20.OUTPUT_PREFIX
    old_v20_args = v20._V2_RUNTIME_ARGS
    old_overlay_args = v20.overlay_mod._V2_RUNTIME_ARGS
    old_overlay_ns_args = v20._overlay_ns.get("_V2_RUNTIME_ARGS")
    old_prefix, old_costed = module.OUTPUT_PREFIX, module.COSTED_NAV_CSV
    try:
        with monkeypatch.context() as isolated:
            isolated.setattr(module, "OUTPUT_DIR", output_dir)
            isolated.setattr(v20, "OUTPUT_DIR", output_dir)
            isolated.setattr(v20.embedded_context, "_load_embedded_base_context", lambda: (
                copy.deepcopy(real_context["reference"]), real_context["base_gross"].copy(),
                real_context["turnover"].copy()))
            if version == "0":
                isolated.setattr(v20.overlay_mod, "OUTPUT_DIR", output_dir)
                isolated.setitem(v20._overlay_ns, "OUTPUT_DIR", output_dir)
                isolated.setitem(v20._overlay_ns, "LEGACY_COSTED_NAV_CSVS", ())
                isolated.setattr(v20, "_V2_RUNTIME_ARGS", v20.parse_v2_args(["信号"]))
                if attack == "concurrent_valid_replacement":
                    original_lock = v20._v2_realtime_output_lock

                    @contextmanager
                    def cleanup_after_writer(*args, **kwargs):
                        # Deterministically exercise the real race boundary:
                        # cleanup already selected this malformed file, then a
                        # realtime writer commits valid bytes under its native
                        # lock before cleanup obtains the same lock.
                        assert read_csv(realtime)[1][0]["version"] == "9.9"
                        with original_lock(*args, **kwargs):
                            v20._overlay_ns["_atomic_write_text"](
                                realtime, legal_bytes.decode("utf-8"), encoding="utf-8")
                            replacement_bytes.append(realtime.read_bytes())
                            assert v20._overlay_ns["realtime_signal_matches_current_v2_0"](realtime)
                        replacement_events.append("native_atomic_writer_before_cleanup_lock")
                        with original_lock(*args, **kwargs):
                            yield

                    isolated.setitem(v20._overlay_ns, "_v2_realtime_output_lock", cleanup_after_writer)
                v20.main()
            else:
                isolated.setattr(module, "LEGACY_COSTED_NAV_CSVS", ())
                isolated.setattr(module, "_load_official_v2_0_out", lambda: real_context["official"].copy())
                if version == "5":
                    isolated.setattr(v25, "stale_v2_5_legacy_retest_outputs", lambda: [])
                module.main([f"--v2{version}-output-prefix", prefix, "信号"])
            stdout = capsys.readouterr().out
            costed = pd.read_csv(output_dir / costed_name, encoding="utf-8-sig")
            formal = pd.read_csv(ROOT / "outputs" / delivery.COSTED[version], encoding="utf-8-sig")
            assert costed["date"].tolist() == formal["date"].tolist()
            pd.testing.assert_frame_equal(costed[["holding", "next_holding", "return_net"]],
                                          formal[["holding", "next_holding", "return_net"]],
                                          check_exact=False, atol=1e-12, rtol=0)
            retained = realtime.exists()
            record("native_close_cli_realtime_cleanup", version=version, attack=attack,
                   retained=retained, bytes_unchanged=retained and realtime.read_bytes() == corrupted,
                   economics_match_formal=True, native_output_dir=str(output_dir),
                   stdout_contains_signal=f"strategy_version: v2.{version}" in stdout,
                   concurrent_replacement_events=replacement_events,
                   replacement_sha256=hashlib.sha256(replacement_bytes[0]).hexdigest()
                   if replacement_bytes else None,
                   final_realtime_sha256=digest(realtime) if retained else None)
            if attack == "legal_dated_artifact":
                assert retained and realtime.read_bytes() == corrupted
            elif attack == "concurrent_valid_replacement":
                assert replacement_events == ["native_atomic_writer_before_cleanup_lock"]
                # Preserve exactly the writer's committed bytes. Windows text
                # serialization may change the copied source's line endings.
                assert retained and realtime.read_bytes() == replacement_bytes[0]
            else:
                assert not retained, f"Native v2.{version} 信号 preserved malformed realtime: {attack}"
    finally:
        if version != "0":
            module.configure_output_paths(old_prefix, old_costed)
        v20.configure_output_paths(old_v20_prefix)
        v20._V2_RUNTIME_ARGS = old_v20_args
        v20.overlay_mod._V2_RUNTIME_ARGS = old_overlay_args
        v20._overlay_ns["_V2_RUNTIME_ARGS"] = old_overlay_ns_args


@pytest.mark.parametrize("version,attack", [
    ("0", "wrong_version"), ("0", "missing_anchor"), ("0", "scale_conflict"),
    ("3", "missing_anchor"), ("3", "scale_conflict"),
    ("5", "missing_anchor"), ("5", "scale_conflict"),
])
def test_native_close_cli_does_not_preserve_malformed_realtime(
    tmp_path, real_context, monkeypatch, capsys, version, attack
):
    native_generation(tmp_path, real_context, monkeypatch, capsys, version, attack)


@pytest.mark.parametrize("version", ("0", "3", "5"))
def test_native_close_cli_preserves_legal_dated_realtime(
    tmp_path, real_context, monkeypatch, capsys, version
):
    # Archived/current-version realtime is retained without claiming it is fresh
    # today. Cleanup has no authority to relabel it or apply a wall-clock TTL.
    native_generation(tmp_path, real_context, monkeypatch, capsys, version, "legal_dated_artifact")


def test_native_v20_cleanup_preserves_concurrent_legal_realtime_writer(
    tmp_path, real_context, monkeypatch, capsys
):
    native_generation(tmp_path, real_context, monkeypatch, capsys, "0", "concurrent_valid_replacement")


@pytest.mark.parametrize("version,module", [("3", v23), ("5", v25)])
def test_realtime_preservation_accepts_equivalent_utc_snapshot(
    tmp_path, real_context, version, module
):
    path = tmp_path / "state_equivalent_utc.csv"
    shutil.copy2(ROOT / "outputs" / f"microcap_top100_mom16_biweekly_live_v2_{version}_realtime_signal.csv", path)
    fields, rows = read_csv(path)
    rows[0]["snapshot_time"] = pd.Timestamp(rows[0]["snapshot_time"]).tz_convert("UTC").isoformat()
    write_csv(path, fields, rows)
    accepted = getattr(module, f"realtime_signal_matches_current_v2_{version}")(path)
    record("realtime_utc_control", version=version, accepted=accepted)
    assert accepted, "An explicitly timezone-aware equivalent snapshot preserves the same Beijing session"


@pytest.mark.parametrize("version,module", [("3", v23), ("5", v25)])
def test_realtime_preservation_rejects_post_close_snapshot(
    tmp_path, real_context, version, module
):
    from datetime import datetime
    from scripts.exchange_calendar import latest_completed_session

    path = tmp_path / "state_after_close_snapshot.csv"
    shutil.copy2(ROOT / "outputs" / f"microcap_top100_mom16_biweekly_live_v2_{version}_realtime_signal.csv", path)
    fields, rows = read_csv(path)
    row = rows[0]
    row["snapshot_time"] = row["quote_trade_date"] + "T16:00:00+08:00"
    completed = latest_completed_session(datetime.fromisoformat(row["snapshot_time"])).isoformat()
    assert completed == row["quote_trade_date"] and completed != row["latest_anchor_trade_date"]
    write_csv(path, fields, rows)
    accepted = getattr(module, f"realtime_signal_matches_current_v2_{version}")(path)
    record("realtime_after_close", version=version, snapshot_time=row["snapshot_time"],
           latest_anchor=row["latest_anchor_trade_date"], independently_completed=completed,
           accepted=accepted)
    assert not accepted, "After confirmed close the intraday row cannot retain the prior-day anchor"


@pytest.mark.parametrize("version,module", [("3", v23), ("5", v25)])
@pytest.mark.parametrize("field,value", [
    ("snapshot_time", "__DROP__"),
    ("quote_trade_date", "2099-01-01"),
    ("latest_anchor_trade_date", "2099-01-01"),
    ("official_close_confirmed_signal", "True"),
    ("trade_state", "close"),
    ("date", "2026-01-05"),
    ("member_rebalance_actionable", "True"),
])
def test_realtime_preservation_requires_internal_time_and_state_consistency(
    tmp_path, real_context, version, module, field, value
):
    path = tmp_path / "state_realtime.csv"
    shutil.copy2(ROOT / "outputs" / f"microcap_top100_mom16_biweekly_live_v2_{version}_realtime_signal.csv", path)
    if field == "member_rebalance_actionable":
        # This genuine intraday row is after an earlier member rebalance. The
        # native realtime generator marks only the immediate execution session
        # actionable, requiring member signal date == latest completed anchor.
        # A close-confirmed next-session plan has a separate contract.
        _, original = read_csv(path)
        assert original[0]["member_rebalance_actionable"] == "False"
        assert original[0]["member_rebalance_required"] == "True"
        assert original[0]["member_rebalance_signal_date"] != original[0]["latest_anchor_trade_date"]
        assert original[0]["member_rebalance_execution_date"] == ""
        record("realtime_member_original", version=version,
               timing=original[0]["signal_timing"],
               member_signal_date=original[0]["member_rebalance_signal_date"],
               latest_anchor=original[0]["latest_anchor_trade_date"],
               quote_date=original[0]["quote_trade_date"],
               execution_date=original[0]["member_rebalance_execution_date"],
               member_actionable=original[0]["member_rebalance_actionable"])
    mutate(path, field, value)
    accepted = getattr(module, f"realtime_signal_matches_current_v2_{version}")(path)
    record("realtime_matcher", version=version, field=field, injected=value, accepted=accepted)
    assert not accepted, f"v2.{version} realtime matcher accepted inconsistent {field}"


@pytest.mark.parametrize("version,module", [("3", v23), ("5", v25)])
def test_realtime_member_action_cannot_be_relabelled_with_execution_date(
    tmp_path, real_context, version, module
):
    path = tmp_path / "state_member_two_field_bypass.csv"
    shutil.copy2(ROOT / "outputs" / f"microcap_top100_mom16_biweekly_live_v2_{version}_realtime_signal.csv", path)
    fields, rows = read_csv(path)
    row = rows[0]
    assert row["member_rebalance_actionable"] == "False"
    assert row["member_rebalance_required"] == "True"
    assert row["member_rebalance_official"] == "True"
    assert row["member_rebalance_signal_date"] < row["latest_anchor_trade_date"]
    assert row["member_rebalance_execution_date"] == ""
    row["member_rebalance_actionable"] = "True"
    row["member_rebalance_execution_date"] = row["quote_trade_date"]
    write_csv(path, fields, rows)
    accepted = getattr(module, f"realtime_signal_matches_current_v2_{version}")(path)
    record("realtime_member_two_field_bypass", version=version,
           member_signal_date=row["member_rebalance_signal_date"],
           latest_anchor=row["latest_anchor_trade_date"],
           quote_date=row["quote_trade_date"],
           execution_date=row["member_rebalance_execution_date"], accepted=accepted)
    assert not accepted, "A historical member signal cannot be moved to the current execution session"


@pytest.fixture(scope="module")
def isolated_delivery(tmp_path_factory, real_context):
    root = tmp_path_factory.mktemp("state_delivery")
    names = set(real_context["report"]["inputs"]) | set(real_context["report"]["artifacts"])
    names.add(delivery.state.REQUIRED_FILES[1])
    for name in names:
        destination = root / name
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(ROOT / name, destination)
    originals = {v: (root / "outputs" / f"microcap_top100_mom16_biweekly_live_v2_{v}_latest_signal.csv").read_bytes()
                 for v in ("0", "3", "5")}
    assert delivery.inspect_outputs(root, real_context["target"])["ok"]
    return root, originals


def diagnostic_delivery_check(root, real_context, monkeypatch, capsys):
    report = delivery.inspect_outputs(root, real_context["target"])
    delivery.write_manifest(root, {**report, "status": "complete" if report["ok"] else "blocked"})
    monkeypatch.setattr(delivery, "__file__", str(root / "scripts" / "top100_delivery.py"))
    monkeypatch.setattr(delivery, "verify_release", lambda _root: "diagnostic_remote_gate_isolated")
    monkeypatch.setattr(delivery, "independent_target", lambda _root: real_context["target"])
    monkeypatch.setattr(delivery, "validate_base_state_for_session", lambda _root, _day: {
        "ok": True, "errors": [], "diagnostic_gate_isolated": True})
    status = delivery.main(["check"])
    return report, status, json.loads(capsys.readouterr().out)


def test_native_delivery_check_accepts_authentic_copy(isolated_delivery, real_context, monkeypatch, capsys):
    root, _ = isolated_delivery
    report, status, output = diagnostic_delivery_check(root, real_context, monkeypatch, capsys)
    record("delivery_check_native_control", inspect_ok=report["ok"], cli_exit=status, errors=output["errors"])
    assert status == 0, output["errors"]


@pytest.mark.parametrize("value", ("2026-09-30 00:00:00", "2026-09-30T00:00:00"))
def test_final_csv_contract_accepts_naive_midnight_serialization(
    isolated_delivery, real_context, monkeypatch, capsys, value
):
    root, originals = isolated_delivery
    path = root / "outputs" / "microcap_top100_mom16_biweekly_live_v2_3_latest_signal.csv"
    try:
        mutate(path, "date", value)
        report, status, output = diagnostic_delivery_check(root, real_context, monkeypatch, capsys)
        record("delivery_midnight_control", value=value, inspect_ok=report["ok"], cli_exit=status,
               errors=output["errors"])
        assert status == 0, output["errors"]
    finally:
        path.write_bytes(originals["3"])


@pytest.mark.parametrize("version", ("0", "3", "5"))
@pytest.mark.parametrize("field,value", [
    ("date", "2026-09-30 12:00:00"),
    ("date", "2026-09-30T00:00:00+08:00"),
    ("momentum_gap", "-999.0"),
    ("microcap_mom", "__DROP__"),
])
def test_final_csv_contract_cannot_recertify_intraday_or_mismatched_signal_fields(
    isolated_delivery, real_context, monkeypatch, capsys, version, field, value
):
    root, originals = isolated_delivery
    path = root / "outputs" / f"microcap_top100_mom16_biweekly_live_v2_{version}_latest_signal.csv"
    try:
        mutate(path, field, value)
        report, status, output = diagnostic_delivery_check(root, real_context, monkeypatch, capsys)
        record("delivery_check_native_cli", version=version, field=field, injected=value,
               inspect_ok=report["ok"], cli_exit=status, errors=output["errors"],
               gate_boundary="real_file_semantics_only_remote_and_base_gates_isolated")
        assert status == 1, f"Native delivery check accepted v2.{version} {field}={value}"
    finally:
        path.write_bytes(originals[version])
