"""Second-round publication-contract attacks against real artifacts and CLI paths.

All writes live under the caller's audit basetemp. Production CSVs are read only.
The delivery CLI tests explicitly isolate its final-file semantics from its
remote-release gate; they cannot be used as production-delivery acceptance.
"""
from __future__ import annotations

import copy
import csv
import hashlib
import json
import os
import shutil
import time
from contextlib import contextmanager
from pathlib import Path

import pandas as pd
import pytest

import microcap_top100_mom16_biweekly_live_v2_0 as v20
import microcap_top100_mom16_biweekly_live_v2_3 as v23
import microcap_top100_mom16_biweekly_live_v2_5 as v25
from scripts import top100_delivery as delivery


ROOT = Path(__file__).resolve().parents[1]
AUDIT = ROOT / "research_reports" / "20261007_audit_doublecheck"
PROBE_LOG = AUDIT / "contract_probes.jsonl"


def record(case: str, **fields: object) -> None:
    AUDIT.mkdir(exist_ok=True)
    with PROBE_LOG.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps({"case": case, **fields}, ensure_ascii=False, default=str) + "\n")


def read_rows(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    with path.open(encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        return list(reader.fieldnames or []), list(reader)


def write_rows(path: Path, fields: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


@pytest.fixture(scope="module")
def authentic():
    proof = json.loads((AUDIT / "baseline_and_freshness.json").read_text(encoding="utf-8"))
    assert proof["fresh_written_artifacts_rechecked_and_unchanged"] is True
    target = proof["latest_completed_session"]
    for name, item in proof["artifacts"].items():
        assert hashlib.sha256(Path(item["path"]).read_bytes()).hexdigest() == item["raw_sha256"], name
    report = delivery.inspect_outputs(ROOT, target)
    assert report["ok"], report["errors"]
    resolved = v20._resolve_base_paths()
    close = v20.base_mod.load_close_df(
        resolved.output_paths["panel_shadow"], resolved.index_csv, max_date=pd.Timestamp(target)
    )
    base_gross = v20.base_mod.run_signal(close).sort_index()
    turnover = pd.read_csv(resolved.output_paths["proxy_turnover"], encoding="utf-8-sig")
    for field in ("rebalance_date", "execution_date", "effective_date", "return_start_date"):
        turnover[field] = pd.to_datetime(turnover[field], format="ISO8601", errors="raise")
    official = pd.read_csv(ROOT / "outputs" / "microcap_top100_mom16_biweekly_live_v2_0_nav.csv",
                           encoding="utf-8-sig", parse_dates=["date"]).set_index("date")
    summary = json.loads((ROOT / delivery.state.REQUIRED_FILES[1]).read_text(encoding="utf-8"))
    before = {item["path"]: item["raw_sha256"] for item in proof["artifacts"].values()}
    record("authentic_baseline", target=target, rows={k: x["rows"] for k, x in proof["artifacts"].items()},
           source_sha256={str(Path(mod.__file__).relative_to(ROOT)): hashlib.sha256(Path(mod.__file__).read_bytes()).hexdigest()
                          for mod in (v20, v23, v25, delivery)})
    yield {"target": target, "report": report, "base_gross": base_gross,
           "turnover": turnover, "official": official, "summary": summary}
    for path, expected_hash in before.items():
        assert hashlib.sha256(Path(path).read_bytes()).hexdigest() == expected_hash, path


@pytest.fixture(scope="module")
def delivery_copy(tmp_path_factory, authentic):
    root = tmp_path_factory.mktemp("contract_delivery")
    names = set(authentic["report"]["inputs"]) | set(authentic["report"]["artifacts"])
    names.add(delivery.state.REQUIRED_FILES[1])
    for name in names:
        destination = root / name
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(ROOT / name, destination)
    originals = {v: (root / "outputs" / f"microcap_top100_mom16_biweekly_live_v2_{v}_latest_signal.csv").read_bytes()
                 for v in ("0", "3", "5")}
    assert delivery.inspect_outputs(root, authentic["target"])["ok"]
    return root, originals


FINAL_CORRUPTIONS = [
    (v, "official_close_confirmed_signal", "False") for v in ("0", "3", "5")
] + [
    (v, "signal_timing", "intraday_hypothetical_if_now_close") for v in ("0", "3", "5")
] + [
    (v, "strategy_version", "v2.0") for v in ("3", "5")
] + [
    (v, "member_rebalance_state", "none") for v in ("0", "3", "5")
] + [
    (v, "target_position_scale", "9") for v in ("0", "3", "5")
] + [
    (v, "position_transition", "False" if v == "3" else "True") for v in ("0", "3", "5")
] + [
    (v, "scale_trade_required", "True") for v in ("0", "3", "5")
 ] + [
    ("3", field, bad) for field in (
        "next_session_target_scale", "raw_next_target_scale", "target_vol_scale_next_session",
        "execution_scale", "raw_scale_delta", "actionable_scale_delta", "scale_delta", "position_scale_delta"
    ) for bad in ("nan", "inf", "unparseable")
 ] + [
    (v, field, "__DROP__") for v in ("0", "3", "5") for field in (
        "version", "signal_timing", "official_close_confirmed_signal", "member_rebalance_state"
    )
 ] + [(v, "strategy_version", "__DROP__") for v in ("3", "5")
 ] + [(v, "version", "__DUP_VERSION__") for v in ("0", "3", "5")
]


@pytest.mark.parametrize("version,field,bad_value", FINAL_CORRUPTIONS)
def test_final_file_corruption_cannot_be_recertified_by_delivery_cli(
    delivery_copy, authentic, monkeypatch, capsys, version, field, bad_value
):
    root, originals = delivery_copy
    path = root / "outputs" / f"microcap_top100_mom16_biweekly_live_v2_{version}_latest_signal.csv"
    fields, rows = read_rows(path)
    assert len(rows) == 1 and field in fields
    old = rows[0][field]
    assert old != bad_value
    if bad_value == "__DROP__":
        rows[0].pop(field)
        fields.remove(field)
    elif bad_value != "__DUP_VERSION__":
        rows[0][field] = bad_value
    try:
        if bad_value == "__DUP_VERSION__":
            with path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.writer(handle)
                writer.writerow(["version", *fields])
                for row in rows:
                    writer.writerow(["9.9", *[row[key] for key in fields]])
            assert pd.read_csv(path).iloc[0]["version"] == 9.9
        else:
            write_rows(path, fields, rows)
        report = delivery.inspect_outputs(root, authentic["target"])
        # This is the same complete-manifest step used after inspect_outputs in
        # the native group-refresh flow. A semantic bug must not self-certify.
        delivery.write_manifest(root, {**report, "status": "complete" if report["ok"] else "blocked"})
        monkeypatch.setattr(delivery, "__file__", str(root / "scripts" / "top100_delivery.py"))
        monkeypatch.setattr(delivery, "verify_release", lambda _root: "diagnostic_remote_gate_isolated")
        monkeypatch.setattr(delivery, "independent_target", lambda _root: authentic["target"])
        # Base-state freshness was independently proven before this fault
        # injection. This test targets written final-file semantics only.
        monkeypatch.setattr(delivery, "validate_base_state_for_session",
                            lambda _root, _day: {"ok": True, "errors": [], "diagnostic_gate_isolated": True})
        status = delivery.main(["check"])
        output = json.loads(capsys.readouterr().out)
        record("final_cli", version=version, field=field, original=old, injected=bad_value,
               inspect_ok=report["ok"], cli_exit=status, errors=output["errors"],
               scope="isolated_real_final_artifact_copy_only")
        assert status == 1, f"v2.{version} corrupted {field} passed native delivery CLI: {output['errors']}"
    finally:
        path.write_bytes(originals[version])


V25_REALTIME_CORRUPTIONS = ["wrong_version", "wrong_strategy_version", "missing_version", "two_rows", "enabled_overheat",
                           "duplicated_version_header"]


def mutate_realtime(path: Path, attack: str) -> None:
    fields, rows = read_rows(path)
    assert len(rows) == 1
    if attack == "wrong_version":
        rows[0]["version"] = "2.0"
    elif attack == "wrong_strategy_version":
        rows[0]["strategy_version"] = "v2.0"
    elif attack == "missing_version":
        fields.remove("version")
        rows[0].pop("version")
    elif attack == "two_rows":
        extra = dict(rows[0])
        extra["version"] = "2.3"
        rows.append(extra)
    elif attack == "enabled_overheat":
        rows[0]["overheat_enabled"] = "True"
        rows[0]["overheat_overlay_enabled"] = "True"
    elif attack == "duplicated_version_header":
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow([*fields, "version"])
            for row in rows:
                writer.writerow([*[row[key] for key in fields], "9.9"])
        return
    else:
        raise AssertionError(attack)
    write_rows(path, fields, rows)


@pytest.mark.parametrize("attack", V25_REALTIME_CORRUPTIONS)
def test_native_v25_signal_cli_does_not_preserve_corrupt_realtime_file(
    tmp_path, authentic, monkeypatch, capsys, attack
):
    assert_native_cli_cleanup(tmp_path, authentic, monkeypatch, capsys, "5", v25, attack)


def assert_native_cli_cleanup(tmp_path, authentic, monkeypatch, capsys, version, module, attack,
                              concurrent_writer=False):
    output_dir = tmp_path / f"contract_native_v2{version}"
    output_dir.mkdir()
    prefix = f"contract_v2{version}"
    realtime_path = output_dir / f"{prefix}_realtime_signal.csv"
    shutil.copy2(ROOT / "outputs" / f"microcap_top100_mom16_biweekly_live_v2_{version}_realtime_signal.csv", realtime_path)
    if attack == "disabled_native_overheat_alias":
        fields, rows = read_rows(realtime_path)
        assert rows[0]["overheat_enabled"] == rows[0]["overheat_overlay_enabled"] == "True"
        rows[0]["overheat_overlay_enabled"] = "False"
        write_rows(realtime_path, fields, rows)
    else:
        mutate_realtime(realtime_path, attack)
    shutil.copy2(ROOT / "outputs" / delivery.COSTED[version], output_dir / f"{prefix}_costed_nav.csv")
    shutil.copy2(ROOT / "outputs" / f"microcap_top100_mom16_biweekly_live_v2_{version}_summary.json",
                 output_dir / f"{prefix}_summary.json")
    before_bytes = realtime_path.read_bytes()
    writer_bytes = (ROOT / "outputs" / f"microcap_top100_mom16_biweekly_live_v2_{version}_realtime_signal.csv").read_bytes()
    previous_v20_prefix = v20.OUTPUT_PREFIX
    previous_version_prefix, previous_version_costed = module.OUTPUT_PREFIX, module.COSTED_NAV_CSV
    previous_v20_args = v20._V2_RUNTIME_ARGS
    previous_version_args = module._ACTIVE_RUNTIME_ARGS
    try:
        with monkeypatch.context() as isolated:
            isolated.setattr(module, "OUTPUT_DIR", output_dir)
            isolated.setattr(v20, "OUTPUT_DIR", output_dir)
            isolated.setattr(module, "LEGACY_COSTED_NAV_CSVS", ())
            if module is v25:
                isolated.setattr(v25, "stale_v2_5_legacy_retest_outputs", lambda: [])
            # Only the write-capable upstream loader is replaced. Its genuine
            # price/signal/cost context was independently reconstructed above.
            isolated.setattr(module, "_load_official_v2_0_out", lambda: authentic["official"].copy())
            isolated.setattr(v20.embedded_context, "_load_embedded_base_context", lambda: (
                copy.deepcopy(authentic["summary"]), authentic["base_gross"].copy(), authentic["turnover"].copy()))
            if concurrent_writer:
                native_lock = getattr(module, f"v2_{version}_realtime_output_lock")

                @contextmanager
                def writer_finishes_before_cleanup_acquires_lock():
                    with native_lock():
                        # Deterministic handoff: a legal writer publishes after
                        # stale classification but before the cleanup recheck.
                        realtime_path.write_bytes(writer_bytes)
                        yield

                isolated.setattr(module, f"v2_{version}_realtime_output_lock",
                                 writer_finishes_before_cleanup_acquires_lock)
            module.main([f"--v2{version}-output-prefix", prefix, "信号"])
            captured = capsys.readouterr().out
            costed = pd.read_csv(output_dir / f"{prefix}_costed_nav.csv", encoding="utf-8-sig")
            formal = pd.read_csv(ROOT / "outputs" / delivery.COSTED[version], encoding="utf-8-sig")
            assert costed["date"].tolist() == formal["date"].tolist()
            pd.testing.assert_frame_equal(costed[["holding", "next_holding", "return_net"]],
                                          formal[["holding", "next_holding", "return_net"]],
                                          check_exact=False, atol=1e-12, rtol=0)
            retained = realtime_path.exists()
            record(f"v2{version}_native_signal_cli_cleanup", attack=attack, corrupt_realtime_retained=retained,
                   retained_bytes_unchanged=retained and realtime_path.read_bytes() == before_bytes,
                   output_includes_signal=f"strategy_version: v2.{version}" in captured,
                   concurrent_writer=concurrent_writer,
                   official_economic_parity=True, isolated_output_dir=str(output_dir))
            if concurrent_writer:
                assert retained and realtime_path.read_bytes() == writer_bytes
                assert getattr(module, f"realtime_signal_matches_current_v2_{version}")(realtime_path)
            else:
                assert not retained, f"Native v2.{version} 信号 CLI kept {attack} realtime artifact"
    finally:
        module.configure_output_paths(previous_version_prefix, previous_version_costed)
        v20.configure_output_paths(previous_v20_prefix)
        v20._V2_RUNTIME_ARGS = previous_v20_args
        module._ACTIVE_RUNTIME_ARGS = previous_version_args


def test_native_v23_signal_cli_rejects_disabled_overheat_alias(tmp_path, authentic, monkeypatch, capsys):
    assert_native_cli_cleanup(tmp_path, authentic, monkeypatch, capsys, "3", v23,
                              "disabled_native_overheat_alias")


def test_native_v23_signal_cli_rejects_duplicate_version_header(tmp_path, authentic, monkeypatch, capsys):
    assert_native_cli_cleanup(tmp_path, authentic, monkeypatch, capsys, "3", v23,
                              "duplicated_version_header")


@pytest.mark.parametrize("version,module", [("3", v23), ("5", v25)])
def test_native_signal_cli_cleanup_keeps_current_concurrent_writer(
    tmp_path, authentic, monkeypatch, capsys, version, module
):
    assert_native_cli_cleanup(tmp_path, authentic, monkeypatch, capsys, version, module,
                              "wrong_version", concurrent_writer=True)


@pytest.mark.parametrize("version,module", [("0", v20.overlay_mod), ("3", v23), ("5", v25)])
@pytest.mark.parametrize("current_active,next_active", [(False, False), (False, True), (True, False), (True, True)])
def test_native_final_signal_accepts_all_four_fixed_one_transitions(
    authentic, version, module, current_active, next_active
):
    frame = pd.read_csv(ROOT / "outputs" / delivery.COSTED[version],
                        encoding="utf-8-sig", parse_dates=["date"]).set_index("date").tail(1).copy()
    active = "long_microcap_top100" if version == "5" else "long_microcap_short_zz1000"
    frame["holding"] = active if current_active else "cash"
    frame["next_holding"] = active if next_active else "cash"
    for field in ("current_execution_scale", "execution_scale", "actual_execution_scale"):
        if field in frame:
            frame[field] = float(current_active)
    for field in ("next_session_target_scale", "raw_next_target_scale", "next_session_actionable_scale",
                  "target_vol_scale_next_session"):
        frame[field] = float(next_active)
    signal = module._build_signal_row(frame, authentic["summary"]).iloc[0].to_dict()
    delivery.validate_final_signal(signal, frame.iloc[0].to_dict(), version)
    assert bool(signal["position_transition"]) == (current_active != next_active)
    assert bool(signal["scale_trade_required"]) is False


def test_native_v25_cli_does_not_reclaim_a_live_aged_generation_lock(tmp_path, monkeypatch):
    output_dir = tmp_path / "contract_live_lock"
    output_dir.mkdir()
    prefix = "contract_live_owner"
    lock_name = f"{prefix}_generation.lock"
    previous_v20_prefix = v20.OUTPUT_PREFIX
    previous_prefix, previous_costed = v25.OUTPUT_PREFIX, v25.COSTED_NAV_CSV
    previous_v20_args, previous_args = v20._V2_RUNTIME_ARGS, v25._ACTIVE_RUNTIME_ARGS
    try:
        with monkeypatch.context() as isolated:
            isolated.setattr(v20, "OUTPUT_DIR", output_dir)
            isolated.setattr(v25, "OUTPUT_DIR", output_dir)
            isolated.setattr(v20, "DEFAULT_V2_LOCK_WAIT_SECONDS", .01)
            isolated.setattr(v20, "DEFAULT_V2_STALE_LOCK_SECONDS", 0.)
            isolated.setattr(v25, "_generate_v2_5_outputs_unlocked",
                             lambda: pytest.fail("a live lock must stop the native CLI before generation"))
            with v20._v2_file_lock(lock_name, wait_timeout_seconds=.01):
                lock_path = output_dir / lock_name
                original = lock_path.read_bytes()
                os.utime(lock_path, (time.time() - 3600, time.time() - 3600))
                with pytest.raises(TimeoutError, match="generation lock"):
                    v25.main(["--v25-output-prefix", prefix, "信号"])
                assert lock_path.read_bytes() == original
            assert not (output_dir / lock_name).exists()
    finally:
        v25.configure_output_paths(previous_prefix, previous_costed)
        v20.configure_output_paths(previous_v20_prefix)
        v20._V2_RUNTIME_ARGS, v25._ACTIVE_RUNTIME_ARGS = previous_v20_args, previous_args


def test_member_rank_header_cannot_have_two_incompatible_meanings(delivery_copy, authentic):
    root, _originals = delivery_copy
    path = root / "outputs" / delivery.BASE_FILES["proxy_members"]
    original = path.read_bytes()
    fields, rows = read_rows(path)
    try:
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(["rank", *fields])
            writer.writerows([999, *[row[key] for key in fields]] for row in rows)
        assert pd.read_csv(path)["rank"].eq(999).all()
        try:
            result = delivery.canonical_member_rebalance(root, authentic["target"])
        except ValueError as error:
            record("duplicate_member_rank_header", rejected=True, error=str(error))
        else:
            record("duplicate_member_rank_header", rejected=False, result=result,
                   pandas_primary_rank=999, dict_reader_keeps_last_rank=True)
            pytest.fail("rank=999 and rank=1..100 in duplicate headers were both certified as one valid list")
    finally:
        path.write_bytes(original)


@pytest.mark.parametrize("attack", V25_REALTIME_CORRUPTIONS[:4])
def test_v23_realtime_identity_rejects_corresponding_faults(tmp_path, attack):
    path = tmp_path / "contract_v23_realtime.csv"
    shutil.copy2(ROOT / "outputs" / "microcap_top100_mom16_biweekly_live_v2_3_realtime_signal.csv", path)
    fields, rows = read_rows(path)
    if attack == "wrong_version":
        rows[0]["version"] = "2.5"
    elif attack == "wrong_strategy_version":
        rows[0]["strategy_version"] = "v2.5"
    elif attack == "missing_version":
        fields.remove("version")
        rows[0].pop("version")
    else:
        rows.append(dict(rows[0]))
    write_rows(path, fields, rows)
    assert not v23.realtime_signal_matches_current_v2_3(path)

