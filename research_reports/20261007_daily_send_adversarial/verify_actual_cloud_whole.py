"""Read-only verification of the actual SMTP run's full delivery artifact."""
from __future__ import annotations

import hashlib
import json
import sys
import zipfile
from pathlib import Path

TASK = Path(__file__).resolve().parent
ROOT = TASK.parent.parent
sys.path.insert(0, str(ROOT))
from scripts import realtime_state_bundle as state
from scripts import top100_cloud_delivery as cloud
from scripts import top100_delivery as delivery

EXPECTED = "2026-09-30"
STRATEGY_SHA = "de6ce1a9f05bf9032d3423b63121f778723db3d0"
AUTO_SHA = "ff81e937effe74f672502177936f8c919d8f11af"
RUN_ID = 37644093633


def dump(name: str, value: object) -> None:
    (TASK / name).write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def main() -> None:
    artifacts = json.loads((TASK / "cloud_artifacts.json").read_text(encoding="utf-8-sig"))["artifacts"]
    found = [a for a in artifacts if a["id"] == 11494840703]
    assert len(found) == 1
    artifact = found[0]
    assert artifact["name"] == "microcap-whole-delivery-validation-state"
    assert not artifact["expired"]
    assert artifact["workflow_run"]["id"] == RUN_ID
    assert artifact["workflow_run"]["head_sha"] == AUTO_SHA
    download = TASK / "cloud_whole_delivery"
    download.mkdir(exist_ok=True)
    bundle, transport = cloud._download_artifact(artifact, download)
    assert artifact["digest"] == "sha256:" + transport["github_artifact_sha256"]
    with zipfile.ZipFile(bundle) as archive:
        integrity = state._verify_bundle_manifest(archive)
        incoming = json.loads(archive.read(delivery.MANIFEST))
        assert incoming["status"] == "complete"
        assert incoming["expected_date"] == EXPECTED
        assert incoming["release_sha"] == STRATEGY_SHA
        assert incoming["inputs"] == delivery.input_hashes(ROOT)
        names = (set(incoming["inputs"]) | set(incoming["artifacts"]) | {delivery.MANIFEST}) - set(cloud.SOURCE_FILES)
        watched = set(incoming["inputs"]) | set(incoming["artifacts"]) | {delivery.MANIFEST}
        before = {name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest() for name in watched}
        staged = download / "staged_readback"
        staged.mkdir(exist_ok=True)
        for name in names:
            target = staged / name
            assert target.resolve().is_relative_to(staged.resolve())
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(archive.read(name))
        for name in cloud.SOURCE_FILES:
            cloud.copy_file(ROOT / name, staged / name)
    report = delivery.validate_manifest(staged, delivery.inspect_outputs(staged, EXPECTED))
    cloud.require_ok(report)
    dump("actual_cloud_whole_check.json", report)
    comparisons = {}
    numeric = ("return_net", "nav_net", "entry_exit_cost", "rebalance_cost", "total_cost", "current_execution_scale")
    for version, filename in delivery.COSTED.items():
        local_info, local_rows = delivery.csv_info(ROOT / "outputs" / filename)
        cloud_info, cloud_rows = delivery.csv_info(staged / "outputs" / filename)
        assert local_info["rows"] == cloud_info["rows"]
        assert local_info["latest_date"] == cloud_info["latest_date"] == EXPECTED
        categorical = ("date", "version", "strategy_revision", "holding", "next_holding")
        same = {key: all(a[key] == b[key] for a, b in zip(local_rows, cloud_rows)) for key in categorical}
        assert all(same.values())
        maximum = {key: max(abs(float(a[key]) - float(b[key])) for a, b in zip(local_rows, cloud_rows)) for key in numeric}
        assert all(value == 0.0 for value in maximum.values())
        _, final_rows = delivery.csv_info(staged / "outputs" / f"microcap_top100_mom16_biweekly_live_v2_{version}_latest_signal.csv")
        assert len(final_rows) == 1
        signal = final_rows[0]
        comparisons[f"v2.{version}"] = {
            "rows": local_info["rows"], "latest_date": EXPECTED,
            "local_sha256": local_info["sha256"], "cloud_sha256": cloud_info["sha256"],
            "same_categorical": same, "max_absolute_difference": maximum,
            "final_csv": {key: signal.get(key) for key in (
                "version", "strategy_version", "strategy_revision", "current_holding", "next_holding",
                "trade_state", "current_execution_scale", "next_session_actionable_scale",
                "member_rebalance_required", "member_rebalance_actionable", "member_rebalance_official",
                "member_rebalance_signal_date", "member_rebalance_execution_date")},
        }
    after = {name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest() for name in watched}
    assert before == after
    result = {
        "ok": True, "scope": "actual_cloud_full_delivery_readback", "run_id": RUN_ID,
        "strategy_sha": STRATEGY_SHA, "automation_sha": AUTO_SHA,
        "artifact_name": artifact["name"], "transport": transport,
        "bundle_sha256": state._sha256(bundle), "integrity_files_verified": len(integrity["files"]),
        "staged_artifacts": len(names), "source_and_base_inputs_match": True,
        "whole_delivery_check": report["ok"], "primary_files_unchanged": before == after,
        "comparisons": comparisons,
    }
    dump("actual_cloud_full_parity.json", result)
    print(json.dumps({key: result[key] for key in ("ok", "run_id", "integrity_files_verified", "staged_artifacts", "primary_files_unchanged")}, ensure_ascii=False))


if __name__ == "__main__":
    main()
