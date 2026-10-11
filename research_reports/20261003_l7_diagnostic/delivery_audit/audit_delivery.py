"""Read-only L7 delivery identity check on local and downloaded run artifacts."""

from __future__ import annotations

import csv
import hashlib
import io
import json
import subprocess
import sys
import zipfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
sys.path.insert(0, str(ROOT))

from scripts import top100_delivery as delivery  # noqa: E402
from scripts import realtime_state_bundle as state  # noqa: E402

PIN = "db889f7d1517763b3d1ff99c3ae024e338514ce9"
DATE = "2026-09-30"


def row(path: Path) -> dict[str, str]:
    with path.open(encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.DictReader(handle))
    if len(rows) != 1:
        raise ValueError(f"expected exactly one final signal row: {path}")
    return rows[0]


def normalized_sha(payload: bytes) -> str:
    return hashlib.sha256(payload.replace(b"\r\n", b"\n")).hexdigest()


def main() -> None:
    manifest = json.loads((ROOT / delivery.MANIFEST).read_text(encoding="utf-8"))
    current = delivery.inspect_outputs(ROOT, DATE)
    stream_errors = list(current["errors"])
    checked = delivery.validate_manifest(ROOT, current)
    source = {}
    for version in ("0", "3", "5"):
        name = f"microcap_top100_mom16_biweekly_live_v2_{version}.py"
        pinned_bytes = subprocess.check_output(["git", "show", f"{PIN}:{name}"], cwd=ROOT)
        source[name] = {
            "pinned_sha256": normalized_sha(pinned_bytes),
            "manifest_sha256": manifest["inputs"][name],
            "current_sha256": current["inputs"][name],
            "pinned_matches_manifest": normalized_sha(pinned_bytes) == manifest["inputs"][name],
        }

    finals = {}
    fields = (
        "version", "strategy_revision", "date", "current_holding", "next_holding",
        "trade_state", "current_execution_scale", "next_session_actionable_scale",
        "member_rebalance_signal_date", "member_rebalance_execution_date",
        "member_rebalance_actionable", "member_rebalance_required",
        "member_rebalance_official", "member_enter_count", "member_exit_count",
        "member_rebalance_label",
    )
    for version, directory in (("0", "microcap"), ("3", "microcap-v23"), ("5", "microcap-v25")):
        name = f"microcap_top100_mom16_biweekly_live_v2_{version}_latest_signal.csv"
        local_path = ROOT / "outputs" / name
        remote_path = HERE / "run_digest" / directory / "outputs" / name
        local, remote = row(local_path), row(remote_path)
        differences = [
            {"field": key, "local": local.get(key), "remote": remote.get(key)}
            for key in sorted(set(local) | set(remote))
            if local.get(key) != remote.get(key)
        ]
        finals[f"v2.{version}"] = {
            "local_file": str(local_path.relative_to(ROOT)),
            "remote_run_file": str(remote_path.relative_to(ROOT)),
            "local_sha256": normalized_sha(local_path.read_bytes()),
            "remote_sha256": normalized_sha(remote_path.read_bytes()),
            "compared_field_count": len(set(local) | set(remote)),
            "difference_count": len(differences),
            "first_difference": differences[0] if differences else None,
            "differences": differences,
            "local_key_fields": {key: local.get(key) for key in fields},
            "remote_key_fields": {key: remote.get(key) for key in fields},
        }

    bundle_path = HERE / "remote_whole" / "microcap-whole-delivery-state.zip"
    bundle_check = None
    if bundle_path.exists():
        with zipfile.ZipFile(bundle_path) as archive:
            bundle_manifest = state._verify_bundle_manifest(archive)
            packed_delivery = json.loads(archive.read(delivery.MANIFEST))
            signal_checks = {}
            for version in ("0", "3", "5"):
                name = f"outputs/microcap_top100_mom16_biweekly_live_v2_{version}_latest_signal.csv"
                bundle_signal = next(csv.DictReader(io.StringIO(archive.read(name).decode("utf-8-sig"))))
                digest_signal = finals[f"v2.{version}"]["remote_key_fields"]
                signal_checks[f"v2.{version}"] = {
                    "bundle_sha256": normalized_sha(archive.read(name)),
                    "bundle_vs_digest_key_fields_match": all(bundle_signal.get(k) == v for k, v in digest_signal.items()),
                    "bundle_vs_digest_all_fields_match": bundle_signal == row(
                        HERE / "run_digest" / ({"0": "microcap", "3": "microcap-v23", "5": "microcap-v25"}[version])
                        / "outputs" / Path(name).name),
                }
            bundle_check = {
                "path": str(bundle_path.relative_to(ROOT)),
                "archive_sha256": normalized_sha(bundle_path.read_bytes()),
                "verified_internal_manifest_file_count": len(bundle_manifest["files"]),
                "delivery_manifest_status": packed_delivery.get("status"),
                "delivery_manifest_expected_date": packed_delivery.get("expected_date"),
                "delivery_manifest_release_sha": packed_delivery.get("release_sha"),
                "signals": signal_checks,
            }

    result = {
        "scope": "read_only_delivery_audit",
        "date": DATE,
        "local_stream_errors": stream_errors,
        "local_manifest_check_ok": checked["ok"],
        "local_manifest_errors": checked["errors"],
        "local_manifest_status": manifest.get("status"),
        "local_manifest_release_sha": manifest.get("release_sha"),
        "local_manifest_verified_at": manifest.get("verified_at"),
        "pinned_strategy_commit": PIN,
        "source_identity": source,
        "final_signal_comparison": finals,
        "remote_bundle": bundle_check,
    }
    (HERE / "CHECKS.json").write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({
        "local_stream_errors": result["local_stream_errors"],
        "local_manifest_errors": result["local_manifest_errors"],
        "source_identity": source,
        "final_differences": {key: (value["difference_count"], value["first_difference"])
                              for key, value in finals.items()},
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
