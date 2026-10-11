"""Read-only final checks for this local diagnostic audit; never certify deployment."""
from __future__ import annotations

import ast
import hashlib
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
AUDIT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
from scripts import top100_delivery as delivery
from scripts import top100_cloud_delivery as cloud


def raw_sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def literal_parameters(source: str, namespace: str = "module") -> dict[str, object]:
    values = {}
    for node in ast.parse(source).body:
        if not isinstance(node, ast.Assign):
            continue
        try:
            value = ast.literal_eval(node.value)
        except (ValueError, TypeError):
            continue
        for target in node.targets:
            if not isinstance(target, ast.Name) or not target.id.isupper():
                continue
            name = target.id
            if name.endswith("_SOURCE"):
                if isinstance(value, str):
                    values.update(literal_parameters(value, name))
            elif isinstance(value, (int, float, bool)) or name in {
                    "VERSION", "STRATEGY_REVISION", "OVERHEAT_KIND", "DEFAULT_SIGNAL_MODEL"}:
                values[f"{namespace}.{name}"] = value
    return values


def main() -> None:
    freshness = json.loads((AUDIT / "baseline_and_freshness.json").read_text(encoding="utf-8"))
    real_inputs = {}
    for name, item in freshness["artifacts"].items():
        current = raw_sha(Path(item["path"]))
        assert current == item["raw_sha256"], name
        real_inputs[name] = {"raw_sha256": current, "rows": item["rows"], "latest_date": item["latest_date"]}

    prior = json.loads((ROOT / "research_reports/20261007_audit_doublecheck/root_official_artifact_check.json").read_text(encoding="utf-8"))
    formal_artifacts = {}
    for name, expected in prior["unchanged_vs_same_day_formal_check"].items():
        current = delivery.sha(ROOT / name)
        assert current == expected, name
        formal_artifacts[name] = current

    inspection = delivery.inspect_outputs(ROOT, freshness["latest_completed_session"])
    assert inspection["ok"], inspection["errors"]
    manifest = json.loads((ROOT / delivery.MANIFEST).read_text(encoding="utf-8"))
    assert manifest["status"] == "blocked", manifest
    helper = "scripts/top100_data_contracts.py"
    assert helper in delivery.RELEASE_FILES and helper in cloud.SOURCE_FILES and helper in inspection["inputs"]
    digest_identity_path = ROOT / "research_reports/20261007_script_adversarial/external_digest_source_identity.json"
    digest_identity = json.loads(digest_identity_path.read_text(encoding="utf-8"))
    assert raw_sha(Path(digest_identity["snapshot"])) == digest_identity["sha256"]
    metadata_files = sorted((ROOT / ".microcap_index_cache/security_meta").glob("*.json"))
    metadata_hash = hashlib.sha256("\n".join(
        f"{path}\0{raw_sha(path)}" for path in metadata_files).encode("utf-8")).hexdigest()
    data_checks = json.loads((AUDIT / "data_real_checks.json").read_text(encoding="utf-8"))["security_meta_readonly"]
    assert len(metadata_files) == data_checks["files"] and metadata_hash == data_checks["content_manifest_sha256"]
    state_validation = json.loads((AUDIT / "state_final_validation.json").read_text(encoding="utf-8"))
    for path, expected in state_validation["real_input_raw_sha256"].items():
        assert raw_sha(Path(path)) == expected, path
    for report_name, source_key in (("data_summary.json", "source_sha256"),
                                    ("math_summary.json", "source_sha256"),
                                    ("state_final_validation.json", "source_sha256")):
        report = json.loads((AUDIT / report_name).read_text(encoding="utf-8"))
        for name, expected in report[source_key].items():
            assert raw_sha(ROOT / name) == expected, (report_name, name)

    params = {}
    for suffix in ("0", "3", "5"):
        name = f"microcap_top100_mom16_biweekly_live_v2_{suffix}.py"
        old = literal_parameters((Path(freshness["old_source_backup"]) / name).read_text(encoding="utf-8-sig"))
        new = literal_parameters((ROOT / name).read_text(encoding="utf-8-sig"))
        assert old == new, name
        params[f"v2.{suffix}"] = {"literal_numeric_and_identity_parameters_checked": len(old), "unchanged": True,
                                   "values": new}
    source_names = list(json.loads((AUDIT / "start_source_manifest.json").read_text(encoding="utf-8")))
    source_names += ["scripts/top100_cloud_delivery.py", helper,
                     "tests/test_frozen_member_continuation_20260917.py", "tests/test_realtime_exchange_calendar.py",
                     "tests/test_round3_data_boundary.py", "tests/test_round3_math_ledger.py",
                     "tests/test_round3_state_contract.py", "tests/test_round3_delivery_boundary.py",
                     str(Path(__file__).relative_to(ROOT)).replace("\\", "/")]
    sources = {}
    for name in dict.fromkeys(source_names):
        path = ROOT / name
        ast.parse(path.read_text(encoding="utf-8-sig"), filename=name)
        sources[name] = {"syntax_checked": True, "sha256": raw_sha(path)}
    (AUDIT / "final_source_manifest.json").write_text(json.dumps(sources, ensure_ascii=False, indent=2), encoding="utf-8")
    result = {"scope": "round3_diagnostic_final_checks_no_production_sync_claim",
              "latest_completed_session": freshness["latest_completed_session"],
              "real_inputs_raw_sha256_verified_unchanged": real_inputs,
              "formal_artifacts_normalized_sha256_verified_unchanged": formal_artifacts,
              "inspect_outputs_ok": True, "formal_delivery_manifest_status": manifest["status"],
              "helper_bound_in_release_inputs_and_cloud_source": True,
              "security_metadata_files_unchanged": len(metadata_files), "security_metadata_manifest_sha256": metadata_hash,
              "external_digest_snapshot": digest_identity,
              "independent_agent_source_bindings_verified": True,
              "state_real_inputs_raw_sha256_verified_unchanged": state_validation["real_input_file_count"],
              "parameters": params, "syntax_and_source_files": len(sources)}
    (AUDIT / "root_official_artifact_check.json").write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"ok": True, "raw_inputs": len(real_inputs), "normalized_artifacts": len(formal_artifacts),
                      "source_files": len(sources), "manifest_status": manifest["status"],
                      "parameters_unchanged": True}, ensure_ascii=False))


if __name__ == "__main__":
    main()
