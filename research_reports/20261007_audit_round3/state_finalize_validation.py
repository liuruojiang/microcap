"""Bind completed round-three diagnostic evidence without touching production."""
from __future__ import annotations

import hashlib
import json
import xml.etree.ElementTree as ET
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
AUDIT = Path(__file__).resolve().parent


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> None:
    log_name = "state_final_verified"
    junit = ET.parse(AUDIT / f"{log_name}.xml").getroot()
    suite = junit if junit.tag == "testsuite" else junit.find("testsuite")
    assert suite is not None
    counts = {key: int(suite.attrib.get(key, "0")) for key in ("tests", "failures", "errors", "skipped")}
    assert counts == {"tests": 46, "failures": 0, "errors": 0, "skipped": 0}, counts
    rows = [json.loads(line) for line in (AUDIT / f"{log_name}_probes.jsonl").read_text(encoding="utf-8").splitlines()]
    start = next(row for row in rows if row["case"] == "real_input_proof")
    end = next(row for row in rows if row["case"] == "real_inputs_and_source_unchanged_after_validation")
    assert start["sources"] == end["sources"]
    assert start["code_sha256"] == end["code_sha256"]
    code = {name.replace("\\", "/"): value for name, value in end["code_sha256"].items()}
    for name, expected in end["code_sha256"].items():
        assert sha(ROOT / name) == expected, name
    for name, expected in end["sources"].items():
        assert sha(Path(name)) == expected, name
    cleanup = [row for row in rows if row["case"] == "native_close_cli_realtime_cleanup"]
    assert len(cleanup) == 11
    malformed = [row for row in cleanup if row["attack"] in {"wrong_version", "missing_anchor", "scale_conflict"}]
    assert len(malformed) == 7 and all(not row["retained"] for row in malformed)
    archived = [row for row in cleanup if row["attack"] == "legal_dated_artifact"]
    assert len(archived) == 3 and all(row["retained"] and row["bytes_unchanged"] for row in archived)
    writer = next(row for row in cleanup if row["attack"] == "concurrent_valid_replacement")
    assert writer["retained"] and writer["replacement_sha256"] == writer["final_realtime_sha256"]
    dependency = json.loads((AUDIT / "state_dependency_binding.json").read_text(encoding="utf-8"))
    assert all(dependency[key] for key in ("in_release_files", "in_input_hashes", "in_cloud_source_files"))
    assert dependency["helper_sha256"] == end["code_sha256"]["scripts/top100_data_contracts.py"]
    before_after = json.loads((AUDIT / "state_frozen_matcher_before_after.json").read_text(encoding="utf-8"))
    assert all(row["before_accepted"] for row in before_after["results"])
    assert all(row["after_accepted"] == (row["attack"] == "authentic_control") for row in before_after["results"])
    old = json.loads((AUDIT / "state_old_findings.json").read_text(encoding="utf-8"))
    source_names = [name for name in code if not name.startswith("tests/")]
    assert len(source_names) == 7
    old_sources = {}
    old_source_backups = {}
    for name in source_names:
        path = ROOT / ".codex_backups" / "20261007_200436" / name
        if name == "scripts/top100_cloud_delivery.py":
            path = ROOT / ".codex_backups" / "20261007_202622" / name
            assert sha(path) == "3243733dff1fbd0daeec831748ebfe2d6da3487ca4e3ca7849a5a2c1f361307f"
        old_sources[name] = sha(path) if path.is_file() else "new_helper_not_in_old_backup_packages"
        old_source_backups[name] = str(path) if path.is_file() else "new_shared_helper"
    for name, expected in old["tested_old_source_sha256"].items():
        assert old_sources[name.replace("\\", "/")] == expected
    manifest = ROOT / "outputs" / "top100_delivery_manifest.json"
    formal_status = json.loads(manifest.read_text(encoding="utf-8"))["status"]
    assert formal_status == "blocked"
    freshness_path = AUDIT / "baseline_and_freshness.json"
    freshness = json.loads(freshness_path.read_text(encoding="utf-8"))
    for item in freshness["artifacts"].values():
        assert end["sources"][item["path"]] == item["raw_sha256"]
    result = {
        "scope": "diagnostic_only",
        "phase": "independent_final_patch_validation",
        "latest_completed_session": start["latest_completed_session"],
        "validation": {**counts, "passed": 46, "time_seconds": suite.attrib["time"],
                       "counterexample_regressions": 37, "legal_controls": 9,
                       "log": f"{log_name}.log", "junit": f"{log_name}.xml", "probes": f"{log_name}_probes.jsonl"},
        "source_sha256": {name: code[name] for name in source_names},
        "test_source_sha256": code["tests/test_round3_state_contract.py"],
        "frozen_old_source_sha256": old_sources,
        "frozen_old_source_backups": old_source_backups,
        "freshness_proof_sha256": sha(freshness_path),
        "freshness_artifacts": freshness["artifacts"],
        "real_input_file_count": len(end["sources"]),
        "real_input_raw_sha256": end["sources"],
        "all_bound_code_and_real_inputs_unchanged": True,
        "native_cli_cleanup": cleanup,
        "new_dependency_binding": dependency,
        "original_red": {"main": old["old_red"], "additional_member_failures": 2,
                         "additional_compatibility_controls": 4,
                         "late_frozen_function_counterexamples": before_after},
        "fixture_corrections": [
            {"log": "state_old_red.log", "reason": "custom v20 prefix changed base panel lookup; delivery copy omitted base summary", "included_as_production_findings": False},
            {"log": "state_new_green.log", "result": "45 PASS + 1 fixture byte-comparison failure", "reason": "native text writer translates existing Windows line endings; valid replacement was retained", "included_as_production_findings": False},
            {"log": "state_new_green_final.log", "result": "45 PASS + 1 fixture namespace lookup failure", "reason": "v20 matcher is in embedded overlay namespace; direct outer module attribute did not exist", "included_as_production_findings": False},
        ],
        "coverage_limits": [
            "Actual full native close-command generation, staged promotion, history audit and cleanup are preserved; upstream write-capable loader is replaced by context recomputed from verified real inputs.",
            "Actual delivery check executes against complete isolated final-file copies; remote and base freshness gates are explicitly isolated.",
            "No complete production actionable=True intraday CSV was available. Three authentic archived controls cover legal nonactionable snapshots. Existing native member contract definition tests use diagnostic generated fixtures.",
            "Frozen function probes compile the exact old function with unchanged parameter constants; they are separate from full native CLI evidence.",
            "No deployment, external email, order, historical migration approval, full strategy performance certification or actual-fill certification.",
        ],
        "formal_delivery": {"status": formal_status, "sha256": sha(manifest), "refresh": start["formal_refresh"]},
    }
    (AUDIT / "state_final_validation.json").write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"scope": result["scope"], "validation": result["validation"],
                      "real_input_file_count": result["real_input_file_count"],
                      "source_sha256": result["source_sha256"]}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
