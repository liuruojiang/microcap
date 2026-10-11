"""Build an evidence manifest from completed diagnostic outputs, without running strategy work."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import xml.etree.ElementTree as ET

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def counts(path):
    root = ET.parse(path).getroot()
    suites = list(root.iter("testsuite"))
    result = {key: sum(int(suite.attrib.get(key, 0)) for suite in suites) for key in
              ["tests", "failures", "errors", "skipped"]}
    result["passed"] = result["tests"] - result["failures"] - result["errors"] - result["skipped"]
    return result


def main():
    baseline = counts(HERE / "data_final_baseline.xml")
    current = counts(HERE / "data_final_current.xml")
    regression = counts(HERE / "data_st_regression.xml")
    assert baseline == {"tests": 57, "failures": 40, "errors": 0, "skipped": 0, "passed": 17}
    assert current == {"tests": 57, "failures": 0, "errors": 0, "skipped": 0, "passed": 57}
    assert regression == {"tests": 125, "failures": 0, "errors": 0, "skipped": 0, "passed": 125}
    proof = json.loads((HERE / "baseline_and_freshness.json").read_text(encoding="utf-8"))
    real = json.loads((HERE / "data_real_checks.json").read_text(encoding="utf-8"))
    probes = json.loads((HERE / "data_probe_after/data_probes_result.json").read_text(encoding="utf-8"))
    source_names = ["microcap_top100_mom16_biweekly_live.py", "microcap_top100_mom16_biweekly_live_v2_0.py",
                    "microcap_top100_mom16_biweekly_live_v2_3.py", "microcap_top100_mom16_biweekly_live_v2_5.py",
                    "scripts/top100_delivery.py", "scripts/top100_cloud_delivery.py", "scripts/top100_data_contracts.py"]
    source_hashes = {name: sha(ROOT / name) for name in source_names}
    for name, digest in probes["source_sha256"].items():
        assert source_hashes[name] == digest, f"Source changed after probe: {name}"
    assert probes["source_unchanged_during_probe"] and real["source_unchanged_during_check"]
    official_input_hashes = {name: sha(detail["path"]) for name, detail in proof["artifacts"].items()}
    assert official_input_hashes == {name: detail["raw_sha256"] for name, detail in proof["artifacts"].items()}
    assert real["official_inputs_unchanged"] and real["security_meta_readonly"]["all_bytes_unchanged_during_read"]
    report = {"scope": "round3_independent_data_boundary_diagnostic_only", "verified_status": "PASS",
              "formal_delivery_status": "BLOCKED", "formal_blocker": proof["formal_refresh"],
              "versions": ["v2.0", "v2.3", "v2.5"], "excluded_version": "v2.6",
              "distinct_finding_groups": 7, "baseline": baseline, "current": current,
              "affected_data_st_regression": regression, "combined_current_passed": current["passed"] + regression["passed"],
              "probe_count": len(probes["probes"]), "source_sha256": source_hashes,
              "old_source_root": ".codex_backups/20261007_200436",
              "old_source_sha256": {name: sha(ROOT / ".codex_backups/20261007_200436" / name)
                                    for name in source_names[:4]},
              "test_sha256": sha(ROOT / "tests/test_round3_data_boundary.py"),
              "official_input_sha256": official_input_hashes, "official_inputs_unchanged": True,
              "real_turnover_rows": real["real_turnover"]["consumed_rows"],
              "security_meta_files_unchanged": real["security_meta_readonly"]["files"],
              "security_meta_content_manifest_sha256": real["security_meta_readonly"]["content_manifest_sha256"],
              "boundaries": ["No formal performance/signal/holding publication", "No full historical PIT/ST certification",
                             "No migration run or clean second-run certification", "No deployment, email, orders or deletion"]}
    evidence_names = ["data_findings.json", "data_review.md", "data_final_baseline.log", "data_final_baseline.xml",
                      "data_final_current.log", "data_final_current.xml", "data_st_regression.log", "data_st_regression.xml",
                      "data_probes.py", "data_real_checks.py", "data_real_checks.json", "data_finalize.py",
                      "data_probe_before_active/data_probes_result.json", "data_probe_after/data_probes_result.json"]
    report["evidence_sha256"] = {name: sha(HERE / name) for name in evidence_names}
    (HERE / "data_summary.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"summary": str(HERE / "data_summary.json"), "current_passed": report["combined_current_passed"],
                      "groups": report["distinct_finding_groups"], "formal_delivery_status": "BLOCKED"}, ensure_ascii=False))


if __name__ == "__main__":
    main()
