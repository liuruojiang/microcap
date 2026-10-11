"""Second independent review: actual artifacts, test mail mode, no transport."""
from __future__ import annotations

import csv
import importlib.util
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import yaml

import probe_content as probe

HERE = Path(__file__).resolve().parent
TEST_ID = "microcap-independent-shape-20261007"


def load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module


def compose(module, case, fault="", version="v2.0"):
    directory = HERE / "test_mode_cases" / module.__name__ / case
    directory.mkdir(parents=True, exist_ok=True)
    args = ["digest", "--out-dir", str(directory / "artifacts"), "--publication-mode", "close_confirmed",
            "--delivery-test-id", TEST_ID, "--strategy-sha", probe.SHA, "--expected-signal-date", "2026-09-30"]
    paths = {}
    for current in probe.VERSIONS:
        row = probe.load_row(current)
        path = directory / f"{current}.csv"
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(row))
            writer.writeheader()
            writer.writerow(row)
        with path.open(encoding="utf-8", newline="") as handle:
            table = list(csv.reader(handle))
        if current == version:
            if fault == "duplicate_row":
                table.append(list(table[1]))
            elif fault == "contradictory_rows":
                old = list(table[1])
                old[table[0].index("date")] = "2026-09-29"
                table.insert(1, old)
            elif fault == "duplicate_header":
                table[0].append("version")
                table[1].append(table[1][table[0].index("version")])
            elif fault == "extra_cell":
                table[1].append("ignored-extra")
            elif fault == "missing_cell":
                table[1].pop()
            elif fault == "bad_revision":
                table[1][table[0].index("strategy_revision")] = "retired_revision"
            with path.open("w", encoding="utf-8", newline="") as handle:
                csv.writer(handle).writerows(table)
            if fault == "missing_file":
                path.rename(directory / f"{current}.csv.intentional_missing")
        cloud_log = probe.CLOUD / probe.VERSION_FOLDERS[current] / f"realtime_signal_{current.replace('.', '_')}_result.txt"
        result = directory / f"{current}.txt"
        result.write_bytes(cloud_log.read_bytes())
        args += ["--result", f"{current}={result}", "--signal-csv", f"{current}={path}", "--exit-code", f"{current}=0"]
        paths[current] = path
    if fault == "duplicate_spec":
        args += ["--signal-csv", f"v2.0={paths['v2.0']}"]
    elif fault == "missing_spec":
        index = args.index(f"v2.5={paths['v2.5']}")
        del args[index-1:index+1]
    elif fault == "extra_spec":
        args += ["--signal-csv", f"v2.6={paths['v2.0']}"]
    elif fault == "duplicate_result":
        args += ["--result", f"v2.0={directory / 'v2.0.txt'}"]
    elif fault == "realtime_test":
        args[args.index("--publication-mode") + 1] = "realtime"
    error = ""
    with patch.object(sys, "argv", args), patch.object(module, "now_bj", return_value=datetime(2026,10,7,23,0,tzinfo=probe.BJ)), patch.dict(os.environ, {"GITHUB_RUN_URL": ""}):
        try:
            module.main()
        except (ValueError, SystemExit) as exc:
            error = type(exc).__name__
    metadata = directory / "artifacts/metadata.json"
    meta = json.loads(metadata.read_text(encoding="utf-8")) if metadata.exists() else None
    return {"case": case, "error": error, "metadata": meta, "path": str(metadata)}


def allows_send(workflow, result):
    step = next(item for item in workflow["jobs"]["send"]["steps"] if item.get("name") == "Send Gmail")
    meta = result["metadata"]
    namespace = {
        "always": lambda: True, "true": True, "false": False,
        "inputs": SimpleNamespace(validation_only=False, delivery_test_id=TEST_ID),
        "steps": SimpleNamespace(delivery_gate=SimpleNamespace(outputs=SimpleNamespace(should_send="true")),
            digest=SimpleNamespace(outcome="failure" if result["error"] else "success", outputs=SimpleNamespace(status=meta["status"] if meta else "")),
            send_intent=SimpleNamespace(outcome="success"),
            publication_mode=SimpleNamespace(outputs=SimpleNamespace(mode="close_confirmed")),
            whole_delivery=SimpleNamespace(outputs=SimpleNamespace(validated="true"))),
    }
    return bool(eval(step["if"].replace("&&", " and ").replace("||", " or "), {"__builtins__": {}}, namespace))


def main():
    workflow_path = probe.AUTO / ".github/workflows/microcap-realtime-digest.yml"
    workflow = yaml.safe_load(workflow_path.read_text(encoding="utf-8"))
    before_path = probe.AUTO / ".codex_backups/20261007_230126/scripts/build_microcap_realtime_digest.py"
    modules = {"before": load_module("test_shape_before", before_path), "after": load_module("test_shape_after", probe.BUILDER)}
    cases = []
    for version in probe.VERSIONS:
        for fault in ("duplicate_row", "contradictory_rows", "duplicate_header", "extra_cell", "missing_cell", "bad_revision", "missing_file"):
            cases.append((f"{fault}_{version}", fault, version))
    for fault in ("control", "duplicate_spec", "missing_spec", "extra_spec", "duplicate_result", "realtime_test"):
        cases.append((fault, "" if fault == "control" else fault, "v2.0"))
    recorded = []
    for name, fault, version in cases:
        states = {phase: compose(module, name, fault, version) for phase, module in modules.items()}
        sent = {phase: allows_send(workflow, value) for phase, value in states.items()}
        assert sent["after"] is (name == "control"), (name, states, sent)
        if name == "control":
            meta = states["after"]["metadata"]
            assert meta["subject"].startswith("[发送验收测试][收盘确认]")
            assert "2026-09-30" in meta["subject"]
            assert "不作为今日交易指令" in meta["body"] and "历史收盘日报样本" in meta["body"]
            assert "## 今日结论" not in meta["body"]
        recorded.append({"name": name, "before_send_step_allows": sent["before"],
                         "after_send_step_allows": sent["after"],
                         "after_builder_error": states["after"]["error"],
                         "after_metadata_status": (states["after"]["metadata"] or {}).get("status"),
                         "after_metadata_path": states["after"]["path"]})
    steps = {step.get("name"): step for step in workflow["jobs"]["send"]["steps"]}
    cache_writes = [step for step in steps.values() if step.get("uses", "").startswith("actions/cache/save@")]
    assert len(cache_writes) == 2 and all("inputs.delivery_test_id == ''" in step["if"] for step in cache_writes)
    assert "inputs.delivery_test_id != '' && 'microcap-verified-state-validation'" in steps["Preserve validated state bundle for long-gap recovery"]["with"]["name"]
    assert "inputs.validation_only == true || inputs.delivery_test_id != ''" in steps["Preserve verified whole delivery for the next trading day"]["with"]["name"]
    report = {"scope": "independent_delivery_test_mode_real_artifact_shape_and_workflow_doublecheck_no_smtp",
              "builder_before_sha256": probe.hash_file(before_path), "builder_after_sha256": probe.hash_file(probe.BUILDER),
              "workflow_sha256": probe.hash_file(workflow_path), "cases": recorded,
              "cases_passed": len(recorded), "cases_failed": 0,
              "before_bad_cases_send_step_allows": sum(item["before_send_step_allows"] for item in recorded if item["name"] != "control"),
              "after_bad_cases_send_step_allows": 0, "test_artifact_cache_isolation_verified": True,
              "sent_email": False}
    (HERE / "test_mode_real_artifact_doublecheck.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"cases":len(recorded), "passed":len(recorded), "failed":0,
                      "before_unsafe_shape_cases":report["before_bad_cases_send_step_allows"], "after_unsafe_shape_cases":0}))


if __name__ == "__main__":
    main()
