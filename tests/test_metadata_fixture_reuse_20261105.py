"""Fixture reuse preserves all original corruption bytes and case isolation."""
import ast
import hashlib
import json
from pathlib import Path

import pytest

OLD_FUNCTION_SHA256 = "0195ddf92a32d1e9a5e0f32e4f02d717cc5df92361213d7d837dc8d8c2192f69"
NAME = "test_existing_unreadable_metadata_never_disappears_into_non_st"
CORRUPTIONS = ["invalid_json", "non_object", "invalid_intervals", "unreadable_path",
               "interval_not_object", "missing_start", "invalid_start", "invalid_end",
               "end_before_start", "numeric_start", "intraday_start", "timezone_end"]


def functions():
    current = Path(__file__).with_name("test_security_meta_fail_closed_20261015.py").read_text(encoding="utf8")
    original = current.replace("        payload = _metadata_payload(ns)\n", "", 1)
    original = original.replace("json.dumps({**payload, \"st_intervals\": None})",
                                "json.dumps({**_metadata_payload(ns), \"st_intervals\": None})", 1)
    original = original.replace("json.dumps({**payload, \"st_intervals\": intervals})",
                                "json.dumps({**_metadata_payload(ns), \"st_intervals\": intervals})", 1)
    old_function = next(n for n in ast.parse(original).body if isinstance(n, ast.FunctionDef) and n.name == NAME)
    assert hashlib.sha256(ast.get_source_segment(original, old_function).encode()).hexdigest() == OLD_FUNCTION_SHA256
    result = []
    for source in (original, current):
        function = next(n for n in ast.parse(source).body if isinstance(n, ast.FunctionDef) and n.name == NAME)
        function.decorator_list = []
        result.append(compile(ast.fix_missing_locations(ast.Module(body=[function], type_ignores=[])), "fixture-case", "exec"))
    return result


def invoke(code, directory, corruption, entry):
    payloads = []
    captured = []
    class InvalidMetadata(RuntimeError):
        pass
    def payload(ns):
        item = {"symbol": "600000", "meta_version": "fixture-equivalence", "st_notice_evidence_sha256": "a" * 64,
                "first_trade_date": "2026-09-02", "last_trade_date": "2026-09-03", "st_intervals": []}
        payloads.append(item)
        return item
    def reject(symbol):
        path = directory / (symbol + ".json")
        captured.append(None if path.is_dir() else path.read_bytes())
        raise InvalidMetadata(symbol)
    ns = {"load_security_meta": reject, "build_security_meta": reject,
          "SecurityMetaHistoryEvidenceUnresolved": InvalidMetadata}
    environment = {"json": json, "pytest": pytest, "_metadata_payload": payload}
    exec(code, environment)
    environment[NAME]((ns, directory), corruption, entry)
    return captured, payloads


@pytest.mark.parametrize("entry", ["metadata", "builder"])
@pytest.mark.parametrize("corruption", CORRUPTIONS)
def test_corruption_bytes_and_original_assertions_are_identical(tmp_path, corruption, entry):
    before, after = functions()
    old_bytes, old_payloads = invoke(before, tmp_path / "old", corruption, entry)
    new_bytes, new_payloads = invoke(after, tmp_path / "new", corruption, entry)
    assert new_bytes == old_bytes
    assert len(old_payloads) == (0 if corruption == "unreadable_path" else 9)
    assert len(new_payloads) == (0 if corruption == "unreadable_path" else 1)


def test_payload_is_private_to_each_case(tmp_path):
    _, after = functions()
    first_bytes, first = invoke(after, tmp_path / "first", "invalid_intervals", "metadata")
    first[0]["symbol"] = "modified-first-only"
    first[0]["st_intervals"].append({"start": "1900-01-01"})
    second_bytes, second = invoke(after, tmp_path / "second", "invalid_intervals", "metadata")
    assert first[0] is not second[0]
    assert first_bytes == second_bytes
    assert second[0]["symbol"] == "600000" and second[0]["st_intervals"] == []
