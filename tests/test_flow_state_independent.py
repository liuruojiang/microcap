"""Independent adversarial checks; generated inputs are local contract fixtures."""
import csv
import hashlib
import json

import pytest

from scripts import st_history_scope as scope
from scripts import st_notice_incremental as daily
from scripts.realtime_state_bundle import _read_csv_header_and_rows
from scripts.st_notice_evidence import StNoticeEvidenceError


def save(root, name, value):
    raw = json.dumps(value, ensure_ascii=False).encode("utf8")
    path = root / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(raw)
    return {"path": name, "sha256": hashlib.sha256(raw).hexdigest()}


def day_bundle(root, tag):
    day = "2026-10-01"
    response = save(root, "daily/page_0001.json", {
        "success": 1, "fixture_tag": tag,
        "data": {"total_hits": 0, "page_index": 1, "page_size": 100, "list": []},
    })
    receipt = save(root, "daily/page_0001.receipt.json", {
        "status": 200, "url": daily.API, "params": daily._params(day, 1),
        "sha256": response["sha256"], "collected_at": day + "T16:00:00+08:00",
        "completed_at": day + "T16:00:01+08:00",
    })
    return save(root, "daily/manifest.json", {
        "schema": daily.SCHEMA, "date": day,
        "pages": [{"response": "page_0001.json", "receipt": "page_0001.receipt.json",
                   "response_sha256": response["sha256"], "receipt_sha256": receipt["sha256"]}],
    })


def archive_for_day(root, reference):
    source_scope = save(root, "scope.json", {"source_files": []})
    return save(root, "archive.json", {"schema_version": 3, "scope": source_scope,
        "daily_increments": [{"date": "2026-10-01", "manifest": reference}]})


def test_daily_parent_and_source_closure_agree(tmp_path):
    day_ref = day_bundle(tmp_path, "A")
    archive = archive_for_day(tmp_path, day_ref)
    real = daily.verify_day(tmp_path / "daily", "2026-10-01")
    result = scope.source_closure(tmp_path, archive["path"], archive["sha256"])
    assert result["daily/manifest.json"] == day_ref["sha256"] == real["manifest_sha256"]
    for name, digest in real["source_files"].items():
        assert result["daily/" + name] == digest


def test_daily_manifest_substitution_cannot_mix_parent_and_page_identities(tmp_path, monkeypatch):
    expected = day_bundle(tmp_path, "A")
    old_manifest = (tmp_path / expected["path"]).read_bytes()
    archive = archive_for_day(tmp_path, expected)
    alternative = day_bundle(tmp_path, "B")
    assert alternative["sha256"] != expected["sha256"]
    real_verify = daily.verify_day

    def parse_then_restore(folder, day, **kwargs):
        # B is internally valid but is not the manifest authenticated by the parent.
        result = real_verify(folder, day, **kwargs)
        assert result["manifest_sha256"] == alternative["sha256"]
        (tmp_path / expected["path"]).write_bytes(old_manifest)
        return result

    monkeypatch.setattr(daily, "verify_day", parse_then_restore)
    with pytest.raises(StNoticeEvidenceError):
        scope.source_closure(tmp_path, archive["path"], archive["sha256"])


def test_daily_manifest_swap_after_authentication_is_rejected(tmp_path, monkeypatch):
    expected = day_bundle(tmp_path, "A")
    old_bytes = {p.name: p.read_bytes() for p in (tmp_path / "daily").iterdir()}
    archive = archive_for_day(tmp_path, expected)
    real_verify = daily.verify_day

    def swap_while_parsing(folder, day, **kwargs):
        alternative = day_bundle(tmp_path, "B")
        result = real_verify(folder, day, **kwargs)
        assert result["manifest_sha256"] == alternative["sha256"]
        (tmp_path / "daily/manifest.json").write_bytes(old_bytes["manifest.json"])
        return result

    monkeypatch.setattr(daily, "verify_day", swap_while_parsing)
    with pytest.raises(StNoticeEvidenceError):
        scope.source_closure(tmp_path, archive["path"], archive["sha256"])


def test_pointer_changed_during_discovery_rejected(tmp_path, monkeypatch):
    archive = save(tmp_path, "archive.json", {"schema_version": 2, "symbols": {}})
    save(tmp_path, scope.POINTER_PATH, {"manifest_path": archive["path"], "manifest_sha256": archive["sha256"]})
    real = scope.read_bound
    first = True

    def mutate(root, spec):
        nonlocal first
        data = real(root, spec)
        if spec["path"] == "archive.json" and first:
            first = False
            (tmp_path / scope.POINTER_PATH).write_bytes(b"{}")
        return data

    monkeypatch.setattr(scope, "read_bound", mutate)
    with pytest.raises(StNoticeEvidenceError):
        scope.source_closure(tmp_path)


def test_daily_result_hash_binds_parsed_manifest_not_later_replacement(tmp_path, monkeypatch):
    expected = day_bundle(tmp_path, "A")
    path = tmp_path / "daily/manifest.json"
    old_manifest = path.read_bytes()
    archive = archive_for_day(tmp_path, expected)
    real_verify = daily.verify_day
    real_read_text = type(path).read_text
    real_read_bytes = type(path).read_bytes
    in_verify = False
    raced = False

    def replace_after_parse_read(self):
        nonlocal raced
        if self == path and in_verify and not raced:
            raced = True
            path.write_bytes(old_manifest)

    def read_then_replace(self, *args, **kwargs):
        text = real_read_text(self, *args, **kwargs)
        replace_after_parse_read(self)
        return text

    def bytes_then_replace(self, *args, **kwargs):
        raw = real_read_bytes(self, *args, **kwargs)
        replace_after_parse_read(self)
        return raw

    def swap_for_real_parser(folder, day, **kwargs):
        nonlocal in_verify
        day_bundle(tmp_path, "B")
        in_verify = True
        try:
            return real_verify(folder, day, **kwargs)
        finally:
            in_verify = False

    monkeypatch.setattr(type(path), "read_text", read_then_replace)
    monkeypatch.setattr(type(path), "read_bytes", bytes_then_replace)
    monkeypatch.setattr(daily, "verify_day", swap_for_real_parser)
    with pytest.raises(StNoticeEvidenceError):
        scope.source_closure(tmp_path, archive["path"], archive["sha256"])
    assert raced, "The actual parser must experience the competing replacement"


def test_streaming_csv_keeps_all_records_and_exact_terminal_multiline_value(tmp_path):
    path = tmp_path / "large.csv"
    with path.open("w", encoding="utf-8-sig", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(["date", "value", "notes"])
        for i in range(25000):
            writer.writerow(["2026-10-01", i, "中间记录"])
        writer.writerow(["2026-11-04", 25000, '最后一行\r\n包含,"引号"'])
    header, count, last = _read_csv_header_and_rows(path)
    assert header == ["date", "value", "notes"]
    assert count == 25001
    assert last == {"date": "2026-11-04", "value": "25000", "notes": '最后一行\r\n包含,"引号"'}
