"""Second independent receipt tampering check; all API and archive inputs isolated."""
import hashlib
import importlib.util
import io
import json
import sys
import zipfile
from datetime import date, datetime, timezone
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml

HERE = Path(__file__).resolve().parent
PRODUCTION = Path(r"D:\动量策略\codex-daily-automation-probe-l7-sync")
sys.path.insert(0, str(PRODUCTION / "scripts"))
import restore_ic_im_v1_3_ledger as downloader
spec = importlib.util.spec_from_file_location("independent_receipt_gate", PRODUCTION / "scripts/check_microcap_delivery.py")
gate = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gate)
ID = "independent-receipt-20261007"
DAY = date(2026, 10, 7)


def documents():
    metadata = {"subject": "[发送验收测试] fixture", "body": "历史收盘样本", "status": "OK",
                "publication_mode": "close_confirmed", "signal_date": "2026-09-30", "delivery_test_id": ID}
    raw = json.dumps(metadata, ensure_ascii=False).encode("utf-8")
    receipt = {"schema_version": 1, "status": "smtp_accepted", "message_id": "<fixture@example.invalid>",
               "accepted_at": "2026-10-07T14:40:00+00:00", "metadata_sha256": hashlib.sha256(raw).hexdigest(),
               "subject_sha256": hashlib.sha256(metadata["subject"].encode("utf-8")).hexdigest(),
               "body_sha256": hashlib.sha256(metadata["body"].encode("utf-8")).hexdigest(),
               "sender": "sender@example.invalid", "recipients": ["recipient@example.invalid"]}
    return metadata, raw, receipt


def verify(raw, receipt, *, workflow=".github/workflows/microcap-realtime-digest.yml", smtp_step="success", duplicate=False):
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("metadata.json", raw)
        if receipt is not None:
            archive.writestr("smtp-accepted.json", json.dumps(receipt))
        if duplicate:
            archive.writestr("artifacts/metadata.json", raw)
    run = {"path": workflow, "head_sha": "1" * 40}
    jobs = {"jobs": [{"name": "send", "steps": [
        {"name": "Send Gmail", "conclusion": smtp_step},
        {"name": "Preserve accepted SMTP receipt", "conclusion": "success"}]}]}
    artifact = {"id": 101, "workflow_run": {"id": 51}, "archive_download_url": "https://api.github.com/repos/fixture/repo/actions/artifacts/101/zip"}
    with patch.object(gate.urllib.request, "urlopen", side_effect=[io.BytesIO(json.dumps(x).encode("utf-8")) for x in (run, jobs)]), patch.object(downloader, "download", return_value=buffer.getvalue()):
        gate.verify_smtp_receipt("fixture/repo", "fixture-only", "https://api.github.com", artifact, DAY, "close_confirmed", ID)


def test_intact_traceable_receipt_control():
    _, raw, receipt = documents()
    verify(raw, receipt)


@pytest.mark.parametrize("fault", ["old_metadata_only", "body_bytes", "metadata_whitespace", "metadata_hash", "subject_hash", "body_hash", "wrong_workflow", "skipped_smtp", "duplicate_metadata", "empty_message_id", "wrong_test_id", "wrong_mode", "future_signal", "naive_accepted_time"])
def test_tampering_and_unproved_transport_are_rejected(fault):
    metadata, raw, receipt = documents()
    options = {}
    if fault == "old_metadata_only":
        receipt = None
    elif fault == "body_bytes":
        raw = raw.replace("历史收盘样本".encode("utf-8"), "修改后样本".encode("utf-8"))
    elif fault == "metadata_whitespace":
        raw += b"\n"
    elif fault == "metadata_hash":
        receipt["metadata_sha256"] = "0" * 64
    elif fault in {"subject_hash", "body_hash"}:
        receipt[fault.replace("hash", "sha256")] = "0" * 64
    elif fault == "wrong_workflow":
        options["workflow"] = "unrelated.yml"
    elif fault == "skipped_smtp":
        options["smtp_step"] = "skipped"
    elif fault == "duplicate_metadata":
        options["duplicate"] = True
    elif fault == "empty_message_id":
        receipt["message_id"] = ""
    elif fault in {"wrong_test_id", "wrong_mode", "future_signal"}:
        key, value = {"wrong_test_id": ("delivery_test_id", "other-test-20261007"), "wrong_mode": ("publication_mode", "realtime"), "future_signal": ("signal_date", "2026-10-08")}[fault]
        metadata[key] = value
        raw = json.dumps(metadata, ensure_ascii=False).encode("utf-8")
        receipt["metadata_sha256"] = hashlib.sha256(raw).hexdigest()
    elif fault == "naive_accepted_time":
        receipt["accepted_at"] = "2026-10-07T14:40:00"
    with pytest.raises(RuntimeError):
        verify(raw, receipt, **options)


@pytest.mark.parametrize("field,value", [
    ("schema_version", 999), ("sender", {"address": "sender@example.invalid"}),
    ("sender", "not-an-email"), ("recipients", "recipient@example.invalid"),
    ("recipients", [None]), ("recipients", ["not-an-email"]),
    ("accepted_at", "2037-10-07T14:40:00+00:00"),
    ("accepted_at", "1999-10-07T14:40:00+00:00"),
])
def test_malformed_receipt_and_impossible_acceptance_time_must_be_rejected(field, value):
    _, raw, receipt = documents()
    receipt[field] = value
    with pytest.raises(RuntimeError):
        verify(raw, receipt)


def test_independent_namespace_unknown_intent_does_not_resend(monkeypatch):
    marker = gate.test_delivery_marker_name(ID, "close_confirmed")
    queried = []
    def fetch(repo, token, name, api):
        queried.append(name)
        return {"artifacts": [{"name": name, "expired": False}]} if name.endswith("-send-intent") else {"artifacts": []}
    monkeypatch.setattr(gate, "fetch_artifacts", fetch)
    monkeypatch.setattr(gate, "now_utc", lambda: datetime(2026, 10, 7, 14, 40, tzinfo=timezone.utc))
    monkeypatch.setattr(sys, "argv", ["gate", "--publication-mode", "close_confirmed", "--delivery-test-id", ID, "--repository", "fixture/repo", "--token", "fixture-only"])
    output = []
    monkeypatch.setattr(gate, "write_outputs", output.append)
    with pytest.raises(RuntimeError, match="uncertain"):
        gate.main()
    assert not output
    assert all("delivery-test" in name and "digest-delivered" not in name for name in queried)
    assert queried.count(marker + "-send-intent") == 1


def test_normal_manual_holiday_gate_is_closed():
    from types import SimpleNamespace
    doc = yaml.safe_load((PRODUCTION / ".github/workflows/microcap-realtime-digest.yml").read_text(encoding="utf-8"))
    expression = doc["jobs"]["send"]["if"].replace("needs.check-trading-day", "needs.calendar")
    namespace = {"always": lambda: True, "true": True, "github": SimpleNamespace(event_name="workflow_dispatch"),
                 "inputs": SimpleNamespace(external_schedule=False, validation_only=False, correction=False, delivery_test_id=""),
                 "needs": SimpleNamespace(regression=SimpleNamespace(result="success"), calendar=SimpleNamespace(result="success", outputs=SimpleNamespace(SHOULD_RUN_MICROCAP="false")))}
    assert eval(expression.replace("&&", " and ").replace("||", " or "), {"__builtins__": {}}, namespace) is False


def test_validation_still_independently_blocks_all_mail_marker_writes():
    doc = yaml.safe_load((PRODUCTION / ".github/workflows/microcap-realtime-digest.yml").read_text(encoding="utf-8"))
    names = {"Recover delivery marker from accepted SMTP receipt", "Mark recovered digest delivered", "Persist normal-send intent before SMTP", "Send Gmail", "Preserve accepted SMTP receipt", "Prepare delivery marker", "Mark digest delivered"}
    steps = {step.get("name"): step for step in doc["jobs"]["send"]["steps"]}
    for name in names:
        assert steps[name]["if"].startswith("inputs.validation_only != true && ")
