"""Test mail isolation and proof-backed recovery of an interrupted delivery."""
import hashlib
import io
import json
import sys
import zipfile
from datetime import date, datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT / "tests"))
import check_microcap_delivery as gate
import build_microcap_realtime_digest as digest
import restore_ic_im_v1_3_ledger as download_helper
import test_microcap_digest_email_body as digest_fixtures

TEST_ID = "microcap-20261007-acceptance"
DAY = date(2026, 10, 7)


def actions_eval(expression, *, holiday, manual=True, external=False,
                 validation=False, correction=False, test_id="", calendar="success"):
    namespace = {
        "always": lambda: True, "true": True, "false": False,
        "github": SimpleNamespace(event_name="workflow_dispatch" if manual else "schedule"),
        "inputs": SimpleNamespace(external_schedule=external, validation_only=validation,
                                  correction=correction, delivery_test_id=test_id),
        "needs": SimpleNamespace(
            regression=SimpleNamespace(result="success"),
            **{"check_trading_day": SimpleNamespace(result=calendar, outputs=SimpleNamespace(
                SHOULD_RUN_MICROCAP="false" if holiday else "true"))}),
    }
    expression = expression.replace("needs.check-trading-day", "needs.check_trading_day")
    return eval(expression.replace("&&", " and ").replace("||", " or "),
                {"__builtins__": {}}, namespace)


@pytest.mark.parametrize("options,expected", [
    ({"holiday": True}, False),
    ({"holiday": True, "manual": False}, False),
    ({"holiday": True, "external": True}, False),
    ({"holiday": True, "external": True, "validation": True}, False),
    ({"holiday": True, "external": True, "correction": True}, False),
    ({"holiday": True, "validation": True}, True),
    ({"holiday": True, "correction": True}, True),
    ({"holiday": True, "test_id": TEST_ID}, True),
    ({"holiday": False}, True),
    ({"holiday": False, "manual": False}, True),
    ({"holiday": True, "validation": True, "calendar": "failure"}, False),
])
def test_actual_workflow_only_explicit_maintenance_bypasses_holiday(options, expected):
    workflow = yaml.safe_load((ROOT / ".github/workflows/microcap-realtime-digest.yml").read_text(encoding="utf-8"))
    assert actions_eval(workflow["jobs"]["send"]["if"], **options) is expected


@pytest.mark.parametrize("value", ["", "short", "x\nBCC-secret", "x/../../a", "验收-20261007", "A" * 12, "a" * 65])
def test_invalid_test_namespace(value):
    with pytest.raises(ValueError):
        gate.test_delivery_marker_name(value, "close_confirmed")


def test_test_namespace_cannot_suppress_normal_daily_marker():
    marker = gate.test_delivery_marker_name(TEST_ID, "close_confirmed")
    assert marker != gate.delivery_marker_name(DAY, "close_confirmed")
    assert marker != gate.delivery_marker_name(DAY)
    assert gate.test_delivery_marker_name(TEST_ID, "close_confirmed") == marker


@pytest.mark.parametrize("extra", [["--correction"], ["--validation-only"], ["--publication-mode", "realtime"]])
def test_test_mode_conflicts_fail_before_network(extra):
    args = ["gate", "--publication-mode", "close_confirmed", "--delivery-test-id", TEST_ID] + extra
    with patch.object(sys, "argv", args), patch.object(gate, "fetch_artifacts") as network:
        with pytest.raises((ValueError, SystemExit)):
            gate.main()
    network.assert_not_called()


def test_test_intent_still_blocks_unknown_smtp():
    marker = gate.test_delivery_marker_name(TEST_ID, "close_confirmed")
    def fetch(_repository, _token, name, _api):
        return {"artifacts": [{"name": name, "expired": False}]} if name.endswith("-send-intent") else {"artifacts": []}
    args = ["gate", "--publication-mode", "close_confirmed", "--delivery-test-id", TEST_ID,
            "--repository", "owner/repo", "--token", "test-only"]
    with patch.object(sys, "argv", args), patch.object(gate, "fetch_artifacts", side_effect=fetch):
        with pytest.raises(RuntimeError, match="delivery is uncertain"):
            gate.main()
    assert "digest-delivered" not in marker


def receipt_documents():
    metadata = {"status": "OK", "publication_mode": "close_confirmed", "signal_date": "2026-09-30",
                "delivery_test_id": TEST_ID, "subject": "[发送验收测试] 样本", "body": "历史收盘验收样本"}
    raw = json.dumps(metadata, ensure_ascii=False).encode("utf-8")
    receipt = {"schema_version": 1, "status": "smtp_accepted", "message_id": "<verified@example.test>",
               "accepted_at": "2026-10-07T15:00:00+00:00",
               "metadata_sha256": hashlib.sha256(raw).hexdigest(),
               "subject_sha256": hashlib.sha256(metadata["subject"].encode("utf-8")).hexdigest(),
               "body_sha256": hashlib.sha256(metadata["body"].encode("utf-8")).hexdigest(),
               "sender": "sender@example.test", "recipients": ["recipient@example.test"]}
    return raw, receipt


def verify_fixture(metadata_raw, receipt, *, steps=True, workflow=True, duplicate=False):
    archive_bytes = io.BytesIO()
    with zipfile.ZipFile(archive_bytes, "w") as archive:
        archive.writestr("metadata.json", metadata_raw)
        if receipt is not None:
            archive.writestr("smtp-accepted.json", json.dumps(receipt))
        if duplicate:
            archive.writestr("artifacts/metadata.json", metadata_raw)
    artifact = {"id": 7, "workflow_run": {"id": 42},
                "archive_download_url": "https://api.github.com/repos/owner/repo/actions/artifacts/7/zip"}
    run = {"path": ".github/workflows/microcap-realtime-digest.yml" if workflow else "unrelated.yml"}
    jobs = {"jobs": [{"name": "send", "steps": [
        {"name": "Send Gmail", "conclusion": "success" if steps else "skipped"},
        {"name": "Preserve accepted SMTP receipt", "conclusion": "success"}]}]}
    responses = [io.BytesIO(json.dumps(value).encode("utf-8")) for value in (run, jobs)]
    with patch.object(gate, "now_utc", return_value=datetime(2026, 10, 7, 15, 0, 1, tzinfo=timezone.utc)), patch.object(gate.urllib.request, "urlopen", side_effect=responses), patch.object(
        download_helper, "download", return_value=archive_bytes.getvalue()):
        gate.verify_smtp_receipt("owner/repo", "test-only", "https://api.github.com",
                                 artifact, DAY, "close_confirmed", TEST_ID)


def test_matching_receipt_and_actual_smtp_step_are_required():
    raw, receipt = receipt_documents()
    verify_fixture(raw, receipt)


@pytest.mark.parametrize("field,value", [
    ("status", "intent"), ("message_id", ""), ("metadata_sha256", "0" * 64),
    ("subject_sha256", "0" * 64), ("body_sha256", "0" * 64),
    ("sender", ""), ("recipients", []), ("accepted_at", "2026-10-07T23:00:00"),
    ("schema_version", True), ("schema_version", 2), ("recipients", "recipient@example.test"),
    ("recipients", [None]), ("sender", {"address": "sender@example.test"}),
    ("sender", "not-an-address"), ("accepted_at", "2037-10-07T15:00:00+00:00"),
    ("accepted_at", "2026-09-29T15:00:00+00:00"),
])
def test_unverified_or_corrupt_receipt_cannot_recover_delivery(field, value):
    raw, receipt = receipt_documents()
    receipt[field] = value
    with pytest.raises(RuntimeError):
        verify_fixture(raw, receipt)


@pytest.mark.parametrize("options", [{"steps": False}, {"workflow": False}, {"duplicate": True}])
def test_unrelated_skipped_or_ambiguous_artifact_does_not_prove_smtp(options):
    raw, receipt = receipt_documents()
    with pytest.raises(RuntimeError):
        verify_fixture(raw, receipt, **options)


def test_old_metadata_only_receipt_is_unknown_without_formal_marker():
    raw, _ = receipt_documents()
    with pytest.raises(RuntimeError, match="content is missing"):
        verify_fixture(raw, None)


def test_same_named_empty_artifact_does_not_recover():
    with pytest.raises(RuntimeError, match="no verifiable workflow artifact"):
        gate.verify_smtp_receipt("owner/repo", "test-only", "https://api.github.com",
                                {"name": "smtp-accepted"}, DAY, "close_confirmed", TEST_ID)


def test_test_email_uses_all_three_final_csvs_and_explicit_historical_label(tmp_path):
    helper = digest_fixtures.MicrocapDigestEmailBodyTests()
    args = ["digest", "--out-dir", str(tmp_path / "mail"), "--publication-mode", "close_confirmed",
            "--delivery-test-id", TEST_ID]
    for version in ("v2.0", "v2.3", "v2.5"):
        row = helper.signal_fields(version)
        row.update(date="2026-09-30", official_close_confirmed_signal="True", signal_timing="close_confirmed")
        path = tmp_path / f"{version}.csv"
        helper.write_csv(path, row)
        result = tmp_path / f"{version}.txt"
        result.write_text("signal\n", encoding="utf-8")
        args += ["--result", f"{version}={result}", "--signal-csv", f"{version}={path}", "--exit-code", f"{version}=0"]
    with patch.object(sys, "argv", args):
        assert digest.main() == 0
    metadata = json.loads((tmp_path / "mail/metadata.json").read_text(encoding="utf-8"))
    assert metadata["status"] == "OK"
    assert metadata["signal_date"] == "2026-09-30"
    assert metadata["delivery_test_id"] == TEST_ID
    assert metadata["subject"].startswith("[发送验收测试][收盘确认]")
    assert "[下个交易日需操作]" not in metadata["subject"]
    assert "不作为今日交易指令" in metadata["body"]
    assert "验收编号：" + TEST_ID in metadata["body"]
