"""Read-only reconciliation of actual Gmail and GitHub receipt evidence.

No SMTP API is imported or called. Authentication stays in process memory.
"""
from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from datetime import datetime, timezone
from email.header import decode_header, make_header
from email.message import EmailMessage
from email.utils import getaddresses, parsedate_to_datetime
from pathlib import Path
from unittest.mock import patch

HERE = Path(__file__).resolve().parent
TASK = HERE.parent
AUTOMATION = Path(r"D:\动量策略\codex-daily-automation-probe-l7-sync")
sys.path.insert(0, str(AUTOMATION / "scripts"))
import check_microcap_delivery as gate

ID = "microcap-20261007-de6ce1a9-01"
REPOSITORY = "liuruojiang/codex-daily-automation-probe"


def read_json(path):
    return json.loads(path.read_text(encoding="utf-8"))


def sha(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def iso(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def decode_mime_header(value: str) -> str:
    return str(make_header(decode_header(value)))


def main() -> int:
    result = {"scope": "independent_actual_receipt_gmail_and_deduplication_readback",
              "checked_at_utc": datetime.now(timezone.utc).isoformat(),
              "external_smtp_submitted_by_this_agent": False}
    try:
        inbox = read_json(TASK / "inbox_message.json")
        run = read_json(TASK / "cloud_run.json")
        holiday = read_json(TASK / "holiday_run.json")
        artifact = next(a for a in read_json(TASK / "cloud_artifacts.json")["artifacts"] if a["id"] == 11494785698)
        folder = TASK / "cloud_artifacts_readback" / artifact["name"]
        metadata_path = folder / "metadata.json"
        receipt_path = folder / "smtp-accepted.json"
        metadata_raw = metadata_path.read_bytes()
        metadata = json.loads(metadata_raw.decode("utf-8"))
        receipt = read_json(receipt_path)
        headers = {h["name"].lower(): h["value"] for h in inbox["payload"]["headers"]}
        gmail_subject = decode_mime_header(headers["subject"])
        gmail_body = inbox["payload"]["body"]["content"]
        expected_body = metadata["body"]
        expected_mime = EmailMessage()
        expected_mime.set_content(expected_body)
        expected_mime_body = expected_mime.get_content()
        result["hashes"] = {
            "metadata_sha256": sha(metadata_raw),
            "metadata_subject_sha256": sha(metadata["subject"].encode("utf-8")),
            "metadata_body_sha256": sha(expected_body.encode("utf-8")),
            "gmail_body_exact_sha256": sha(gmail_body.encode("utf-8")),
            "gmail_body_lf_sha256": sha(gmail_body.replace("\r\n", "\n").encode("utf-8")),
            "expected_mime_body_sha256": sha(expected_mime_body.encode("utf-8")),
            "gate_source_sha256": sha((AUTOMATION / "scripts/check_microcap_delivery.py").read_bytes()),
        }
        assert "INBOX" in inbox["label_ids"]
        assert headers["message-id"] == receipt["message_id"]
        assert gmail_subject == metadata["subject"]
        assert result["hashes"]["metadata_sha256"] == receipt["metadata_sha256"]
        assert result["hashes"]["metadata_subject_sha256"] == receipt["subject_sha256"]
        assert result["hashes"]["metadata_body_sha256"] == receipt["body_sha256"]
        assert gmail_body.replace("\r\n", "\n") == expected_mime_body
        recipients = [address.casefold() for _, address in getaddresses([headers["to"]])]
        senders = [address.casefold() for _, address in getaddresses([headers["from"]])]
        assert recipients == [address.casefold() for address in receipt["recipients"]]
        assert senders == [receipt["sender"].casefold()]
        assert metadata["status"] == "OK"
        assert metadata["delivery_test_id"] == ID
        assert metadata["publication_mode"] == "close_confirmed"
        assert metadata["signal_date"] == "2026-09-30"
        assert metadata["subject"].startswith("[发送验收测试][收盘确认]")
        assert "不作为今日交易指令" in expected_body
        send_job = next(job for job in run["jobs"] if job["name"] == "send")
        steps = {step["name"]: step for step in send_job["steps"]}
        for name in ("Send Gmail", "Preserve accepted SMTP receipt", "Mark digest delivered"):
            assert steps[name]["conclusion"] == "success"
        smtp = steps["Send Gmail"]
        accepted_at = iso(receipt["accepted_at"])
        assert iso(smtp["startedAt"]) <= accepted_at <= iso(smtp["completedAt"])
        message_date = parsedate_to_datetime(headers["date"])
        assert iso(smtp["startedAt"]) <= message_date <= accepted_at
        internal_date = inbox["internal_date"]
        received_at = datetime.fromtimestamp(int(internal_date) / 1000, timezone.utc) if internal_date.isdigit() else iso(internal_date)
        assert abs((received_at - accepted_at).total_seconds()) <= 60
        assert run["databaseId"] == 37644093633
        assert run["status"] == "completed" and run["conclusion"] == "success"
        assert artifact["workflow_run"]["id"] == run["databaseId"]
        assert artifact["workflow_run"]["head_sha"] == run["headSha"]
        result["inbox"] = {"gmail_message_id": inbox["id"], "rfc_message_id": receipt["message_id"],
                           "inbox_label_confirmed": True, "subject_exact_match": True,
                           "body_mime_exact_match": True,
                           "body_comparison_normalization": "SMTP CRLF to LF; compare exactly to EmailMessage.set_content canonical MIME text, including mandatory terminal LF",
                           "original_metadata_body_characters": len(expected_body),
                           "delivered_mime_body_characters": len(gmail_body.replace("\r\n", "\n")),
                           "sender_and_recipients_match": True, "recipients_count": len(recipients),
                           "message_date_utc": message_date.isoformat(), "received_at_utc": received_at.isoformat(),
                           "accepted_at_utc": accepted_at.isoformat(), "receipt_within_smtp_step": True}
        result["workflow"] = {"run_id": run["databaseId"], "head_sha": run["headSha"],
                              "receipt_artifact_id": artifact["id"], "receipt_artifact_name": artifact["name"],
                              "send_gmail": smtp["conclusion"],
                              "receipt_upload": steps["Preserve accepted SMTP receipt"]["conclusion"],
                              "delivery_marker": steps["Mark digest delivered"]["conclusion"]}
        token = subprocess.run(["gh", "auth", "token"], capture_output=True, text=True, check=True).stdout.strip()
        assert token
        gate.verify_smtp_receipt(REPOSITORY, token, "https://api.github.com", artifact,
                                 gate.beijing_delivery_date(gate.now_utc()), "close_confirmed", ID)
        result["live_receipt_verifier"] = {"status": "ok", "artifact_id": artifact["id"],
                                           "scope": "actual GitHub run, jobs and downloadable artifact"}
        outputs = []
        args = ["gate", "--publication-mode", "close_confirmed", "--delivery-test-id", ID,
                "--repository", REPOSITORY, "--token", token]
        with patch.object(sys, "argv", args), patch.object(gate, "write_outputs", outputs.append):
            assert gate.main() == 0
        token = ""
        assert len(outputs) == 1
        assert outputs[0]["should_send"] == "false"
        assert outputs[0]["marker_name"] == gate.test_delivery_marker_name(ID, "close_confirmed")
        result["same_id_live_deduplication"] = outputs[0]
        holiday_jobs = {job["name"]: job for job in holiday["jobs"]}
        assert holiday["databaseId"] == 37644531206
        assert holiday["status"] == "completed" and holiday["conclusion"] == "success"
        assert holiday_jobs["check-trading-day"]["conclusion"] == "success"
        assert holiday_jobs["send"]["conclusion"] == "skipped"
        assert holiday_jobs["regression"]["conclusion"] == "skipped"
        result["actual_holiday_run"] = {"run_id": holiday["databaseId"], "head_sha": holiday["headSha"],
                                        "calendar": "success", "send": "skipped", "regression": "skipped",
                                        "smtp_submitted": False}
        result["input_file_sha256"] = {str(p.relative_to(TASK)): sha(p.read_bytes()) for p in
            [TASK / "inbox_message.json", TASK / "cloud_run.json", TASK / "cloud_artifacts.json", TASK / "holiday_run.json", metadata_path, receipt_path]}
        result["status"] = "PASS"
    except Exception as exc:
        result["status"] = "FAILED"
        result["exception_type"] = type(exc).__name__
    (HERE / "actual_receipt_verification.json").write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"status": result["status"], "external_smtp_submitted_by_this_agent": False,
                      "receipt_verified_live": "live_receipt_verifier" in result,
                      "inbox_matched": "inbox" in result,
                      "same_id_should_send": result.get("same_id_live_deduplication", {}).get("should_send"),
                      "holiday_send": result.get("actual_holiday_run", {}).get("send"),
                      "exception_type": result.get("exception_type")}, ensure_ascii=False))
    return 0 if result["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
