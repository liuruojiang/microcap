"""Read-only fault fixtures for the two mail audit findings. Never opens SMTP."""
from __future__ import annotations

import hashlib
import io
import json
import sys
import zipfile
from datetime import date, datetime, timezone
from pathlib import Path
from unittest.mock import patch

AUTOMATION = Path(r"D:\Codex\workspaces\microcap-no-overheat-daily")
sys.path.insert(0, str(AUTOMATION / "scripts"))
import check_microcap_delivery as gate
import restore_ic_im_v1_3_ledger as download_helper


def cross_midnight_receipt() -> dict:
    metadata = {"status": "OK", "publication_mode": "close_confirmed",
                "signal_date": "2026-11-02", "subject": "close", "body": "frozen"}
    raw = json.dumps(metadata).encode()
    receipt = {
        "schema_version": 1, "status": "smtp_accepted",
        "message_id": "<fixture@example.test>",
        "accepted_at": "2026-11-02T16:05:00+00:00",
        "metadata_sha256": hashlib.sha256(raw).hexdigest(),
        "subject_sha256": hashlib.sha256(b"close").hexdigest(),
        "body_sha256": hashlib.sha256(b"frozen").hexdigest(),
        "sender": "a@example.test", "recipients": ["b@example.test"],
    }
    payload = io.BytesIO()
    with zipfile.ZipFile(payload, "w") as archive:
        archive.writestr("metadata.json", raw)
        archive.writestr("smtp-accepted.json", json.dumps(receipt))
    artifact = {"id": 7, "workflow_run": {"id": 42},
                "archive_download_url": "https://api.github.com/repos/owner/repo/actions/artifacts/7/zip"}
    run = {"path": ".github/workflows/microcap-realtime-digest.yml"}
    jobs = {"jobs": [{"name": "send", "steps": [
        {"name": "Send Gmail", "conclusion": "success"},
        {"name": "Preserve accepted SMTP receipt", "conclusion": "success"}]}]}
    responses = [io.BytesIO(json.dumps(value).encode()) for value in (run, jobs)]
    with patch.object(gate, "now_utc", return_value=datetime(2026, 11, 3, 1, tzinfo=timezone.utc)), \
            patch.object(gate.urllib.request, "urlopen", side_effect=responses), \
            patch.object(download_helper, "download", return_value=payload.getvalue()):
        try:
            gate.verify_smtp_receipt("owner/repo", "fixture", "https://api.github.com",
                                     artifact, date(2026, 11, 2), "close_confirmed")
        except RuntimeError as error:
            assert str(error) == "BLOCKED: SMTP receipt dates do not match delivery", error
            return {"finding": "cross_midnight_receipt", "reproduced": True, "result": str(error)}
    raise AssertionError("Expected the audited cross-midnight rejection; implementation may have changed")


def recovery_intent_invisible() -> dict:
    recovery_intent = "microcap-close-recovery-20261102-send-intent"
    queried = []
    output = []

    def fetch(repository, token, name, api_url):
        queried.append(name)
        return {"artifacts": [{"name": recovery_intent, "expired": False}]} \
            if name == recovery_intent else {"artifacts": []}

    with patch.object(sys, "argv", ["gate", "--repository", "owner/repo", "--token", "fixture",
                                    "--publication-mode", "close_confirmed"]), \
            patch.object(gate, "now_utc", return_value=datetime(2026, 11, 2, 10, tzinfo=timezone.utc)), \
            patch.object(gate, "fetch_artifacts", side_effect=fetch), \
            patch.object(gate, "write_outputs", side_effect=output.append):
        assert gate.main() == 0
    assert output[0]["should_send"] == "true", output
    assert recovery_intent not in queried, queried
    return {"finding": "recovery_intent_invisible_to_normal_gate", "reproduced": True,
            "should_send": output[0]["should_send"], "queried_names": queried}


if __name__ == "__main__":
    print(json.dumps({"fixtures_only": True, "smtp_calls": 0,
                      "results": [cross_midnight_receipt(), recovery_intent_invisible()]},
                     ensure_ascii=False, indent=2))
