"""Independent transport fault injection against preserved production source.

All credentials, SMTP clients and GitHub responses below are explicit fixtures.
No SMTP, GitHub write, strategy refresh or primary-output mutation is possible.
"""
from __future__ import annotations

import json
import os
import re
import smtplib
import sys
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import yaml

HERE = Path(__file__).resolve().parent
SNAPSHOT = HERE / "source_snapshot"
sys.path.insert(0, str(SNAPSHOT))
import check_mail_transport as probe
import check_microcap_delivery as gate
import mail_utils


def fixture_environment(monkeypatch, tls):
    for key, value in {
        "MAIL_SERVER": "smtp.invalid",
        "MAIL_PORT": "465" if tls == "ssl" else "587",
        "MAIL_USERNAME": "fixture-user",
        "MAIL_PASSWORD": "fixture-only",
        "MAIL_FROM": "sender@example.invalid",
        "MAIL_TO": "first@example.invalid,second@example.invalid",
        "MAIL_USE_SSL": "true" if tls == "ssl" else "false",
    }.items():
        monkeypatch.setenv(key, value)


def smtp_fixture(monkeypatch):
    smtp = MagicMock()
    smtp.__enter__.return_value = smtp
    smtp.send_message.return_value = {}
    monkeypatch.setattr(mail_utils.smtplib, "SMTP_SSL", lambda *a, **k: smtp)
    monkeypatch.setattr(mail_utils.smtplib, "SMTP", lambda *a, **k: smtp)
    return smtp


@pytest.mark.parametrize("tls", ["ssl", "starttls"])
@pytest.mark.parametrize("fault", ["accepted", "login_refused", "all_refused", "partial_refused", "data_timeout", "data_rejected", "quit_timeout", "disconnected"])
def test_smtp_faults_never_retry_or_silently_accept_partial_delivery(monkeypatch, tls, fault):
    fixture_environment(monkeypatch, tls)
    smtp = smtp_fixture(monkeypatch)
    if fault == "login_refused":
        smtp.login.side_effect = smtplib.SMTPAuthenticationError(535, b"fixture rejected")
    elif fault == "all_refused":
        smtp.send_message.side_effect = smtplib.SMTPRecipientsRefused({"first@example.invalid": (550, b"fixture rejected")})
    elif fault == "partial_refused":
        smtp.send_message.return_value = {"second@example.invalid": (550, b"fixture rejected")}
    elif fault == "data_timeout":
        smtp.send_message.side_effect = TimeoutError("fixture ambiguous DATA acknowledgement")
    elif fault == "data_rejected":
        smtp.send_message.side_effect = smtplib.SMTPDataError(554, b"fixture rejected")
    elif fault == "quit_timeout":
        smtp.__exit__.side_effect = TimeoutError("fixture QUIT timeout after accepted DATA")
    elif fault == "disconnected":
        smtp.send_message.side_effect = smtplib.SMTPServerDisconnected("fixture disconnected")
    if fault == "accepted":
        assert mail_utils.send_mail("isolated fixture", "isolated body") is None
    else:
        with pytest.raises((smtplib.SMTPException, TimeoutError)):
            mail_utils.send_mail("isolated fixture", "isolated body")
    assert smtp.send_message.call_count == (0 if fault == "login_refused" else 1)
    assert smtp.login.call_count == 1
    assert smtp.starttls.call_count == (0 if tls == "ssl" else 1)


@pytest.mark.parametrize("state", ["empty", "marker", "receipt", "intent", "expired", "receipt_and_intent", "marker_and_intent", "validation", "correction"])
def test_gate_state_matrix(monkeypatch, state):
    marker = gate.delivery_marker_name(datetime(2026, 9, 30).date(), "close_confirmed")
    names = {
        "empty": [], "marker": [marker], "receipt": [marker + "-smtp-accepted"],
        "intent": [marker + "-send-intent"], "expired": [marker],
        "receipt_and_intent": [marker + "-smtp-accepted", marker + "-send-intent"],
        "marker_and_intent": [marker, marker + "-send-intent"],
        "validation": [marker + "-send-intent"], "correction": [marker],
    }[state]
    artifacts = [{"name": name, "expired": state == "expired"} for name in names]
    queried = []
    def fetch(repo, token, name, api):
        queried.append(name)
        return {"artifacts": [item for item in artifacts if item["name"] == name]}
    monkeypatch.setattr(gate, "fetch_artifacts", fetch)
    monkeypatch.setattr(gate, "now_utc", lambda: datetime(2026, 9, 30, 8, tzinfo=timezone.utc))
    argv = ["gate", "--publication-mode", "close_confirmed", "--repository", "fixture/repo", "--token", "fixture-only"]
    if state in {"validation", "correction"}:
        argv.append("--validation-only" if state == "validation" else "--correction")
    monkeypatch.setattr(sys, "argv", argv)
    outputs = []
    monkeypatch.setattr(gate, "write_outputs", outputs.append)
    if state == "intent":
        with pytest.raises(RuntimeError, match="BLOCKED.*uncertain"):
            gate.main()
        assert not outputs
    else:
        assert gate.main() == 0
        assert outputs[0]["should_send"] == str(state in {"empty", "expired", "validation", "correction"}).lower()
        assert outputs[0]["recover_marker"] == str(state in {"receipt", "receipt_and_intent"}).lower()
    if state in {"validation", "correction"}:
        assert not queried


@pytest.mark.parametrize("tls", ["ssl", "starttls"])
@pytest.mark.parametrize("refused", [False, True])
def test_readiness_is_envelope_only_and_resets_on_recipient_failure(monkeypatch, tls, refused):
    fixture_environment(monkeypatch, tls)
    smtp = smtp_fixture(monkeypatch)
    smtp.mail.return_value = (250, b"fixture accepted")
    smtp.rcpt.side_effect = [(250, b"fixture accepted"), (550 if refused else 250, b"fixture response")]
    smtp.rset.return_value = (250, b"fixture reset")
    if refused:
        with pytest.raises(RuntimeError, match="rejected recipient"):
            probe.probe()
    else:
        result = probe.probe()
        assert result["message_sent"] is False
        assert result["recipients_accepted"] == 2
    assert smtp.rset.call_count == 1
    assert smtp.send_message.call_count == 0
    assert smtp.data.call_count == 0


def test_invalid_publication_mode_is_rejected_without_github(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["gate", "--publication-mode", "unknown", "--validation-only"])
    monkeypatch.setattr(gate, "fetch_artifacts", lambda *a: pytest.fail("must reject before GitHub"))
    with pytest.raises(SystemExit) as result:
        gate.main()
    assert result.value.code == 2


@pytest.mark.xfail(strict=True, reason="Confirmed prior-workflow gap: normal manual dispatch bypasses holidays without correction or validation")
def test_normal_manual_holiday_dispatch_must_not_run_send_job():
    document = yaml.safe_load((SNAPSHOT / "microcap-realtime-digest.yml").read_text(encoding="utf-8"))
    expression = document["jobs"]["send"]["if"]
    expression = expression.replace("always()", "True")
    values = {"needs.regression.result": "'success'", "needs.check-trading-day.result": "'success'",
              "github.event_name": "'workflow_dispatch'", "inputs.external_schedule": "False",
              "needs.check-trading-day.outputs.SHOULD_RUN_MICROCAP": "'false'"}
    for key in sorted(values, key=len, reverse=True):
        expression = expression.replace(key, values[key])
    expression = re.sub(r"\btrue\b", "True", expression)
    expression = expression.replace("&&", " and ").replace("||", " or ")
    assert eval(expression, {"__builtins__": {}}, {}) is False


def test_receipt_provenance_gap_is_reproduced_without_assuming_real_delivery(monkeypatch):
    """The old source accepts a bare name; record this diagnostic evidence gap."""
    marker = gate.delivery_marker_name(datetime(2026, 9, 30).date(), "close_confirmed")
    unproven_receipt = {"name": marker + "-smtp-accepted", "expired": False,
                       "workflow_run": {"id": 999, "head_sha": "0" * 40}}
    monkeypatch.setattr(gate, "fetch_artifacts", lambda repo, token, name, api: {"artifacts": [unproven_receipt] if name.endswith("-smtp-accepted") else []})
    monkeypatch.setattr(gate, "now_utc", lambda: datetime(2026, 9, 30, 8, tzinfo=timezone.utc))
    monkeypatch.setattr(sys, "argv", ["gate", "--publication-mode", "close_confirmed", "--repository", "fixture/repo", "--token", "fixture-only"])
    outputs = []
    monkeypatch.setattr(gate, "write_outputs", outputs.append)
    assert gate.main() == 0
    assert outputs[0]["should_send"] == "false"
    assert outputs[0]["recover_marker"] == "true"
    (HERE / "unverified_receipt_reproduction.json").write_text(json.dumps({"source": "preserved production source", "smtp_sent": False, "fixture": unproven_receipt, "observed_output": outputs[0]}, indent=2) + "\n", encoding="utf-8")
