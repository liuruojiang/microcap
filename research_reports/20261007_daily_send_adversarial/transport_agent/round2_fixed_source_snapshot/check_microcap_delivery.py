from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import re
import urllib.parse
import urllib.request
import zipfile
from datetime import date, datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo


BJ = ZoneInfo("Asia/Shanghai")


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def beijing_delivery_date(value: datetime) -> date:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(BJ).date()


def delivery_marker_name(value: date, publication_mode: str | None = None) -> str:
    if publication_mode is None:
        return f"microcap-realtime-digest-delivered-{value.isoformat()}"
    if publication_mode not in {"realtime", "close_confirmed"}:
        raise ValueError(f"unsupported publication mode: {publication_mode}")
    return f"microcap-v2-{publication_mode}-digest-delivered-{value.isoformat()}"


def test_delivery_marker_name(test_id: str, publication_mode: str) -> str:
    if re.fullmatch(r"[a-z0-9][a-z0-9-]{7,63}", test_id, flags=re.ASCII) is None:
        raise ValueError("delivery test ID must contain 8-64 lowercase ASCII letters, digits or hyphens")
    if publication_mode != "close_confirmed":
        raise ValueError("delivery tests require close-confirmed publication")
    # Reusing an ID on another day must still suppress the same acceptance mail.
    return f"microcap-v2-{publication_mode}-delivery-test-{test_id}"


def marker_exists(payload: dict[str, object], marker_name: str) -> bool:
    artifacts = payload.get("artifacts", [])
    if not isinstance(artifacts, list):
        return False
    return any(
        isinstance(item, dict)
        and item.get("name") == marker_name
        and item.get("expired") is not True
        for item in artifacts
    )


def should_send(*, correction: bool, marker_already_exists: bool) -> bool:
    return correction or not marker_already_exists


def legacy_marker_mode(repository: str, token: str, api_url: str, artifact: dict, day: date) -> str:
    """Old date-only markers require their own run's successful metadata as proof."""
    from restore_ic_im_v1_3_ledger import api_request, download
    run_id = (artifact.get("workflow_run") or {}).get("id")
    if not isinstance(run_id, int) or run_id <= 0:
        raise RuntimeError("BLOCKED: legacy marker has no verifiable workflow run")
    base = f"{api_url.rstrip('/')}/repos/{repository}/actions"
    with urllib.request.urlopen(api_request(f"{base}/runs/{run_id}/artifacts?per_page=100", token), timeout=30) as response:
        payload = json.loads(response.read().decode("utf-8"))
    reports = [item for item in payload.get("artifacts", []) if isinstance(item, dict)
               and item.get("name") == "microcap-realtime-digest" and item.get("expired") is not True]
    if len(reports) != 1:
        raise RuntimeError("BLOCKED: legacy marker has no unique, nonexpired digest artifact")
    report = reports[0]
    if report.get("archive_download_url") != f"{base}/artifacts/{report.get('id')}/zip":
        raise RuntimeError("BLOCKED: unexpected legacy digest artifact URL")
    with zipfile.ZipFile(io.BytesIO(download(report, token))) as archive:
        entries = [item for item in archive.infolist() if item.filename in {"artifacts/metadata.json", "metadata.json"}]
        if len(entries) != 1 or entries[0].file_size > 1024 * 1024:
            raise RuntimeError("BLOCKED: legacy digest metadata missing, ambiguous or too large")
        meta = json.loads(archive.read(entries[0]).decode("utf-8"))
    if (not isinstance(meta, dict) or meta.get("status") != "OK"
            or meta.get("signal_date") != day.isoformat()
            or meta.get("publication_mode") not in {"realtime", "close_confirmed"}):
        raise RuntimeError("BLOCKED: legacy digest date/status/mode cannot be verified")
    return meta["publication_mode"]


def fetch_artifacts(
    repository: str,
    token: str,
    marker_name: str,
    api_url: str = "https://api.github.com",
) -> dict[str, object]:
    quoted_name = urllib.parse.quote(marker_name, safe="")
    url = f"{api_url.rstrip('/')}/repos/{repository}/actions/artifacts?name={quoted_name}&per_page=100"
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "microcap-delivery-gate",
        },
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        payload = json.loads(response.read().decode("utf-8"))
    if not isinstance(payload, dict) or not isinstance(payload.get("artifacts"), list):
        raise RuntimeError("GitHub artifacts response must contain an artifacts list")
    return payload


def verify_smtp_receipt(repository: str, token: str, api_url: str, artifact: dict,
                        day: date, publication_mode: str, test_id: str = "") -> None:
    """A matching artifact name alone cannot prove that SMTP accepted a digest."""
    from restore_ic_im_v1_3_ledger import api_request, download

    base = f"{api_url.rstrip('/')}/repos/{repository}/actions"
    run_id = (artifact.get("workflow_run") or {}).get("id")
    artifact_id = artifact.get("id")
    if (not isinstance(run_id, int) or run_id <= 0 or not isinstance(artifact_id, int)
            or artifact_id <= 0 or artifact.get("archive_download_url") != f"{base}/artifacts/{artifact_id}/zip"):
        raise RuntimeError("BLOCKED: SMTP receipt has no verifiable workflow artifact")
    with urllib.request.urlopen(api_request(f"{base}/runs/{run_id}", token), timeout=30) as response:
        run = json.loads(response.read().decode("utf-8"))
    if run.get("path") != ".github/workflows/microcap-realtime-digest.yml":
        raise RuntimeError("BLOCKED: SMTP receipt belongs to an unexpected workflow")
    with urllib.request.urlopen(api_request(f"{base}/runs/{run_id}/jobs?per_page=100", token), timeout=30) as response:
        jobs = json.loads(response.read().decode("utf-8"))
    send_jobs = [job for job in jobs.get("jobs", []) if job.get("name") == "send"]
    if len(send_jobs) != 1:
        raise RuntimeError("BLOCKED: SMTP receipt has no unique send job")
    steps = {step.get("name"): step.get("conclusion") for step in send_jobs[0].get("steps", [])}
    if any(steps.get(name) != "success" for name in ("Send Gmail", "Preserve accepted SMTP receipt")):
        raise RuntimeError("BLOCKED: workflow did not confirm SMTP acceptance and receipt upload")
    with zipfile.ZipFile(io.BytesIO(download(artifact, token))) as archive:
        values = {}
        for basename, limit in (("metadata.json", 1024 * 1024), ("smtp-accepted.json", 64 * 1024)):
            matches = [entry for entry in archive.infolist()
                       if entry.filename in {basename, f"artifacts/{basename}"}]
            if len(matches) != 1 or matches[0].file_size > limit:
                raise RuntimeError("BLOCKED: SMTP receipt content is missing, ambiguous or too large")
            raw = archive.read(matches[0])
            values[basename] = (raw, json.loads(raw.decode("utf-8")))
    metadata_raw, metadata = values["metadata.json"]
    _, receipt = values["smtp-accepted.json"]
    if not isinstance(metadata, dict) or not isinstance(receipt, dict):
        raise RuntimeError("BLOCKED: invalid SMTP receipt document")
    if (metadata.get("status") != "OK" or metadata.get("publication_mode") != publication_mode
            or metadata.get("delivery_test_id", "") != test_id):
        raise RuntimeError("BLOCKED: SMTP receipt metadata identity does not match delivery")
    try:
        signal_day = date.fromisoformat(metadata.get("signal_date", ""))
        accepted_at = datetime.fromisoformat(receipt.get("accepted_at", ""))
    except (TypeError, ValueError) as exc:
        raise RuntimeError("BLOCKED: invalid SMTP receipt dates") from exc
    if (signal_day > day or (not test_id and signal_day != day) or accepted_at.tzinfo is None
            or signal_day.isoformat() != metadata.get("signal_date")
            or beijing_delivery_date(accepted_at) < signal_day
            or accepted_at > now_utc()
            or (not test_id and beijing_delivery_date(accepted_at) != day)):
        raise RuntimeError("BLOCKED: SMTP receipt dates do not match delivery")
    def valid_address(value: object) -> bool:
        return (isinstance(value, str) and value.count("@") == 1
                and all(value.split("@")) and not any(char.isspace() for char in value)
                and not any(char in value for char in "<>,;"))

    recipients = receipt.get("recipients")
    if (receipt.get("status") != "smtp_accepted"
            or type(receipt.get("schema_version")) is not int or receipt.get("schema_version") != 1
            or re.fullmatch(r"<[^\s<>]+@[^\s<>]+>", str(receipt.get("message_id", ""))) is None
            or receipt.get("metadata_sha256") != hashlib.sha256(metadata_raw).hexdigest()
            or receipt.get("subject_sha256") != hashlib.sha256(str(metadata.get("subject", "")).encode("utf-8")).hexdigest()
            or receipt.get("body_sha256") != hashlib.sha256(str(metadata.get("body", "")).encode("utf-8")).hexdigest()
            or not valid_address(receipt.get("sender")) or not isinstance(recipients, list)
            or not recipients or not all(valid_address(item) for item in recipients)):
        raise RuntimeError("BLOCKED: SMTP receipt lacks a matching acceptance record")


def write_outputs(values: dict[str, str]) -> None:
    rendered = "".join(f"{key}={value}\n" for key, value in values.items())
    output_path = os.environ.get("GITHUB_OUTPUT", "").strip()
    if output_path:
        with Path(output_path).open("a", encoding="utf-8") as handle:
            handle.write(rendered)
    else:
        print(rendered, end="")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--correction", action="store_true")
    parser.add_argument("--validation-only", action="store_true")
    parser.add_argument("--delivery-test-id", default="")
    parser.add_argument("--publication-mode", choices=("realtime", "close_confirmed"), default="realtime")
    parser.add_argument("--repository", default=os.environ.get("GITHUB_REPOSITORY", ""))
    parser.add_argument("--token", default=os.environ.get("GITHUB_TOKEN", ""))
    parser.add_argument("--api-url", default=os.environ.get("GITHUB_API_URL", "https://api.github.com"))
    args = parser.parse_args()

    if args.delivery_test_id and (args.correction or args.validation_only):
        parser.error("delivery test cannot be combined with correction or validation-only")

    delivery_date = beijing_delivery_date(now_utc())
    marker_name = (test_delivery_marker_name(args.delivery_test_id, args.publication_mode)
                   if args.delivery_test_id else delivery_marker_name(delivery_date, args.publication_mode))
    marker_already_exists = False
    recover_marker = False
    # Validation exercises the same downstream build route, but the workflow
    # independently disables every mail/receipt/intent/marker write step.
    if not args.correction and not args.validation_only:
        if not args.repository or not args.token:
            raise SystemExit("GITHUB_REPOSITORY and GITHUB_TOKEN are required for scheduled delivery checks")
        payload = fetch_artifacts(args.repository, args.token, marker_name, args.api_url)
        marker_already_exists = marker_exists(payload, marker_name)
        if not marker_already_exists and not args.delivery_test_id:
            legacy_name = delivery_marker_name(delivery_date)
            if marker_name != legacy_name:
                legacy = fetch_artifacts(args.repository, args.token, legacy_name, args.api_url)
                matches = [item for item in legacy.get("artifacts", []) if isinstance(item, dict)
                           and item.get("name") == legacy_name and item.get("expired") is not True]
                for item in matches:
                    if legacy_marker_mode(args.repository, args.token, args.api_url, item, delivery_date) == args.publication_mode:
                        marker_already_exists = True
                        break
        if not marker_already_exists:
            receipt_name = marker_name + "-smtp-accepted"
            receipts = fetch_artifacts(args.repository, args.token, receipt_name, args.api_url)
            if marker_exists(receipts, receipt_name):
                matches = [item for item in receipts["artifacts"]
                           if item.get("name") == receipt_name and item.get("expired") is not True]
                for item in matches:
                    verify_smtp_receipt(args.repository, args.token, args.api_url, item,
                                        delivery_date, args.publication_mode, args.delivery_test_id)
                marker_already_exists = True
                recover_marker = True
        if not marker_already_exists:
            intent_name = marker_name + "-send-intent"
            pending = fetch_artifacts(args.repository, args.token, intent_name, args.api_url)
            if marker_exists(pending, intent_name):
                raise RuntimeError("BLOCKED: prior SMTP send intent has no completion marker; delivery is uncertain, reconcile Gmail before explicit correction")

    send = should_send(
        correction=args.correction,
        marker_already_exists=marker_already_exists,
    )
    write_outputs(
        {
            "should_send": str(send).lower(),
            "delivery_date": delivery_date.isoformat(),
            "marker_name": marker_name,
            "subject_prefix": ("发送验收测试" if args.delivery_test_id else
                               "验收不发送" if args.validation_only else ("纠正版" if args.correction else "")),
            "validation_only": str(args.validation_only).lower(),
            "delivery_test_id": args.delivery_test_id,
            "recover_marker": str(recover_marker).lower(),
        }
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
