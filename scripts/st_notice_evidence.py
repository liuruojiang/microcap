"""Read-only, exact-source bindings for reviewed CNInfo ST notice events.

This module authenticates the reviewed registry against archived bytes and the
provider's notice identity. It does not infer decisions from PDFs, fetch data,
change the notice-date policy, or accept actions supplied by notice rows.
"""
from __future__ import annotations

from datetime import date, datetime
import hashlib
import json
from pathlib import Path
import re
from typing import Any
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo

RULES_REVISION = "st-body-evidence-20261029-v1"
SCHEMA_VERSION = 1
DATE_POLICY = "notice_date_exclusion_inclusive_exit_unchanged"
RISK_CATEGORIES = frozenset({"delisting", "other", "unspecified"})


class StNoticeEvidenceError(ValueError):
    """Source evidence is absent, malformed, conflicting, or mismatched."""


def _fail(message: str) -> None:
    raise StNoticeEvidenceError(message)


def _day(value: Any, *, strict: bool = False) -> str:
    """Normalize provider timestamps to their Shanghai publication calendar day.

    Naive timestamps are already Shanghai local time; aware values are converted
    before extracting the date. Registry dates remain strict ISO dates.
    """
    if strict:
        if not isinstance(value, str) or re.fullmatch(r"\d{4}-\d{2}-\d{2}", value) is None:
            _fail("Evidence date must be an ISO calendar date")
        return date.fromisoformat(value).isoformat()
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            value = value.astimezone(ZoneInfo("Asia/Shanghai"))
        return date.fromisoformat(value.date().isoformat()).isoformat()
    if isinstance(value, date):
        return date.fromisoformat(value.isoformat()).isoformat()
    if not isinstance(value, str):
        _fail("Notice date is missing or malformed")
    return _day(datetime.fromisoformat(value.replace("Z", "+00:00")))


def _text(record: dict, key: str) -> str:
    value = record.get(key)
    if not isinstance(value, str) or not value or value != value.strip():
        _fail(f"Evidence field {key} is missing or malformed")
    return value


def _registry(root: Path | str) -> tuple[dict[str, dict], str]:
    directory = (Path(root) / "data" / "st_notice_evidence").resolve()
    registry_path = directory / "registry.json"
    if registry_path.resolve().parent != directory:
        _fail("Registry path escapes the evidence directory")
    raw = registry_path.read_bytes()
    payload = json.loads(raw.decode("utf-8-sig"))
    if not isinstance(payload, dict):
        _fail("Evidence registry must be an object")
    if type(payload.get("schema_version")) is not int or payload["schema_version"] != SCHEMA_VERSION:
        _fail("Unsupported evidence schema_version")
    if payload.get("rules_revision") != RULES_REVISION:
        _fail("Unsupported evidence rules_revision")
    if payload.get("date_policy") != DATE_POLICY:
        _fail("Evidence date policy differs from the approved notice-date policy")
    events = payload.get("events")
    if not isinstance(events, list) or not events:
        _fail("Evidence registry requires events")
    verified: dict[str, dict] = {}
    for record in events:
        if not isinstance(record, dict):
            _fail("Evidence event must be an object")
        symbol = _text(record, "symbol")
        event_id = _text(record, "announcement_id")
        if re.fullmatch(r"\d{6}", symbol) is None or re.fullmatch(r"\d+", event_id) is None:
            _fail("Evidence symbol or announcement_id is malformed")
        notice_date = _day(record.get("notice_date"), strict=True)
        _text(record, "title")
        source_url = _text(record, "source_url")
        url = urlsplit(source_url)
        if (url.scheme not in {"https", "http"} or url.hostname not in {"static.cninfo.com.cn", "www.cninfo.com.cn"}
                or url.username is not None or url.password is not None or url.fragment or url.query):
            _fail("Evidence source_url is not an authenticated CNInfo source")
        expected_path = rf"/finalpage/{re.escape(notice_date)}/{re.escape(event_id)}\.(?i:pdf)"
        if re.fullmatch(expected_path, url.path) is None:
            _fail("Evidence source_url path disagrees with its notice date or announcement_id")
        relative = Path(_text(record, "relative_pdf_path"))
        pdf = (directory / relative).resolve()
        if relative.is_absolute() or not pdf.is_relative_to(directory) or pdf.suffix.lower() != ".pdf":
            _fail("Evidence PDF path escapes the evidence directory or is not a PDF")
        expected_hash = _text(record, "sha256")
        if re.fullmatch(r"[0-9a-f]{64}", expected_hash) is None:
            _fail("Evidence SHA256 is malformed")
        pdf_bytes = pdf.read_bytes()
        if not pdf_bytes.startswith(b"%PDF-") or hashlib.sha256(pdf_bytes).hexdigest() != expected_hash:
            _fail(f"Evidence PDF content hash mismatch for {event_id}")
        actions = record.get("risk_actions")
        # Empty is a reviewed no-event decision (e.g. only a possible warning).
        if (not isinstance(actions, dict) or any(k not in RISK_CATEGORIES or type(v) is not bool
                                                for k, v in actions.items())):
            _fail("Evidence risk_actions must contain only typed risk-category booleans")
        if event_id in verified and verified[event_id] != record:
            _fail(f"Conflicting duplicate evidence announcement_id {event_id}")
        verified[event_id] = record
    return verified, hashlib.sha256(raw).hexdigest()


def evidence_fingerprint(root: Path | str) -> str:
    """Return registry-byte SHA256 only after all source-byte bindings pass."""
    try:
        return _registry(root)[1]
    except StNoticeEvidenceError:
        raise
    except Exception as exc:
        raise StNoticeEvidenceError(f"Cannot verify ST evidence registry: {type(exc).__name__}: {exc}") from exc


def evidence_source_files(root: Path | str) -> list[str]:
    """Files whose exact content is required by release and state transport."""
    records, _ = _registry(root)
    return ["scripts/st_notice_evidence.py", "data/st_notice_evidence/registry.json"] + sorted({
        "data/st_notice_evidence/" + Path(record["relative_pdf_path"]).as_posix()
        for record in records.values()
    })


def load_verified_events(root: Path | str, symbol: str, first_trade: Any,
                         last_trade: Any, notices: Any) -> dict[str, dict]:
    """Bind in-range reviewed events to exact provider rows; no network/writes.

    `notices` may be a DataFrame or list of record dictionaries. Unknown IDs
    never inherit registered actions, even with the same title/date. Any row
    claiming a registered ID must match its issuer, date, URL and original title.
    """
    try:
        registry, fingerprint = _registry(root)
        if not isinstance(symbol, str) or re.fullmatch(r"\d{6}", symbol) is None:
            _fail("Target issuer symbol must contain exactly six digits")
        first, last = _day(first_trade), _day(last_trade)
        if first > last:
            _fail("Evidence date range is reversed")
        required = {key: value for key, value in registry.items()
                    if value["symbol"] == symbol and first <= value["notice_date"] <= last}
        rows = notices.to_dict("records") if hasattr(notices, "to_dict") else notices
        if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
            _fail("Notices must be a list of records or a DataFrame")
        matched: set[str] = set()
        for row in rows:
            event_id = row.get("announcement_id")
            if not isinstance(event_id, str) or event_id not in registry:
                continue
            event = registry[event_id]
            if (row.get("issuer_symbol") != event["symbol"] or event["symbol"] != symbol
                    or _day(row.get("notice_date")) != event["notice_date"]
                    or row.get("source_url") != event["source_url"] or row.get("title") != event["title"]):
                _fail(f"Notice identity mismatch for certified announcement {event_id}")
            if event_id in required:
                matched.add(event_id)
        missing = sorted(set(required) - matched)
        if missing:
            _fail("Certified announcements missing from provider notices: " + ",".join(missing))
        return {key: {**record, "risk_actions": dict(record["risk_actions"]),
                      "registry_sha256": fingerprint, "rules_revision": RULES_REVISION}
                for key, record in required.items()}
    except StNoticeEvidenceError:
        raise
    except Exception as exc:
        raise StNoticeEvidenceError(f"Cannot bind ST notice evidence: {type(exc).__name__}: {exc}") from exc
