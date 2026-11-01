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

RULES_REVISION = "st-body-evidence-20261030-v5"
SCHEMA_VERSION = 5
DATE_POLICY = "notice_date_exclusion_inclusive_exit_unchanged"
RISK_CATEGORIES = frozenset({"delisting", "other", "unspecified"})


class StNoticeEvidenceError(ValueError):
    """Source evidence is absent, malformed, conflicting, or mismatched."""


def _fail(message: str) -> None:
    raise StNoticeEvidenceError(message)


def is_equity_issuer(row: dict, symbol: str) -> bool:
    """Index and equity six-digit codes overlap; bind both asset class and venue."""
    venue = "1" if symbol.startswith("6") else "0" if symbol.startswith(("0", "3")) else None
    codes = row.get("codes")
    return venue is not None and isinstance(codes, list) and any(
        isinstance(code, dict) and code.get("stock_code") == symbol
        and code.get("market_code") == venue
        and "A" in str(code.get("ann_type", "")).split(",")
        for code in codes
    )


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


def _artifact(directory: Path, spec: dict, suffix: str) -> bytes:
    """Authenticate an auxiliary artifact without following escaped paths."""
    if not isinstance(spec, dict):
        _fail("Missing conflict evidence artifact")
    relative = Path(_text(spec, "relative_path"))
    path = (directory / relative).resolve()
    if relative.is_absolute() or not path.is_relative_to(directory) or path.suffix.lower() != suffix:
        _fail("Conflict evidence path escapes evidence directory or has wrong type")
    digest = _text(spec, "sha256")
    raw = path.read_bytes()
    if re.fullmatch(r"[0-9a-f]{64}", digest) is None or hashlib.sha256(raw).hexdigest() != digest:
        _fail("Conflict evidence content hash mismatch")
    if suffix == ".pdf" and not raw.startswith(b"%PDF-"):
        _fail("Conflict evidence is not a PDF")
    return raw


def _conflict_evidence(directory: Path, record: dict) -> None:
    """Validate an explicitly reviewed mirror, retaining the CNInfo row identity."""
    if record.get("source_type") == "reviewed_eastmoney_original":
        _eastmoney_evidence(directory, record)
        return
    if record.get("source_type") == "reviewed_cninfo_body_unavailable":
        _unavailable_evidence(directory, record)
        return
    if record.get("source_type") != "reviewed_cninfo_body_conflict":
        if record.get("source_type") is not None or any(key in record for key in ("body_source_url", "source_conflict", "conflict_evidence", "unavailable_evidence", "body_unavailable", "native_evidence")):
            _fail("Unreviewed body source override")
        return
    if record.get("source_conflict") is not True:
        _fail("Reviewed source conflict flag missing")
    if re.fullmatch(r"\d{4}-\d+", _text(record, "body_notice_number")) is None:
        _fail("Reviewed body notice number malformed")
    url = urlsplit(_text(record, "body_source_url"))
    if (url.scheme != "https" or url.netloc != "pdf.dfcfw.com" or url.query or url.fragment
            or re.fullmatch(r"/pdf/H2_AN\d+_1\.pdf", url.path) is None):
        _fail("Unsupported reviewed mirror URL")
    evidence = record.get("conflict_evidence")
    if not isinstance(evidence, dict) or set(evidence) != {"provider_response", "conflicting_pdf", "crosscheck", "review"}:
        _fail("Incomplete source conflict evidence")
    original = _artifact(directory, evidence["conflicting_pdf"], ".pdf")
    if evidence["conflicting_pdf"].get("source_url") != record["source_url"]:
        _fail("Conflicting PDF source differs from provider identity")
    if hashlib.sha256(original).hexdigest() == record["sha256"]:
        _fail("Conflicting PDF cannot certify the corrected body")
    # A serialized API response archive, not an attestation of raw HTTP bytes.
    api = json.loads(_artifact(directory, evidence["provider_response"], ".json"))
    notices = api.get("announcements") if isinstance(api, dict) else None
    if not isinstance(notices, list):
        _fail("Conflict provider response has no announcements")
    matches = [r for r in notices if str(r.get("announcementId")) == record["announcement_id"]]
    if len(matches) != 1:
        _fail("Conflict provider response announcement identity missing or duplicated")
    row = matches[0]
    stamp = row.get("announcementTime")
    if type(stamp) not in (int, float):
        _fail("Conflict provider timestamp malformed")
    published = datetime.fromtimestamp(stamp / 1000, ZoneInfo("Asia/Shanghai")).date().isoformat()
    if (row.get("secCode") != record["symbol"] or row.get("announcementTitle") != record["title"]
            or published != record["notice_date"]
            or "/" + str(row.get("adjunctUrl")) != urlsplit(record["source_url"]).path):
        _fail("Conflict provider response identity mismatch")
    cross = evidence["crosscheck"]
    cross_url = urlsplit(_text(cross, "source_url"))
    day = record["notice_date"]
    if (cross_url.scheme != "https" or cross_url.netloc != "paper.cnstock.com"
            or cross_url.query or cross_url.fragment
            or re.fullmatch(rf"/html/{day[:7]}/{day[8:]}/content_\d+\.htm", cross_url.path) is None):
        _fail("Unsupported independent disclosure crosscheck URL")
    _artifact(directory, cross, ".html")
    review = json.loads(_artifact(directory, evidence["review"], ".json"))
    binding = {key: record.get(key) for key in (
        "symbol", "announcement_id", "notice_date", "title", "source_url", "body_source_url",
        "body_notice_number", "relative_pdf_path", "sha256", "risk_actions", "actual_effective_date")}
    if (not isinstance(review, dict) or review.get("status") != "body_and_independent_disclosure_verified"
            or review.get("event") != binding
            or review.get("evidence") != {k: v for k, v in evidence.items() if k != "review"}):
        _fail("Reviewed conflict binding differs from event or evidence")


def _unavailable_evidence(directory: Path, record: dict) -> None:
    """A missing original body can use a separately identified, reviewed mirror."""
    if record.get("body_unavailable") is not True or "source_conflict" in record or "conflict_evidence" in record:
        _fail("Unavailable body must not claim a conflicting original PDF")
    if re.fullmatch(r"\d{4}-\d+", _text(record, "body_notice_number")) is None:
        _fail("Reviewed body notice number malformed")
    body = urlsplit(_text(record, "body_source_url"))
    art = _text(record, "body_announcement_id")
    if (re.fullmatch(r"AN\d+", art) is None or body.scheme != "https" or body.netloc != "pdf.dfcfw.com"
            or body.query or body.fragment or body.path != f"/pdf/H2_{art}_1.pdf"):
        _fail("Unavailable body mirror identity malformed")
    evidence = record.get("unavailable_evidence")
    expected = {"provider_response", "mirror_response", "crosscheck", "failure_record", "review"}
    if not isinstance(evidence, dict) or set(evidence) != expected:
        _fail("Incomplete unavailable-body evidence")
    api = json.loads(_artifact(directory, evidence["provider_response"], ".json"))
    rows = api.get("announcements") if isinstance(api, dict) else None
    if not isinstance(rows, list):
        _fail("Unavailable-body provider response malformed")
    matched = [r for r in rows if isinstance(r, dict) and str(r.get("announcementId")) == record["announcement_id"]]
    if len(matched) != 1:
        _fail("Unavailable-body provider identity missing or duplicated")
    row = matched[0]
    stamp = row.get("announcementTime")
    if type(stamp) not in (int, float):
        _fail("Unavailable-body provider timestamp malformed")
    published = datetime.fromtimestamp(stamp / 1000, ZoneInfo("Asia/Shanghai")).date().isoformat()
    # Identical to the real fetch_cninfo_st_notices HTML-highlight cleanup.
    title = re.sub(r"<[^>]+>", "", str(row.get("announcementTitle", ""))).strip()
    if (row.get("secCode") != record["symbol"] or title != record["title"] or published != record["notice_date"]
            or "/" + str(row.get("adjunctUrl")) != urlsplit(record["source_url"]).path):
        _fail("Unavailable-body provider identity mismatch")
    mirror = json.loads(_artifact(directory, evidence["mirror_response"], ".json"))
    if not isinstance(mirror, dict) or mirror.get("success") != 1:
        _fail("Mirror response was not successful")
    rows = mirror.get("data", {}).get("list")
    if not isinstance(rows, list):
        _fail("Mirror response has no notices")
    matched = [r for r in rows if isinstance(r, dict) and r.get("art_code") == art]
    if len(matched) != 1:
        _fail("Mirror announcement identity missing or duplicated")
    row = matched[0]
    codes = row.get("codes")
    if (not is_equity_issuer(row, record["symbol"])
            or _day(row.get("notice_date")) != record["notice_date"]
            or row.get("title") != record.get("body_provider_title")
            or record["body_provider_title"].split(":", 1)[-1] != record["title"]):
        _fail("Mirror issuer, date or title mismatch")
    cross = evidence["crosscheck"]
    url = urlsplit(_text(cross, "source_url"))
    if (url.scheme != "https" or url.netloc != "static.cninfo.com.cn" or url.query or url.fragment
            or re.fullmatch(r"/finalpage/\d{4}-\d{2}-\d{2}/\d+\.(?i:pdf)", url.path) is None):
        _fail("Unavailable-body independent crosscheck URL invalid")
    _artifact(directory, cross, ".pdf")
    failure = json.loads(_artifact(directory, evidence["failure_record"], ".json"))
    if (not isinstance(failure, dict) or failure.get("source_url") != record["source_url"]
            or failure.get("status") != "original_body_unavailable" or failure.get("original_pdf_obtained") is not False
            or failure.get("record_type") not in {"recorded_tool_failure", "operator_recorded_failure"}):
        _fail("Original-body failure record missing or mismatched")
    review = json.loads(_artifact(directory, evidence["review"], ".json"))
    keys = ("symbol", "announcement_id", "notice_date", "title", "source_url", "body_source_url",
            "body_notice_number", "body_announcement_id", "body_provider_title", "relative_pdf_path", "sha256",
            "risk_actions", "actual_effective_date")
    if (not isinstance(review, dict) or review.get("status") != "mirror_body_and_independent_disclosure_verified"
            or review.get("event") != {key: record.get(key) for key in keys}
            or review.get("evidence") != {k: v for k, v in evidence.items() if k != "review"}):
        _fail("Unavailable-body review binding mismatch")


def _eastmoney_evidence(directory: Path, record: dict) -> None:
    if record.get("source_provider") != "eastmoney" or any(k in record for k in ("source_conflict", "body_unavailable", "conflict_evidence", "unavailable_evidence")):
        _fail("Native Eastmoney provider/exception identity mismatch")
    event_id = record["announcement_id"]
    expected_url = f"https://pdf.dfcfw.com/pdf/H2_{event_id}_1.pdf"
    if record.get("source_url") != expected_url or record.get("body_source_url") != expected_url:
        _fail("Native Eastmoney body URL differs from original AN identity")
    evidence = record.get("native_evidence")
    if not isinstance(evidence, dict) or set(evidence) not in (
            {"provider_response", "review"}, {"provider_response", "review", "issuer_corroboration"}):
        _fail("Native Eastmoney evidence incomplete")
    corroboration = evidence.get("issuer_corroboration")
    if corroboration is not None:
        if (not isinstance(corroboration, dict) or re.fullmatch(
                r"https://static\.cninfo\.com\.cn/finalpage/\d{4}-\d{2}-\d{2}/\d+\.PDF",
                str(corroboration.get("source_url", ""))) is None):
            _fail("Native issuer corroboration must identify an official CNInfo PDF")
        if not _artifact(directory, corroboration, ".pdf").startswith(b"%PDF-"):
            _fail("Native issuer corroboration is not PDF bytes")
    spec = evidence["provider_response"]
    if spec.get("source_url") != "https://np-anotice-stock.eastmoney.com/api/security/ann":
        _fail("Native Eastmoney API source mismatch")
    api = json.loads(_artifact(directory, spec, ".json"))
    if not isinstance(api, dict) or api.get("success") != 1:
        _fail("Native Eastmoney API failure")
    rows = api.get("data", {}).get("list")
    if not isinstance(rows, list):
        _fail("Native Eastmoney API rows missing")
    matched = [r for r in rows if isinstance(r, dict) and r.get("art_code") == event_id]
    if len(matched) != 1:
        _fail("Native Eastmoney ID missing or duplicated")
    row = matched[0]
    prefix = record.get("removed_issuer_prefix")
    if not isinstance(prefix, str) or (prefix and not prefix.endswith((":", "："))):
        _fail("Native issuer prefix transform invalid")
    if (row.get("title") != record.get("provider_title") or row.get("title") != prefix + record["title"]
            or _day(row.get("notice_date")) != record["notice_date"]
            or not is_equity_issuer(row, record["symbol"])):
        _fail("Native Eastmoney issuer/date/title identity mismatch")
    review = json.loads(_artifact(directory, evidence["review"], ".json"))
    keys = ("symbol", "announcement_id", "notice_date", "title", "provider_title", "removed_issuer_prefix",
            "source_provider", "source_url", "body_source_url", "body_notice_number", "relative_pdf_path",
            "sha256", "risk_actions", "actual_effective_date")
    if (not isinstance(review, dict) or review.get("status") != "native_provider_and_body_verified"
            or review.get("event") != {k: record.get(k) for k in keys}
            or review.get("evidence") != {key: value for key, value in evidence.items() if key != "review"}):
        _fail("Native Eastmoney human review binding mismatch")


def _exchange_html_evidence(directory: Path, record: dict, raw: bytes) -> None:
    """An exchange original need not be PDF; bind HTML, publication and issuer independently."""
    import html
    if record.get("source_provider") != "sse" or any(k in record for k in ("native_evidence", "source_conflict", "body_unavailable")):
        _fail("Exchange HTML provider identity mismatch")
    day=record["notice_date"];url=record["source_url"]
    if record["announcement_id"] != "SSE_"+day.replace("-", "")+"_"+record["symbol"]:
        _fail("Exchange HTML event identity mismatch")
    if re.fullmatch(r"https://www\.sse\.com\.cn/disclosure/credibility/supervision/dynamic/c/c_"+day.replace("-", "")+r"_\d+\.shtml",url) is None:
        _fail("Exchange HTML URL/publication date mismatch")
    text=html.unescape(re.sub(r"<[^>]+>"," ",raw.decode("utf-8")))
    compact=re.sub(r"\s+","",text)
    if any(str(record.get(k) or "") not in compact or not record.get(k) for k in ("title","issuer_name","issuer_short_name")) or day not in text:
        _fail("Exchange HTML title/issuer/publication mismatch")
    evidence=record.get("exchange_evidence")
    if not isinstance(evidence,dict) or set(evidence)!={"provider_response","receipt","review"}:
        _fail("Exchange HTML evidence incomplete")
    receipt=json.loads(_artifact(directory,evidence["receipt"],".json"))
    if receipt.get("status")!=200 or receipt.get("url")!=url or receipt.get("sha256")!=hashlib.sha256(raw).hexdigest():
        _fail("Exchange HTML receipt mismatch")
    if evidence["provider_response"].get("source_url") != "https://np-anotice-stock.eastmoney.com/api/security/ann":
        _fail("Exchange issuer corroboration source mismatch")
    api=json.loads(_artifact(directory,evidence["provider_response"],".json"))
    if api.get("success")!=1 or not any(is_equity_issuer(row,record["symbol"]) and record["issuer_short_name"] in row.get("title","") for row in api.get("data",{}).get("list",[])):
        _fail("Exchange HTML lacks actual equity issuer corroboration")
    review=json.loads(_artifact(directory,evidence["review"],".json"))
    binding={k:record.get(k) for k in ("symbol","announcement_id","notice_date","title","issuer_name","issuer_short_name","source_provider","source_url","relative_html_path","sha256","risk_actions","actual_effective_date")}
    if review.get("status")!="official_exchange_html_and_equity_identity_verified" or review.get("event")!=binding or review.get("evidence")!={k:v for k,v in evidence.items() if k!="review"}:
        _fail("Exchange HTML review binding mismatch")


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
        is_html = record.get("source_type") == "reviewed_exchange_html"
        event_pattern = r"SSE_\d{8}_\d{6}" if is_html else r"AN\d+" if record.get("source_type") == "reviewed_eastmoney_original" else r"\d+"
        if re.fullmatch(r"\d{6}", symbol) is None or re.fullmatch(event_pattern, event_id) is None:
            _fail("Evidence symbol or announcement_id is malformed")
        notice_date = _day(record.get("notice_date"), strict=True)
        if record.get("source_type") == "reviewed_eastmoney_original":
            # Preserve provider whitespace exactly. Native identity below binds
            # raw_title == removed_prefix + title; trimming would lose evidence.
            native_title = record.get("title")
            if not isinstance(native_title, str) or not native_title.strip():
                _fail("Native evidence title is missing or malformed")
        else:
            _text(record, "title")
        source_url = _text(record, "source_url")
        url = urlsplit(source_url)
        if record.get("source_type") != "reviewed_eastmoney_original" and not is_html:
            if (url.scheme not in {"https", "http"} or url.hostname not in {"static.cninfo.com.cn", "www.cninfo.com.cn"}
                    or url.username is not None or url.password is not None or url.fragment or url.query):
                _fail("Evidence source_url is not an authenticated CNInfo source")
            expected_path = rf"/finalpage/{re.escape(notice_date)}/{re.escape(event_id)}\.(?i:pdf)"
            if re.fullmatch(expected_path, url.path) is None:
                _fail("Evidence source_url path disagrees with its notice date or announcement_id")
        relative = Path(_text(record, "relative_html_path" if is_html else "relative_pdf_path"))
        pdf = (directory / relative).resolve()
        if relative.is_absolute() or not pdf.is_relative_to(directory) or pdf.suffix.lower() != (".html" if is_html else ".pdf"):
            _fail("Evidence PDF path escapes the evidence directory or is not a PDF")
        expected_hash = _text(record, "sha256")
        if re.fullmatch(r"[0-9a-f]{64}", expected_hash) is None:
            _fail("Evidence SHA256 is malformed")
        pdf_bytes = pdf.read_bytes()
        if (not is_html and not pdf_bytes.startswith(b"%PDF-")) or hashlib.sha256(pdf_bytes).hexdigest() != expected_hash:
            _fail(f"Evidence PDF content hash mismatch for {event_id}")
        if is_html:
            _exchange_html_evidence(directory, record, pdf_bytes)
        else:
            _conflict_evidence(directory, record)
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
    paths = {
        "data/st_notice_evidence/" + Path(record.get("relative_html_path") or record["relative_pdf_path"]).as_posix()
        for record in records.values()
    }
    for record in records.values():
        for evidence_key in ("conflict_evidence", "unavailable_evidence", "native_evidence", "exchange_evidence"):
            for artifact in record.get(evidence_key, {}).values():
                paths.add("data/st_notice_evidence/" + Path(artifact["relative_path"]).as_posix())
    return ["scripts/st_notice_evidence.py", "scripts/st_notice_archive.py", "data/st_notice_evidence/registry.json"] + sorted(paths)


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
            if event.get("source_provider") in {"eastmoney","sse"} and row.get("source_provider") != event["source_provider"]:
                _fail("Native notice row provider identity missing or mismatched")
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
