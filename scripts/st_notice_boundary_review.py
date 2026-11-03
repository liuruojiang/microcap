"""Document-bound adjudication of reviewed late non-ST notices, never a generic waiver."""
from __future__ import annotations

import hashlib
import importlib
import json
from pathlib import Path

from scripts.st_notice_evidence import StNoticeEvidenceError

DIRECTORY = Path(__file__).resolve().parents[1] / "data/st_notice_boundary_reviews"
SOURCE_FILES = ["scripts/st_notice_boundary_review.py"] + [
    "data/st_notice_boundary_reviews/" + name
    for name in ("registry.json", "document.pdf", "document.txt", "legacy_certificate.json")
]
SOURCE_FILES += ["data/st_notice_boundary_reviews/" + identity + suffix
                 for identity in ("AN202610291831173818", "AN202610291831173819")
                 for suffix in (".pdf", ".txt")]
SOURCE_FILES += ["data/st_notice_boundary_reviews/reviewed_20261103/registry.json"]
SOURCE_FILES += ["data/st_notice_boundary_reviews/reviewed_20261103/" + identity + suffix
                 for identity in ("AN202611021831303677", "AN202611021831316700",
                                  "AN202611021831321775", "AN202611021831322365")
                 for suffix in (".pdf", ".txt")]
SOURCE_FILES += ["data/st_notice_boundary_reviews/reviewed_20261104/registry.json"]
SOURCE_FILES += ["data/st_notice_boundary_reviews/reviewed_20261104/" + identity + suffix
                 for identity in ("AN202611031831376420", "AN202611031831373422")
                 for suffix in (".pdf", ".txt", ".receipt.json")]
LEGACY_CERTIFICATE_SHA = "1b883caf14d9ab433ccab128448dac7fef804b7ec37439327246f4313206d9e9"


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _review_exact_20261103_batch(previous, current, precision):
    """One reviewed snapshot pair, including progress notices with existing ST.

    This only adjudicates transport-boundary changes. The downstream historical
    ST interval, lifecycle, and return-prefix invariance gates remain mandatory.
    """
    registry_path = DIRECTORY / "reviewed_20261103/registry.json"
    if not registry_path.exists():
        return None  # Isolated legacy fixtures do not exercise this new batch.
    raw = registry_path.read_bytes()
    review = json.loads(raw)
    stable = lambda rows: {k: {field: value for field, value in row.items() if field != "display_time"}
                           for k, row in rows.items()}
    if (stable(previous) != stable(review["previous_rows"])
            or stable(current) != stable(review["current_rows"])):
        return None
    precision(review["previous_rows"], previous)
    precision(review["current_rows"], current)
    if (review.get("schema_version") != 1
            or review.get("decision") != "exact_source_review_no_history_rewrite"
            or sha(canonical(review["previous_rows"])) != review["previous_native_sha256"]
            or sha(canonical(review["current_rows"])) != review["current_native_sha256"]
            or review["boundary_day"] != "2026-11-02"):
        raise StNoticeEvidenceError("Exact boundary batch registry differs from reviewed source")
    for document in review["documents"].values():
        for artifact in document.values():
            path = (DIRECTORY / artifact["path"]).resolve()
            if not path.is_relative_to(DIRECTORY.resolve()) or sha(path.read_bytes()) != artifact["sha256"]:
                raise StNoticeEvidenceError("Exact boundary batch document identity changed")
    added = sorted(current.keys() - previous.keys())
    changed = sorted(k for k in previous.keys() & current.keys()
                     if stable({k: previous[k]}) != stable({k: current[k]}))
    if (previous.keys() - current.keys() or len(added) != 452 or len(changed) != 10
            or review["expected_added_count"] != 452 or review["expected_metadata_count"] != 10):
        raise StNoticeEvidenceError("Exact boundary batch scope changed")
    scope = importlib.import_module("scripts.st_history_scope")
    base = importlib.import_module("microcap_top100_mom16_biweekly_live_v2_0")
    from scripts.st_notice_evidence import is_equity_issuer

    def selectors(row):
        return [symbol for symbol in {code["stock_code"] for code in row["codes"]}
                if is_equity_issuer(row, symbol)
                and scope._daily_notice_row(row, symbol, review["boundary_day"], {}) is not None]

    progress = review["progress"]
    if set(progress) != {"AN202611021831316700", "AN202611021831321775", "AN202611021831322365"}:
        raise StNoticeEvidenceError("Exact progress review scope changed")
    for identity in added:
        row = current[identity]
        actions = base.freq_mod.classify_st_notice_risk_actions(row["title"])
        selected = selectors(row)
        if identity in progress:
            item = progress[identity]
            if (item["decision"] != "existing_other_risk_progress_no_new_effective_event"
                    or item["document"] != identity or identity not in review["documents"]
                    or item["expected_title_actions"] != {"other": True}
                    or actions != item["expected_title_actions"] or selected != [item["symbol"]]):
                raise StNoticeEvidenceError("Reviewed existing-ST progress classification changed")
        elif actions != {} or selected:
            raise StNoticeEvidenceError("Unreviewed risk action in exact boundary batch")
    for identity in changed:
        old, new = previous[identity], current[identity]
        fields = {k for k in old.keys() | new.keys() if k != "display_time" and old.get(k) != new.get(k)}
        if not fields <= {"codes", "listing_state"}:
            raise StNoticeEvidenceError("Reviewed IPO metadata fields changed")
        for row in (old, new):
            if base.freq_mod.classify_st_notice_risk_actions(row["title"]) != {} or selectors(row):
                raise StNoticeEvidenceError("Reviewed IPO metadata now has a risk action")
    if review["ipo_document"] != "AN202611021831303677" or review["ipo_listing_date"] != "2026-11-03":
        raise StNoticeEvidenceError("Reviewed IPO evidence changed")
    common = {k: dict(previous[k], display_time=current[k]["display_time"]) for k in previous}
    result = precision(previous, common)
    precision_count = result["changed_event_count"]
    all_changed = sorted(set(added) | {k for k in previous if previous[k] != current[k]})
    result.update(policy="exact_reviewed_native_boundary_batch_20261103_v1",
                  precision_changed_event_count=precision_count, changed_event_count=len(all_changed),
                  changed_event_ids_sha256=sha(json.dumps(all_changed, separators=(",", ":")).encode()),
                  reviewed_non_st_added_ids=sorted(set(added) - set(progress)),
                  reviewed_existing_st_progress_ids=sorted(progress),
                  reviewed_metadata_changed_ids=changed, removed_event_ids=[],
                  review_registry_sha256=sha(raw), boundary_native_content_unchanged=False)
    return result


def _review_exact_20261104_transport(previous, current, precision):
    """Only the reviewed 1435-to-1518 source observation, never new ST history."""
    from datetime import datetime, timezone
    path = DIRECTORY / "reviewed_20261104/registry.json"
    if not path.exists():
        return None
    raw = path.read_bytes()
    review = json.loads(raw)
    stable = lambda rows: {key: {k: v for k, v in row.items() if k != "display_time"} for key, row in rows.items()}
    if stable(previous) != stable(review["previous_rows"]) or stable(current) != stable(review["current_rows"]):
        return None
    if (review.get("schema_version") != 1 or review.get("decision") != "exact_boundary_transport_no_new_effective_st_20261104_v1"
            or review.get("boundary_day") != "2026-11-03" or len(previous) != 1435 or len(current) != 1518
            or review.get("previous_native_sha256") != "cc1f328dcb2fc1445157ef03a2607a0defe65a8bad7ab88525f1176880f23e70"
            or review.get("current_native_sha256") != "1ade7a27298e2717a1b684f08caa07dfbe20e56cd4bcb76bb7f7e9fc97455f51"
            or sha(canonical(review["previous_rows"])) != "cc1f328dcb2fc1445157ef03a2607a0defe65a8bad7ab88525f1176880f23e70"
            or sha(canonical(review["current_rows"])) != "1ade7a27298e2717a1b684f08caa07dfbe20e56cd4bcb76bb7f7e9fc97455f51"
            or review.get("historical_authority_changed") is not False or review.get("existing_st_intervals_changed") is not False):
        raise StNoticeEvidenceError("Exact late boundary transport review identity differs")
    precision(review["previous_rows"], previous)
    precision(review["current_rows"], current)
    added = sorted(current.keys() - previous.keys())
    changed = sorted(key for key in previous if stable({key: previous[key]}) != stable({key: current[key]}))
    if (previous.keys() - current.keys() or len(added) != 83 or review.get("expected_added_count") != 83
            or review.get("expected_removed_ids") != [] or added != review["expected_added_ids"]
            or len(changed) != 7 or set(changed) != set(review["metadata_changes"])):
        raise StNoticeEvidenceError("Exact late boundary source scope changed")
    expected_docs = {"AN202611031831376420": "7d2487a7c62f7152911cc7096d7cfd56255e173033eed6a2d107d7704327e242",
                     "AN202611031831373422": "d304d9a8175c49ad3da42070cf8f3cadfd167661cec33ccba1758bde9ab49415"}
    safe_headers = {"date", "last-modified", "content-type", "content-length", "cache-control", "etag", "content-encoding"}
    if set(review["documents"]) != set(expected_docs):
        raise StNoticeEvidenceError("Exact late boundary documents differ")
    for identity, artifacts in review["documents"].items():
        if set(artifacts) != {"pdf", "text", "receipt"} or artifacts["pdf"]["sha256"] != expected_docs[identity]:
            raise StNoticeEvidenceError("Exact reviewed issuer PDF differs")
        blobs = {}
        for name, artifact in artifacts.items():
            suffix = {"pdf": ".pdf", "text": ".txt", "receipt": ".receipt.json"}[name]
            if artifact["path"] != "reviewed_20261104/" + identity + suffix:
                raise StNoticeEvidenceError("Boundary document is outside its declared release closure")
            document = (DIRECTORY / artifact["path"]).resolve()
            if not document.is_relative_to(DIRECTORY.resolve()):
                raise StNoticeEvidenceError("Boundary review document escapes source root")
            blobs[name] = document.read_bytes()
            if sha(blobs[name]) != artifact["sha256"]:
                raise StNoticeEvidenceError("Boundary review document bytes changed")
        receipt = json.loads(blobs["receipt"])
        expected_url = "https://pdf.dfcfw.com/pdf/H2_" + identity + "_1.pdf"
        headers = receipt.get("response_headers", {})
        if (not blobs["pdf"].startswith(b"%PDF-") or not blobs["text"].decode("utf-8").strip()
                or receipt.get("requested_url") != expected_url or receipt.get("final_url") != expected_url
                or receipt.get("status") != 200 or receipt.get("tls_verify_configuration") is not True
                or receipt.get("redirects_allowed") is not False or receipt.get("raw_sha256") != expected_docs[identity]
                or receipt.get("raw_bytes") != len(blobs["pdf"]) or receipt.get("native_row") != review["current_rows"][identity]
                or "request_headers" in receipt or "headers" in receipt or not isinstance(headers, dict)
                or any(k.lower() not in safe_headers or not isinstance(v, str) or "\r" in v or "\n" in v for k, v in headers.items())):
            raise StNoticeEvidenceError("Boundary issuer document receipt/source identity changed")
        started, completed = datetime.fromisoformat(receipt["observation_start"]), datetime.fromisoformat(receipt["observation_end"])
        if started.tzinfo is None or completed.tzinfo is None or not started <= completed <= datetime.now(timezone.utc):
            raise StNoticeEvidenceError("Boundary document observation clock invalid")
    root = Path(__file__).resolve().parents[1]
    if (sha((root / "data/st_notice_evidence/registry.json").read_bytes()) != "fa5ef1e1dd797a563b6afaca22d9615307a7d3cef2bea90037fda7495eda6373"
            or sha((root / "data/st_notice_evidence/AN202611021831333702.pdf").read_bytes()) != "b868b032a014d663043fb0f77951106e6f60ec32eaa408a135ca06971b022877"):
        raise StNoticeEvidenceError("Previously approved ST evidence changed")
    scope = importlib.import_module("scripts.st_history_scope")
    base = importlib.import_module("microcap_top100_mom16_biweekly_live_v2_0")
    from scripts.st_notice_evidence import is_equity_issuer
    def selectors(row):
        return sorted(symbol for symbol in {code["stock_code"] for code in row["codes"]} if is_equity_issuer(row, symbol)
                      and scope._daily_notice_row(row, symbol, review["boundary_day"], {}) is not None)
    progress_id = "AN202611031831376420"
    progress = review["progress"][progress_id]
    if (set(review["progress"]) != {progress_id}
            or progress["decision"] != "existing_delisting_and_other_risk_progress_no_new_effective_event"
            or progress.get("symbol") != "301030" or progress.get("risk_actions") != {}
            or progress.get("actual_effective_date") is not None
            or progress.get("pdf_existing_effective_date") != "2026-05-06"
            or progress.get("existing_strategy_notice_date_interval_start") != "2026-04-30"):
        raise StNoticeEvidenceError("Exact existing-ST progress scope differs")
    for identity in added:
        row = current[identity]
        actions = base.freq_mod.classify_st_notice_risk_actions(row["title"])
        if actions != {} or selectors(row) != (["301030"] if identity == progress_id else []):
            raise StNoticeEvidenceError("New/unreviewed ST action in late boundary observation")
    from scripts.st_notice_evidence import _registry, load_verified_events
    registered, _ = _registry(root)
    for identity in changed:
        old, new = previous[identity], current[identity]
        fields = {k for k in old.keys() | new.keys() if k != "display_time" and old.get(k) != new.get(k)}
        if fields != set(review["metadata_changes"][identity]):
            raise StNoticeEvidenceError("Reviewed source metadata fields differ")
        if {key: {"old": old[key], "new": new[key]} for key in fields} != review["metadata_changes"][identity]:
            raise StNoticeEvidenceError("Reviewed source metadata values differ")
        if identity == "AN202611031831373422":
            if fields != {"sort_date"} or (old["sort_date"], new["sort_date"]) != ("2026-11-03 12:00:00", "2026-11-03 12:01:22"):
                raise StNoticeEvidenceError("Reviewed sort metadata changed")
        elif (fields != {"codes"} or len(old["codes"]) != 1 or len(new["codes"]) != 1
              or old["codes"][0]["stock_code"] != "000972"
              or old["codes"][0]["short_name"] != "中基健康" or new["codes"][0]["short_name"] != "*ST中基"
              or {k:v for k,v in old["codes"][0].items() if k != "short_name"} != {k:v for k,v in new["codes"][0].items() if k != "short_name"}):
            raise StNoticeEvidenceError("Reviewed issuer display metadata changed")
        if selectors(old) != selectors(new):
            raise StNoticeEvidenceError("Reviewed source metadata changed a risk selector")
        if identity in registered:
            event = registered[identity]
            old_bound = scope._daily_notice_row(old, event["symbol"], review["boundary_day"], registered)
            new_bound = scope._daily_notice_row(new, event["symbol"], review["boundary_day"], registered)
            if old_bound != new_bound or old_bound is None:
                raise StNoticeEvidenceError("Previously approved risk issuer/body binding changed")
            # The official consumer selects verified body actions before its
            # title-only parser; an actual court-acceptance title must never be
            # reparsed without the already approved exact issuer document.
            load_verified_events(root, event["symbol"], review["boundary_day"], review["boundary_day"], [old_bound])
        elif base.freq_mod.classify_st_notice_risk_actions(old["title"]) != base.freq_mod.classify_st_notice_risk_actions(new["title"]):
            raise StNoticeEvidenceError("Reviewed source metadata changed a risk action")
    common = {key: dict(previous[key], display_time=current[key]["display_time"]) for key in previous}
    result = precision(previous, common)
    changed_all = sorted(set(added) | {key for key in previous if previous[key] != current[key]})
    result.update(policy="exact_reviewed_late_boundary_transport_20261104_v1", precision_changed_event_count=result["changed_event_count"],
                  changed_event_count=len(changed_all), changed_event_ids_sha256=sha(json.dumps(changed_all, separators=(",", ":")).encode()),
                  reviewed_non_st_added_ids=[key for key in added if key != progress_id], reviewed_existing_st_progress_ids=[progress_id],
                  reviewed_metadata_changed_ids=changed, removed_event_ids=[], review_registry_sha256=sha(raw), boundary_native_content_unchanged=False)
    return result


def reviewed_additions(previous, current, precision):
    error = "ST boundary native events changed; exact-hash history review required; pointer preserved"
    exact_transport = _review_exact_20261104_transport(previous, current, precision)
    if exact_transport is not None:
        return exact_transport
    exact = _review_exact_20261103_batch(previous, current, precision)
    if exact is not None:
        return exact
    added = sorted(set(current) - set(previous))
    if set(previous) - set(current):
        raise StNoticeEvidenceError(error)
    raw = (DIRECTORY / "registry.json").read_bytes()
    registry = json.loads(raw)
    if registry.get("schema_version") != 1:
        raise StNoticeEvidenceError("Invalid boundary review registry")
    reviews = registry["reviews"]
    evidence = {}
    for identity in added:
        review = reviews.get(identity)
        if not review or review.get("decision") != "no_stock_st_action":
            raise StNoticeEvidenceError(error)
        row = current[identity]
        stable = {k: v for k, v in row.items() if k != "display_time"}
        if sha(canonical(stable)) != review["native_without_display_time_sha256"]:
            raise StNoticeEvidenceError(error)
        precision({identity: review["native_row"]}, {identity: row})
        for key in ("document", "text"):
            path = (DIRECTORY / review[key]).resolve()
            if not path.is_relative_to(DIRECTORY.resolve()) or sha(path.read_bytes()) != review[key + "_sha256"]:
                raise StNoticeEvidenceError("Boundary review document identity changed")
        scope = importlib.import_module("scripts.st_history_scope")
        base = importlib.import_module("microcap_top100_mom16_biweekly_live_v2_0")
        if (scope._daily_notice_row(row, review["symbol"], review["boundary_day"], {}) is not None
                or base.freq_mod.classify_st_notice_risk_actions(row["title"]) != {}):
            raise StNoticeEvidenceError("Reviewed boundary addition now has an ST action")
        evidence[identity] = {"native_row_sha256": sha(canonical(row)),
                              "document_sha256": review["document_sha256"],
                              "text_sha256": review["text_sha256"]}
    common = {k: v for k, v in current.items() if k in previous}
    metadata = {}
    for identity, old in previous.items():
        new = common[identity]
        stable = lambda row: {k: v for k, v in row.items() if k != "display_time"}
        if stable(old) == stable(new):
            continue
        review = registry.get("metadata_changes", {}).get(identity)
        if (not review or review.get("decision") != "no_stock_st_action"
                or review.get("changed_field") != "listing_state"
                or stable(old) != stable(review["previous_row"])
                or stable(new) != stable(review["current_row"])):
            raise StNoticeEvidenceError(error)
        for key in ("document", "text"):
            path = (DIRECTORY / review[key]).resolve()
            if not path.is_relative_to(DIRECTORY.resolve()) or sha(path.read_bytes()) != review[key + "_sha256"]:
                raise StNoticeEvidenceError("Boundary review document identity changed")
        scope = importlib.import_module("scripts.st_history_scope")
        base = importlib.import_module("microcap_top100_mom16_biweekly_live_v2_0")
        for row in (old, new):
            if (scope._daily_notice_row(row, review["symbol"], review["boundary_day"], {}) is not None
                    or base.freq_mod.classify_st_notice_risk_actions(row["title"]) != {}):
                raise StNoticeEvidenceError("Reviewed metadata change now has an ST action")
        normalized = dict(new, listing_state=old["listing_state"])
        precision({identity: old}, {identity: normalized})
        common[identity] = normalized
        metadata[identity] = {"previous_native_sha256": sha(canonical(old)),
                              "current_native_sha256": sha(canonical(new)),
                              "document_sha256": review["document_sha256"]}
    if not added and not metadata:
        raise StNoticeEvidenceError(error)
    result = precision(previous, common)
    precision_count = result["changed_event_count"]
    changed = sorted([k for k in previous if previous[k] != current[k]] + added)
    result.update(policy="document_reviewed_non_st_boundary_additions_v1",
                  precision_changed_event_count=precision_count, changed_event_count=len(changed),
                  changed_event_ids_sha256=sha(json.dumps(changed, separators=(",", ":")).encode()),
                  reviewed_non_st_added_ids=added, removed_event_ids=[],
                  review_registry_sha256=sha(raw), reviewed_evidence=evidence,
                  boundary_native_content_unchanged=False)
    if metadata:
        result["reviewed_metadata_changes"] = metadata
    return result


def legacy_summary(previous, current, precision, reviewed):
    """Read exact archived v1 proofs; new refreshes always emit document-reviewed proofs."""
    raw = (DIRECTORY / "legacy_certificate.json").read_bytes()
    if sha(raw) != LEGACY_CERTIFICATE_SHA:
        raise StNoticeEvidenceError("Archived boundary certificate changed")
    cert = json.loads(raw)
    added = reviewed["reviewed_non_st_added_ids"]
    if added != cert["allowed_added_ids"]:
        raise StNoticeEvidenceError("Legacy boundary proof has a different reviewed scope")
    identity = added[0]
    registry = json.loads((DIRECTORY / "registry.json").read_bytes())["reviews"][identity]
    result = precision(previous, {k: v for k, v in current.items() if k in previous})
    result.update(policy="exact_reviewed_non_st_addition_plus_original_precision_v1",
                  precision_changed_event_count=result["changed_event_count"],
                  changed_event_count=reviewed["changed_event_count"],
                  changed_event_ids_sha256=reviewed["changed_event_ids_sha256"],
                  reviewed_non_st_added_ids=added, removed_event_ids=[],
                  certificate_sha256=LEGACY_CERTIFICATE_SHA,
                  adapter_canonical_lf_sha256=registry["legacy_adapter_canonical_lf_sha256"],
                  added_native_row_sha256=sha(canonical(current[identity])),
                  boundary_native_content_unchanged=False)
    return result
