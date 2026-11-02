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
LEGACY_CERTIFICATE_SHA = "1b883caf14d9ab433ccab128448dac7fef804b7ec37439327246f4313206d9e9"


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def reviewed_additions(previous, current, precision):
    error = "ST boundary native events changed; exact-hash history review required; pointer preserved"
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
