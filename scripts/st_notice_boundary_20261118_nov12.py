"""Exact all-market reobservation of the November 12 notice boundary."""
import hashlib
import json
from pathlib import Path

from scripts.st_notice_boundary_20261111 import _projection, _selectors
from scripts.st_notice_boundary_20261117 import canonical, stable
from scripts.st_notice_evidence import StNoticeEvidenceError

REGISTRY = "data/st_notice_boundary_reviews/reviewed_20261118_nov12/registry.json"
REGISTRY_SHA256 = "63dd766b1cb9cb6b370d0631a539e6db78420f511bcd86b2dc87aa6185d11143"
PREVIOUS_SHA256 = "5138ad5b71f7d988d8f98c0f314328e94c7164a0969f87a00bbe9cf067fbae1a"
OBSERVED_SHA256 = "b21f65ff410deebc7de6771095db88c9e286f90f4eeb1b81edc7dcb9bb708d7b"
NOV26_OBSERVED_SHA256 = "ea22c3456352c8276195f64bb820d9fd5f08fbfe62c14359f389e315e2aec19b"
NOV26_DOCUMENT_DIR = "data/st_notice_boundary_reviews/reviewed_20261126_nov12"
NOV26_DOCUMENTS = {
    "AN202611111831634167": "ca6e2715ce822c1d538b4d9617321d35f52af360a7760773556621b908e5c4a0",
    "AN202611121831657801": "7be8bf19ca549029dab8795dc3f27def64e4d2d43739a8bc3e2126bb5acd278c",
}
SOURCE_FILES = ["scripts/st_notice_boundary_20261118_nov12.py", REGISTRY] + [
    f"{NOV26_DOCUMENT_DIR}/{identity}{suffix}"
    for identity in NOV26_DOCUMENTS
    for suffix in (".pdf", ".receipt.json")
]


def digest(payload):
    return hashlib.sha256(payload).hexdigest()


def reviewed_boundary(previous, current, precision):
    original, observed = stable(previous), stable(current)
    observed_sha = digest(canonical(observed))
    if (digest(canonical(original)) != PREVIOUS_SHA256
            or observed_sha not in {OBSERVED_SHA256, NOV26_OBSERVED_SHA256}):
        return None
    root = Path(__file__).resolve().parents[1]
    policy = "exact_nov12_catalog_reobservation_20261118_v1"
    reviewed = observed
    if observed_sha == NOV26_OBSERVED_SHA256:
        from scripts.st_notice_boundary_20261123_nov19 import _verify_document

        reviewed = observed.copy()
        for identity, expected in NOV26_DOCUMENTS.items():
            row = observed[identity]
            if row["listing_state"] != "2":
                raise StNoticeEvidenceError("November 12 catalog status differs from the reviewed observation")
            reviewed[identity] = {**row, "listing_state": "0"}
            _verify_document(root / NOV26_DOCUMENT_DIR, identity, expected,
                             f"https://pdf.dfcfw.com/pdf/H2_{identity}_1.pdf")
        if digest(canonical(reviewed)) != OBSERVED_SHA256:
            raise StNoticeEvidenceError("November 12 catalog differs beyond the two documented statuses")
        policy = "exact_nov12_catalog_reobservation_20261126_v1"
    raw = (root / REGISTRY).read_bytes().replace(b"\r\n", b"\n")
    if digest(raw) != REGISTRY_SHA256:
        raise StNoticeEvidenceError("Reviewed November 12 catalog registry changed")
    registry = json.loads(raw)
    added = sorted(current.keys() - previous.keys())
    if previous.keys() - current.keys() or added != registry["added_ids"]:
        raise StNoticeEvidenceError("Unexpected November 12 catalog identity change")
    changed_fields = {
        key: sorted(field for field in original[key].keys() | reviewed[key].keys()
                    if original[key].get(field) != reviewed[key].get(field))
        for key in original if original[key] != reviewed[key]
    }
    if changed_fields != registry["catalog_field_changes"]:
        raise StNoticeEvidenceError("Unreviewed November 12 catalog field change")

    import microcap_top100_mom16_biweekly_live_v2_0 as base
    if (any(_selectors(current[key]) or base.freq_mod.classify_st_notice_risk_actions(current[key]["title"])
            for key in added) or _projection(previous) != _projection(current)):
        raise StNoticeEvidenceError("Reviewed November 12 batch changes native ST decisions")

    comparison = {}
    for key in previous:
        comparison[key] = {**original[key], "display_time": current[key]["display_time"]}
    result = precision(previous, comparison)
    if observed_sha == NOV26_OBSERVED_SHA256:
        result["catalog_listing_state_changed_ids"] = sorted(NOV26_DOCUMENTS)
    changed = sorted(set(added) | {key for key in previous if previous[key] != current[key]})
    return dict(result, policy=policy,
                review_registry_sha256=REGISTRY_SHA256, changed_event_count=len(changed),
                changed_event_ids_sha256=digest(json.dumps(changed, separators=(",", ":")).encode()),
                reviewed_non_st_added_ids=added,
                reviewed_metadata_changed_ids=sorted(key for key in original if original[key] != observed[key]),
                removed_event_ids=[], risk_projection_unchanged=True,
                boundary_native_content_unchanged=False, original_archive_boundary_preserved=True,
                historical_state_rewrite_authorized=False, target_freshness_granted=False)
