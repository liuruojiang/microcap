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
SOURCE_FILES = ["scripts/st_notice_boundary_20261118_nov12.py", REGISTRY]


def digest(payload):
    return hashlib.sha256(payload).hexdigest()


def reviewed_boundary(previous, current, precision):
    original, observed = stable(previous), stable(current)
    if (digest(canonical(original)) != PREVIOUS_SHA256
            or digest(canonical(observed)) != OBSERVED_SHA256):
        return None
    raw = (Path(__file__).resolve().parents[1] / REGISTRY).read_bytes().replace(b"\r\n", b"\n")
    if digest(raw) != REGISTRY_SHA256:
        raise StNoticeEvidenceError("Reviewed November 12 catalog registry changed")
    registry = json.loads(raw)
    added = sorted(current.keys() - previous.keys())
    if previous.keys() - current.keys() or added != registry["added_ids"]:
        raise StNoticeEvidenceError("Unexpected November 12 catalog identity change")
    changed_fields = {
        key: sorted(field for field in original[key].keys() | observed[key].keys()
                    if original[key].get(field) != observed[key].get(field))
        for key in original if original[key] != observed[key]
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
    changed = sorted(set(added) | {key for key in previous if previous[key] != current[key]})
    return dict(result, policy="exact_nov12_catalog_reobservation_20261118_v1",
                review_registry_sha256=REGISTRY_SHA256, changed_event_count=len(changed),
                changed_event_ids_sha256=digest(json.dumps(changed, separators=(",", ":")).encode()),
                reviewed_non_st_added_ids=added, reviewed_metadata_changed_ids=sorted(changed_fields),
                removed_event_ids=[], risk_projection_unchanged=True,
                boundary_native_content_unchanged=False, original_archive_boundary_preserved=True,
                historical_state_rewrite_authorized=False, target_freshness_granted=False)
