"""Review one provider catalog replacement observed at the November 17 boundary."""
from pathlib import Path
import hashlib
import json

from scripts.st_notice_boundary_20261111 import _projection, _selectors
from scripts.st_notice_boundary_20261117 import canonical, stable
from scripts.st_notice_evidence import StNoticeEvidenceError

REGISTRY = "data/st_notice_boundary_reviews/reviewed_20261118_nov17/registry.json"
DOCUMENT = "data/st_notice_boundary_reviews/reviewed_20261118_nov17/AN202611181831814752.pdf"
REGISTRY_SHA256 = "49775a45fe215056e23713486144e663291b9d2afb54fe972e2f5701751ef2a4"
PREVIOUS_STABLE_SHA256 = "f4ca12b12a4264d0aa3350cdace4262481ee8131ee176b51cb27939db88d5ecb"
CURRENT_STABLE_SHA256 = "94fa1722c70095ef36c2d497c46a790e035e0a26679b615f10fd70e7e73c0b0f"
SOURCE_FILES = ["scripts/st_notice_boundary_20261118_nov17.py", REGISTRY, DOCUMENT]


def digest(payload):
    return hashlib.sha256(payload).hexdigest()


def reviewed_boundary(previous, current, precision):
    if (digest(canonical(stable(previous))) != PREVIOUS_STABLE_SHA256
            or digest(canonical(stable(current))) != CURRENT_STABLE_SHA256):
        return None
    root = Path(__file__).resolve().parents[1]
    raw = (root / REGISTRY).read_bytes().replace(b"\r\n", b"\n")
    if digest(raw) != REGISTRY_SHA256:
        raise StNoticeEvidenceError("Reviewed November 17 catalog registry changed")
    review = json.loads(raw)
    previous_id, current_id = review["previous_id"], review["current_id"]
    if (previous.keys() - current.keys() != {previous_id}
            or current.keys() - previous.keys() != {current_id}):
        raise StNoticeEvidenceError("Unreviewed November 17 announcement change")
    document = (root / DOCUMENT).read_bytes()
    if digest(document) != review["issued_document"]["sha256"]:
        raise StNoticeEvidenceError("Reviewed November 17 issuer document changed")
    old_row, new_row = previous[previous_id], current[current_id]
    precision({previous_id: review["previous_row"]}, {previous_id: old_row})
    precision({current_id: review["current_row"]}, {current_id: new_row})

    import microcap_top100_mom16_biweekly_live_v2_0 as base
    if (_selectors(old_row) or _selectors(new_row)
            or base.freq_mod.classify_st_notice_risk_actions(old_row["title"])
            or base.freq_mod.classify_st_notice_risk_actions(new_row["title"])
            or _projection(previous) != _projection(current)):
        raise StNoticeEvidenceError("Reviewed catalog change alters native risk decisions")

    # Align the comparison view while keeping both source observations intact.
    comparison = {key: value for key, value in current.items() if key != current_id}
    comparison[previous_id] = old_row
    result = precision(previous, comparison)
    changed = sorted((previous.keys() ^ current.keys()) |
                     {key for key in previous.keys() & current.keys() if previous[key] != current[key]})
    return dict(result, policy="exact_nov17_catalog_replacement_20261118_v1",
                review_registry_sha256=REGISTRY_SHA256,
                changed_event_count=len(changed),
                changed_event_ids_sha256=digest(json.dumps(changed, separators=(",", ":")).encode()),
                reviewed_non_st_added_ids=[current_id], reviewed_non_st_removed_ids=[previous_id],
                removed_event_ids=[previous_id], risk_projection_unchanged=True,
                boundary_native_content_unchanged=False, original_archive_boundary_preserved=True,
                historical_state_rewrite_authorized=False, target_freshness_granted=False)
