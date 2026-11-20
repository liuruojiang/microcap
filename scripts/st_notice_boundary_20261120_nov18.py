"""Review the observed November 18 catalog without replacing its archive."""
from hashlib import sha256
from pathlib import Path

from scripts.st_notice_boundary_20261111 import _projection, _selectors
from scripts.st_notice_boundary_20261117 import canonical, stable
from scripts.st_notice_evidence import StNoticeEvidenceError

PREVIOUS_SHA256 = "f849e958530ac9f2bb6ce93bc544f4a87651e8d6f3d861101cc2f08d92ce2aa4"
OBSERVED_SHA256 = "87801329697e49bdb4e834498f8d437479806d88cbeb581d5c4a1b8a7079d227"
DOCUMENT_DIR = "data/st_notice_boundary_reviews/reviewed_20261120_nov18"
DOCUMENTS = {
    "AN202611181831831804": "dad992bd9851d4b1b3827652a27163659b0d634b645beaccdc6ca97bffcb565d",
    "AN202611181831823458": "562a840d45c1032b9acd94669b6f10acc3cc9c33b9d4c3e79cf058c2a9602818",
    "AN202611181831830289": "0dc4d7e8da68fc0b112bb46fd0ed9398363922841fa5b92c88f387ee5ad0fd5a",
    "AN202611181831831561": "82b0bca064ba2fb2f458c007f7134f208eb1b971258c3e283201ef30d3bded93",
    "AN202611181831828821": "e8360054a69409f8e06e6b9f941f0db7e9f4deaa6e342919997a95d921a1ed2c",
}
SOURCE_FILES = ["scripts/st_notice_boundary_20261120_nov18.py"] + [
    f"{DOCUMENT_DIR}/{identity}{suffix}"
    for identity in DOCUMENTS for suffix in (".pdf", ".receipt.json")
]


def reviewed_boundary(previous, current, precision):
    original, observed = stable(previous), stable(current)
    if (sha256(canonical(original)).hexdigest() != PREVIOUS_SHA256
            or sha256(canonical(observed)).hexdigest() != OBSERVED_SHA256):
        return None

    changed = {identity for identity in original if original[identity] != observed[identity]}
    if original.keys() != observed.keys() or changed != DOCUMENTS.keys():
        raise StNoticeEvidenceError("November 18 catalog identities differ from the review")

    root = Path(__file__).resolve().parents[1] / DOCUMENT_DIR
    for identity, digest in DOCUMENTS.items():
        if (sha256((root / f"{identity}.pdf").read_bytes()).hexdigest() != digest
                or _selectors(previous[identity]) or _selectors(current[identity])):
            raise StNoticeEvidenceError("November 18 catalog document or native selection differs")
    if _projection(previous) != _projection(current):
        raise StNoticeEvidenceError("November 18 catalog changes native ST decisions")

    comparison = {
        identity: dict(row, display_time=current[identity]["display_time"])
        for identity, row in original.items()
    }
    result = precision(previous, comparison)
    all_changes = sorted(identity for identity in previous if previous[identity] != current[identity])
    return dict(result, policy="exact_nov18_catalog_reobservation_20261120_v1",
                changed_event_count=len(all_changes),
                changed_event_ids_sha256=sha256(canonical(all_changes)).hexdigest(),
                reviewed_metadata_changed_ids=sorted(changed), risk_projection_unchanged=True,
                boundary_native_content_unchanged=False, original_archive_boundary_preserved=True,
                historical_state_rewrite_authorized=False, target_freshness_granted=False)
