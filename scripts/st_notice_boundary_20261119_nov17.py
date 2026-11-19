"""Admit the exact November 17 catalog replacement with one bond tag revision."""
from pathlib import Path

from scripts.st_notice_boundary_20261111 import _projection, _selectors
from scripts.st_notice_boundary_20261117 import canonical, stable
from scripts.st_notice_boundary_20261118_nov17 import (
    PREVIOUS_STABLE_SHA256, digest, reviewed_boundary as reviewed_replacement,
)
from scripts.st_notice_evidence import StNoticeEvidenceError

NOTICE_ID = "AN202611171831788060"
CURRENT_STABLE_SHA256 = "5a2c34ca638797e84aba69e0ce085165956a6ada95414082050ed4cd1219f135"
DOCUMENT = "data/st_notice_boundary_reviews/reviewed_20261119_nov17/AN202611171831788060.pdf"
DOCUMENT_SHA256 = "75fcf34575859c9428850f90a5eb6d48854004eceaa67b3578a0c115186a8d04"
SOURCE_FILES = [
    "scripts/st_notice_boundary_20261119_nov17.py",
    DOCUMENT,
    DOCUMENT.removesuffix(".pdf") + ".receipt.json",
]


def reviewed_boundary(previous, current, precision):
    if (digest(canonical(stable(previous))) != PREVIOUS_STABLE_SHA256
            or digest(canonical(stable(current))) != CURRENT_STABLE_SHA256):
        return None
    document = (Path(__file__).resolve().parents[1] / DOCUMENT).read_bytes()
    old_row, new_row = previous[NOTICE_ID], current[NOTICE_ID]
    if (digest(document) != DOCUMENT_SHA256 or _selectors(old_row) or _selectors(new_row)
            or _projection(previous) != _projection(current)):
        raise StNoticeEvidenceError("Reviewed November 17 bond catalog observation differs")

    # Match the previously reviewed replacement in the comparison view only.
    comparison = dict(current)
    comparison[NOTICE_ID] = dict(new_row, codes=old_row["codes"])
    result = reviewed_replacement(previous, comparison, precision)
    if result is None:
        raise StNoticeEvidenceError("Reviewed November 17 catalog replacement differs")
    changed = sorted((previous.keys() ^ current.keys()) | {
        key for key in previous.keys() & current.keys() if previous[key] != current[key]
    })
    return dict(result, changed_event_count=len(changed),
                changed_event_ids_sha256=digest(canonical(changed)),
                policy="exact_nov17_catalog_bond_classification_20261119_v1",
                reviewed_metadata_changed_ids=[NOTICE_ID], bond_document_sha256=DOCUMENT_SHA256)
