"""Exact November 26 catalog reobservation with one existing-ST progress notice."""

import json
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path

from scripts.st_notice_boundary_20261111 import _selectors
from scripts.st_notice_boundary_20261117 import canonical, stable
from scripts.st_notice_evidence import StNoticeEvidenceError

PREVIOUS = '68d842413da1a08d0115a87b018677e9383bed12a518fdf999d5af5b3697841c'
OBSERVED = '10339e36ea919c69413b3dc710155778d94959b1c4553abb3cdfcd73829182ef'
PROGRESS = 'AN202611261832068027'
DOCUMENT_DIR = 'data/st_notice_boundary_reviews/reviewed_20261127_nov26'
PDF_SHA256 = '3e8e3343e9601b04d20ecce934be08e7f23077aeadc17054b649f8dfdc00d6ed'
RECEIPTS = {
    PROGRESS + '.receipt.json': f'https://pdf.dfcfw.com/pdf/H2_{PROGRESS}_1.pdf',
    '300326-cninfo.receipt.json': 'https://static.cninfo.com.cn/finalpage/2026-11-26/1225689465.PDF',
}
SOURCE_FILES = ['scripts/st_notice_boundary_20261127_nov26.py'] + [
    f'{DOCUMENT_DIR}/{name}' for name in (PROGRESS + '.pdf', *RECEIPTS)
]


def _verify_progress_document():
    directory = Path(__file__).resolve().parents[1] / DOCUMENT_DIR
    document = (directory / (PROGRESS + '.pdf')).read_bytes()
    if (sha256(document).hexdigest() != PDF_SHA256
            or not document.startswith(b'%PDF-') or b'%%EOF' not in document[-1024:]):
        raise StNoticeEvidenceError('November 26 progress notice PDF differs')
    for name, url in RECEIPTS.items():
        receipt = json.loads((directory / name).read_bytes())
        start = datetime.fromisoformat(receipt['observed_start'])
        end = datetime.fromisoformat(receipt['observed_end'])
        if (receipt['url'] != url or receipt['actual_url'] != url
                or receipt['http_status'] != 200 or receipt['tls_verification'] is not True
                or receipt['raw_sha256'] != PDF_SHA256 or receipt['bytes'] != len(document)
                or start.tzinfo is None or end.tzinfo is None
                or not datetime.fromisoformat('2026-11-27T09:00:00+08:00') <= start <= end <= datetime.now(timezone.utc)):
            raise StNoticeEvidenceError('November 26 progress notice document receipt differs')


def reviewed_boundary(previous, current, precision):
    if (sha256(canonical(stable(previous))).hexdigest() != PREVIOUS
            or sha256(canonical(stable(current))).hexdigest() != OBSERVED):
        return None

    common = {identity: current[identity] for identity in previous}
    result = precision(previous, common)
    added = current.keys() - previous.keys()
    if len(previous) != 1168 or len(current) != 1648 or len(added) != 480 or PROGRESS not in added:
        raise StNoticeEvidenceError('November 26 catalog scope differs')

    from microcap_top100_mom16_biweekly_live_v2_0 import freq_mod

    for identity in added:
        row = current[identity]
        expected_selectors = {'300326'} if identity == PROGRESS else set()
        if (set(_selectors(row)) != expected_selectors
                or freq_mod.classify_st_notice_risk_actions(row['title']) != {}):
            raise StNoticeEvidenceError('November 26 catalog addition changes a native risk action')

    # The issuer's signed 2026-068 notice says the request is still in the
    # supplemental-material stage. It records no approval or effective date.
    _verify_progress_document()
    changed = sorted(added | {identity for identity in previous if previous[identity] != common[identity]})
    return dict(result, policy='exact_nov26_catalog_reobservation_20261127_v1',
                precision_changed_event_count=result['changed_event_count'],
                changed_event_count=len(changed), changed_event_ids_sha256=sha256(canonical(changed)).hexdigest(),
                reviewed_non_st_added_ids=sorted(added - {PROGRESS}),
                reviewed_existing_st_progress_ids=[PROGRESS], removed_event_ids=[],
                previous_stable_native_sha256=PREVIOUS, current_stable_native_sha256=OBSERVED,
                original_archive_boundary_preserved=True, historical_state_rewrite_authorized=False,
                boundary_native_content_unchanged=False)
