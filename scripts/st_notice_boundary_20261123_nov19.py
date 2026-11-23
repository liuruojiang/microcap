"""Exact November 19 catalog reobservation at the November 23 morning slot."""
import json
from hashlib import sha256
from pathlib import Path

from scripts.st_notice_boundary_20261111 import _projection
from scripts.st_notice_boundary_20261117 import canonical, stable
from scripts.st_notice_evidence import StNoticeEvidenceError


PREVIOUS_SHA256 = 'a81c3aeac2f7e232e8a732cb10d4e8eab8c1454e799b2431e830a3816b893ddd'
OBSERVED_SHA256 = 'f55e76ec54598efc08b52c83e787633cf63f7a3691a0abae544cd5c263023400'
DOCUMENT_DIR = 'data/st_notice_boundary_reviews/reviewed_20261123_nov19'
DOCUMENTS = {
    'AN202611181831832632': '97b606829f29d93d6dcde679f5cdc46d3c727f66e120c22bcf704f29cd55939e',
    'AN202611181831832633': '439e95c6feeeae940429dbd915180a565c79cf253977f40f3ae226eb342d30d2',
    'AN202611191831858712': '63659bc83bd5bbeb560dc02e32e4660de70eba386fbeb82b288364209e5f6935',
    'AN202611191831862308': 'c2d4ad17cae375c2ab731f412481afd0057b5ff8a46363d7f73935b7beef3fdd',
    'AN202611191831863466': '04d6ed4a61ab23d24da1612116f4efd66d07d30117b09a6269f8338381908ecf',
    'AN202611191831863671': '2a54b941f100be02c08696ef52021863ad7372c01fba588e0908b8ba28543077',
    'AN202611191831865341': '62beedda5464602eff31b0a8010f4c4bfd96b42d89b1d4fc044487b312dd8a1b',
    'AN202611191831865410': 'e9ff3b1df14abc149d780592b4def818d7107c3927f3ccfd5527ed42e92ee0a6',
    'AN202611191831865941': 'b05e93ea69dd90b542515306b8e050fe94064aea719bfb76454b246b175b036f',
    'AN202611191831866202': 'aaa4b284296d2345b144f7faf2a7593e918315523bc1fbf6697b838b0118b9a4',
    'AN202611191831867271': '72e920a57155ac8fb1d5efe268321e5966bc53ac42539991d5be68cfe9b0897a',
}
SOURCE_FILES = ['scripts/st_notice_boundary_20261123_nov19.py'] + [
    f'{DOCUMENT_DIR}/{identity}{suffix}' for identity in DOCUMENTS for suffix in ('.pdf', '.receipt.json')
]


def reviewed_boundary(previous, current, precision):
    original, observed = stable(previous), stable(current)
    if (sha256(canonical(original)).hexdigest() != PREVIOUS_SHA256
            or sha256(canonical(observed)).hexdigest() != OBSERVED_SHA256):
        return None
    if original.keys() != observed.keys():
        raise StNoticeEvidenceError('November 19 catalog identities differ from the review')
    changed = {identity for identity in original if original[identity] != observed[identity]}
    if changed != DOCUMENTS.keys() or _projection(previous) != _projection(current):
        raise StNoticeEvidenceError('November 19 catalog changes native ST decisions')

    documents = Path(__file__).resolve().parents[1] / DOCUMENT_DIR
    for identity, expected in DOCUMENTS.items():
        raw = (documents / f'{identity}.pdf').read_bytes()
        receipt = json.loads((documents / f'{identity}.receipt.json').read_bytes())
        url = f'https://pdf.dfcfw.com/pdf/H2_{identity}_1.pdf'
        if (sha256(raw).hexdigest() != expected or receipt['raw_sha256'] != expected
                or not raw.startswith(b'%PDF-') or b'%%EOF' not in raw[-1024:]
                or receipt['url'] != url or receipt['actual_url'] != url
                or receipt['http_status'] != 200 or receipt['tls_verification'] is not True
                or receipt['bytes'] != len(raw)):
            raise StNoticeEvidenceError('November 19 catalog issuer document differs')

    comparison = {identity: dict(row, display_time=current[identity]['display_time']) for identity, row in original.items()}
    result = precision(previous, comparison)
    all_changes = sorted(identity for identity in previous if previous[identity] != current[identity])
    return dict(result, policy='exact_nov19_catalog_reobservation_20261123_v1',
                changed_event_count=len(all_changes), changed_event_ids_sha256=sha256(canonical(all_changes)).hexdigest(),
                reviewed_metadata_changed_ids=sorted(changed), risk_projection_unchanged=True,
                boundary_native_content_unchanged=False, original_archive_boundary_preserved=True,
                historical_state_rewrite_authorized=False, target_freshness_granted=False)
