"""Document-bound November 19 catalog reobservations."""
import json
from hashlib import sha256
from pathlib import Path

from scripts.st_notice_boundary_20261111 import _projection
from scripts.st_notice_boundary_20261117 import canonical, stable
from scripts.st_notice_evidence import StNoticeEvidenceError


PREVIOUS_SHA256 = 'a81c3aeac2f7e232e8a732cb10d4e8eab8c1454e799b2431e830a3816b893ddd'
OBSERVED_SHA256 = 'f55e76ec54598efc08b52c83e787633cf63f7a3691a0abae544cd5c263023400'
NOV24_OBSERVED_SHA256 = '807b6055ea8ecb04f4a8096fdf59a75cdfc0de9cbe5c4eb8712dadfa93dcdc59'
NOV25_OBSERVED_SHA256 = '40f5c35fb9afcb524371805be9894259c3c3d6372833e492aa1865f9b4359db7'
NOV26_OBSERVED_SHA256 = '4ec67fed4bafbf601feaf50b73a14ed9acc626c39e3303e617ac8501c16fef32'
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
NOV24_DOCUMENTS = {
    **DOCUMENTS,
    'AN202611181831834523': '5c0ace58ed753e5af0bbd13a2cd7c25e2f29a40b12c2929b7bb9a486ba7e1e7a',
    'AN202611181831834533': '00c9b83b156862c5930ae2aae6c85c8c4d329bdab826b016f51e02ab2b6adf26',
}
# CSRC decisions 3261 and 3268 were signed November 19. The feed
# displays November 24 at 19:32 for both notices. Neither executes an
# issuance or changes an effective ST event.
NOV25_ADDED_DOCUMENTS = {
    'AN202611241831999540': '68d559c1598e514ad5d8a439c4eb93673045ea428db4f5247d0dd185e74d6732',
    'AN202611241831999541': '37fed643cddb17dd8c8ad4cc0b6e8fa457038ade53d5cb1ad0ea6522b340076d',
}
# November 26 reuses the previous catalog case and observes one later short
# name on an older, unchanged bank-freeze notice. The signed November 25
# notice documents the separate November 26 implementation.
NOV26_RENAMED_NOTICE = 'AN202611191831863672'
NOV26_DOCUMENTS = {
    **NOV24_DOCUMENTS,
    NOV26_RENAMED_NOTICE: 'efedaaf62c139a52dfe42e787c116804803190e83fa727d793d055712e0f7991',
}
NOV26_ST_NOTICE = (
    '300670-st-20261125', '6371c94f99f47996137f563d158d3947358964918e8f293b1ed46c63741f3401',
    'https://static.cninfo.com.cn/finalpage/2026-11-25/1225686832.PDF',
)
SOURCE_FILES = ['scripts/st_notice_boundary_20261123_nov19.py'] + [
    f'{DOCUMENT_DIR}/{identity}{suffix}' for identity in (*NOV26_DOCUMENTS, *NOV25_ADDED_DOCUMENTS, NOV26_ST_NOTICE[0]) for suffix in ('.pdf', '.receipt.json')
]

def _verify_document(documents, identity, expected, url):
    raw = (documents / f'{identity}.pdf').read_bytes()
    receipt = json.loads((documents / f'{identity}.receipt.json').read_bytes())
    if (sha256(raw).hexdigest() != expected or receipt['raw_sha256'] != expected
            or not raw.startswith(b'%PDF-') or b'%%EOF' not in raw[-1024:]
            or receipt['url'] != url or receipt['actual_url'] != url
            or receipt['http_status'] != 200 or receipt['tls_verification'] is not True
            or receipt['bytes'] != len(raw)):
        raise StNoticeEvidenceError('November 19 catalog issuer document differs')


def reviewed_boundary(previous, current, precision):
    original, observed = stable(previous), stable(current)
    observed_sha = sha256(canonical(observed)).hexdigest()
    reviewed = {
        OBSERVED_SHA256: (DOCUMENTS, {}, 'exact_nov19_catalog_reobservation_20261123_v1'),
        NOV24_OBSERVED_SHA256: (NOV24_DOCUMENTS, {}, 'exact_nov19_catalog_reobservation_20261124_v1'),
        NOV25_OBSERVED_SHA256: (NOV24_DOCUMENTS, NOV25_ADDED_DOCUMENTS,
                               'exact_nov19_catalog_reobservation_20261125_v1'),
        NOV26_OBSERVED_SHA256: (NOV26_DOCUMENTS, NOV25_ADDED_DOCUMENTS,
                               'exact_nov19_catalog_reobservation_20261126_v1'),
    }
    if (sha256(canonical(original)).hexdigest() != PREVIOUS_SHA256
            or observed_sha not in reviewed):
        return None
    expected_docs, added_docs, policy = reviewed[observed_sha]
    if original.keys() - observed.keys() or observed.keys() - original.keys() != added_docs.keys():
        raise StNoticeEvidenceError('November 19 catalog identities differ from the review')
    changed = {identity for identity in original if original[identity] != observed[identity]}
    if changed != expected_docs.keys() or _projection(previous) != _projection(current):
        raise StNoticeEvidenceError('November 19 catalog changes native ST decisions')

    documents = Path(__file__).resolve().parents[1] / DOCUMENT_DIR
    for identity, expected in {**expected_docs, **added_docs}.items():
        _verify_document(documents, identity, expected, f'https://pdf.dfcfw.com/pdf/H2_{identity}_1.pdf')
    if observed_sha == NOV26_OBSERVED_SHA256:
        _verify_document(documents, *NOV26_ST_NOTICE)

    comparison = {identity: dict(row, display_time=current[identity]['display_time']) for identity, row in original.items()}
    result = precision(previous, comparison)
    all_changes = sorted(added_docs.keys() | {key for key in previous if previous[key] != current[key]})
    if added_docs:
        result['reviewed_non_st_added_ids'] = sorted(added_docs)
    return dict(result, policy=policy,
                changed_event_count=len(all_changes), changed_event_ids_sha256=sha256(canonical(all_changes)).hexdigest(),
                reviewed_metadata_changed_ids=sorted(changed), risk_projection_unchanged=True,
                boundary_native_content_unchanged=False, original_archive_boundary_preserved=True,
                historical_state_rewrite_authorized=False, target_freshness_granted=False)
