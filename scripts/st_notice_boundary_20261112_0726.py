"""Exact 07:26 UTC D11 catalog observation; keep the original archive.

The seven source facts already reviewed on Nov12 retain their document gates.
This supplement admits only the exact observed BSE-920128 category removal.
Unknown fields, snapshots, identities and ST semantics remain hard failures.
"""
from datetime import datetime
from pathlib import Path
import copy
import hashlib
import json


PREFIX = 'data/st_notice_boundary_reviews/reviewed_20261112_0726/'
REGISTRY = PREFIX + 'registry.json'
REGISTRY_SHA = '18275ccba2690db15ac36f89d141d90003925464d7fd744a20fcbe4534ea21b7'
PREVIOUS_NATIVE_SHA = '75f1237e5118d2116fd1181e9f315245bd1a181a06caa7a8b43077a4b833e791'
CURRENT_NATIVE_SHA = '2b3226596ef5660bad139070bc41ca7b3af5cf144f6329b7295a6aa46aaa923c'
PREVIOUS_STABLE_SHA = '27ab78dfda644ab4df7cdfa4d69b493143b924e80d50c1fc781c8b70f053148e'
CURRENT_STABLE_SHA = '5957ccf4bf74470e50af050741fea0e2bcb15f4c0e13a8d6fb8e0e8243346259'
EXTRA_ID = 'AN202611111831628757'
ARTIFACTS = ('manifest.json', *[f'page_{number:04}.{suffix}'
    for number in range(1, 14) for suffix in ('json', 'receipt.json')])
SOURCE_FILES = ['scripts/st_notice_boundary_20261112_0726.py', REGISTRY] + [PREFIX + name for name in ARTIFACTS]


def _sha(raw):
    return hashlib.sha256(raw).hexdigest()


def _canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':')).encode('utf-8')


def _stable(rows):
    return {identity: {key: value for key, value in row.items() if key != 'display_time'}
        for identity, row in rows.items()}


def _require(condition, message):
    from scripts.st_notice_evidence import StNoticeEvidenceError
    if not condition:
        raise StNoticeEvidenceError('Exact Nov12 07:26 observation: ' + message)


def load_review(root=None):
    from scripts import st_notice_boundary_20261112 as original
    from scripts.st_notice_incremental import verify_day
    root = Path(root) if root is not None else Path(__file__).resolve().parents[1]
    raw = (root / REGISTRY).read_bytes()
    _require(_sha(raw) == REGISTRY_SHA, 'registry bytes changed')
    review = json.loads(raw)
    _require(review['decision'] == 'exact_0726_catalog_observation_no_history_rewrite'
        and review['boundary_day'] == '2026-11-11'
        and review['original_review_registry_sha256'] == original.REGISTRY_SHA
        and review['historical_authority_changed'] is False
        and review['historical_state_rewrite_authorized'] is False
        and review['target_freshness_granted'] is False
        and set(review['artifacts']) == set(ARTIFACTS), 'review scope changed')
    # Preserve and execute the earlier document/name/bond/issuer adjudication.
    prior_review, prior_rows = original.load_review(root)
    for name, digest in review['artifacts'].items():
        _require(_sha((root / PREFIX / name).read_bytes()) == digest, 'artifact bytes changed: ' + name)
    verified = verify_day(root / PREFIX, '2026-11-11',
        required_asof=datetime.fromisoformat(review['observation_floor']))
    _require(verified['manifest_sha256'] == review['current_manifest_sha256']
        and verified['observation_start'] == review['observation_start']
        and verified['observation_end'] == review['observation_end'], 'original observation identity changed')
    manifest = json.loads((root / PREFIX / 'manifest.json').read_bytes())
    current = {row['art_code']: row for page in manifest['pages']
        for row in json.loads((root / PREFIX / page['response']).read_bytes())['data']['list']}
    rows = dict(previous=prior_rows['previous'], current=current, prior_current=prior_rows['current'])
    _require(_sha(_canonical(rows['previous'])) == PREVIOUS_NATIVE_SHA
        and _sha(_canonical(current)) == CURRENT_NATIVE_SHA
        and _sha(_canonical(_stable(rows['previous']))) == PREVIOUS_STABLE_SHA
        and _sha(_canonical(_stable(current))) == CURRENT_STABLE_SHA, 'original native snapshot bytes changed')
    return review, rows


def reviewed_boundary(previous, current, precision):
    # Existing precision policy is the only variation allowed around raw snapshots.
    if (_sha(_canonical(_stable(previous))) != PREVIOUS_STABLE_SHA
        or _sha(_canonical(_stable(current))) != CURRENT_STABLE_SHA):
        return None
    from scripts import st_notice_boundary_20261112 as original
    from scripts.st_notice_boundary_20261111 import _selectors, _projection
    from scripts.st_notice_evidence import is_equity_issuer
    import microcap_top100_mom16_biweekly_live_v2_0 as base
    review, rows = load_review()
    precision(rows['previous'], previous)
    precision(rows['current'], current)
    prior = rows['prior_current']
    _require(current.keys() == prior.keys() and len(previous) == 1244 and len(current) == 1247,
        'native identity set changed')
    delta = {identity: sorted(field for field in prior[identity].keys() | current[identity].keys()
        if field != 'display_time' and prior[identity].get(field) != current[identity].get(field))
        for identity in prior if _stable({identity: prior[identity]}) != _stable({identity: current[identity]})}
    _require(delta == {EXTRA_ID: ['columns']}, 'additional unreviewed source differences')
    before, after = prior[EXTRA_ID], current[EXTRA_ID]
    _require(before['codes'] == after['codes']
        and before['codes'] == [{'stock_code': '920128', 'short_name': '胜业电气',
            'market_code': '0', 'ann_type': 'A,BJA', 'inner_code': '21420514080001'}]
        and before['columns'] == [{'column_code': '001003001001003', 'column_name': '专项说明/独立意见'},
            {'column_code': '001003002001', 'column_name': '保荐/核查意见'}]
        and after['columns'] == [{'column_code': '001003001001003', 'column_name': '专项说明/独立意见'}],
        'BSE issuer or exact category values changed')
    for row in (before, after):
        _require(not is_equity_issuer(row, '920128') and not _selectors(row)
            and base.freq_mod.classify_st_notice_risk_actions(row['title']) == {},
            'BSE record now has native ST semantics')
    # A comparison copy delegates all seven earlier facts to their original gates.
    # Frozen raw rows, pages, clocks, pointer and archive are never modified.
    comparison = copy.deepcopy(current)
    comparison[EXTRA_ID]['columns'] = copy.deepcopy(before['columns'])
    result = original.reviewed_boundary(previous, comparison, precision)
    _require(result is not None and result['risk_projection_unchanged'] is True,
        'earlier document review did not accept the original source facts')
    old_projection, new_projection = _projection(previous), _projection(current)
    _require(old_projection == new_projection
        and _sha(_canonical(old_projection)) == review['risk_projection_sha256'], 'native risk projection changed')
    metadata = {}
    for identity in previous.keys() & current.keys():
        fields = {field: dict(previous=previous[identity].get(field), current=current[identity].get(field))
            for field in previous[identity].keys() | current[identity].keys()
            if field != 'display_time' and previous[identity].get(field) != current[identity].get(field)}
        if fields:
            metadata[identity] = fields
    _require(metadata == review['metadata_changes'] and len(metadata) == 5, 'exact metadata scope changed')
    changed = sorted((current.keys() - previous.keys())
        | {identity for identity in previous if previous[identity] != current[identity]})
    underlying = dict(result)
    result.update(policy='exact_native_catalog_observation_20261112_0726_v1',
        underlying_boundary_review=underlying, review_registry_sha256=REGISTRY_SHA,
        original_review_registry_sha256=original.REGISTRY_SHA,
        reviewed_metadata_changes=metadata, additional_catalog_review_ids=[EXTRA_ID],
        changed_event_count=len(changed),
        changed_event_ids_sha256=_sha(json.dumps(changed, separators=(',', ':')).encode()),
        previous_native_sha256=PREVIOUS_NATIVE_SHA, current_native_sha256=CURRENT_NATIVE_SHA,
        previous_stable_native_sha256=PREVIOUS_STABLE_SHA, current_stable_native_sha256=CURRENT_STABLE_SHA,
        current_manifest_sha256=review['current_manifest_sha256'],
        original_observation_start=review['observation_start'], original_observation_end=review['observation_end'],
        original_archive_boundary_preserved=True, target_freshness_granted=False,
        historical_state_rewrite_authorized=False, all_market_PIT_certified=False,
        boundary_native_content_unchanged=False)
    return result
