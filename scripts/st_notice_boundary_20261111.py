"""Exact 10-Nov source delta; no historical state or out-of-scope PIT waiver."""
from pathlib import Path
import copy
import hashlib
import json

PREFIX = 'data/st_notice_boundary_reviews/reviewed_20261111/'
REGISTRY = PREFIX + 'registry.json'
REGISTRY_SHA = 'f1d8fcfcc665f3b4a705ff8d1cea2717170288fb98680426b9b98120a0f0c97d'
ARTIFACTS = tuple(folder + '/' + name
    for folder, count in (('previous', 11), ('current', 15), ('day11', 13))
    for name in ('manifest.json', *[f'page_{n:04}.{suffix}'
        for n in range(1, count + 1) for suffix in ('json', 'receipt.json')]))
ARTIFACTS += tuple(identity + suffix
    for identity in ('AN202611091831555385', 'AN202611101831591478')
    for suffix in ('.pdf', '.txt', '.receipt.json'))
SOURCE_FILES = ['scripts/st_notice_boundary_20261111.py', REGISTRY] + [PREFIX + name for name in ARTIFACTS]


def _sha(raw):
    return hashlib.sha256(raw).hexdigest()


def _canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':')).encode()


def _require(condition, message):
    from scripts.st_notice_evidence import StNoticeEvidenceError
    if not condition:
        raise StNoticeEvidenceError('Exact Nov11 boundary: ' + message)


def _native(root, folder, day):
    from scripts.st_notice_incremental import verify_day
    directory = root / PREFIX / folder
    verify_day(directory, day)
    manifest = json.loads((directory / 'manifest.json').read_bytes())
    return {row['art_code']: row for page in manifest['pages']
            for row in json.loads((directory / page['response']).read_bytes())['data']['list']}


def load_review(root=None):
    root = Path(root) if root is not None else Path(__file__).resolve().parents[1]
    raw = (root / REGISTRY).read_bytes()
    _require(_sha(raw) == REGISTRY_SHA, 'registry bytes changed')
    review = json.loads(raw)
    _require(set(review['artifacts']) == set(ARTIFACTS), 'artifact inventory changed')
    for name, digest in review['artifacts'].items():
        _require(_sha((root / PREFIX / name).read_bytes()) == digest, 'artifact bytes changed: ' + name)
    rows = {folder: _native(root, folder, day) for folder, day in
            (('previous', '2026-11-10'), ('current', '2026-11-10'), ('day11', '2026-11-11'))}
    for folder in ('previous', 'current'):
        _require(_sha(_canonical(rows[folder])) == review[folder + '_native_sha256'],
                 folder + ' native rows changed')
    for identity in ('AN202611091831555385', 'AN202611101831591478'):
        pdf = (root / PREFIX / (identity + '.pdf')).read_bytes()
        receipt = json.loads((root / PREFIX / (identity + '.receipt.json')).read_bytes())
        url = 'https://pdf.dfcfw.com/pdf/H2_' + identity + '_1.pdf'
        _require(pdf.startswith(b'%PDF-') and b'%%EOF' in pdf[-1024:]
                 and receipt['url'] == receipt['actual_url'] == url
                 and receipt['http_status'] == 200 and receipt['tls_verification'] is True
                 and receipt['redirects_allowed'] is False
                 and receipt['raw_sha256'] == _sha(pdf) and receipt['bytes'] == len(pdf),
                 'issuer document receipt differs')
    return review, rows


def _selectors(row):
    from scripts.st_notice_evidence import is_equity_issuer
    from scripts.st_history_scope import _daily_notice_row
    return {symbol: selected for symbol in {code['stock_code'] for code in row['codes']}
            if is_equity_issuer(row, symbol)
            and (selected := _daily_notice_row(row, symbol, row['notice_date'][:10], {})) is not None}


def _projection(rows):
    import microcap_top100_mom16_biweekly_live_v2_0 as base
    result = {}
    for identity, row in rows.items():
        actions = base.freq_mod.classify_st_notice_risk_actions(row['title'])
        selected = _selectors(row)
        if actions or selected:
            result[identity] = dict(actions=actions, selected=selected)
    return result


def reviewed_boundary(previous, current, precision):
    review, rows = load_review()
    stable = lambda values: {key: {field: value for field, value in row.items() if field != 'display_time'}
                             for key, row in values.items()}
    # An unknown snapshot cannot inherit this exact observation review.
    if stable(previous) != stable(rows['previous']) or stable(current) != stable(rows['current']):
        return None
    precision(rows['previous'], previous)
    precision(rows['current'], current)
    _require(len(previous) == 1072 and len(current) == 1401 and not previous.keys() - current.keys(),
             'snapshot counts/removals differ')
    added = sorted(current.keys() - previous.keys())
    _require(added == review['added_ids'] and len(added) == 329, 'added identity set differs')
    # Known actual category-selected risk notices are positive controls. Empty
    # title actions alone must never hide delisting warnings from this selector.
    old_warning, new_warning = 'AN202611091831558117', 'AN202611101831591478'
    _require(set(_selectors(previous[old_warning])) == {'002731'}
             and set(_selectors(rows['day11'][new_warning])) == {'002731'}
             and new_warning not in previous and new_warning not in current,
             'actual delisting selector positive control failed')
    import microcap_top100_mom16_biweekly_live_v2_0 as base
    for identity in added:
        _require(not _selectors(current[identity])
                 and base.freq_mod.classify_st_notice_risk_actions(current[identity]['title']) == {},
                 'addition now has native risk semantics')
    before_projection, after_projection = _projection(previous), _projection(current)
    _require(before_projection == after_projection, 'existing risk projection changed')
    _require(set(before_projection) == {old_warning, 'AN202611091831555385'},
             'known original risk inventory changed')
    scope = set(review['scope_symbols'])
    in_scope = {identity: sorted({code['stock_code'] for code in current[identity]['codes']} & scope)
                for identity in added}
    in_scope = {identity: symbols for identity, symbols in in_scope.items() if symbols}
    _require(len(scope) == 300 and in_scope == review['in_scope_added_ids'] and len(in_scope) == 17,
             'reviewed current300 scope differs')
    metadata = 'AN202611091831555385'
    common = {identity: copy.deepcopy(current[identity]) for identity in previous}
    # Exact source snapshots already pin every field. Only this documented
    # issuer display name differs, and its original risk event remains selected.
    _require(previous[metadata]['codes'][0]['stock_code'] == current[metadata]['codes'][0]['stock_code'] == '688053'
             and previous[metadata]['codes'][0]['short_name'] == 'ST思科瑞'
             and current[metadata]['codes'][0]['short_name'] == '思科瑞', 'issuer display transition differs')
    common[metadata]['codes'][0]['short_name'] = previous[metadata]['codes'][0]['short_name']
    result = precision(previous, common)
    changed = sorted(set(added) | {identity for identity in previous if previous[identity] != current[identity]})
    result.update(policy='exact_native_source_delta_boundary_20261111_v1',
        precision_changed_event_count=result['changed_event_count'], changed_event_count=len(changed),
        changed_event_ids_sha256=_sha(json.dumps(changed, separators=(',', ':')).encode()),
        reviewed_non_st_added_ids=added, removed_event_ids=[],
        reviewed_metadata_changes={metadata: review['metadata']}, review_registry_sha256=REGISTRY_SHA,
        native_risk_projection_sha256=_sha(_canonical(before_projection)), risk_projection_unchanged=True,
        scope_symbols_sha256=_sha(_canonical(review['scope_symbols'])), reviewed_current_scope_added_ids=sorted(in_scope),
        out_of_scope_added_count=312, all_market_PIT_certified=False,
        historical_state_rewrite_authorized=False, boundary_native_content_unchanged=False)
    return result
