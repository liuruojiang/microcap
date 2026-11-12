"""One exact 11-Nov catalog re-observation; retain the original ST archive."""
from datetime import datetime
from pathlib import Path
import copy
import hashlib
import json
import re

PREFIX = 'data/st_notice_boundary_reviews/reviewed_20261112/'
REGISTRY = PREFIX + 'registry.json'
REGISTRY_SHA = '7c66c3334d3faeaff724ebf2721f6a738dc5fee0f22dfb5a6675cdc7d23ac6e0'
PREVIOUS_STABLE_SHA = '27ab78dfda644ab4df7cdfa4d69b493143b924e80d50c1fc781c8b70f053148e'
CURRENT_STABLE_SHA = '709a76eba3fdebfc0b458d0f6347e60037a3062a3743674cd497d20533e02587'
DOCUMENT_IDS = ('AN202611111831629479', 'AN202611111831630633',
                'AN202611111831630634', 'AN202611111831631588',
                'AN202611111831632312', 'AN202611111831636086',
                'AN202611111831636184')
ARTIFACTS = tuple(folder + '/' + name for folder in ('previous', 'current')
                  for name in ('manifest.json', *[f'page_{n:04}.{suffix}'
                      for n in range(1, 14) for suffix in ('json', 'receipt.json')]))
ARTIFACTS += tuple(identity + suffix for identity in DOCUMENT_IDS
                   for suffix in ('.pdf', '.txt', '.receipt.json'))
SOURCE_FILES = ['scripts/st_notice_boundary_20261112.py', REGISTRY] + [PREFIX + name for name in ARTIFACTS]
METADATA_FIELDS = {'AN202611111831629479': {'columns'},
                   'AN202611111831630633': {'codes'},
                   'AN202611111831630634': {'codes'},
                   'AN202611111831631588': {'codes'}}


def _sha(raw):
    return hashlib.sha256(raw).hexdigest()


def _canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':')).encode()


def _stable(rows):
    return {identity: {key: value for key, value in row.items() if key != 'display_time'}
            for identity, row in rows.items()}


def _require(condition, message):
    from scripts.st_notice_evidence import StNoticeEvidenceError
    if not condition:
        raise StNoticeEvidenceError('Exact Nov12 observation: ' + message)


def load_review(root=None):
    from scripts.st_notice_incremental import verify_day
    root = Path(root) if root is not None else Path(__file__).resolve().parents[1]
    raw = (root / REGISTRY).read_bytes()
    _require(_sha(raw) == REGISTRY_SHA, 'registry bytes changed')
    review = json.loads(raw)
    _require(review['decision'] == 'exact_20261112_observation_no_history_rewrite'
             and review['boundary_day'] == '2026-11-11'
             and review['historical_authority_changed'] is False
             and review['historical_state_rewrite_authorized'] is False
             and set(review['artifacts']) == set(ARTIFACTS), 'review scope changed')
    for name, digest in review['artifacts'].items():
        _require(_sha((root / PREFIX / name).read_bytes()) == digest, 'artifact bytes changed: ' + name)
    rows = {}
    for folder in ('previous', 'current'):
        directory = root / PREFIX / folder
        verify_day(directory, '2026-11-11', **({'required_asof': datetime.fromisoformat(review['observation_floor'])}
                                             if folder == 'current' else {}))
        manifest = json.loads((directory / 'manifest.json').read_bytes())
        rows[folder] = {row['art_code']: row for page in manifest['pages']
                        for row in json.loads((directory / page['response']).read_bytes())['data']['list']}
        _require(_sha(_canonical(rows[folder])) == review[folder + '_native_sha256'], 'native rows changed')
    for identity in DOCUMENT_IDS:
        pdf = (root / PREFIX / (identity + '.pdf')).read_bytes()
        text = (root / PREFIX / (identity + '.txt')).read_bytes()
        receipt = json.loads((root / PREFIX / (identity + '.receipt.json')).read_bytes())
        started, ended = (datetime.fromisoformat(receipt[key]) for key in ('http_start', 'http_complete'))
        _require(pdf.startswith(b'%PDF-') and b'%%EOF' in pdf[-1024:]
                 and receipt['url'] == 'https://pdf.dfcfw.com/pdf/H2_' + identity + '_1.pdf'
                 and receipt['http_status'] == 200 and receipt['tls_verification'] is True
                 and receipt['raw_sha256'] == _sha(pdf) and receipt['raw_bytes'] == len(pdf)
                 and receipt['text_sha256'] == _sha(text.replace(b'\r\n', b'\n'))
                 and receipt['native_row'] == rows['current'][identity]
                 and started.tzinfo is not None and ended.tzinfo is not None
                 and started <= ended <= datetime.now(ended.tzinfo), 'issuer document identity changed')
    for identity in ('AN202611111831630633', 'AN202611111831631588'):
        text = (root / PREFIX / (identity + '.txt')).read_text(encoding='utf-8')
        _require('证券简称启用日期' in text
                 and re.search(r'2026\s*年\s*11\s*月\s*12\s*日', text), 'ordinary name effective date changed')
    return review, rows


def _metadata_checks(previous, current, review):
    from scripts.st_notice_boundary_20261111 import _selectors
    from scripts.st_notice_evidence import is_equity_issuer
    import microcap_top100_mom16_biweekly_live_v2_0 as base
    changed = {}
    for identity in previous.keys() & current.keys():
        old, new = previous[identity], current[identity]
        fields = {key for key in old.keys() | new.keys()
                  if key != 'display_time' and old.get(key) != new.get(key)}
        if fields:
            _require(fields == METADATA_FIELDS.get(identity), 'unknown metadata fields')
            changed[identity] = {key: {'previous': old[key], 'current': new[key]} for key in fields}
            for row in (old, new):
                _require(base.freq_mod.classify_st_notice_risk_actions(row['title']) == {}
                         and not _selectors(row), 'metadata now changes ST selection')
    _require(changed == review['metadata_changes'] and set(changed) == set(METADATA_FIELDS), 'metadata values changed')
    for identity, symbol, before, after in (
            ('AN202611111831630633', '300901', '中胤时尚', '中胤科技'),
            ('AN202611111831630634', '300901', '中胤时尚', '中胤科技'),
            ('AN202611111831631588', '301015', '百洋医药', '百洋制药')):
        old, new = previous[identity], current[identity]
        _require(is_equity_issuer(old, symbol) and is_equity_issuer(new, symbol), 'equity issuer changed')
        old_code = [code for code in old['codes'] if code['stock_code'] == symbol]
        _require(len(old_code) == len(new['codes']) == 1
                 and old_code[0]['short_name'] == before and new['codes'][0]['short_name'] == after
                 and {key: value for key, value in old_code[0].items() if key != 'short_name'}
                     == {key: value for key, value in new['codes'][0].items() if key != 'short_name'}, 'ordinary name issuer fields changed')
        if symbol == '301015':
            bond = [code for code in old['codes'] if code['stock_code'] != symbol]
            _require(len(bond) == 1 and bond[0]['stock_code'] == '123194'
                     and bond[0]['ann_type'] == 'KZZ,Bond' and not is_equity_issuer(old, '123194'), 'non-equity catalog removal changed')
        else:
            _require(len(old['codes']) == 1, 'ordinary name catalog scope changed')
    identity = 'AN202611111831629479'
    _require(previous[identity]['codes'] == current[identity]['codes']
             and is_equity_issuer(current[identity], '300368')
             and previous[identity]['columns'] == [{'column_code': '001002006014', 'column_name': '诉讼仲裁'},
                                                  {'column_code': '001002008', 'column_name': '其他'}]
             and current[identity]['columns'] == [{'column_code': '001002008', 'column_name': '其他'}], 'ordinary litigation catalog changed')


def reviewed_boundary(previous, current, precision):
    # Unknown observations retain every original native guard; this is not a waiver.
    if (_sha(_canonical(_stable(previous))) != PREVIOUS_STABLE_SHA
            or _sha(_canonical(_stable(current))) != CURRENT_STABLE_SHA):
        return None
    review, rows = load_review()
    precision(rows['previous'], previous)
    precision(rows['current'], current)
    _require(len(previous) == 1244 and len(current) == 1247 and not previous.keys() - current.keys(), 'native identity set changed')
    added = sorted(current.keys() - previous.keys())
    _require(added == review['added_ids'] and len(added) == 3, 'addition identities changed')
    from scripts.st_notice_inv_research_boundary import is_unconsumed_inv_research, is_unconsumed_equity_notice
    from scripts.st_notice_boundary_20261111 import _selectors, _projection
    _require(is_unconsumed_equity_notice('AN202611111831632312', current['AN202611111831632312'])
             and all(is_unconsumed_inv_research(identity, current[identity])
                     for identity in ('AN202611111831636086', 'AN202611111831636184')), 'new record now has native risk semantics')
    _metadata_checks(previous, current, review)
    positive = 'AN202611101831591478'
    _require(set(_selectors(previous[positive])) == set(_selectors(current[positive])) == {'002731'}, 'risk selector positive control failed')
    before, after = _projection(previous), _projection(current)
    _require(before == after and set(before) == {positive}
             and _sha(_canonical(before)) == review['risk_projection_sha256'], 'full native risk projection changed')
    # Compare precision on a copy only; raw pages and the original archive remain immutable.
    common = {identity: copy.deepcopy(current[identity]) for identity in previous}
    for identity, fields in METADATA_FIELDS.items():
        for field in fields:
            common[identity][field] = copy.deepcopy(previous[identity][field])
    result = precision(previous, common)
    changed = sorted(set(added) | {key for key in previous if previous[key] != current[key]})
    result.update(policy='exact_native_catalog_observation_20261112_v1',
        precision_changed_event_count=result['changed_event_count'], changed_event_count=len(changed),
        changed_event_ids_sha256=_sha(json.dumps(changed, separators=(',', ':')).encode()),
        reviewed_non_st_added_ids=added, removed_event_ids=[], reviewed_metadata_changes=review['metadata_changes'],
        review_registry_sha256=REGISTRY_SHA, previous_stable_native_sha256=PREVIOUS_STABLE_SHA,
        current_stable_native_sha256=CURRENT_STABLE_SHA, native_risk_projection_sha256=review['risk_projection_sha256'],
        risk_projection_unchanged=True, original_archive_boundary_preserved=True,
        historical_state_rewrite_authorized=False, all_market_PIT_certified=False,
        boundary_native_content_unchanged=False)
    return result
