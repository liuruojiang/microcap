"""Real frozen pages/documents; mutations are explicit software counterexamples."""
import copy
from datetime import datetime, timedelta
import hashlib
import json
from pathlib import Path
import shutil

import pytest

from scripts import st_notice_boundary_20261112 as reviewed
from scripts import st_notice_refresh as notice
from scripts.st_history_scope import source_closure
from scripts.st_notice_incremental import _provider_time
from scripts.st_notice_evidence import StNoticeEvidenceError

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope='module')
def real():
    return reviewed.load_review()


def observations(real):
    return copy.deepcopy(real[1]['previous']), copy.deepcopy(real[1]['current'])


def test_exact_original_pages_keep_real_risk_projection_and_input_bytes(real):
    old, new = observations(real)
    before = copy.deepcopy((old, new))
    result = notice._boundary_precision_drift(old, new)
    assert result == reviewed.reviewed_boundary(old, new, notice._precision_drift_only)
    assert result['policy'] == 'exact_native_catalog_observation_20261112_v1'
    assert len(result['reviewed_non_st_added_ids']) == 3
    assert set(result['reviewed_metadata_changes']) == set(reviewed.METADATA_FIELDS)
    assert result['risk_projection_unchanged'] and result['original_archive_boundary_preserved']
    assert result['historical_state_rewrite_authorized'] is False
    assert result['all_market_PIT_certified'] is False
    assert (old, new) == before


def test_original_documents_date_ordinary_names_to_Nov12_not_prior_history(real):
    for identity in ('AN202611111831630633', 'AN202611111831631588'):
        text = (ROOT / reviewed.PREFIX / (identity + '.txt')).read_text(encoding='utf-8')
        assert '证券简称启用日期' in text and '2026 年 11 月 12 日' in text
    assert all(row['notice_date'].startswith('2026-11-11')
               for key, row in real[1]['current'].items()
               if key in set(real[0]['added_ids']) | set(reviewed.METADATA_FIELDS))


@pytest.mark.parametrize('fault', ['title', 'column', 'equity_code', 'bond_code', 'A_removed',
    'new_ST_name', 'ordinary_name', 'prior_notice_date', 'future_notice_date', 'identity',
    'unknown_added', 'missing_added', 'missing_existing', 'unknown_field', 'old_snapshot',
    'clock_day', 'clock_close', 'clock_one_second'])
def test_unreviewed_source_or_risk_change_cannot_inherit_exact_review(real, fault):
    old, new = observations(real)
    key = 'AN202611111831632312'
    if fault == 'title':
        new[key]['title'] = '关于实施其他风险警示的公告'
    elif fault == 'column':
        new['AN202611111831629479']['columns'] = [{'column_code': '001002004003', 'column_name': '风险警示'}]
    elif fault == 'equity_code':
        new['AN202611111831630633']['codes'][0]['stock_code'] = '000001'
    elif fault == 'bond_code':
        old['AN202611111831631588']['codes'][0]['stock_code'] = '123195'
    elif fault == 'A_removed':
        new['AN202611111831631588']['codes'][0]['ann_type'] = 'KZZ,Bond'
    elif fault == 'new_ST_name':
        new['AN202611111831630633']['codes'][0]['short_name'] = '*ST中胤'
    elif fault == 'ordinary_name':
        new['AN202611111831630633']['codes'][0]['short_name'] = '另一个名称'
    elif fault in ('prior_notice_date', 'future_notice_date'):
        new[key]['notice_date'] = '2026-11-' + ('10' if fault == 'prior_notice_date' else '12') + ' 00:00:00'
    elif fault == 'identity':
        new[key]['art_code'] = 'AN999999999999999999'
    elif fault == 'unknown_added':
        row = copy.deepcopy(new['AN202611111831636086'])
        row['art_code'] = 'AN999999999999999999'
        new[row['art_code']] = row
    elif fault == 'missing_added':
        new.pop('AN202611111831636086')
    elif fault == 'missing_existing':
        new.pop('AN202611101831591478')
    elif fault == 'unknown_field':
        new[key]['listing_state'] = '9'
    elif fault == 'old_snapshot':
        old['AN202611111831629479']['title'] += ' unknown'
    elif fault == 'clock_day':
        new[key]['display_time'] = '2026-11-12 09:00:00.000'
    elif fault == 'clock_close':
        new[key]['display_time'] = '2026-11-11 14:59:59.999'
    else:
        new[key]['display_time'] = (_provider_time(new[key]['display_time']) + timedelta(seconds=1)).replace(tzinfo=None).isoformat(sep=' ')
    with pytest.raises((StNoticeEvidenceError, ValueError, KeyError)):
        notice._boundary_precision_drift(old, new)


def test_original_subsecond_precision_policy_still_applies(real):
    old, new = observations(real)
    key = 'AN202611111831632312'
    stamp = _provider_time(new[key]['display_time']) + timedelta(microseconds=1000)
    new[key]['display_time'] = stamp.replace(tzinfo=None).isoformat(sep=' ', timespec='microseconds')
    assert notice._boundary_precision_drift(old, new)['risk_projection_unchanged']


@pytest.mark.parametrize('name', ['previous/page_0001.json', 'current/page_0001.json',
    'current/page_0001.receipt.json', 'AN202611111831630633.pdf',
    'AN202611111831631588.txt', 'AN202611111831632312.receipt.json', 'registry.json'])
def test_raw_original_pages_documents_and_receipts_remain_hash_bound(tmp_path, name):
    destination = tmp_path / reviewed.PREFIX
    shutil.copytree(ROOT / reviewed.PREFIX, destination)
    target = destination / name
    target.write_bytes(target.read_bytes() + b'counterexample corruption')
    with pytest.raises((StNoticeEvidenceError, ValueError)):
        reviewed.load_review(tmp_path)


def test_actual_risk_selector_positive_control_cannot_silently_disappear(real, monkeypatch):
    import scripts.st_history_scope as scope
    old, new = observations(real)
    monkeypatch.setattr(scope, '_daily_notice_row', lambda *args: None)
    with pytest.raises(StNoticeEvidenceError, match='positive control'):
        reviewed.reviewed_boundary(old, new, notice._precision_drift_only)


def test_real_pages_resume_native_same_boundary_without_replacing_history(tmp_path):
    """Software archive envelope, original real 1244/1247 pages, no mocked native edge."""
    prefix = tmp_path / notice.PREFIX
    old_folder = prefix / 'seed/2026-11-11'
    generation = prefix / 'refresh_prepared_software_archive_envelope'
    shutil.copytree(ROOT / reviewed.PREFIX / 'previous', old_folder)
    shutil.copytree(ROOT / reviewed.PREFIX / 'current', generation / '2026-11-11')
    # This small envelope tests transport, not a substitute for the production PIT archive.
    scope = prefix / 'software-scope.json'
    scope.write_text(json.dumps({'end_date': '2026-11-10', 'source_files': []}), encoding='utf-8')
    spec = lambda path: {'path': path.relative_to(tmp_path).as_posix(),
                         'sha256': hashlib.sha256(path.read_bytes()).hexdigest()}
    archive = prefix / 'software-archive.json'
    archive.write_text(json.dumps({'schema_version': 3, 'scope': spec(scope),
        'daily_increments': [{'date': '2026-11-11', 'manifest': spec(old_folder / 'manifest.json')}]}), encoding='utf-8')
    pointer = tmp_path / notice.POINTER_PATH
    pointer.write_text(json.dumps({'manifest_path': spec(archive)['path'],
                                   'manifest_sha256': spec(archive)['sha256']}), encoding='utf-8')
    previous_generation = prefix / 'refresh_previous_software_archive_envelope'
    shutil.copytree(ROOT / reviewed.PREFIX / 'previous', previous_generation / '2026-11-11')
    previous_clock = min(datetime.fromisoformat(json.loads(path.read_bytes())['collected_at'])
                         for path in (previous_generation / '2026-11-11').glob('*.receipt.json'))
    notice.refresh_notice_archive(root=tmp_path, target='2026-11-11',
        resume_generation=previous_generation, required_asof=previous_clock)
    old_freshness = (tmp_path / notice.FRESHNESS).read_bytes()
    before = {path: path.read_bytes() for path in [pointer, archive, scope, *old_folder.iterdir()]}
    source_before = source_closure(tmp_path)
    new_before = {p: p.read_bytes() for p in (generation / '2026-11-11').iterdir()}
    expected_clock = min(datetime.fromisoformat(json.loads(raw)['collected_at'])
                         for path, raw in new_before.items() if path.name.endswith('.receipt.json')).isoformat()
    class NoNetwork:
        calls = 0
        def get(self, *args, **kwargs):
            self.calls += 1
            pytest.fail('Frozen prepared generation must not collect again')
    network = NoNetwork()
    result = notice.refresh_notice_archive(root=tmp_path, target='2026-11-11', session=network,
        resume_generation=generation, required_asof=datetime.fromisoformat('2026-11-12T09:00:00+08:00'))
    assert result['authority_changed'] is False and network.calls == 0
    assert all(path.read_bytes() == raw for path, raw in before.items())
    assert all(path.read_bytes() == raw for path, raw in new_before.items())
    assert source_closure(tmp_path) == source_before
    proof = json.loads((tmp_path / notice.FRESHNESS).read_bytes())
    assert proof['authority'] == proof['previous_authority']
    assert proof['required_asof'] == expected_clock
    assert (generation / 'previous_freshness.json').read_bytes() == old_freshness
    assert (previous_generation / 'proof.json').read_bytes() == old_freshness
    assert (previous_generation / 'previous_pointer.json').read_bytes() == before[pointer]
    assert proof['boundary_display_time_precision_drift']['policy'] == 'exact_native_catalog_observation_20261112_v1'
    assert not (generation / 'archive.json').exists()
    assert notice.freshness_source_files(tmp_path)
