"""Exact real D11 re-observation; all mutations are software counterexamples."""
import copy
from datetime import datetime, timedelta
import hashlib
import json
from pathlib import Path
import shutil

import pytest

from scripts import st_notice_boundary_20261112 as original
from scripts import st_notice_boundary_20261112_0726 as reviewed
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


def test_original_failed_boundary_now_passes_exact_profile_without_mutation(real):
    old, new = observations(real)
    before = copy.deepcopy((old, new))
    result = notice._boundary_precision_drift(old, new)
    assert result['policy'] == 'exact_native_catalog_observation_20261112_0726_v1'
    assert result['underlying_boundary_review']['policy'] == 'exact_native_catalog_observation_20261112_v1'
    assert result['precision_changed_event_count'] == 919
    assert result['maximum_delta_microseconds'] == 205000
    assert result['crossed_integer_second_count'] == 11
    assert len(result['reviewed_non_st_added_ids']) == 3
    assert len(result['reviewed_metadata_changes']) == 5
    assert result['additional_catalog_review_ids'] == [reviewed.EXTRA_ID]
    assert result['risk_projection_unchanged'] and result['original_archive_boundary_preserved']
    assert result['historical_state_rewrite_authorized'] is False
    assert result['target_freshness_granted'] is result['all_market_PIT_certified'] is False
    assert (old, new) == before


def test_existing_exact_review_keeps_its_original_summary(real):
    old = real[1]['previous']
    current = real[1]['prior_current']
    assert notice._boundary_precision_drift(old, current) == original.reviewed_boundary(
        old, current, notice._precision_drift_only)
    assert reviewed.reviewed_boundary(old, current, notice._precision_drift_only) is None


@pytest.mark.parametrize('fault', ['risk_title', 'ST_column', 'unknown_column', 'issuer', 'asset_class',
    'market', 'ordinary_name', 'unknown_field', 'old_snapshot', 'rename_fact', 'missing_existing',
    'missing_added', 'unreviewed_added', 'notice_day', 'one_second', 'cross_close', 'display_day'])
def test_unknown_snapshot_and_ST_changes_cannot_inherit_exact_review(real, fault):
    old, new = observations(real)
    row = new[reviewed.EXTRA_ID]
    if fault == 'risk_title':
        row['title'] = '胜业电气:关于实施其他风险警示的公告'
    elif fault in {'ST_column', 'unknown_column'}:
        row['columns'] = [{'column_code': '001002004003' if fault == 'ST_column' else '001003002002',
            'column_name': '风险警示' if fault == 'ST_column' else '新分类'}]
    elif fault == 'issuer':
        row['codes'][0]['stock_code'] = '000001'
    elif fault == 'asset_class':
        row['codes'][0]['ann_type'] = 'A'
    elif fault == 'market':
        row['codes'][0]['market_code'] = '1'
    elif fault == 'ordinary_name':
        row['codes'][0]['short_name'] = '另一个名称'
    elif fault == 'unknown_field':
        row['listing_state'] = '9'
    elif fault == 'old_snapshot':
        old[reviewed.EXTRA_ID]['title'] += ' unknown'
    elif fault == 'rename_fact':
        new['AN202611111831630633']['codes'][0]['short_name'] = '*ST中胤'
    elif fault == 'missing_existing':
        new.pop('AN202611101831591478')
    elif fault == 'missing_added':
        new.pop('AN202611111831636086')
    elif fault == 'unreviewed_added':
        extra = copy.deepcopy(new['AN202611111831636086'])
        extra['art_code'] = 'AN999999999999999999'
        new[extra['art_code']] = extra
    elif fault == 'notice_day':
        row['notice_date'] = '2026-11-12 00:00:00'
    elif fault == 'one_second':
        row['display_time'] = (_provider_time(row['display_time']) + timedelta(seconds=1)).replace(tzinfo=None).isoformat(sep=' ')
    elif fault == 'cross_close':
        row['display_time'] = '2026-11-11 14:59:59.999'
    else:
        row['display_time'] = '2026-11-12 17:00:00.701'
    with pytest.raises((StNoticeEvidenceError, ValueError, KeyError)):
        notice._boundary_precision_drift(old, new)


def test_original_precision_policy_is_preserved_for_known_snapshot(real):
    old, new = observations(real)
    row = new[reviewed.EXTRA_ID]
    row['display_time'] = (_provider_time(row['display_time']) + timedelta(microseconds=1000)).replace(tzinfo=None).isoformat(sep=' ')
    assert notice._boundary_precision_drift(old, new)['risk_projection_unchanged']


def test_real_positive_ST_control_cannot_be_reclassified_empty(real, monkeypatch):
    import scripts.st_history_scope as scope
    old, new = observations(real)
    monkeypatch.setattr(scope, '_daily_notice_row', lambda *_args: None)
    with pytest.raises(StNoticeEvidenceError, match='positive control'):
        reviewed.reviewed_boundary(old, new, notice._precision_drift_only)


@pytest.mark.parametrize('name', ['manifest.json', 'page_0001.json', 'page_0001.receipt.json', 'registry.json'])
def test_raw_observation_clocks_and_pages_remain_hash_bound(tmp_path, name):
    for prefix in (original.PREFIX, reviewed.PREFIX):
        shutil.copytree(ROOT / prefix, tmp_path / prefix)
    target = tmp_path / reviewed.PREFIX / name
    target.write_bytes(target.read_bytes() + b'counterexample corruption')
    with pytest.raises((StNoticeEvidenceError, ValueError)):
        reviewed.load_review(tmp_path)


def test_original_document_gates_remain_required(tmp_path):
    for prefix in (original.PREFIX, reviewed.PREFIX):
        shutil.copytree(ROOT / prefix, tmp_path / prefix)
    target = tmp_path / original.PREFIX / 'AN202611111831630633.pdf'
    target.write_bytes(target.read_bytes() + b'counterexample corruption')
    with pytest.raises(StNoticeEvidenceError, match='artifact bytes changed'):
        reviewed.load_review(tmp_path)


def test_new_profile_is_in_native_source_transport_closure():
    from scripts.st_notice_boundary_review import SOURCE_FILES
    assert set(reviewed.SOURCE_FILES) <= set(SOURCE_FILES)
    assert all((ROOT / name).is_file() for name in reviewed.SOURCE_FILES)
    assert reviewed.REGISTRY_SHA == hashlib.sha256((ROOT / reviewed.REGISTRY).read_bytes()).hexdigest()


def test_actual_refresh_consumes_exact_raw_pages_without_rewriting_boundary(tmp_path):
    """Real Native edge, software envelope. This grants no D12 formal readiness."""
    prefix = tmp_path / notice.PREFIX
    seed = prefix / 'software-seed/2026-11-11'
    generation = prefix / 'refresh_software_0726_envelope'
    shutil.copytree(ROOT / original.PREFIX / 'previous', seed)
    shutil.copytree(ROOT / reviewed.PREFIX, generation / '2026-11-11')
    # registry.json is review provenance, not part of the collected day.
    (generation / '2026-11-11/registry.json').unlink()
    scope = prefix / 'software-scope.json'
    scope.write_text(json.dumps(dict(end_date='2026-11-10', source_files=[])), encoding='utf-8')
    spec = lambda path: dict(path=path.relative_to(tmp_path).as_posix(),
        sha256=hashlib.sha256(path.read_bytes()).hexdigest())
    archive = prefix / 'software-archive.json'
    archive.write_text(json.dumps(dict(schema_version=3, scope=spec(scope), daily_increments=[
        dict(date='2026-11-11', manifest=spec(seed / 'manifest.json'))])), encoding='utf-8')
    pointer = tmp_path / notice.POINTER_PATH
    pointer.write_text(json.dumps(dict(manifest_path=spec(archive)['path'],
        manifest_sha256=spec(archive)['sha256'])), encoding='utf-8')
    previous_generation = prefix / 'refresh_software_original_envelope'
    shutil.copytree(ROOT / original.PREFIX / 'previous', previous_generation / '2026-11-11')
    old_clock = min(datetime.fromisoformat(json.loads(path.read_bytes())['collected_at'])
        for path in (previous_generation / '2026-11-11').glob('*.receipt.json'))
    class NoNetwork:
        calls = 0
        def get(self, *_args, **_kwargs):
            self.calls += 1
            pytest.fail('Frozen original pages must not be recollected')
    network = NoNetwork()
    notice.refresh_notice_archive(root=tmp_path, target='2026-11-11', session=network,
        resume_generation=previous_generation, required_asof=old_clock)
    old_freshness = (tmp_path / notice.FRESHNESS).read_bytes()
    watched = [pointer, archive, scope, *seed.iterdir(), *(generation / '2026-11-11').iterdir()]
    before = {path: path.read_bytes() for path in watched}
    closure = source_closure(tmp_path)
    result = notice.refresh_notice_archive(root=tmp_path, target='2026-11-11', session=network,
        resume_generation=generation, required_asof=datetime.fromisoformat('2026-11-12T09:00:00+08:00'))
    assert network.calls == 0 and result['authority_changed'] is False
    assert all(path.read_bytes() == raw for path, raw in before.items())
    assert source_closure(tmp_path) == closure
    proof = json.loads((tmp_path / notice.FRESHNESS).read_bytes())
    assert proof['authority'] == proof['previous_authority']
    assert proof['boundary_display_time_precision_drift']['policy'] == 'exact_native_catalog_observation_20261112_0726_v1'
    assert (generation / 'previous_freshness.json').read_bytes() == old_freshness
    assert notice.freshness_source_files(tmp_path)
    assert not (generation / 'archive.json').exists()
