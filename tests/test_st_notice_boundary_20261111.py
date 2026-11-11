"""Exact real 1072->1401 observation, with positive risk-selector controls."""
import copy
from datetime import timedelta
from pathlib import Path
import shutil
import pytest

from scripts import st_notice_boundary_20261111 as reviewed
from scripts.st_notice_refresh import _boundary_precision_drift, _precision_drift_only
from scripts.st_notice_incremental import _provider_time
from scripts.st_notice_evidence import StNoticeEvidenceError

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope='module')
def real():
    return reviewed.load_review()


def observations(real):
    return copy.deepcopy(real[1]['previous']), copy.deepcopy(real[1]['current'])


def test_real_delisting_warning_is_positive_selector_control(real):
    _, rows = real
    for folder, identity in [('previous', 'AN202611091831558117'), ('day11', 'AN202611101831591478')]:
        assert set(reviewed._selectors(rows[folder][identity])) == {'002731'}
    assert 'AN202611101831591478' not in rows['current']
    # It is already within the delisting period, not a fresh entry decision.
    text = (ROOT / reviewed.PREFIX / 'AN202611101831591478.txt').read_text(encoding='utf-8')
    assert '第七次风险提示公告' in text and '2026 年 10 月 23 日' in text
    assert '第十四个交易日' in text and '2026 年 11 月 12 日' in text
    assert '2026-121' in text


def test_real_exact_boundary_retains_risk_and_limits_scope(real):
    old, new = observations(real)
    result = _boundary_precision_drift(old, new)
    assert result == reviewed.reviewed_boundary(old, new, _precision_drift_only)
    assert result['risk_projection_unchanged'] and not result['boundary_native_content_unchanged']
    assert result['precision_changed_event_count'] == 789 and result['changed_event_count'] == 1118
    assert len(result['reviewed_non_st_added_ids']) == 329
    assert len(result['reviewed_current_scope_added_ids']) == 17 and result['out_of_scope_added_count'] == 312
    assert result['all_market_PIT_certified'] is False
    assert result['historical_state_rewrite_authorized'] is False
    assert set(result['reviewed_metadata_changes']) == {'AN202611091831555385'}
    assert reviewed._projection(old) == reviewed._projection(new)


@pytest.mark.parametrize('fault', ['title', 'category', 'issuer', 'notice_date', 'extra', 'missing',
    'old_name', 'risk_name', 'clock', 'daily11_mixed_into_boundary'])
def test_unknown_row_or_clock_cannot_inherit_exact_review(real, fault):
    old, new = observations(real)
    key = real[0]['added_ids'][0]
    if fault == 'title':new[key]['title'] = '关于实施其他风险警示的公告'
    elif fault == 'category':new[key]['columns'] = [{'column_code': '001002004003', 'column_name': 'ST'}]
    elif fault == 'issuer':new[key]['codes'][0]['stock_code'] = '000001'
    elif fault == 'notice_date':new[key]['notice_date'] = '2026-11-11 00:00:00'
    elif fault == 'extra':new['AN999999999999999999'] = dict(new[key], art_code='AN999999999999999999')
    elif fault == 'missing':new.pop(key)
    elif fault == 'old_name':new['AN202611091831555385']['codes'][0]['short_name'] = 'unreviewed'
    elif fault == 'risk_name':new['AN202611091831555385']['codes'][0]['stock_code'] = '000001'
    elif fault == 'clock':new[key]['display_time'] = '2026-11-10 14:59:59:999'
    else:
        identity = 'AN202611101831591478';new[identity] = copy.deepcopy(real[1]['day11'][identity])
    with pytest.raises((StNoticeEvidenceError, ValueError, KeyError)):
        _boundary_precision_drift(old, new)


def test_only_original_subsecond_precision_policy_applies(real):
    old, new = observations(real)
    key = real[0]['added_ids'][0]
    value = _provider_time(new[key]['display_time']) + timedelta(microseconds=1000)
    new[key]['display_time'] = value.replace(tzinfo=None).isoformat(sep=' ', timespec='microseconds')
    assert reviewed.reviewed_boundary(old, new, _precision_drift_only)['risk_projection_unchanged']


@pytest.mark.parametrize('name', ['current/page_0001.json', 'current/page_0001.receipt.json',
    'AN202611091831555385.pdf', 'AN202611101831591478.receipt.json', 'registry.json'])
def test_original_page_receipt_and_documents_are_bound(tmp_path, name):
    destination = tmp_path / reviewed.PREFIX
    shutil.copytree(ROOT / reviewed.PREFIX, destination)
    target = destination / name
    target.write_bytes(target.read_bytes() + b'changed')
    with pytest.raises((StNoticeEvidenceError, ValueError)):
        reviewed.load_review(tmp_path)


def test_selector_false_negative_cannot_certify_empty_projection(real, monkeypatch):
    old, new = observations(real)
    monkeypatch.setattr(reviewed, '_selectors', lambda row: {})
    with pytest.raises(StNoticeEvidenceError, match='positive control'):
        reviewed.reviewed_boundary(old, new, _precision_drift_only)
