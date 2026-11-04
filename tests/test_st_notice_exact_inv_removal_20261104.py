"""Real frozen source pair plus deliberately altered negative cases, no network."""
import copy
import json
from pathlib import Path

import pytest

from scripts import st_notice_refresh as m
from scripts.st_notice_evidence import StNoticeEvidenceError

FIXTURE = Path(__file__).parent / 'fixtures/st_notice_removed_inv_20261104.json'
ID = 'AN202611041831417267'


@pytest.fixture
def pair():
    value = json.loads(FIXTURE.read_bytes())
    return value['previous'], value['current']


def test_exact_actual_source_pair_passes_without_mutating_either_archive(pair):
    old, new = pair
    before = copy.deepcopy(pair)
    result = m._boundary_precision_drift(old, new)
    assert result['policy'] == 'exact_non_st_investor_removal_20261104_v1'
    assert result['removed_event_ids'] == [ID]
    assert result['precision_changed_event_count'] == 804
    assert result['changed_event_count'] == 805
    assert result['boundary_native_content_unchanged'] is False
    assert pair == before
    assert m._boundary_summary_matches(result, m._boundary_precision_drift(old, new), old, new)


@pytest.mark.parametrize('mutation', ['extra_deletion', 'unknown_addition', 'real_st_deletion',
    'removed_title', 'removed_issuer', 'removed_ann_type', 'removed_category', 'removed_day',
    'removed_time', 'common_st_title', 'common_category', 'common_date', 'common_price_field',
    'precision_one_second', 'precision_other_day', 'precision_cross_close', 'both_same_alteration'])
def test_unreviewed_or_disguised_changes_are_rejected(pair, mutation):
    old, new = pair
    common = next(iter(new))
    if mutation == 'extra_deletion': del new[common]
    elif mutation == 'unknown_addition': new['AN999999999999999999'] = copy.deepcopy(new[common])
    elif mutation == 'real_st_deletion':
        risk = next(k for k, row in new.items() if '风险警示' in row['title'])
        del new[risk]
    elif mutation == 'removed_title': old[ID]['title'] = '关于实施其他风险警示的公告'
    elif mutation == 'removed_issuer': old[ID]['codes'][0]['stock_code'] = '600000'
    elif mutation == 'removed_ann_type': old[ID]['codes'][0]['ann_type'] = 'A'
    elif mutation == 'removed_category': old[ID]['columns'][0]['column_code'] = '001002004003'
    elif mutation == 'removed_day': old[ID]['notice_date'] = '2026-11-05 00:00:00'
    elif mutation == 'removed_time': old[ID]['display_time'] = '2026-11-04 14:59:59:000'
    elif mutation == 'common_st_title': new[common]['title'] += '风险警示'
    elif mutation == 'common_category': new[common]['columns'] = [{'column_code':'001002004003'}]
    elif mutation == 'common_date': new[common]['notice_date'] = '2026-11-05 00:00:00'
    elif mutation == 'common_price_field': new[common]['price'] = '1'
    elif mutation == 'precision_one_second':
        from datetime import timedelta
        new[common]['display_time'] = (m._provider_time(old[common]['display_time']) + timedelta(seconds=1)).isoformat()
    elif mutation == 'precision_other_day': new[common]['display_time'] = '2026-11-05 12:00:00:000'
    elif mutation == 'precision_cross_close': new[common]['display_time'] = '2026-11-04 15:00:00:000'
    else:
        old[common]['title'] += 'changed'; new[common]['title'] += 'changed'
    with pytest.raises(StNoticeEvidenceError):
        m._boundary_precision_drift(old, new)


def test_original_precision_guard_remains_in_force(pair):
    old, new = pair
    with pytest.raises(StNoticeEvidenceError): m._precision_drift_only(old, new)
    common = {k:v for k,v in old.items() if k != ID}
    assert m._precision_drift_only(common, new)['changed_event_count'] == 804


def test_non_removed_boundary_has_original_behavior(pair):
    old, new = pair
    assert m._boundary_precision_drift(old, old)['changed_event_count'] == 0
    common = {k:v for k,v in old.items() if k != ID}
    assert m._boundary_precision_drift(common, new)['policy'] == 'display_time_same_day_lt_1s_no_close_crossing_v1'


def test_native_classifier_regression_cannot_self_approve(pair, monkeypatch):
    import microcap_top100_mom16_biweekly_live_v2_0 as base
    old, new = pair
    monkeypatch.setattr(base.freq_mod, 'classify_st_notice_risk_actions', lambda title: {'other': True})
    with pytest.raises(StNoticeEvidenceError): m._boundary_precision_drift(old, new)


def test_native_equity_classifier_regression_cannot_self_approve(pair, monkeypatch):
    from scripts import st_notice_evidence
    old, new = pair
    monkeypatch.setattr(st_notice_evidence, 'is_equity_issuer', lambda row,symbol: True)
    with pytest.raises(StNoticeEvidenceError): m._boundary_precision_drift(old, new)
