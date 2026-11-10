"""Actual registration boundary, plus counterexamples for scope and clocks."""
import copy
import gzip
import hashlib
import json
from pathlib import Path

import pytest
from scripts import st_notice_refresh as refresh
from scripts.st_notice_evidence import StNoticeEvidenceError

FIXTURE = Path(__file__).parent / 'fixtures/ipo_boundary_20261109'
IDENTITY = 'AN202611091831554859'


@pytest.fixture
def pair():
    with gzip.open(FIXTURE / 'native_pair.json.gz', 'rb') as stream:
        value = json.load(stream)
    return value['previous'], value['current']


def test_real_prelisting_addition_preserves_sources_and_original_precision(pair):
    before = copy.deepcopy(pair)
    result = refresh._boundary_precision_drift(*pair)
    assert result['policy'] == 'exact_prelisting_registration_20261109_v1'
    assert result['reviewed_non_st_added_ids'] == [IDENTITY]
    assert result['precision_changed_event_count'] == 910
    assert result['risk_projection_unchanged'] is True
    assert pair == before
    raw = (FIXTURE / (IDENTITY + '.pdf')).read_bytes()
    receipt = json.loads((FIXTURE / 'receipt.json').read_bytes())
    assert hashlib.sha256(raw).hexdigest() == receipt['sha256'] == 'ec90a6dcb813d0c12283a71c75f10cea1a57cc5d69a6419daa9c6d3338e3bfcd'
    assert receipt['bytes'] == len(raw) == 796570
    assert receipt['pages'] == 2


@pytest.mark.parametrize('change', ['risk', 'listed', 'extra', 'remove', 'existing', 'clock', 'old_clock'])
def test_unreviewed_native_or_clock_changes_cannot_use_exception(pair, change):
    old, new = pair
    if change == 'risk': new[IDENTITY]['title'] += '实施退市风险警示'
    elif change == 'listed': new[IDENTITY]['codes'][0]['stock_code'] = '600000'
    elif change == 'extra': new['unreviewed'] = copy.deepcopy(new[IDENTITY])
    elif change == 'remove': del new[next(iter(old))]
    elif change == 'existing': new[next(iter(old))]['title'] = 'changed'
    elif change == 'clock': new[IDENTITY]['display_time'] = '2026-11-06 14:59:59'
    else: old[next(iter(old))]['display_time'] = '2026-11-06 14:59:59'
    with pytest.raises(StNoticeEvidenceError): refresh._boundary_precision_drift(old, new)


@pytest.mark.parametrize('selector', ['risk', 'equity'])
def test_native_eligibility_remains_authoritative(pair, monkeypatch, selector):
    if selector == 'risk':
        import microcap_top100_mom16_biweekly_live_v2_0 as base
        monkeypatch.setattr(base.freq_mod, 'classify_st_notice_risk_actions', lambda *args: {'risk': True})
    else:
        from scripts import st_notice_evidence
        monkeypatch.setattr(st_notice_evidence, 'is_equity_issuer', lambda *args: True)
    with pytest.raises(StNoticeEvidenceError): refresh._boundary_precision_drift(*pair)
