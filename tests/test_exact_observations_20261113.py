import copy
import pytest
from scripts import st_notice_boundary_20261113 as review
from scripts import st_notice_refresh as notice
from scripts import share_change_source as share

@pytest.fixture(scope='module')
def rows():return review.load_review()[1]

def test_original_failed_snapshot_passes_without_mutating_raw_rows(rows):
    old,new=copy.deepcopy(rows['previous']),copy.deepcopy(rows['current'])
    before=copy.deepcopy((old,new));r=notice._boundary_precision_drift(old,new)
    assert r['policy']=='exact_native_catalog_observation_20261113_v1'
    assert r['risk_projection_unchanged'] and r['original_archive_boundary_preserved']
    assert not r['historical_state_rewrite_authorized'] and not r['target_freshness_granted']
    assert len(r['reviewed_non_st_added_ids'])==5
    assert (old,new)==before

@pytest.mark.parametrize('field,value',[('title','实施退市风险警示'),('listing_state','9'),('notice_date','2026-11-12 00:00:00')])
def test_unreviewed_snapshot_changes_fail_closed(rows,field,value):
    old,new=copy.deepcopy(rows['previous']),copy.deepcopy(rows['current'])
    new['AN202611121831660319'][field]=value
    with pytest.raises(Exception):notice._boundary_precision_drift(old,new)

def test_risk_issuer_cannot_disappear(rows):
    old,new=copy.deepcopy(rows['previous']),copy.deepcopy(rows['current'])
    del new['AN202611101831591478']
    with pytest.raises(Exception):notice._boundary_precision_drift(old,new)

def test_future_structure_profile_is_only_the_reviewed_issuer():
    r=share._structure_review('000663')
    assert r['symbol']=='000663' and r['total_shares']==336397452
    assert set(r['row_sha256_by_day'])=={'2026-11-16'}
    assert share._structure_review('301429')==share.EXACT_FUTURE_STRUCTURE_301429
    assert share._structure_review('000664')['symbol']!='000664'
