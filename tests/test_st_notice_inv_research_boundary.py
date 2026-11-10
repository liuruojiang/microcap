"""Real added INV source observation; software transport checks, no PIT rewrite."""
import copy
import gzip
import hashlib
import json
from pathlib import Path

import pytest

from scripts.st_notice_boundary_20261110 import REGISTRY
from scripts.st_notice_inv_research_boundary import is_unconsumed_inv_research
from scripts.st_notice_refresh import _boundary_precision_drift
from scripts.st_notice_evidence import StNoticeEvidenceError

ROOT=Path(__file__).resolve().parents[1]
FIXTURE=Path(__file__).parent/'fixtures/inv_research_boundary_20261110'
ADDED='AN202611101831580875'


def observations():
    review=json.loads((ROOT/REGISTRY).read_bytes())
    current=json.loads(gzip.decompress((FIXTURE/'current_rows.json.gz').read_bytes()))
    return copy.deepcopy(review['previous_rows']),current,review


def test_real_480_to_487_retains_exact_review_and_does_not_mutate_sources():
    previous,current,review=observations()
    before=copy.deepcopy((previous,current))
    result=_boundary_precision_drift(previous,current)
    assert len(previous)==480 and len(current)==487
    assert result['non_equity_added_ids']==[ADDED]
    underlying=result['underlying_boundary_review']
    assert underlying['policy']=='exact_morning_non_st_boundary_20261110_v1'
    assert underlying['reviewed_non_st_added_ids']==review['added']
    assert result['risk_projection_unchanged'] and not result['boundary_native_content_unchanged']
    assert (previous,current)==before


def test_real_486_to_487_uses_original_precision_after_one_non_equity_addition():
    _,current,review=observations()
    result=_boundary_precision_drift(review['current_rows'],current)
    assert result['non_equity_added_ids']==[ADDED]
    assert result['underlying_boundary_review']['policy']=='display_time_same_day_lt_1s_no_close_crossing_v1'


def test_original_reviewed_480_to_486_receipt_policy_is_unchanged():
    previous,_,review=observations()
    result=_boundary_precision_drift(previous,review['current_rows'])
    assert result['policy']=='exact_morning_non_st_boundary_20261110_v1'
    assert 'non_equity_added_ids' not in result


def test_real_added_row_is_bound_to_original_raw_page_and_receipt():
    value=json.loads((FIXTURE/'provenance.json').read_bytes())
    for name,digest in value['files'].items():
        assert hashlib.sha256((FIXTURE/name).read_bytes()).hexdigest()==digest
    spec=value['source_page'];raw=(FIXTURE/spec['response']).read_bytes()
    receipt=json.loads((FIXTURE/spec['receipt']).read_bytes())
    assert hashlib.sha256(raw).hexdigest()==spec['response_sha256']
    assert hashlib.sha256((FIXTURE/spec['receipt']).read_bytes()).hexdigest()==spec['receipt_sha256']
    row=json.loads((FIXTURE/'row.json').read_bytes())
    assert row in json.loads(raw)['data']['list']
    assert is_unconsumed_inv_research(ADDED,row)
    assert receipt['collected_at'] and value['current_count']==487


@pytest.mark.parametrize('fault',['equity','mixed_equity','second_equity','risk_title','other_column','extra_column','column_name',
                                 'wrong_venue','wrong_issuer','empty_codes','bad_identity','unknown_equity',
                                 'changed_reviewed_INV','changed_existing','deleted_existing','deleted_reviewed_INV'])
def test_equity_risk_wrong_identity_or_other_boundary_changes_still_fail(fault):
    previous,current,review=observations();row=current[ADDED]
    if fault=='equity':row['codes'][0]['ann_type']='A'
    elif fault=='mixed_equity':row['codes'][0]['ann_type']='INV,A'
    elif fault=='second_equity':row['codes'].append(dict(ann_type='A',stock_code='300001',market_code='0'))
    elif fault=='risk_title':row['title']='兴业银锡:关于股票交易被实施退市风险警示的公告'
    elif fault=='other_column':row['columns'][0]['column_code']='001002004003'
    elif fault=='extra_column':row['columns'].append(dict(column_code='001002004003',column_name='ST'))
    elif fault=='column_name':row['columns'][0]['column_name']='other category'
    elif fault=='wrong_venue':row['codes'][0]['market_code']='1'
    elif fault=='wrong_issuer':row['codes'][0]['stock_code']='900426'
    elif fault=='empty_codes':row['codes']=[]
    elif fault=='bad_identity':row['art_code']='AN000000000000000001'
    elif fault=='unknown_equity':
        extra=copy.deepcopy(row);extra['art_code']='AN000000000000000001';extra['codes'][0]['ann_type']='A'
        current[extra['art_code']]=extra
    elif fault in ('changed_reviewed_INV','deleted_reviewed_INV'):
        identity=next(k for k in review['added'] if all(c['ann_type']=='INV' for c in current[k]['codes']))
        if fault=='changed_reviewed_INV':current[identity]['title']+=' changed'
        else:current.pop(identity)
    elif fault=='changed_existing':current[next(iter(previous))]['title']+=' changed'
    else:current.pop(next(iter(previous)))
    with pytest.raises(StNoticeEvidenceError):_boundary_precision_drift(previous,current)


def test_exact_profile_registry_hash_is_not_optional(monkeypatch):
    import scripts.st_notice_boundary_20261110 as morning
    previous,current,_=observations()
    monkeypatch.setattr(morning,'REGISTRY_SHA','0'*64)
    with pytest.raises(StNoticeEvidenceError,match='registry changed'):_boundary_precision_drift(previous,current)


def test_helper_is_part_of_native_release_evidence_inventory():
    from scripts.st_notice_boundary_review import SOURCE_FILES
    assert 'scripts/st_notice_inv_research_boundary.py' in SOURCE_FILES
