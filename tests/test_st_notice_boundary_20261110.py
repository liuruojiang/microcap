import copy
import json
from pathlib import Path
import pytest
from scripts.st_notice_boundary_20261110 import reviewed_boundary,REGISTRY
from scripts.st_notice_refresh import _precision_drift_only


def observations():
    value=json.loads((Path(__file__).resolve().parents[1]/REGISTRY).read_bytes())
    return copy.deepcopy(value['previous_rows']),copy.deepcopy(value['current_rows'])


def test_real_exact_morning_source_pair_preserves_native_risk_selection():
    old,new=observations()
    proof=reviewed_boundary(old,new,_precision_drift_only)
    assert proof['risk_projection_unchanged']
    assert len(proof['reviewed_non_st_added_ids'])==6
    assert not proof['boundary_native_content_unchanged']


@pytest.mark.parametrize('change',['risk_title','category','issuer','addition','deletion','clock'])
def test_unknown_pair_never_inherits_exact_review(change):
    old,new=observations();key='AN202611091831559867'
    if change=='risk_title':new[key]['title']='300471:实施退市风险警示'
    elif change=='category':new[key]['columns']=[{'column_code':'001002004003','column_name':'ST'}]
    elif change=='issuer':new[key]['codes'][0]['stock_code']='000001'
    elif change=='addition':new['unknown']=copy.deepcopy(new[key])
    elif change=='deletion':new.pop(key)
    else:new[key]['display_time']='2026-11-09 14:59:00:000'
    if change=='clock':
        with pytest.raises(ValueError):reviewed_boundary(old,new,_precision_drift_only)
    else:assert reviewed_boundary(old,new,_precision_drift_only) is None
