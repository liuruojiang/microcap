"""Added-only semantic invariants based on real source rows, not PIT history."""
import copy,json
from pathlib import Path
import pytest
from scripts.st_notice_boundary_20261110 import REGISTRY
from scripts.st_notice_inv_research_boundary import is_unconsumed_equity_notice
from scripts.st_notice_refresh import _boundary_precision_drift
from scripts.st_notice_evidence import StNoticeEvidenceError


@pytest.fixture
def rows():
    review=json.loads((Path(__file__).resolve().parents[1]/REGISTRY).read_bytes())
    eligible=[(k,v) for k,v in review['current_rows'].items() if is_unconsumed_equity_notice(k,v)]
    assert len(eligible)>2
    # Removing a known original row is a software counterfactual used to test
    # the selector invariant, never a claim about its earlier availability.
    old=dict(eligible[:1]);new=dict(eligible[:2]);return copy.deepcopy(old),copy.deepcopy(new)


def test_added_real_ordinary_equity_notice_has_no_native_st_input_effect(rows):
    old,new=rows;before=copy.deepcopy((old,new));proof=_boundary_precision_drift(old,new)
    assert proof['native_unconsumed_equity_added_ids']==sorted(new.keys()-old.keys())
    assert proof['risk_projection_unchanged'] and (old,new)==before


@pytest.mark.parametrize('fault',['risk','recovery','category','asset','mixed','same_issuer_non_equity','venue','missing_category','old_changed','removed'])
def test_consumed_unknown_or_changed_records_still_require_original_review(rows,fault):
    old,new=rows;key=next(k for k in new if k not in old);row=new[key]
    if fault=='risk':row['title']='关于股票交易被实施退市风险警示的公告'
    elif fault=='recovery':row['title']='关于恢复上市的公告'
    elif fault=='category':row['columns']=[dict(column_code='001002004003',column_name='ST')]
    elif fault=='asset':row['codes'][0]['ann_type']='BOND'
    elif fault=='mixed':row['codes'][0]['ann_type']='A,INV'
    elif fault=='same_issuer_non_equity':
        extra=copy.deepcopy(row['codes'][0]);extra['ann_type']='BOND';row['codes'].append(extra)
    elif fault=='venue':row['codes'][0]['market_code']='wrong'
    elif fault=='missing_category':row['columns']=[]
    elif fault=='old_changed':new[next(iter(old))]['title']+=' changed'
    else:new.pop(next(iter(old)))
    with pytest.raises(StNoticeEvidenceError):_boundary_precision_drift(old,new)
