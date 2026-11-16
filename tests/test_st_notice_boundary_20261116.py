import copy
import pytest
from scripts.st_notice_boundary_20261116 import load_review,reviewed_boundary
from scripts.st_notice_refresh import _boundary_precision_drift
from scripts.st_notice_boundary_20261111 import _projection

def test_exact_real_reobservation_keeps_native_projection_and_inputs():
 _,rows=load_review();old,new=rows['previous'],rows['current'];saved=copy.deepcopy(rows)
 r=_boundary_precision_drift(old,new)
 assert r['policy']=='exact_native_catalog_observation_20261116_v1'
 assert r['risk_projection_unchanged'] and _projection(old)==_projection(new)
 assert r['removed_event_ids']==['AN202611121831663341'] and rows==saved
 assert r['historical_state_rewrite_authorized'] is False

@pytest.mark.parametrize('mode',['risk_title','remove_extra','changed_stock','changed_listing'])
def test_unreviewed_mutations_fail_closed(mode):
 _,rows=load_review();old,new=rows['previous'],copy.deepcopy(rows['current'])
 identity='AN202611131831681746'
 if mode=='risk_title':new[identity]['title']='*ST风险警示实施公告'
 elif mode=='remove_extra':del new[next(iter(new))]
 elif mode=='changed_stock':new[identity]['codes'][0]['stock_code']='000001'
 else:new['AN202611111831629972']['listing_state']='2'
 with pytest.raises(Exception):_boundary_precision_drift(old,new)
