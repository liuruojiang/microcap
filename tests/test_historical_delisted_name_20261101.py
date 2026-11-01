"""Controlled caps exercise actual ranking and actual PIT lifecycle/ST gates."""
import numpy as np
import pandas as pd
import pytest
import microcap_top100_mom16_biweekly_live as standalone
import microcap_top100_mom16_biweekly_live_v2_0 as v20

@pytest.fixture(params=[standalone,v20.base_mod])
def base(request):return request.param

def test_historical_delisted_name_remains_eligible_live_excludes(base):
 day=pd.Timestamp('2018-01-04');dates=pd.DatetimeIndex([day])
 caps={day:{'000018':1.,'000001':3.,'000002':2.,'000003':np.nan,'000004':0.}}
 names={'000018':'神城A退','000001':'平安银行','000002':'ST测试'}
 assert base.build_live_target_members_map(caps,dates,names,top_n=1,exclude_current_st_names=False)[day]==['000018']
 assert base.build_live_target_members_map(caps,dates,names,top_n=1,exclude_current_st_names=True)[day]==['000001']
 # Even a current *ST or delisting annotation must not back-project into history.
 names['000018']='*ST退市摘牌'
 assert base.build_live_target_members_map(caps,dates,names,top_n=1,exclude_current_st_names=False)[day]==['000018']

@pytest.mark.parametrize('freq',[standalone.freq_mod,v20.freq_mod])
def test_actual_lifecycle_and_st_gate_not_relaxed(freq):
 dates=pd.to_datetime(['2018-01-04','2019-04-29','2020-01-07','2020-01-08'])
 meta={'list_date':'1992-06-16','delist_date':'2020-01-07','st_intervals':[{'start':'2019-04-29','end':None}]}
 active=freq.build_active_status_series(meta,dates)
 st=freq.build_st_status_series(meta,dates)
 caps=pd.Series(1.,index=dates).where(active,np.nan).where(~st,np.nan)
 assert caps.notna().tolist()==[True,False,False,False]
 assert active.tolist()==[True,True,False,False]
