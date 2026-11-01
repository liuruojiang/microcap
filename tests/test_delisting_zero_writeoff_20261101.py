import ast
from pathlib import Path
import numpy as np,pandas as pd,pytest
import analyze_top100_rebalance_frequency as standalone
import microcap_top100_mom16_biweekly_live_v2_0 as v

@pytest.fixture(params=['standalone','embedded'])
def module(request):return standalone if request.param=='standalone' else v.freq_mod

def run(mod,rebalance_second=False,missing=False,delist=True,timing='close'):
 dates=pd.to_datetime(['2020-01-02','2020-01-03','2020-01-06','2020-01-07','2020-01-08'])
 returns=pd.DataFrame({'000001':[0,0,np.nan,np.nan,np.nan],'000002':[0,0,.1,.2,.1],'000003':[0,0,0,.4,.2]},index=dates)
 if not delist:returns['000001']=.05
 if missing:returns.loc[dates[2],'000002']=np.nan
 tradable=pd.DataFrame(True,index=dates,columns=returns.columns)
 targets={dates[0]:['000001','000002']}
 if rebalance_second:targets[dates[2]]=['000002','000003']
 args=dict(trading_dates=dates,returns_df=returns,target_members_map=targets,rebalance_dates=pd.DatetimeIndex(targets),buyable_df=tradable,sellable_df=tradable,one_side_cost_rate=.001,top_n=2,execution_timing=timing)
 return mod.simulate_rebalance_path(**args,delist_dates={'000001':pd.Timestamp('2020-01-04')} if delist else {}),args

def test_single_writeoff_weekend_original_denominator_and_survivor_mean(module):
 (index,turn,effective),_=run(module)
 assert index.daily_return.iloc[2]==pytest.approx(-.45)
 assert index.daily_return.iloc[3]==pytest.approx(.2)
 assert index.holding_count.tolist()==[0,2,1,1,1]
 assert index.attrs['terminal_effective_members']==['000002']
 assert index.attrs['terminal_as_of']=='2020-01-08'
 events=index.attrs['delisting_writeoff_events'];assert len(events)==1
 assert events[0]['date']=='2020-01-06' and events[0]['writeoff_weight']==.5 and events[0]['is_trade'] is False
 assert turn.exit_count.sum()==0 and len(turn)==1

def test_same_day_scheduled_close_fills_later_without_fake_sale(module):
 (index,turn,effective),_=run(module,rebalance_second=True)
 assert index.daily_return.iloc[2]==pytest.approx(-.45)
 assert index.daily_return.iloc[3]==pytest.approx(.3)
 event=turn.iloc[-1];assert event.exit_count==0 and event.entry_count==1 and event.two_side_cost_rate==.0005
 assert effective[pd.Timestamp('2020-01-06')]==['000002','000003']

def test_unknown_return_remains_failclosed(module):
 with pytest.raises(RuntimeError,match='Unvalued held'):run(module,missing=True)

def test_normal_input_exact_parity_to_saved_actual_function(module):
 fixture=Path(__file__).parent/'fixtures/delisting_parity_actual'
 mode='standalone' if module is standalone else 'embedded'
 code=(fixture/(mode+'.py')).read_text(encoding='utf-8')
 fn=next(n for n in ast.parse(code).body if isinstance(n,ast.FunctionDef) and n.name=='simulate_rebalance_path')
 ns=dict(module.simulate_rebalance_path.__globals__);exec(compile(ast.Module(body=[fn],type_ignores=[]),'<saved actual simulator>','exec'),ns)
 (actual,args)=run(module,delist=False,rebalance_second=True);old=ns['simulate_rebalance_path'](**args)
 actual[0].attrs={}  # New audit metadata is intentional; all numerical/index columns must match exactly.
 pd.testing.assert_frame_equal(actual[0],old[0]);pd.testing.assert_frame_equal(actual[1],old[1]);assert actual[2]==old[2]

def test_reconstruction_removes_between_schedule_without_filling(monkeypatch):
 days=pd.bdate_range('2020-01-02','2020-01-08');targets={days[0]:['000001','000002']}
 ns=v.base_mod._reconstruct_effective_members_from_saved_targets.__globals__
 monkeypatch.setattr(ns['freq_mod'],'load_security_master',lambda:pd.DataFrame({'symbol':['000001','000002'],'delist_date':['2020-01-04',None]}))
 monkeypatch.setitem(ns,'_load_buy_sell_for_symbols',lambda symbols,dates,workers:(pd.DataFrame(True,index=dates,columns=symbols),pd.DataFrame(True,index=dates,columns=symbols)))
 assert v.base_mod._reconstruct_effective_members_from_saved_targets(targets,days,days[-1],1)==['000002']
