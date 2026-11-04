import ast,sys
from pathlib import Path
import numpy as np,pandas as pd,pytest
sys.path.insert(0,str(Path(__file__).resolve().parent));sys.path.insert(0,str(Path(__file__).resolve().parents[2]))
from run_isolated_tail_20261030 import append_tail,actual_members
import microcap_top100_mom16_biweekly_live_v2_0 as v

def test_same_actual_tail_loop_and_missing_return_blocks():
 # Execute the exact production loop AST, not a second handwritten expected formula.
 fn=next(n for n in ast.parse(v.BASE_SOURCE).body if isinstance(n,ast.FunctionDef) and n.name=='try_extend_proxy_index_without_rebalance')
 loop=next(n for n in fn.body if isinstance(n,ast.For) and isinstance(n.target,ast.Name) and n.target.id=='dt' and isinstance(n.iter,ast.Name) and n.iter.id=='missing_dates')
 members=[f'{x:06}' for x in range(100)];dates=pd.to_datetime(['2026-10-29','2026-10-30','2026-11-02'])
 # Unequal returns over two days distinguish daily equal-weight from drifting weights.
 returns=pd.DataFrame([np.zeros(100),np.arange(100)/1000,np.arange(100)[::-1]/2000],index=dates,columns=members)
 proxy=pd.DataFrame(dict(date=[dates[0]],close=[1234.0],daily_return=[0.]))
 ns=dict(pd=pd,np=np,missing_dates=dates[1:],returns_df=returns,effective_members=members,TOP_N=100,current_level=1234.0,new_rows=[])
 exec(compile(ast.Module(body=[loop],type_ignores=[]),'<actual frozen tail loop>','exec'),ns)
 actual=append_tail(proxy,returns,members,dates).iloc[1:]
 np.testing.assert_array_equal(actual.close.to_numpy(),pd.DataFrame(ns['new_rows']).close.to_numpy())
 np.testing.assert_array_equal(actual.daily_return.to_numpy(),pd.DataFrame(ns['new_rows']).daily_return.to_numpy())
 drift=1234.0*np.prod(1+returns.iloc[1:],axis=0).mean()
 assert abs(actual.close.iloc[-1]-drift)>1e-5
 for bad in [returns.drop(columns=members[0]),returns.mask(returns.index.to_series().eq(dates[-1]),np.nan,axis=0)]:
  with pytest.raises(ValueError):append_tail(proxy,bad,members,dates)

def test_real_executed_members_keep_blocked_exit(tmp_path,monkeypatch):
 days=pd.to_datetime(['2026-10-15','2026-10-29']);old=[f'{x:06}' for x in range(100)];target=old[1:]+['000100']
 rows=[dict(rebalance_date=d,rank=i+1,symbol=s) for d,m in zip(days,[old,target]) for i,s in enumerate(m)]
 path=tmp_path/'members.csv';pd.DataFrame(rows).to_csv(path,index=False)
 cols=old+['000100'];buy=pd.DataFrame(True,index=days,columns=cols);sell=buy.copy();sell.loc[days[-1],old[0]]=False
 ns=v.base_mod._reconstruct_effective_members_from_saved_targets.__globals__
 monkeypatch.setattr(ns['freq_mod'],'load_security_master',lambda:pd.DataFrame({'symbol':old+['000100'],'delist_date':[None]*101}))
 monkeypatch.setitem(ns,'_load_buy_sell_for_symbols',lambda *args:(buy,sell))
 result=actual_members(v.base_mod,path,days,days[-1],1)
 assert len(result)==100 and set(result)==set(old) and set(result)!=set(target)

def test_export_rejects_changed_source_and_wrong_members(tmp_path):
 import json
 from export_executed100_20261029 import validate_identity,validate_members,digest
 root=tmp_path;candidate=root/'candidate';candidate.mkdir();(root/'source_manifest.json').write_text('{}')
 for name in ('members.csv','turnover.csv','proxy.csv'):(candidate/name).write_text('controlled original')
 report=dict(as_of='2026-10-29',approved=False,formal_written=False,source_manifest_sha256=digest(root/'source_manifest.json'))
 for name in ('members.csv','turnover.csv','proxy.csv'):report[name]={'sha256':digest(candidate/name)}
 (candidate/'diagnostic_result.json').write_text(json.dumps(report))
 validate_identity(candidate,root)
 valid=[f'{i:06}' for i in range(100)];validate_members(valid)
 for bad in [valid[:-1],valid[:-1]+[valid[0]],valid[:-1]+['bad']]:
  with pytest.raises(ValueError):validate_members(bad)
 (candidate/'members.csv').write_text('changed target')
 with pytest.raises(ValueError,match='artifact identity'):validate_identity(candidate,root)
 (candidate/'members.csv').write_text('controlled original');(root/'source_manifest.json').write_text('{"changed":true}')
 with pytest.raises(ValueError,match='source identity'):validate_identity(candidate,root)

def test_real_reconstruction_receives_complete_calendar_and_restores(tmp_path,monkeypatch):
 days=pd.bdate_range('2026-10-12','2026-10-29');reb=days[[0,-1]];old=[f'{i:06}' for i in range(100)]
 path=tmp_path/'members.csv';pd.DataFrame([dict(rebalance_date=d,rank=i+1,symbol=s) for d in reb for i,s in enumerate(old)]).to_csv(path,index=False)
 namespace=v.base_mod._reconstruct_effective_members_from_saved_targets.__globals__;original=namespace['_minimal_tradeability_dates']
 monkeypatch.setattr(namespace['freq_mod'],'load_security_master',lambda:pd.DataFrame({'symbol':old,'delist_date':[None]*100}))
 assert len(original(days,reb))<len(days)
 def complete(symbols,trading_dates,max_workers):
  assert trading_dates.equals(days)
  frame=pd.DataFrame(True,index=trading_dates,columns=symbols)
  return frame,frame.copy()
 monkeypatch.setitem(namespace,'_load_buy_sell_for_symbols',complete)
 assert actual_members(v.base_mod,path,days,days[-1],1)==old
 assert namespace['_minimal_tradeability_dates'] is original
 def failure(*args):raise RuntimeError('controlled source failure')
 monkeypatch.setitem(namespace,'_load_buy_sell_for_symbols',failure)
 with pytest.raises(RuntimeError,match='controlled source failure'):actual_members(v.base_mod,path,days,days[-1],1)
 assert namespace['_minimal_tradeability_dates'] is original

def test_real_terminal_simulation_derivation_preserves_execution_cost_and_history():
 from run_isolated_tail_20261030 import derive_terminal_return_start
 from scripts.top100_data_contracts import normalise_turnover_table,expected_rebalance_costs
 days=pd.bdate_range(end='2026-10-29',periods=140);anchor=days[-1];calendar=days.append(pd.DatetimeIndex([pd.Timestamp('2026-10-30')]))
 symbols=[f'{i:06}' for i in range(100)];returns=pd.DataFrame(0.,index=days,columns=symbols);trade=pd.DataFrame(True,index=days,columns=symbols)
 _,turn,eff=v.freq_mod.simulate_rebalance_path(trading_dates=days,returns_df=returns,target_members_map={anchor:symbols},rebalance_dates=pd.DatetimeIndex([anchor]),buyable_df=trade,sellable_df=trade,one_side_cost_rate=.001,top_n=100,execution_timing='close')
 assert len(turn)==1 and pd.isna(turn.return_start_date.iloc[0]) and eff[anchor]==symbols
 original=turn.copy(deep=True);derived,notes=derive_terminal_return_start(turn,calendar,anchor)
 pd.testing.assert_frame_equal(turn,original)
 assert derived.return_start_date.iloc[0]==pd.Timestamp('2026-10-30') and len(notes)==1
 pd.testing.assert_series_equal(expected_rebalance_costs(days,turn),expected_rebalance_costs(days,derived))
 assert expected_rebalance_costs(calendar,derived).loc[anchor]==.001
 assert expected_rebalance_costs(calendar,derived).iloc[-1]==0
 close=pd.DataFrame({'microcap':100*np.exp(np.arange(140)*.002),'hedge':100*np.exp(np.arange(140)*.0002)},index=days)
 gross=v.base_mod.apply_momentum_gap_exit_buffer(v.base_mod.run_signal(close).sort_index(),v.V2_0_MOMENTUM_GAP_EXIT_BUFFER)
 before=v.overlay_mod.apply_v2_0_execution(gross,normalise_turnover_table(turn));after=v.overlay_mod.apply_v2_0_execution(gross,normalise_turnover_table(derived))
 pd.testing.assert_frame_equal(before[['return_net','nav_net']],after[['return_net','nav_net']])
 for changed in [dict(execution_timing='next_open'),dict(execution_date=days[-2]),dict(return_start_date='bad'),dict(holding_count_after=99)]:
  bad=turn.copy().astype(object)
  for key,value in changed.items():bad.loc[0,key]=value
  with pytest.raises(ValueError):derive_terminal_return_start(bad,calendar,anchor)
