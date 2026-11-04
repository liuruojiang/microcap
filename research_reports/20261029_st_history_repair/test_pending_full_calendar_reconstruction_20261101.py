"""Test the unapplied minimal production patch against actual embedded functions."""
import ast,sys
from pathlib import Path
import pandas as pd
sys.path.insert(0,str(Path(__file__).resolve().parents[2]))
import microcap_top100_mom16_biweekly_live_v2_0 as v

def test_unapplied_patch_uses_full_dates_and_real_constraints():
 original=next(n for n in ast.parse(v.BASE_SOURCE).body if isinstance(n,ast.FunctionDef) and n.name=='_reconstruct_effective_members_from_saved_targets')
 source=ast.unparse(original)
 before='trade_dates = _minimal_tradeability_dates(panel_dates, rebalance_dates)'
 assert before in source or "trade_dates = pd.DatetimeIndex(panel_dates)" in source
 source=source.replace(before,"trade_dates = pd.DatetimeIndex(panel_dates)\n    trade_dates = trade_dates[(trade_dates >= pd.Timestamp(freq_mod.START_DATE)) & (trade_dates <= pd.Timestamp(as_of_date).normalize())]")
 ns=dict(v.base_mod._reconstruct_effective_members_from_saved_targets.__globals__)
 days=pd.bdate_range('2026-10-12','2026-10-29');old=[f'{i:06}' for i in range(100)];target=old[1:]+['000100']
 assert len(ns['_minimal_tradeability_dates'](days,days[[0,-1]]))<len(days)
 def actual_input(symbols,received,workers):
  assert received.equals(days)
  buy=pd.DataFrame(True,index=received,columns=symbols);sell=buy.copy();sell.loc[days[-1],old[0]]=False
  return buy,sell
 ns['_load_buy_sell_for_symbols']=actual_input
 exec(compile(source,'<unapplied production reconstruction patch>','exec'),ns)
 result=ns['_reconstruct_effective_members_from_saved_targets']({days[0]:old,days[-1]:target},days,days[-1],1)
 assert len(result)==100 and set(result)==set(old) and set(result)!=set(target)
