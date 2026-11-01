import pandas as pd
import microcap_top100_mom16_biweekly_live_v2_0 as v

def test_actual_reconstruction_preserves_full_calendar_and_blocked_holdover(monkeypatch):
 dates=pd.bdate_range('2026-10-12','2026-10-29');old=[f'{i:06}' for i in range(100)];new=old[1:]+['000100']
 ns=v.base_mod._reconstruct_effective_members_from_saved_targets.__globals__
 monkeypatch.setattr(ns['freq_mod'],'load_security_master',lambda:pd.DataFrame({'symbol':old+['000100'],'delist_date':[None]*101}))
 assert len(ns['_minimal_tradeability_dates'](dates,dates[[0,-1]]))<len(dates)
 def load(symbols,received,workers):
  assert received.equals(dates)
  buy=pd.DataFrame(True,index=received,columns=symbols);sell=buy.copy();sell.loc[dates[-1],old[0]]=False
  return buy,sell
 monkeypatch.setitem(ns,'_load_buy_sell_for_symbols',load)
 result=v.base_mod._reconstruct_effective_members_from_saved_targets({dates[0]:old,dates[-1]:new},dates.append(pd.DatetimeIndex([pd.Timestamp('2026-10-30')])),dates[-1],1)
 assert len(result)==100 and set(result)==set(old) and set(result)!=set(new)
