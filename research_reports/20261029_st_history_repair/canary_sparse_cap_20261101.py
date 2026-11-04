import sys,os,json,cProfile,pstats,io,time,importlib
from pathlib import Path
from run_three_version_diagnostic_20261101 import offline_guard,bind_frozen_calendar,digest,isolate_modules,frozen_metadata
B=Path(__file__).resolve().parent;root=B/'replay/runtime_20261029';calendar=B/'replay/independent_sessions_20261030.csv'
os.chdir(root);sys.path.insert(0,str(root));sys.dont_write_bytecode=True;os.environ['MPLCONFIGDIR']=str(root/'.mplconfig');os.environ['MPLBACKEND']='Agg'
offline_guard(root);bind_frozen_calendar(calendar,'e602ed766beaf8aab4c45cdfc2d8cad6ea182bcc08024142598a4bee96bb1fe2','2026-10-29')
import pandas as pd
v=importlib.import_module('microcap_top100_mom16_biweekly_live_v2_0');isolate_modules(v,root);v._sync_embedded_base_config()
from scripts.st_history_scope import validated_source_batch
dates=pd.DatetimeIndex(pd.to_datetime(pd.read_csv(calendar).date));dates=dates[(dates>=pd.Timestamp(v.freq_mod.START_DATE))&(dates<=pd.Timestamp('2026-10-29'))];caps=v.base_mod.build_biweekly_rebalance_dates(dates)
records=[]
with frozen_metadata(v.freq_mod.SECURITY_META_DIR,v.freq_mod.load_universe()):
 with validated_source_batch(root):
  full=v.freq_mod.load_symbol_cache('600000',dates,caps,v.base_mod.TRADE_CONSTRAINT_MODE,True)
  sparse=pd.DatetimeIndex(sorted(set(caps)|{dates.min(),dates.max()}))
  try:
   limited=v.freq_mod.load_symbol_cache('600000',sparse,caps,v.base_mod.TRADE_CONSTRAINT_MODE,True)
   records.append(dict(status='returned',cap_equal=full[2].equals(limited[2]),buy_equal=full[3].reindex(caps).equals(limited[3].reindex(caps))))
  except Exception as exc:records.append(dict(status='blocked',error=str(exc),type=type(exc).__name__))
report=dict(symbol='600000',full_dates=len(dates),sparse_dates=len(sparse),cap_dates=len(caps),full_success=full[0]=='600000',sparse=records,approved=False,formal_written=False)
out=root/'sparse_cap_equivalence_600000_20261101.json'
if out.exists():raise RuntimeError('No overwrite')
out.write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf-8');print(json.dumps(report))
