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
profile=cProfile.Profile()
with frozen_metadata(v.freq_mod.SECURITY_META_DIR,v.freq_mod.load_universe()):
 with validated_source_batch(root):
  start=time.perf_counter();profile.enable()
  result=v.freq_mod.load_symbol_cache('600000',dates,caps,v.base_mod.TRADE_CONSTRAINT_MODE,True)
  profile.disable();elapsed=time.perf_counter()-start
stream=io.StringIO();pstats.Stats(profile,stream=stream).strip_dirs().sort_stats('cumulative').print_stats(65)
report=dict(symbol='600000',seconds=elapsed,result_symbol=result[0],trading_dates=len(dates),cap_dates=len(caps),scope='real full load_symbol_cache profile, excluding batch setup/exit, metadata unchanged',profile=stream.getvalue())
out=root/'profile_symbol_cache_600000_20261101.json'
if out.exists():raise RuntimeError('No overwrite')
out.write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf-8');print(stream.getvalue());print('ELAPSED',elapsed)
