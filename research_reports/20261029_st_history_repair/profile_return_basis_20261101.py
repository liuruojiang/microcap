import sys,os,json,cProfile,pstats,io,time
from pathlib import Path
from run_three_version_diagnostic_20261101 import offline_guard,bind_frozen_calendar,digest
B=Path(__file__).resolve().parent;root=B/'replay/runtime_20261029';calendar=B/'replay/independent_sessions_20261030.csv'
os.chdir(root);sys.path.insert(0,str(root));sys.dont_write_bytecode=True
offline_guard(root);bind_frozen_calendar(calendar,'e602ed766beaf8aab4c45cdfc2d8cad6ea182bcc08024142598a4bee96bb1fe2','2026-10-29')
import pandas as pd
from scripts.stock_return_pipeline import load_return_prices
master=pd.read_csv(root/'.microcap_index_cache/security_master.csv',dtype={'symbol':str},parse_dates=['list_date','delist_date'])
symbol='600000';raw=root/'.microcap_index_cache/prices_raw'/f'{symbol}.csv';manifest=root/'.microcap_index_cache/return_basis/manifests'/f'{symbol}.json';before=digest(manifest)
profile=cProfile.Profile();start=time.perf_counter();profile.enable()
frame=load_return_prices(root=root,symbol=symbol,raw_path=raw,master=master,as_of='2026-10-29',allow_network=False)
profile.disable();elapsed=time.perf_counter()-start
assert before==digest(manifest)
stream=io.StringIO();pstats.Stats(profile,stream=stream).strip_dirs().sort_stats('cumulative').print_stats(45)
report=dict(symbol=symbol,seconds=elapsed,rows=len(frame),manifest_unchanged=True,scope='actual offline read profile; no production change',profile=stream.getvalue())
out=root/'profile_return_basis_600000_20261101.json'
if out.exists():raise RuntimeError('Do not overwrite')
out.write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf-8');print(stream.getvalue());print('ELAPSED',elapsed)
