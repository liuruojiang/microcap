import sys,os,json
from pathlib import Path
from run_three_version_diagnostic_20261101 import offline_guard,bind_frozen_calendar,digest
B=Path(__file__).resolve().parent;root=B/'replay/runtime_20261029';calendar=B/'replay/independent_sessions_20261030.csv'
os.chdir(root);sys.path.insert(0,str(root));sys.dont_write_bytecode=True
out=root/'guarded_return_basis_canary_20261101.json'
if out.exists():raise RuntimeError('Refuse overwrite')
offline_guard(root)
binding=bind_frozen_calendar(calendar,'e602ed766beaf8aab4c45cdfc2d8cad6ea182bcc08024142598a4bee96bb1fe2','2026-10-29')
import pandas as pd
from scripts.stock_return_pipeline import load_return_prices
master=pd.read_csv(root/'.microcap_index_cache/security_master.csv',dtype={'symbol':str},parse_dates=['list_date','delist_date'])
records=[]
for symbol in ['000001','000018','600000']:
 raw=root/'.microcap_index_cache/prices_raw'/(symbol+'.csv');manifest=root/'.microcap_index_cache/return_basis/manifests'/(symbol+'.json');before=digest(manifest)
 frame=load_return_prices(root=root,symbol=symbol,raw_path=raw,master=master,as_of='2026-10-29',allow_network=False)
 if before!=digest(manifest):raise RuntimeError('Canary changed existing manifest')
 records.append(dict(symbol=symbol,rows=len(frame),latest=str(frame.date.max()),manifest_sha256=before,raw_sha256=digest(raw)))
report=dict(scope='actual offline return basis canary',approved=False,formal_written=False,calendar=binding,records=records)
out.write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf-8');print(json.dumps(report))
