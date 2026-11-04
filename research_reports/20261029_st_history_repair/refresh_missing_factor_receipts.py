"""Only two known timestamp-less pilot pairs; bounded production adapter in research."""
from pathlib import Path
import sys,json,shutil
import pandas as pd
B=Path(__file__).resolve().parent;sys.path.insert(0,str(B.parents[1]))
from scripts.stock_return_basis_store import load_or_build
mapping=json.loads((B/'raw_install_normalized_20261029/install_manifest.json').read_text(encoding='utf-8'))
master=pd.read_csv(B/'cross_source_official/security_master_combined_candidate.csv',dtype={'symbol':str}).set_index('symbol')
sessions=json.loads((B/'cross_source_prices/missing_session_review/exchange_sessions_20261028.json').read_text())
root=B/'refreshed_pilot_factor_receipts_20261030';source=B/'return_basis_candidate_20261030/sources'
for symbol in ['000418','000559']:
 old=source/symbol
 if all(json.loads((old/(k+'.json')).read_text()).get('retrieved_at_utc') for k in ['qfq','hfq']):continue
 folder=root/'sources'/symbol
 if not folder.exists():shutil.copytree(old,folder)
 row=next(r for r in mapping['records'] if r['symbol']==symbol);m=master.loc[symbol]
 frame,manifest=load_or_build(basis_root=root,symbol=symbol,raw_path=row['candidate_path'],as_of='2026-10-29',sessions=sessions,list_date=str(m.list_date),delist_date=None if pd.isna(m.delist_date) else str(m.delist_date),allow_network=True)
 archive=root/'original_receipts'/symbol
 if not archive.exists():shutil.copytree(old,archive)
 for kind in ['qfq','hfq']:
  for ext in ['.js','.json']:shutil.copy2(folder/(kind+ext),old/(kind+ext))
 print(symbol,'fresh real receipt pair',flush=True)
