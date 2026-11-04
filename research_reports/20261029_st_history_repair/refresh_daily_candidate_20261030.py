from pathlib import Path
import json, hashlib, shutil, sys, datetime
ROOT=Path(__file__).resolve().parents[2]; sys.path.insert(0,str(ROOT))
from scripts.stock_daily_inputs import refresh_daily_inputs
from scripts.exchange_calendar import sessions_for_day
B=ROOT/'research_reports/20261029_st_history_repair'; R=B/'daily_inputs_20261030'; C=R/'.microcap_index_cache'
def sha(p): return hashlib.sha256(p.read_bytes()).hexdigest()
def save(name,obj): (R/name).write_text(json.dumps(obj,ensure_ascii=False,indent=2),encoding='utf-8')
R.mkdir(exist_ok=True)
raw=json.loads((B/'raw_install_normalized_20261029/install_manifest.json').read_text(encoding='utf-8'))
status=json.loads((B/'trade_status_candidate/manifest.json').read_text(encoding='utf-8'))
if not (R/'seed.json').exists():
 C.mkdir(exist_ok=True); (C/'prices_raw').mkdir(exist_ok=True)
 for row in raw['records']:
  p=Path(row['candidate_path']); assert sha(p)==row['candidate_sha256']; shutil.copyfile(p,C/'prices_raw'/f"{row['symbol']}.csv")
 p=Path(status['master_path']); assert sha(p)==status['master_sha256']; shutil.copyfile(p,C/'security_master.csv')
 shutil.copytree(B/'trade_status_candidate/.microcap_index_cache/trade_status',C/'trade_status',dirs_exist_ok=True)
 save('seed.json',{'raw_manifest_sha256':sha(B/'raw_install_normalized_20261029/install_manifest.json'),'status_manifest_sha256':sha(B/'trade_status_candidate/manifest.json'),'symbols':[r['symbol'] for r in raw['records']],'formal_written':False})
sessions=sessions_for_day(datetime.date(2026,10,30)); save('sessions.json',[str(d) for d in sessions if str(d)<='2026-10-30'])
results=[]
try:
 for offset in range(0,len(raw['records']),100):
  group=[r['symbol'] for r in raw['records'][offset:offset+100]]
  rows=refresh_daily_inputs(root=R,symbols=group,as_of='2026-10-30')
  results.extend(rows); save('progress.json',{'completed':len(results),'total':len(raw['records']),'results':results}); print(len(results),flush=True)
 save('refresh_result.json',{'approved':False,'formal_written':False,'target':'2026-10-30','results':results})
except Exception as e:
 save('blocked.json',{'type':type(e).__name__,'error':str(e),'completed':len(results),'next_group':group}); raise
