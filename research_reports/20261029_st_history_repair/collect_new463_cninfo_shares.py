"""Research-only original-provider complete share records, with exact raw receipts."""
from pathlib import Path
from datetime import datetime,timezone
import argparse,csv,hashlib,json,time
import numpy as np
import pandas as pd
import requests,akshare as ak
RUN=Path(__file__).resolve().parent
OUT=RUN/'new463_cninfo_shares_20261029'
class StopGroup(RuntimeError):pass
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def save(p,r):p.write_text(json.dumps(r,ensure_ascii=False,indent=2),encoding='utf-8')
def collect(sym):
 rawpath=OUT/'raw'/f'{sym}.json';requestpath=OUT/'requests'/f'{sym}.json';orig=requests.post
 def capture(url,*args,**kw):
  kw['timeout']=15;r=orig(url,*args,**kw);rawpath.write_bytes(r.content)
  save(requestpath,{'url':url,'params':kw.get('params'),'status':r.status_code,'sha256':sha(rawpath),'retrieved_at_utc':datetime.now(timezone.utc).isoformat(),'source':'akshare.stock_share_change_cninfo installed original request','query_cutoff':'2026-10-29'})
  if r.status_code in (401,403,429):raise StopGroup(f'HTTP {r.status_code}')
  r.raise_for_status();return r
 if rawpath.exists():
  receipt=json.loads(requestpath.read_text());assert sha(rawpath)==receipt['sha256']
 else:
  requests.post=capture
  try:
   ak.stock_share_change_cninfo(symbol=sym,start_date='19000101',end_date='20261029')
  finally:requests.post=orig
 data=json.loads(rawpath.read_bytes());rows=data['records']
 if data.get('resultcode')!=200 or len(rows)!=int(data['count']) or len(rows)!=int(data['total']) or not rows:raise ValueError('Incomplete/empty original-provider records; not proof of no share history')
 if any(str(r.get('SECCODE'))!=sym for r in rows):raise ValueError('Original-provider security identity mismatch')
 dates=pd.to_datetime([r['VARYDATE'] for r in rows],errors='raise');ann=pd.to_datetime([r['DECLAREDATE'] for r in rows],errors='raise');values=pd.to_numeric([r['F003N'] for r in rows],errors='raise')
 if dates.isna().any() or not np.isfinite(values).all() or not (values>0).all():raise ValueError('Invalid share dates/values')
 if (dates>pd.Timestamp('2026-10-29')).any():raise ValueError('Provider returned change dates beyond requested cutoff')
 full=OUT/'parsed'/f'{sym}.csv';pd.DataFrame(rows).to_csv(full,index=False,encoding='utf-8-sig')
 pairs=pd.DataFrame({'change_date':dates,'declare_date':ann,'total_shares_10k':values,'reason':[r.get('F002V') for r in rows]})
 contradictions=pairs.groupby('change_date').total_shares_10k.nunique();conflicts=[str(d.date()) for d,n in contradictions.items() if n>1]
 normalized=OUT/'normalized_candidate'/f'{sym}.csv'
 if not conflicts:
  clean=pairs.sort_values(['change_date','declare_date']).drop_duplicates('change_date',keep='last')
  clean[['change_date','total_shares_10k','reason']].to_csv(normalized,index=False)
 return {'symbol':sym,'approved':False,'status':'complete_raw_history' if not conflicts else 'blocked_same_date_quantity_conflict','raw_path':str(rawpath),'raw_sha256':sha(rawpath),'request_path':str(requestpath),'request_sha256':sha(requestpath),'parsed_path':str(full),'parsed_sha256':sha(full),'candidate_path':str(normalized) if not conflicts else None,'candidate_sha256':sha(normalized) if not conflicts else None,'rows':len(rows),'first_change':str(dates.min().date()),'last_change':str(dates.max().date()),'last_declaration':None if ann.isna().all() else str(ann.max().date()),'missing_declaration_dates':int(ann.isna().sum()),'query_cutoff':'2026-10-29','unit':'10k shares, original F003N','conflicting_dates':conflicts,'date_semantics':'VARYDATE and DECLAREDATE both retained in raw/parsed, missing declarations remain unknown; no automatic PIT or issuer override approval'}
def main():
 p=argparse.ArgumentParser();p.add_argument('--symbols',nargs='+');p.add_argument('--all',action='store_true');p.add_argument('--active',action='store_true');p.add_argument('--revalidate',action='store_true');a=p.parse_args();assert sum([bool(a.symbols),a.all,a.active])==1
 for d in ['raw','requests','receipts','parsed','normalized_candidate']:(OUT/d).mkdir(parents=True,exist_ok=True)
 old={r['symbol'] for r in csv.DictReader((RUN/'resume_audit/delivery_old4975_input_coverage.csv').open(encoding='utf-8-sig'))};allsyms={r['symbol'] for r in json.loads((RUN/'raw_install_normalized_20261029/install_manifest.json').read_text())['records']};new=sorted(allsyms-old);assert len(new)==463
 master={r['symbol']:r for r in csv.DictReader((RUN/'cross_source_official/security_master_combined_candidate.csv').open(encoding='utf-8-sig'))}
 selected=([s for s in new if not master[s]['delist_date']] if a.active else new) if (a.all or a.active) else a.symbols;assert set(selected)<=set(new);records=[];consecutive=0;stop=False
 for sym in selected:
  f=OUT/'receipts'/f'{sym}.json'
  if f.exists():
   r=json.loads(f.read_text());
   if r.get('raw_path'):assert sha(Path(r['raw_path']))==r['raw_sha256']
   if not (a.revalidate and r['status']=='blocked' and r.get('raw_path')):
    records.append(r);continue
   history=OUT/'validation_history';history.mkdir(exist_ok=True);(history/(sym+'_'+sha(f)+'.json')).write_bytes(f.read_bytes())
  try:r=collect(sym);consecutive=0
  except Exception as e:
   r={'symbol':sym,'approved':False,'status':'blocked','error_type':type(e).__name__,'error':str(e)}
   raw=OUT/'raw'/f'{sym}.json';request=OUT/'requests'/f'{sym}.json'
   if raw.exists():r.update(raw_path=str(raw),raw_sha256=sha(raw))
   if request.exists():r.update(request_path=str(request),request_sha256=sha(request))
   consecutive=consecutive+1 if isinstance(e,(requests.RequestException,StopGroup)) else 0;stop=isinstance(e,StopGroup) or consecutive>=3
  save(f,r);records.append(r);save(OUT/'progress.json',{'selected':len(selected),'processed':len(records),'statuses':{s:sum(x['status']==s for x in records) for s in {x['status'] for x in records}},'stopped':stop})
  if len(records)%20==0 or not (a.all or a.active):print(sym,r['status'],r.get('error',''),flush=True)
  if stop:break
  time.sleep(.2)
 save(OUT/('batch_summary.json' if (a.all or a.active) else 'canary_summary.json'),{'approved':False,'formal_written':False,'records':records,'stopped':stop})
if __name__=='__main__':main()
