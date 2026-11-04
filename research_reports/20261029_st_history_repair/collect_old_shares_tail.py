"""Research-only bounded equity tail, preserving old source and explicit date semantics."""
from pathlib import Path
import argparse,csv,hashlib,json,math,time,sys
from datetime import datetime,timezone
from decimal import Decimal
import requests
RUN=Path(__file__).resolve().parent
sys.path.insert(0,str(RUN/'cross_source_shares'))
from collect_eastmoney_equity import template,StopBatch
OUT=RUN/'old4975_shares_tail_20261029'
TARGET='2026-10-29'
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def save(p,x):p.write_text(json.dumps(x,ensure_ascii=False,indent=2),encoding='utf-8')
def read(p):return list(csv.DictReader(p.open(encoding='utf-8-sig',newline='')))
def collect(row,session):
 sym=row['symbol'];source=Path(row['shares_path']);old=read(source);sourcehash=sha(source)
 assert old and all(x['change_date']<=TARGET for x in old)
 anchor=max(x['change_date'] for x in old);oldvalue=Decimal(next(x['total_shares_10k'] for x in old if x['change_date']==anchor))*10000
 url,params=template(sym+('.SH' if sym.startswith('6') else '.SZ'));code=params['filter'].split('"')[1]
 folder=OUT/'raw'/sym;folder.mkdir(parents=True,exist_ok=True)
 rows=[];requests_log=[];seen=set();count=pages=None;covered=False
 for page in range(1,21):
  params['pageNumber']=str(page);path=folder/f'page{page}.json'
  if path.exists():raw=path.read_bytes();origin='saved_raw'
  else:
   time.sleep(.2);r=session.get(url,params=params,timeout=25)
   if r.status_code in (401,403,429):raise StopBatch(f'HTTP {r.status_code}')
   r.raise_for_status();raw=r.content;path.write_bytes(raw);origin='live'
  payload=json.loads(raw)
  if not payload.get('success'):raise ValueError('provider unsuccessful')
  result=payload['result'];batch=result['data']
  if count is None:count=int(result['count']);pages=int(result['pages'])
  if (count,pages)!=(int(result['count']),int(result['pages'])) or pages!=math.ceil(count/int(params['pageSize'])):raise ValueError('pagination changed')
  if not batch or len(batch)>int(params['pageSize']):raise ValueError('empty/oversized page')
  for x in batch:
   if x['SECUCODE']!=code or x['SECURITY_CODE']!=sym:raise ValueError('identity mismatch')
   d=x['END_DATE'][:10];value=Decimal(str(x['TOTAL_SHARES']))
   if d in seen or not value.is_finite() or value<=0 or value%1:raise ValueError('duplicate date/invalid shares')
   if rows and d>=rows[-1]['END_DATE'][:10]:raise ValueError('not descending')
   seen.add(d);rows.append(x)
  requests_log.append({'url':url,'params':dict(params),'raw_path':str(path),'raw_sha256':sha(path),'origin':origin,'retrieved_utc':datetime.now(timezone.utc).isoformat()})
  save(folder/'requests.json',requests_log)
  if rows[-1]['END_DATE'][:10]<=anchor:covered=True;break
  if page==pages:break
 if not covered:raise ValueError('old anchor not reached within bounded source pages')
 atanchor=next(x for x in rows if x['END_DATE'][:10]<=anchor)
 record={'symbol':sym,'approved':False,'input_path':str(source),'input_sha256':sourcehash,'query_cutoff':TARGET,'anchor_date':anchor,'provider_anchor_date':atanchor['END_DATE'][:10],'provider_anchor_shares':str(atanchor['TOTAL_SHARES']),'old_anchor_shares':str(oldvalue),'requests':requests_log,'provider_pages_total':pages,'pages_read':len(requests_log),'date_semantics':'Provider END_DATE; issuer effective timing not certified'}
 if Decimal(str(atanchor['TOTAL_SHARES']))!=oldvalue:
  record.update(status='blocked_anchor_quantity_conflict');return record
 new=[x for x in rows if anchor<x['END_DATE'][:10]<=TARGET]
 candidate=OUT/'normalized_candidate'/f'{sym}.csv'
 with candidate.open('w',encoding='utf-8',newline='') as f:
  w=csv.DictWriter(f,fieldnames=['change_date','total_shares_10k','reason'],lineterminator='\n');w.writeheader();w.writerows(old)
  for x in reversed(new):w.writerow({'change_date':x['END_DATE'][:10],'total_shares_10k':str(Decimal(str(x['TOTAL_SHARES']))/10000),'reason':'Eastmoney END_DATE candidate; effective timing pending'})
 assert sha(source)==sourcehash
 record.update(status='candidate_tail_source_queried',new_provider_events=len(new),candidate_path=str(candidate),candidate_sha256=sha(candidate),historical_rows_unchanged=True,future_events_excluded=sum(x['END_DATE'][:10]>TARGET for x in rows))
 return record
def main():
 p=argparse.ArgumentParser();p.add_argument('--symbols',nargs='+');p.add_argument('--all',action='store_true');a=p.parse_args();assert bool(a.symbols)!=a.all
 for d in ['raw','receipts','normalized_candidate']:(OUT/d).mkdir(parents=True,exist_ok=True)
 inputs={r['symbol']:r for r in read(RUN/'resume_audit/delivery_old4975_input_coverage.csv')};syms=sorted(inputs) if a.all else a.symbols
 session=requests.Session();session.trust_env=False;results=[];fails=0;stop=False
 for sym in syms:
  path=OUT/'receipts'/f'{sym}.json'
  if path.exists():results.append(json.loads(path.read_text()));continue
  try:r=collect(inputs[sym],session);fails=0
  except Exception as e:
   r={'symbol':sym,'approved':False,'status':'blocked','error':str(e),'error_type':type(e).__name__};fails+=1;stop=isinstance(e,StopBatch) or fails>=3
  save(path,r);results.append(r)
  save(OUT/'progress.json',{'processed':len(results),'selected':len(syms),'statuses':{s:sum(x['status']==s for x in results) for s in {x['status'] for x in results}},'stopped':stop})
  if len(results)%25==0 or not a.all:print(sym,r['status'],r.get('error',''),flush=True)
  if stop:break
 save(OUT/('batch_summary.json' if a.all else 'canary_summary.json'),{'approved':False,'records':results,'stopped':stop,'formal_written':False})
if __name__=='__main__':main()
