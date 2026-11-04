from pathlib import Path
import pandas as pd,json,requests,hashlib,datetime,re,argparse
B=Path(__file__).resolve().parent;O=B/'resume_audit/current_names_tencent_20261101';O.mkdir(exist_ok=True);master=B/'cross_source_official/security_master_combined_candidate.csv';m=pd.read_csv(master,dtype=str).fillna('');assert m.symbol.is_unique
listed=pd.to_datetime(m.list_date,errors='raise');delisted=pd.to_datetime(m.delist_date.replace('',None),errors='raise');cut=pd.Timestamp('2026-10-30');eligible=m.loc[m.symbol.str.fullmatch('[036][0-9]{5}') & m.exchange.isin(['SSE','SZSE']) & (listed<=cut) & (delisted.isna()|(cut<delisted))].copy();assert ((eligible.symbol.str.startswith('6') & eligible.exchange.eq('SSE'))|(~eligible.symbol.str.startswith('6')&eligible.exchange.eq('SZSE'))).all();eligible=eligible.sort_values('symbol');keys=[('sh' if s.startswith('6') else 'sz')+s for s in eligible.symbol]; batches=[keys[:80]]+[keys[i:i+500] for i in range(80,len(keys),500)]
def sha(x):return hashlib.sha256(x).hexdigest()
def collect(n):
 batch=batches[n];p=O/f'batch_{n:03}.gbk';rec=O/f'batch_{n:03}.receipt.json';url='https://qt.gtimg.cn/q='+','.join(batch)
 if not p.exists():
  start=datetime.datetime.now(datetime.timezone.utc).isoformat();r=requests.get(url,timeout=30,headers={'Referer':'https://finance.qq.com/','User-Agent':'Mozilla/5.0'});p.write_bytes(r.content);rec.write_text(json.dumps(dict(url=url,requested=batch,status=r.status_code,started_at=start,completed_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),sha256=sha(r.content)),indent=2));r.raise_for_status()
 receipt=json.loads(rec.read_text());raw=p.read_bytes();assert receipt['sha256']==sha(raw) and receipt['requested']==batch and receipt['status']==200
 rows=[]
 for line in raw.decode('gbk',errors='strict').split(';'):
  if not line.strip():continue
  match=re.fullmatch(r'\s*v_((?:sh|sz)\d{6})="([^"]*)"\s*',line);assert match,line[:100]
  key,payload=match.groups();parts=payload.split('~');assert key in batch
  if len(parts)<=30:rows.append(dict(quote_key=key,symbol=key[2:],status='missing_payload',name='',quote_time='',source_file=str(p)));continue
  assert parts[2]==key[2:];name=parts[1].strip();qt=parts[30];assert name;dt=datetime.datetime.strptime(qt,'%Y%m%d%H%M%S');assert dt.date()<=datetime.date(2026,11,1)
  rows.append(dict(quote_key=key,symbol=key[2:],status='observed',name=name,quote_time=qt,quote_date=str(dt.date()),quote_is_target_close=dt.date()==cut.date(),risk_name=bool(re.match(r'^(?:\*?ST|PT|S\*?ST)',name.upper().replace(' ',''))),source_file=str(p)))
 assert len({x['quote_key'] for x in rows})==len(rows);return rows
if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('--all',action='store_true');a=p.parse_args();rows=[]
 for n in range(len(batches) if a.all else 1):rows+=collect(n);print('batch',n,'rows',len(rows),flush=True)
 report=dict(scope='current observed name filter only; never historical ST interval',master_sha256=sha(master.read_bytes()),eligibility_cutoff='2026-10-30',list_boundary='list_date <= cutoff < delist_date; empty delist means still listed',master_rows=len(m),eligible_count=len(keys),requested_count=len(keys) if a.all else len(batches[0]),rows=rows,missing_symbols=sorted(set(eligible.symbol)-{r['symbol'] for r in rows if r['status']=='observed'}),stale_quote_symbols=[r['symbol'] for r in rows if r.get('quote_is_target_close') is False],risk_name_symbols=[r['symbol'] for r in rows if r.get('risk_name')],observed_at=datetime.datetime.now(datetime.timezone.utc).isoformat())
 (O/('snapshot.json' if a.all else 'canary.json')).write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf-8');print({k:v for k,v in report.items() if k not in ['rows','missing_symbols','stale_quote_symbols','risk_name_symbols']});print('missing',len(report['missing_symbols']),'stale',len(report['stale_quote_symbols']),'ST',len(report['risk_name_symbols']))
