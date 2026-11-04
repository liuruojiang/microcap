"""Current original-provider records, not independent issuer truth."""
import json,hashlib,datetime,time
from pathlib import Path
import requests,akshare as ak
B=Path(__file__).resolve().parent;root=B/'cninfo_share_conflicts_probe_20261030';root.mkdir(exist_ok=True)
original=requests.post
symbols=[r['symbol'] for r in json.loads((B/'old4975_shares_tail_20261029/final_independent_manifest.json').read_text(encoding='utf-8'))['conflicts'] if r['symbol'] not in ['002869','000422']]
for symbol in symbols:
 if (root/(symbol+'_receipt.json')).exists() or (root/(symbol+'_failure.json')).exists():continue
 def capture(url,*args,**kw):
  kw['timeout']=15;response=original(url,*args,**kw);raw=response.content
  (root/(symbol+'_response.json')).write_bytes(raw)
  record=dict(url=url,params=kw.get('params'),status=response.status_code,sha256=hashlib.sha256(raw).hexdigest(),retrieved_at_utc=datetime.datetime.now(datetime.timezone.utc).isoformat())
  (root/(symbol+'_receipt.json')).write_text(json.dumps(record,ensure_ascii=False,indent=2),encoding='utf-8');response.raise_for_status();return response
 requests.post=capture
 try:
  time.sleep(.3);frame=ak.stock_share_change_cninfo(symbol=symbol,start_date='20250101',end_date='20261029');frame.to_csv(root/(symbol+'_parsed.csv'),index=False);print(symbol,len(frame),flush=True)
 except Exception as e:
  (root/(symbol+'_failure.json')).write_text(json.dumps(dict(type=type(e).__name__,error=str(e)),ensure_ascii=False),encoding='utf-8');print(symbol,type(e).__name__,flush=True);break
 finally:requests.post=original
