import json,hashlib,datetime
from pathlib import Path
import requests,akshare as ak
root=Path('research_reports/20261029_st_history_repair/cninfo_share_api_probe_20261030');root.mkdir(exist_ok=True)
original=requests.post
for symbol in ['002869','000422']:
 def capture(url,*args,**kw):
  kw['timeout']=15
  response=original(url,*args,**kw)
  raw=response.content;(root/(symbol+'_response.json')).write_bytes(raw)
  record=dict(url=url,params=kw.get('params'),status=response.status_code,sha256=hashlib.sha256(raw).hexdigest(),retrieved_at_utc=datetime.datetime.now(datetime.timezone.utc).isoformat())
  (root/(symbol+'_receipt.json')).write_text(json.dumps(record,ensure_ascii=False,indent=2),encoding='utf-8')
  response.raise_for_status();return response
 requests.post=capture
 try:
  frame=ak.stock_share_change_cninfo(symbol=symbol,start_date='20250101',end_date='20261029');frame.to_csv(root/(symbol+'_parsed.csv'),index=False); print(symbol,len(frame),flush=True)
 except Exception as e:
  (root/(symbol+'_failure.json')).write_text(json.dumps(dict(type=type(e).__name__,error=str(e)),ensure_ascii=False),encoding='utf-8');print(symbol,type(e).__name__,flush=True);break
 finally:requests.post=original
