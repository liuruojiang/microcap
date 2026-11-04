from pathlib import Path
import json,datetime,hashlib,socket
import pandas as pd
import baostock as bs
B=Path(__file__).resolve().parent;O=B/'share_impact_priority_20261101/point_review';O.mkdir(exist_ok=True);f=pd.read_csv(B/'share_impact_priority_20261101/near_old_formal_cutoff.csv',dtype={'symbol':str});f['year']=f.rebalance_date.str[:4]
fields='date,code,close,preclose,volume,tradestatus,isST,adjustflag';socket.setdefaulttimeout(15)
login=bs.login();assert login.error_code=='0';records=[]
try:
 for (sym,year),group in f.groupby(['symbol','year']):
  label=sym+'_'+year;p=O/(label+'_baostock.json')
  if p.exists():continue
  code=('sh.' if sym.startswith('6') else 'sz.')+sym;start=group.rebalance_date.min();end=group.rebalance_date.max();res=bs.query_history_k_data_plus(code,fields,start_date=start,end_date=end,frequency='d',adjustflag='3');rows=[]
  while res.error_code=='0' and res.next():rows.append(res.get_row_data())
  o=dict(request=dict(code=code,fields=fields,start=start,end=end,frequency='d',adjustflag='3'),error_code=res.error_code,error_msg=res.error_msg,fields=res.fields,rows=rows);raw=json.dumps(o,ensure_ascii=False,indent=2).encode();p.write_bytes(raw)
  receipt=dict(provider='baostock',retrieved_at_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),request=o['request'],response_sha256=hashlib.sha256(raw).hexdigest(),query_scope='targeted historical share-conflict dates; not full-history certification');(O/(label+'_receipt.json')).write_text(json.dumps(receipt,indent=2));assert res.error_code=='0'
  wanted=set(group.rebalance_date);picked=[dict(zip(res.fields,r)) for r in rows if r[0] in wanted];assert {r['date'] for r in picked}==wanted
  records.extend(picked);print(sym,year,picked,flush=True)
finally:bs.logout()
(O/'point_source_rows.json').write_text(json.dumps(records,indent=2))
