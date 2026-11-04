from pathlib import Path
import requests,json,hashlib,datetime
B=Path('research_reports/20261029_st_history_repair/delisting_source3_20261101');B.mkdir(exist_ok=True);url='https://np-anotice-stock.eastmoney.com/api/security/ann'
for s,start,end in [('600321','2024-05-01','2024-06-30'),('300431','2020-08-20','2020-11-11'),('300216','2020-07-01','2020-09-17')]:
 rows=[];page=1
 while True:
  params=dict(stock_list=s,f_node=0,s_node=0,ann_type='A',page_size=100,page_index=page,sr=-1,begin_time=start,end_time=end);r=requests.get(url,params=params,timeout=15);p=B/(s+'_page'+str(page)+'.json');p.write_bytes(r.content);(p.with_suffix('.receipt.json')).write_text(json.dumps(dict(url=url,params=params,status=r.status_code,collected_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),sha256=hashlib.sha256(r.content).hexdigest()),indent=2),encoding='utf8');r.raise_for_status();d=r.json()['data'];assert r.json()['success']==1 and int(d['page_index'])==page;rows+=d['list']
  if len(rows)>=int(d['total_hits']):break
  page+=1
 assert len(rows)==int(d['total_hits']);print(s,len(rows))
 for r in rows:print(r['notice_date'][:10],r['art_code'],r['title'])
