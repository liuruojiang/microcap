from pathlib import Path
import requests,json,hashlib,datetime
D=Path('research_reports/20261029_st_history_repair/delisting_source3_20261101')
urls={'600321_exchange':'https://www.sse.com.cn/disclosure/announcement/listing/stock/c/c_20240620_10759044.shtml','300431_exchange':'https://www.szse.cn/disclosure/notice/t20200828_581149.html','300216_exchange':'https://www.szse.cn/disclosure/notice/t20200714_579593.html','600321_transfer':'https://rdfile.dzh.com.cn/new/dzh/tb_gg/2024/8/15/5B08476FEC9AC40272299F73C7002F18_10378944.pdf','300431_transfer':'https://money.finance.sina.com.cn/corp/view/vCB_AllBulletinDetail.php?id=8648726&stockid=300431'}
for name,url in urls.items():
 try:
  r=requests.get(url,timeout=12);p=D/(name+'.raw');p.write_bytes(r.content);(D/(name+'.receipt.json')).write_text(json.dumps(dict(url=url,status=r.status_code,sha256=hashlib.sha256(r.content).hexdigest(),collected_at=datetime.datetime.now(datetime.timezone.utc).isoformat()),indent=2),encoding='utf8');print(name,r.status_code,len(r.content),flush=True)
 except Exception as e:print(name,type(e).__name__,str(e),flush=True)
