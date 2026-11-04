from pathlib import Path
import requests,hashlib,json,re,datetime
p=Path('research_reports/20261029_st_history_repair/new463_cninfo_shares_20261029/issuer_latest_review');p.mkdir(exist_ok=True)
urls={'002592':'https://money.finance.sina.com.cn/corp/view/vCB_AllBulletinDetail.php?id=12673262&stockid=002592','003004':'https://vip.stock.finance.sina.com.cn/corp/view/vCB_AllBulletinDetail.php?id=12647109&stockid=003004'}
for s,u in urls.items():
 r=requests.get(u,timeout=20);r.raise_for_status(); (p/f'{s}.html').write_bytes(r.content);r.encoding=r.apparent_encoding
 links=re.findall(r'https?[^\s\"<>]+\.pdf[^\s\"<>]*',r.text)
 print(s,links)
 (p/f'{s}_receipt.json').write_text(json.dumps({'url':u,'status':r.status_code,'sha256':hashlib.sha256(r.content).hexdigest(),'retrieved_at_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'pdf_links':links},indent=2),encoding='utf-8')
