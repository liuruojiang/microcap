"""Bounded issuer documents for share-anchor reconciliation; research only."""
import datetime,hashlib,json
from pathlib import Path
import requests,fitz
from bs4 import BeautifulSoup
B=Path(__file__).resolve().parent/'old4975_shares_tail_20261029/issuer_evidence';B.mkdir(exist_ok=True)
sources=[('000762_cancel2025','https://static.cninfo.com.cn/finalpage/2025-11-06/1224787968.PDF'),('000503_annual2025','https://static.cninfo.com.cn/finalpage/2026-04-29/1225227480.PDF'),('000422_annual2025','https://vip.stock.finance.sina.com.cn/corp/view/vCB_AllBulletinDetail.php?id=12190431&stockid=000422')]
sources += [
 ('000825_annual2025','https://money.finance.sina.com.cn/corp/view/vCB_AllBulletinDetail.php?id=12235649&stockid=000825'),
 ('000830_completion2025','https://epaper.cs.com.cn/zgzqb/html/2025-08/27/nw.D110000zgzqb_20250827_2-B028.htm'),
 ('000948_q3_2025','https://www.nantian.com.cn/uploads/files/2025/1229/9a1aSGkzPYslOJUxfAcVfvunQqsGLdHpCxx0uQ6r.pdf'),
 ('001266_h1_2026','https://vip.stock.finance.sina.com.cn/corp/view/vCB_AllBulletinDetail.php?id=12571495'),
 ('002135_q2_conversion2026','https://disc.static.szse.cn/download/disc/disk03/finalpage/2026-07-02/dcf996ce-7141-45fb-9008-c9039405561c.PDF'),
 ('002317_completion2025','https://static.cninfo.com.cn/finalpage/2025-07-31/1224341670.PDF'),
 ('002605_completion2026','https://paper.cnstock.com/html/2026-01/30/content_2175642.htm'),
 ('002646_annual2025','https://money.finance.sina.com.cn/corp/view/vCB_AllBulletinDetail.php?id=12219545&stockid=002646'),
 ('002678_annual2025','https://disc.static.szse.cn/disc/disk03/finalpage/2026-03-31/b68477eb-cda7-4288-b8f1-c8482a6a6458.PDF'),
 ('002851_issuance2026','https://money.finance.sina.com.cn/corp/view/vCB_AllBulletinDetail.php?id=11951749&stockid=002851'),
 ('002869_completion2025','https://static.cninfo.com.cn/finalpage/2025-11-26/1224825365.PDF'),
 ('001266_completion2026','https://static.cninfo.com.cn/finalpage/2026-01-10/1224927727.PDF'),
 ('002460_annual2025','https://www.hkexnews.hk/listedco/listconews/sehk/2026/0331/2026033100487_c.pdf'),
 ('000948_annual2025','https://static.cninfo.com.cn/finalpage/2026-04-21/1225129380.PDF'),
 ('002646_completion2025','https://static.cninfo.com.cn/finalpage/2025-09-03/1224635461.PDF'),
 ('000948_annual2025_actual','https://money.finance.sina.com.cn/corp/view/vCB_AllBulletinDetail.php?id=12122236&stockid=000948'),
]
sources += [('002935_completion2025', 'https://static.cninfo.com.cn/finalpage/2025-07-08/1224091379.PDF'), ('002989_asof20260324', 'https://money.finance.sina.com.cn/corp/view/vCB_AllBulletinDetail.php?id=12024261&stockid=002989'), ('300253_asof20260120', 'https://money.finance.sina.com.cn/corp/view/vCB_AllBulletinDetail.php?id=11931786&stockid=300253'), ('300254_completion2026', 'https://static.cninfo.com.cn/finalpage/2026-03-24/1225024471.PDF'), ('300332_asof20260318', 'https://money.finance.sina.com.cn/corp/view/vCB_AllBulletinDetail.php?id=12003548&stockid=300332'), ('300404_completion2026', 'https://disc.static.szse.cn/download/disc/disk03/finalpage/2026-03-25/9540ccb1-f35c-48ad-81ec-c3a70d697720.PDF'), ('600061_completion2026', 'https://static.cninfo.com.cn/finalpage/2026-03-17/1225012481.PDF')]
''
sources += [('300145_completion2026', 'https://static.cninfo.com.cn/finalpage/2026-02-04/1224964743.PDF')]
sources += [('002460_conversion20251210', 'https://static.cninfo.com.cn/finalpage/2025-12-10/1224862042.PDF')]
sources += [('002135_asof20260323', 'https://paper.cnstock.com/html/2026-03/24/content_2191284.htm')]
sources += [('688235_h1_2025', 'https://static.cninfo.com.cn/finalpage/2025-08-30/1224624603.PDF'), ('603535_h1_2025', 'https://money.finance.sina.com.cn/corp/view/vCB_AllBulletinDetail.php?id=11371361&stockid=603535')]
sources += [('603535_unlock20250826', 'https://static.cninfo.com.cn/finalpage/2025-08-21/1224525639.PDF'), ('603501_placement20260211', 'https://money.finance.sina.com.cn/corp/view/vCB_AllBulletinDetail.php?id=11956585&stockid=603501'), ('605006_completion20260119', 'https://paper.cnstock.com/html/2026-01/22/content_2172854.htm')]
for name,url in sources:
 p=B/(name+'.json')
 if p.exists():continue
 r=dict(url=url,retrieved_at_utc=datetime.datetime.now(datetime.timezone.utc).isoformat())
 try:
  response=requests.get(url,timeout=20);body=response.content;(B/(name+'.bin')).write_bytes(body);r.update(status=response.status_code,sha256=hashlib.sha256(body).hexdigest());response.raise_for_status()
  if body.startswith(b'%PDF'):text='\n'.join(page.get_text() for page in fitz.open(stream=body,filetype='pdf'));r['format']='PDF'
  else:response.encoding=response.apparent_encoding;text=BeautifulSoup(response.text,'html.parser').get_text('\n',strip=True);r['format']='HTML'
  (B/(name+'.txt')).write_text(text,encoding='utf-8');r['body_received']=True
 except Exception as e:r.update(body_received=False,error=str(e))
 p.write_text(json.dumps(r,ensure_ascii=False,indent=2),encoding='utf-8');print(name,r)
