from pathlib import Path
import requests,json,hashlib,datetime,concurrent.futures
from bs4 import BeautifulSoup
import fitz
O=Path(__file__).resolve().parent/'share_impact_priority_20261101/point_review/issuer';O.mkdir(exist_ok=True)
urls={
'002076_registration':'https://money.finance.sina.com.cn/corp/view/vISSUE_MarketBulletinDetail.php?id=1644643&stockid=002076',
'300313_legal':'https://static.cninfo.com.cn/finalpage/2018-05-19/1204974530.PDF',
'603269_listing':'https://file.finance.sina.com.cn/211.154.219.97%3A9494/MRGG/CNSESH_STOCK/2020/2020-7/2020-07-31/6470520.PDF',
'600381_resume':'https://epaper.stcn.com/paper/zqsb/page/1/2014-06/27/B046/20140627B046_pdf.pdf',
'002625_halt':'https://epaper.stcn.com/paper/zqsb/html/2015-02/13/content_658061.htm',
'603377_st_entry':'https://www.sse.com.cn/disclosure/listedinfo/announcement/c/new/2024-04-30/603377_20240430_IJUO.pdf',
'603557_st_continue':'https://q.stock.sohu.com/newpdf/202457351387.pdf',
'603557_st_history':'https://paper.cnstock.com/html/2025-04/30/content_2062569.htm',
'603007_st_history':'https://paper.cnstock.com/html/2023-05/18/content_1768128.htm',
}
def one(item):
 name,url=item;p=O/(name+'.bin')
 if p.exists():return {'name':name,'cached':True}
 try:
  r=requests.get(url,timeout=20);p.write_bytes(r.content);rec={'name':name,'url':url,'status':r.status_code,'sha256':hashlib.sha256(r.content).hexdigest(),'retrieved_at_utc':datetime.datetime.now(datetime.timezone.utc).isoformat()};(O/(name+'_receipt.json')).write_text(json.dumps(rec,ensure_ascii=False,indent=2));r.raise_for_status()
  if r.content.startswith(b'%PDF'):text='\n'.join(page.get_text() for page in fitz.open(stream=r.content,filetype='pdf'))
  else:text=BeautifulSoup(r.content,'html.parser').get_text(' ',strip=True)
  (O/(name+'.txt')).write_text(text,encoding='utf-8');return rec
 except Exception as e:return {'name':name,'error':str(e)}
with concurrent.futures.ThreadPoolExecutor(max_workers=2) as ex:
 for result in ex.map(one,urls.items()):print(result,flush=True)
