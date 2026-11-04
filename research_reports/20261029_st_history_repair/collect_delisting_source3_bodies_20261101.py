from pathlib import Path
import json,hashlib,requests,datetime,pypdf
D=Path('research_reports/20261029_st_history_repair/delisting_source3_20261101');targets={'600321':['AN202405301634958143','AN202406201636717835'],'300431':['AN202008281403338603','AN202009181414650205','AN202011091427766100'],'300216':['AN202007141391655662','AN202008041396389325','AN202009151413033474']}
for s,arts in targets.items():
 rows=json.loads((D/(s+'_page1.json')).read_bytes())['data']['list']
 for art in arts:
  row=next(r for r in rows if r['art_code']==art);url=f'https://pdf.dfcfw.com/pdf/H2_{art}_1.pdf';r=requests.get(url,timeout=15);r.raise_for_status();assert r.content.startswith(b'%PDF-');p=D/(art+'.pdf');p.write_bytes(r.content);rec=dict(url=url,status=r.status_code,sha256=hashlib.sha256(r.content).hexdigest(),collected_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),source_api=s+'_page1.json',source_index=rows.index(row),notice_date=row['notice_date'],title=row['title']);p.with_suffix('.receipt.json').write_text(json.dumps(rec,ensure_ascii=False,indent=2),encoding='utf8');text='\n'.join(page.extract_text() or '' for page in pypdf.PdfReader(p).pages);p.with_suffix('.txt').write_text(text,encoding='utf8');print(s,art,len(text))
