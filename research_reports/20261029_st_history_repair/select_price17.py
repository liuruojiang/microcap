import json,pathlib
B=pathlib.Path('research_reports/20261029_st_history_repair'); R=pathlib.Path.cwd(); O=B/'source_recovery'; targets=[]
wanted={'AN201304250003213763','AN201203020004852211','AN202005081379476925'}
files=list((O/'price17_windows_20261101').glob('*/*/page_*.json'))
for y in ['2011','2013','2020']:files+=list((O/'global_risk_years_20261029'/y).glob('page_*.json'))
for p in files:
 if '.receipt.' in p.name:continue
 d=json.loads(p.read_text(encoding='utf-8'))
 for idx,row in enumerate(d['data']['list']):
  title=row['title'];ident=row['art_code']; window='price17_windows' in str(p)
  if not (ident in wanted or (window and any(k in title for k in ['实行退市风险','实施退市风险','撤销股票交易','实施其他风险','实行其他风险','将实施其他风险']))):continue
  symbol=next((c['stock_code'] for c in row['codes'] if c['stock_code'] in ['000585','000594','000595','000662','000671','000673','000691','000760','000835','000972','000979','002450','002473','002496','002604','002680','002699']),None)
  if not symbol:continue
  targets.append(dict(symbol=symbol,event=dict(announcement_id=ident,issuer_symbol=symbol,notice_date=row['notice_date'][:10],provider_notice_date=row['notice_date'],provider_title=title,title=title,source_url='https://pdf.dfcfw.com/pdf/H2_'+ident+'_1.pdf',source_row_binding=dict(path=str(p),index=idx))))
unique={r['event']['announcement_id']:r for r in targets}; (O/'price17_pdf_targets.json').write_text(json.dumps(list(unique.values()),ensure_ascii=False,indent=2),encoding='utf-8');print([(r['symbol'],r['event']['title']) for r in unique.values()])
s=(B/'fetch_global399_price45_20261101.py').read_text(encoding='utf-8').replace('global399_price45_review_20261101','price17_pdf_review_20261101').replace('global399_review_price45_20261101.json','price17_pdf_targets.json');(B/'fetch_price17_20261101.py').write_text(s,encoding='utf-8')
