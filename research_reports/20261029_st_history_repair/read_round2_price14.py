import pathlib,json,re,sys
p=pathlib.Path('research_reports/20261029_st_history_repair/source_recovery');a=json.load(open(p/'round2_price13_plus_300029.json',encoding='utf-8'))
for i in range(int(sys.argv[1]),int(sys.argv[2])):
 r=a[i];f=p/'round2_price14_pdf_20261101'/(r['event']['announcement_id']+'.txt'); print('\nINDEX',i,r['symbol'])
 if not f.exists():print('MISSING');continue
 t=f.read_text(encoding='utf-8'); pages=re.split(r'\[PAGE \d+\]',t)[1:]
 for j,pg in enumerate(pages):
  flat=re.sub(r'\s+','',pg); sentences=re.split('(?<=[。；])',flat)
  hits=[s for s in sentences if any(k in s for k in ['起始日','日起','开市','仍将','继续被','继续实施','不确定','可能被','股票简称','不存在','不停牌'])]
  print('P',j+1,' '.join(hits)[:2600])
