"""Bounded unauthenticated public-source canaries; raw bytes retained."""
from pathlib import Path
import requests,json,hashlib,datetime,io
import pandas as pd
B=Path(__file__).resolve().parent/'alternative_name_sources_20261101'
B.mkdir(exist_ok=True)
tasks=[('szse_shortnames','https://www.szse.cn/api/report/ShowReport',dict(SHOWTYPE='xlsx',CATALOGID='SSGSGMXX',TABKEY='tab2'), 'xlsx')]
tasks += [('sina_'+s,f'https://vip.stock.finance.sina.com.cn/corp/go.php/vCI_CorpInfo/stockid/{s}.phtml',{},'html') for s in ['002072','600403','000001']]
for name,url,params,extension in tasks:
    receipt=B/(name+'.receipt.json')
    if receipt.exists():
        print(name,'existing receipt preserved',flush=True);continue
    row=dict(url=url,params=params,retrieved_at_utc=datetime.datetime.now(datetime.timezone.utc).isoformat())
    try:
        r=requests.get(url,params=params,timeout=20,headers={'User-Agent':'Mozilla/5.0','Referer':'https://www.szse.cn/' if name.startswith('szse') else 'https://finance.sina.com.cn/'})
        p=B/(name+'.'+extension);p.write_bytes(r.content)
        row.update(status=r.status_code,sha256=hashlib.sha256(r.content).hexdigest(),bytes=len(r.content))
        r.raise_for_status()
        if extension=='xlsx':
            frame=pd.read_excel(io.BytesIO(r.content),dtype=str);frame.to_csv(B/(name+'.csv'),index=False,encoding='utf-8-sig')
            row.update(rows=len(frame),columns=list(frame.columns),canaries=frame.loc[frame.astype(str).apply(lambda col:col.str.contains('002072|000001',regex=True)).any(axis=1)].fillna('').to_dict('records'))
        else:
            r.encoding='gb18030';text=r.text
            tables=pd.read_html(io.StringIO(text))
            hits=[]
            for t in tables:
                hit=t[t.astype(str).apply(lambda c:c.str.contains('更名历史|曾用|变更日期',regex=True)).any(axis=1)]
                if not hit.empty:hits.extend(hit.fillna('').astype(str).values.tolist())
            row.update(name_history_rows=hits)
    except Exception as exc:row['error']=type(exc).__name__+': '+str(exc)
    receipt.write_text(json.dumps(row,ensure_ascii=False,indent=2),encoding='utf-8')
    print(name,row,flush=True)
