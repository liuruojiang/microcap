import json,requests,hashlib,datetime,concurrent.futures,fitz
from pathlib import Path
B=Path('research_reports/20261029_st_history_repair/source_recovery');O=B/'global399_sync42_bodies_20261101';O.mkdir(exist_ok=True);items=json.loads((B/'global399_review_sync45_20261101.json').read_text());files={p.name:p for p in B.rglob('*.pdf')}
def one(r):
 e=r['event'];id=e['announcement_id'];p=files.get(id+'.pdf',O/(id+'.pdf'));receipt=p.with_suffix('.receipt.json')
 try:
  if not p.exists():
   response=requests.get(e['source_url'],timeout=20);p.write_bytes(response.content);receipt.write_text(json.dumps(dict(url=e['source_url'],status=response.status_code,sha256=hashlib.sha256(response.content).hexdigest(),collected_at=datetime.datetime.now(datetime.timezone.utc).isoformat()),indent=2));response.raise_for_status()
  raw=p.read_bytes();assert raw.startswith(b'%PDF');text='\n'.join(pg.get_text() for pg in fitz.open(stream=raw,filetype='pdf'));tp=O/(r['symbol']+'.txt');tp.write_text(text,encoding='utf-8');return dict(symbol=r['symbol'],event=e,pdf_path=str(p),receipt_path=str(receipt),sha256=hashlib.sha256(raw).hexdigest(),text_path=str(tp))
 except Exception as exc:return dict(symbol=r['symbol'],event=e,error=str(exc))
rows=[]
with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
 for r in pool.map(one,items):rows.append(r);print(r['symbol'],r.get('error','ok'),flush=True)
(O/'sources.json').write_text(json.dumps(rows,ensure_ascii=False,indent=2),encoding='utf-8')
