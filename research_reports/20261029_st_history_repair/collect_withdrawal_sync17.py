import json,requests,hashlib,datetime,concurrent.futures,fitz
from pathlib import Path
B=Path('research_reports/20261029_st_history_repair/source_recovery');O=B/'withdrawal_sync17_20261101';O.mkdir(exist_ok=True);items=[r for r in json.loads((B/'name_gap_target_events_20261101.json').read_text()) if r['reason']=='unknown_presample_withdrawal'][18:]
def one(r):
 e=r['events'][0];id=e['announcement_id'];p=B/'body_verification'/f'{id}.pdf';receipt=p.with_suffix('.receipt.json')
 if not p.exists():
  p=O/f'{id}.pdf';receipt=p.with_suffix('.receipt.json')
  if not p.exists():
   resp=requests.get(e['source_url'],timeout=20);p.write_bytes(resp.content);receipt.write_text(json.dumps(dict(url=e['source_url'],status=resp.status_code,sha256=hashlib.sha256(resp.content).hexdigest(),collected_at=datetime.datetime.now(datetime.timezone.utc).isoformat()),indent=2));resp.raise_for_status()
 raw=p.read_bytes();assert raw.startswith(b'%PDF');text='\n'.join(pg.get_text() for pg in fitz.open(stream=raw,filetype='pdf'));t=O/f'{r["symbol"]}.txt';t.write_text(text,encoding='utf-8');return dict(symbol=r['symbol'],event=e,pdf_path=str(p),receipt_path=str(receipt),sha256=hashlib.sha256(raw).hexdigest(),text_path=str(t))
rows=[]
with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
 for r in pool.map(one,items):rows.append(r);print(r['symbol'],flush=True)
(O/'sources.json').write_text(json.dumps(rows,ensure_ascii=False,indent=2),encoding='utf-8')
