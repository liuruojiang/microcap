"""Preserve issuer PDF originals for independently assigned 18-event review."""
from pathlib import Path
import concurrent.futures,datetime,hashlib,json,threading
import requests,fitz
B=Path(__file__).resolve().parent;ROOT=B.parents[1]
O=B/'source_recovery/presample18_price_review_20261101';O.mkdir(exist_ok=True)
selected=[r for r in json.loads((B/'source_recovery/name_gap_target_events_20261101.json').read_text()) if r['reason']=='unknown_presample_withdrawal'][:18]
STOP=threading.Event()
def sha(raw):return hashlib.sha256(raw).hexdigest()
def one(item):
    e=item['events'][0];ident=e['announcement_id'];p=O/(ident+'.pdf');receipt=O/(ident+'.receipt.json')
    binding=e['source_row_binding'];api=ROOT/binding['path'];api_raw=api.read_bytes();row=json.loads(api_raw)['data']['list'][binding['index']]
    assert row['art_code']==ident and row['notice_date']==e['provider_notice_date'] and row['title']==e['provider_title']
    assert e['issuer_symbol'] in {r['stock_code'] for r in row['codes']}
    result=dict(symbol=item['symbol'],event=e,api_response_sha256=sha(api_raw))
    try:
        reused=next((x for x in [B/'source_recovery/body_verification'/(ident+'.pdf'),ROOT/'data/st_notice_evidence'/(ident+'.pdf')] if x.exists()),None)
        if p.exists() and receipt.exists():
            r=json.loads(receipt.read_text());raw=p.read_bytes();assert r['status'] in (200,'local_reuse') and r['sha256']==sha(raw)
        elif reused is not None:
            raw=reused.read_bytes();p.write_bytes(raw);receipt.write_text(json.dumps(dict(url=e['source_url'],status='local_reuse',sha256=sha(raw),reused_from=str(reused),retrieved_at_utc=None),indent=2))
        else:
            if STOP.is_set():raise RuntimeError('Source group stopped')
            r=requests.get(e['source_url'],timeout=15);raw=r.content;p.write_bytes(raw)
            receipt.write_text(json.dumps(dict(url=e['source_url'],status=r.status_code,sha256=sha(raw),retrieved_at_utc=datetime.datetime.now(datetime.timezone.utc).isoformat()),indent=2))
            if r.status_code in (401,403,429):STOP.set()
            r.raise_for_status()
        assert raw.startswith(b'%PDF'), 'not a PDF'
        with fitz.open(stream=raw,filetype='pdf') as doc:
            text='\n'.join(f'\n[PAGE {i+1}]\n'+page.get_text() for i,page in enumerate(doc));pages=len(doc)
        (O/(ident+'.txt')).write_text(text,encoding='utf-8')
        result.update(status='body_ready_for_review',pdf_path=str(p.relative_to(ROOT)),pdf_sha256=sha(raw),text_path=str((O/(ident+'.txt')).relative_to(ROOT)),pages=pages)
    except Exception as exc:result.update(status='blocked',error=repr(exc))
    return result
with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:results=list(pool.map(one,selected))
(O/'fetch_manifest.json').write_text(json.dumps(results,ensure_ascii=False,indent=2),encoding='utf-8')
for r in results:print(r['symbol'],r['status'],r.get('pages'),r.get('error',''),flush=True)
