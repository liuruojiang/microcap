"""Observed RPT_IPO_ABSTOCK public pagination; never formal ST authority."""
from pathlib import Path
import requests,json,hashlib,datetime,time
B=Path(__file__).resolve().parent
O=B/'alternative_name_sources_20261101/eastmoney_global'
def sha(raw):return hashlib.sha256(raw).hexdigest()
def save(path,data):path.write_text(json.dumps(data,ensure_ascii=False,indent=2),encoding='utf-8')
first=json.loads((O/'page1.json').read_bytes());receipt=json.loads((O/'page1.receipt.json').read_text())
assert receipt['sha256']==sha((O/'page1.json').read_bytes()) and first['success']
count=first['result']['count'];pages=first['result']['pages'];effective_size=len(first['result']['data'])
assert pages==(count+effective_size-1)//effective_size and pages<=100
requests_log=[];frames={}
for page in [1,pages,*range(2,pages)]:
    path=O/f'page{page}.json';rp=O/f'page{page}.receipt.json';params=dict(receipt['params'],pageNumber=page)
    if not rp.exists():
        if (O/'GROUP_STOP.json').exists():raise RuntimeError('Source group stopped; no blind retry')
        row=dict(url=receipt['url'],params=params,retrieved_at_utc=datetime.datetime.now(datetime.timezone.utc).isoformat())
        try:
            time.sleep(.2)
            response=requests.get(receipt['url'],params=params,timeout=15)
            path.write_bytes(response.content);row.update(status=response.status_code,sha256=sha(response.content))
            save(rp,row)
            if response.status_code in (401,403,429):save(O/'GROUP_STOP.json',row)
            response.raise_for_status()
        except Exception as exc:
            if not rp.exists():row['error']=repr(exc);save(rp,row)
            raise
    row=json.loads(rp.read_text());raw=path.read_bytes()
    assert row['status']==200 and row['sha256']==sha(raw) and row['params']==params
    payload=json.loads(raw);assert payload['success'] and payload['code']==0
    result=payload['result'];assert result['count']==count and result['pages']==pages
    data=result['data'];expected=effective_size if page<pages else count-effective_size*(pages-1)
    assert len(data)==expected
    frames[page]=data;requests_log.append(dict(page=page,rows=len(data),response_sha256=row['sha256']))
    print(page,len(data),flush=True)
rows=[row for page in range(1,pages+1) for row in frames[page]]
keys=[(r['SECUCODE'],r['CHANGE_DATE'],int(r['RANK'])) for r in rows]
assert len(rows)==count and len(set(keys))==count and keys==sorted(keys)
assert all(r['SECURITY_CODE']==r['SECUCODE'].split('.')[0] and r['CHANGE_AFTER_FN'] for r in rows)
canaries=[]
for symbol in ['002072.SZ','600403.SH','000001.SZ']:
    single=json.loads((O.parent/f'eastmoney_namechange_{symbol}.json').read_bytes())['result']['data']
    global_rows=[r for r in rows if r['SECUCODE']==symbol]
    assert global_rows==single,f'global/single mismatch: {symbol}'
    canaries.append(dict(symbol=symbol,rows=len(single),single_global_equal=True))
mapping=json.loads((B/'raw_install_normalized_20261029/install_manifest.json').read_text(encoding='utf-8'))
scope={r['symbol'] for r in mapping['records']};covered={r['SECURITY_CODE'] for r in rows}
save(O/'manifest.json',dict(formal_approved=False,formal_written=False,provider='Eastmoney',reportName='RPT_IPO_ABSTOCK',
    source_query_date_bounds=None,requested_page_size=receipt['params']['pageSize'],effective_page_size=effective_size,
    pages=pages,count=count,unique_symbols=len(covered),scope_symbols=len(scope),scope_covered=len(scope&covered),
    scope_missing=sorted(scope-covered),canaries=canaries,requests=requests_log,
    missing_announcement_dates=True,completeness_boundary='All rows exposed by this provider query; not independent proof of all historical changes'))
save(O/'normalized_all_rows.json',rows)
print('PASS',count,'rows',pages,'pages','scope covered',len(scope&covered),'missing',len(scope-covered),flush=True)
