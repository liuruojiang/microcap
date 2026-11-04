"""Bounded single-security historical name evidence, never formal ST state.

An exact externally prepared symbol list is required; no built-in whole universe.
Existing successful canaries are reused with their original receipts/timestamps.
"""
from pathlib import Path
import argparse,concurrent.futures,datetime,hashlib,json,threading,time,uuid
import requests

B=Path(__file__).resolve().parent
URL='https://webapi.cninfo.com.cn/api/stock/p_stock2109'
PROBE=B/'cninfo_name_history_probe_20261101'
REUSE={'002072':'single_no_date','000001':'000001_single','600403':'600403_single','600831':'600831_single'}
STOP=threading.Event();LOCAL=threading.local();ENCODER=None;ENCODER_LOCK=threading.Lock()

def sha(raw):return hashlib.sha256(raw).hexdigest()
def dump(path,value):
    path.parent.mkdir(parents=True,exist_ok=True)
    tmp=path.with_name(path.name+'.'+uuid.uuid4().hex+'.tmp')
    tmp.write_text(json.dumps(value,ensure_ascii=False,indent=2,allow_nan=False),encoding='utf-8');tmp.replace(path)

def parse(raw,symbol):
    obj=json.loads(raw)
    if obj.get('resultcode')!=200:raise ValueError('Provider response unsuccessful')
    rows=obj.get('records')
    if not isinstance(rows,list):raise ValueError('Missing records array')
    if type(obj.get('count')) is not int or type(obj.get('total')) is not int:raise ValueError('Missing integer count/total')
    if obj['count']>=20000 or obj['total']>=20000:raise ValueError('Provider 20000-record cap reached; completeness unproven')
    if obj['count']!=obj['total'] or obj['count']!=len(rows):raise ValueError('Truncated/inconsistent count/total')
    for row in rows:
        required={'SECCODE','SECNAME','STARTDATE','DECLAREDATE','F002V'}
        if not required.issubset(row) or row['SECCODE']!=symbol:raise ValueError('Wrong issuer/schema')
        if not isinstance(row['SECNAME'],str) or not row['SECNAME'].strip():raise ValueError('Missing new name')
        if not isinstance(row['STARTDATE'],str):raise ValueError('Missing effective date')
        if datetime.date.fromisoformat(row['STARTDATE']).isoformat()!=row['STARTDATE']:raise ValueError('Invalid effective date')
        if row['DECLAREDATE'] is not None:
            if datetime.date.fromisoformat(row['DECLAREDATE']).isoformat()!=row['DECLAREDATE']:raise ValueError('Invalid announcement date')
        if row['F002V'] is not None and not isinstance(row['F002V'],str):raise ValueError('Invalid old name')
    return rows

def check_receipt(receipt,raw,symbol):
    if receipt.get('status')!=200 or receipt.get('url')!=URL or receipt.get('params')!={'scode':symbol}:
        raise ValueError('Receipt is not the successful unbounded single-code query')
    if receipt.get('method','GET')!='GET':raise ValueError('Unexpected request method')
    if receipt.get('sha256',receipt.get('response_sha256'))!=sha(raw):raise ValueError('Response hash mismatch')
    stamp=datetime.datetime.fromisoformat(receipt['retrieved_at_utc'].replace('Z','+00:00'))
    if stamp.tzinfo is None or stamp>datetime.datetime.now(datetime.timezone.utc):raise ValueError('Invalid real receipt timestamp')
    return parse(raw,symbol)

def compare(symbol,rows,emrows):
    def name(value):return ''.join((value or '').split()).upper()
    em=sorted([r for r in emrows if r['SECURITY_CODE']==symbol],key=lambda r:(r['CHANGE_DATE'],r['RANK']))
    emkeys={(r['CHANGE_DATE'][:10],name(r['CHANGE_AFTER_FN'])) for r in em}
    cnkeys={(r['STARTDATE'],name(r['SECNAME'])) for r in rows}
    differences=[]
    for r in rows:
        prior=[e for e in em if e['CHANGE_DATE'][:10]<r['STARTDATE']]
        if prior and r['F002V'] is not None and name(prior[-1]['CHANGE_AFTER_FN'])!=name(r['F002V']):
            differences.append(dict(effective_date=r['STARTDATE'],cninfo_old_name=r['F002V'],eastmoney_previous_name=prior[-1]['CHANGE_AFTER_FN']))
    # The first EM row is often the listing name, not a name-change event;
    # keep its separate absence visible instead of requiring an invented change.
    first_key=(em[0]['CHANGE_DATE'][:10],name(em[0]['CHANGE_AFTER_FN'])) if em else None
    return dict(eastmoney_rows=len(em),cninfo_rows=len(rows),
        cninfo_missing_name_events=[list(k) for k in sorted(emkeys-cnkeys) if k!=first_key],
        eastmoney_initial_name_missing_from_cninfo=first_key not in cnkeys if first_key else None,
        cninfo_events_not_in_eastmoney=[list(k) for k in sorted(cnkeys-emkeys)],
        old_name_disagreements=differences,
        null_announcement_dates=sum(r['DECLAREDATE'] is None for r in rows),
        announcement_after_effective=[r for r in rows if r['DECLAREDATE'] and r['DECLAREDATE']>r['STARTDATE']],
        duplicate_event_keys=len(rows)-len(cnkeys),
        same_name_risk_category_changes_not_observable=True)

def collect(symbol,out,emrows,offline=False):
    folder=out/symbol;index=folder/'index.json'
    if index.exists():
        saved=json.loads(index.read_text(encoding='utf-8'))
        if saved['status']=='failed':return saved
        rp=B/saved['receipt_path'];path=B/saved['response_path'];raw=path.read_bytes()
        rows=check_receipt(json.loads(rp.read_text()),raw,symbol)
        assert saved['response_sha256']==sha(raw)
        return saved
    path=rp=None
    try:
        retained=sorted((folder/'attempts').glob('*/receipt.json'))
        if retained:
            rp=retained[-1];path=rp.with_name('response.json');raw=path.read_bytes();receipt=json.loads(rp.read_text())
            rows=check_receipt(receipt,raw,symbol);origin='recovered_original_attempt'
        elif symbol in REUSE:
            stem=REUSE[symbol];path=PROBE/(stem+'_response.json');rp=PROBE/(stem+'_receipt.json')
            raw=path.read_bytes();receipt=json.loads(rp.read_text());rows=check_receipt(receipt,raw,symbol);origin='original_canary'
        else:
            if offline:raise ValueError('No original canary available in offline mode')
            if STOP.is_set() or (out/'GROUP_STOP.json').exists():raise ValueError('Group stopped after auth/rate-limit response')
            if ENCODER is None:raise RuntimeError('Encoder must be initialized once before worker startup')
            if not hasattr(LOCAL,'session'):LOCAL.session=requests.Session()
            stamp=datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')
            path=folder/'attempts'/stamp/'response.json';rp=path.with_name('receipt.json');path.parent.mkdir(parents=True,exist_ok=True)
            receipt=dict(url=URL,method='GET',params={'scode':symbol},requested_at_utc=datetime.datetime.now(datetime.timezone.utc).isoformat())
            try:
                time.sleep(.25)
                with ENCODER_LOCK:public_code=ENCODER.call('getResCode1')
                response=LOCAL.session.get(URL,params={'scode':symbol},timeout=15,
                    headers={'Accept-Enckey':public_code,'Origin':'https://webapi.cninfo.com.cn','Referer':'https://webapi.cninfo.com.cn/','User-Agent':'Mozilla/5.0'})
                raw=response.content;path.write_bytes(raw)
                receipt.update(status=response.status_code,sha256=sha(raw),retrieved_at_utc=datetime.datetime.now(datetime.timezone.utc).isoformat());dump(rp,receipt)
                if response.status_code in (401,403,429):STOP.set();dump(out/'GROUP_STOP.json',receipt)
                response.raise_for_status()
                body_status=json.loads(raw).get('resultcode')
                if str(body_status) in {'401','403','429'}:
                    STOP.set();dump(out/'GROUP_STOP.json',dict(receipt,provider_resultcode=body_status))
                    raise ValueError('Provider auth/rate-limit code; entire source group stopped')
                rows=check_receipt(receipt,raw,symbol)
            except Exception as exc:
                if not rp.exists():receipt.update(status='transport_error',error=repr(exc),retrieved_at_utc=datetime.datetime.now(datetime.timezone.utc).isoformat());dump(rp,receipt)
                raise
            origin='live_single_code_no_date_bounds'
        result=dict(symbol=symbol,status='collected_not_certified' if rows else 'empty_unproven',origin=origin,
            response_path=str(path.relative_to(B)),receipt_path=str(rp.relative_to(B)),response_sha256=sha(raw),rows=len(rows),
            source_query_date_bounds=None,formal_approved=False,comparison=compare(symbol,rows,emrows))
    except Exception as exc:
        result=dict(symbol=symbol,status='failed',error=type(exc).__name__+': '+str(exc),formal_approved=False)
        if rp is not None and rp.exists():result['receipt_path']=str(rp.relative_to(B))
        if path is not None and path.exists():result.update(response_path=str(path.relative_to(B)),response_sha256=sha(path.read_bytes()))
    dump(index,result);return result

def main():
    global ENCODER
    ap=argparse.ArgumentParser();ap.add_argument('--symbols-file',required=True);ap.add_argument('--workers',type=int,choices=[1,2],default=1);ap.add_argument('--execute',action='store_true');ap.add_argument('--offline',action='store_true');ap.add_argument('--output',default=str(B/'cninfo_name_history_targeted_20261101'));args=ap.parse_args()
    source=Path(args.symbols_file).resolve();source_bytes=source.read_bytes();payload=json.loads(source_bytes)
    symbols=payload if isinstance(payload,list) else payload['symbols']
    if not isinstance(symbols,list) or not symbols or len(set(symbols))!=len(symbols) or not all(isinstance(s,str) and len(s)==6 and s.isascii() and s.isdigit() for s in symbols):raise ValueError('Exact unique six-digit symbol list required')
    out=Path(args.output).resolve()
    if not out.is_relative_to(B.resolve()):raise ValueError('Research output must remain inside task evidence directory')
    em_path=B/'alternative_name_sources_20261101/eastmoney_global/normalized_all_rows.json';em_bytes=em_path.read_bytes();emrows=json.loads(em_bytes)
    print('plan',len(symbols),'symbols',args.workers,'workers',flush=True)
    if not args.execute:return
    if not args.offline:
        import akshare as ak
        namespace=ak.stock_share_change_cninfo.__globals__
        ENCODER=namespace['py_mini_racer'].MiniRacer()
        ENCODER.eval(namespace['_get_file_content_cninfo']('cninfo.js'))
    out.mkdir(parents=True,exist_ok=True);results=[];start=time.monotonic()
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as pool:
        for row in pool.map(lambda s:collect(s,out,emrows,args.offline),symbols,buffersize=args.workers):
            results.append(row)
            progress=dict(formal_approved=False,formal_written=False,symbol_list_path=str(source),symbol_list_sha256=sha(source_bytes),eastmoney_rows_sha256=sha(em_bytes),requested=len(symbols),completed=len(results),elapsed_seconds=round(time.monotonic()-start,2),results=results)
            dump(out/'progress.json',progress);print(len(results),row['symbol'],row['status'],flush=True)
    assert source.read_bytes()==source_bytes and em_path.read_bytes()==em_bytes
    dump(out/'manifest.json',progress)

if __name__=='__main__':main()
