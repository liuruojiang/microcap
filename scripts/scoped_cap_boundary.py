"""Original-rule boundary check from two dated market-wide source pages.

The wide snapshot screens for overlooked candidates; it never refreshes deep
price/share/ratio histories outside the maintained scope or grants membership.
"""
from __future__ import annotations

from datetime import datetime,timezone
from decimal import Decimal
import json
from pathlib import Path
import re

import pandas as pd

from scripts import scoped_source_consumer as sources
from scripts.st_history_scope import (source_closure,read_bound,validate_scope,
    normalized_risk_rows,symbol_name_changes,_daily_notice_row)
from scripts.st_notice_incremental import verify_day
from scripts.st_notice_evidence import _registry,load_verified_events

URL='https://datacenter-web.eastmoney.com/api/data/v1/get'
FIELDS='SECURITY_CODE,SECUCODE,SECURITY_NAME_ABBR,TRADE_DATE,TOTAL_SHARES,TOTAL_MARKET_CAP,CLOSE_PRICE'


def request_params(day,page):
    return dict(reportName='RPT_VALUEANALYSIS_DET',columns=FIELDS,source='WEB',client='WEB',
        sortColumns='SECURITY_CODE',sortTypes='1',pageSize=5000,pageNumber=page,filter=f"(TRADE_DATE='{day}')")


def read_snapshot(root,spec,day):
    files=spec['files'];directory=spec['directory'];rows=[];stable={};expected=None;page=1
    while True:
        names=[f'page-{page:02d}.json',f'page-{page:02d}.receipt.json'];bodies=[]
        for name in names:
            relative=directory+'/'+name
            path=sources.bound(root,relative);raw=path.read_bytes()
            if files.get(name)!=sources.sha(raw):raise ValueError('Dated boundary page bytes differ')
            stable[relative]=sources.sha(raw);bodies.append(raw)
        raw,receipt_raw=bodies;receipt=json.loads(receipt_raw)
        stamp=datetime.fromisoformat(receipt['fetched_at'])
        close=datetime.fromisoformat(day+'T15:00:00+08:00')
        if (receipt.get('url')!=URL or receipt.get('params')!=request_params(day,page)
                or receipt.get('sha256')!=sources.sha(raw) or receipt.get('tls_verify') is not True
                or stamp.tzinfo is None or not close<=stamp<=datetime.now(timezone.utc)):
            raise ValueError('Boundary source query, TLS or original observation clock differs')
        response=json.loads(raw)
        if response.get('success') is not True:raise ValueError('Boundary source rejected its query')
        payload=response['result'];count=payload['count'];pages=payload['pages'];part=payload['data']
        if (type(count) is not int or type(pages) is not int or count<1 or not 1<=pages<=10
                or pages!=(count+4999)//5000 or not isinstance(part,list) or not part
                or len(part)>5000 or (page<pages and len(part)!=5000)):
            raise ValueError('Boundary source pagination is incomplete')
        if expected is None:expected=(count,pages)
        if expected!=(count,pages):raise ValueError('Boundary source pagination changed')
        for row in part:
            s=row['SECURITY_CODE'];venue=('BJ' if str(s).startswith(('4','8','920')) else
                'SH' if str(s).startswith(('6','9')) else 'SZ' if str(s).startswith(('0','2','3')) else None)
            if (not isinstance(s,str) or re.fullmatch('[0-9]{6}',s) is None or venue is None
                    or row.get('SECUCODE')!=s+'.'+venue or row.get('TRADE_DATE') not in (day,day+' 00:00:00')):
                raise ValueError('Boundary source symbol, venue or session differs')
        rows.extend(part)
        if page==pages:break
        page+=1
    codes=[r['SECURITY_CODE'] for r in rows]
    if len(codes)!=expected[0] or len(set(codes))!=len(codes) or codes!=sorted(codes):
        raise ValueError('Boundary source has missing, duplicate or unordered issuers')
    if set(files)!={Path(n).name for n in stable}:raise ValueError('Boundary archive has undeclared page roles')
    return rows,stable


def _positive(value):
    result=Decimal(str(value))
    if not result.is_finite() or result<=0:raise ValueError('Invalid boundary capitalization input')
    return result


def st_exclusions(root,base,symbols,day,authority,witness_dir,*,materialize=False):
    """Source-bounded current-day exclusion, not invented issuer metadata."""
    manifest=json.loads(sources.bound(root,authority[0]).read_bytes())
    if sources.sha(sources.bound(root,authority[0]).read_bytes())!=authority[1]:
        raise ValueError('Boundary notice archive identity changed')
    closure=source_closure(root,*authority)
    closure.pop('.microcap_index_cache/st_notice_archive_pointer.json',None)
    scope=json.loads(read_bound(root,manifest['scope']));data=validate_scope(root,scope)
    registry,_=_registry(root);rows={s:normalized_risk_rows(data,s) for s in symbols}
    cursor=pd.Timestamp(data['end_date'])+pd.Timedelta(days=1)
    for part in manifest.get('daily_increments',[]):
        if part['date']!=str(cursor.date()):raise ValueError('Boundary notice dates are not contiguous')
        read_bound(root,part['manifest']);folder=sources.bound(root,part['manifest']['path']).parent
        verified=verify_day(folder,part['date'])
        if part['date']<=day:
            for symbol in symbols:
                for item in verified['by_symbol'].get(symbol,[]):
                    row=_daily_notice_row(item,symbol,part['date'],registry)
                    if row:rows[symbol].append(row)
        cursor+=pd.Timedelta(days=1)
    if cursor<=pd.Timestamp(day):raise ValueError('Boundary notice authority does not reach its cap date')
    excluded={}
    for symbol in symbols:
        notices=pd.DataFrame([r for r in rows[symbol] if r['notice_date']<=day])
        events=load_verified_events(root,symbol,data['start_date'],day,notices)
        intervals=base.freq_mod.build_st_intervals_from_notices(pd.Timestamp(data['start_date']),
            pd.Timestamp(day),notices,verified_events=events)
        if bool(base.freq_mod.build_st_status_series({'st_intervals':intervals},pd.DatetimeIndex([day])).iloc[0]):
            excluded[symbol]=dict(reason='native_PIT_ST',intervals=intervals)
        else:
            relative=witness_dir+'/'+symbol+'.json'
            path=sources.bound(root,relative)
            if not path.is_file() and materialize:
                current=sources.bound(root,'.microcap_index_cache/security_meta/'+symbol+'.json')
                if current.is_file():sources.atomic(path,current.read_bytes())
            if path.is_file():
                raw=path.read_bytes();closure[relative]=sources.sha(raw)
                if not bool(base.freq_mod.build_active_status_series(json.loads(raw),pd.DatetimeIndex([day])).iloc[0]):
                    excluded[symbol]=dict(reason='native_lifecycle')
    return excluded,closure


def verify(root,base,day,caps,scope,spec,authority,*,materialize=False):
    rows,files=read_snapshot(root,spec,day);by_symbol={r['SECURITY_CODE']:r for r in rows}
    master=base.freq_mod.load_security_master()
    listed=pd.to_datetime(master.list_date,errors='raise')
    delisted=pd.to_datetime(master.delist_date,errors='raise')
    minimum=int(((listed<=pd.Timestamp(day))&(delisted.isna()|(delisted>pd.Timestamp(day)))).sum())
    if len(rows)<minimum:
        raise ValueError('Whole-market boundary coverage is smaller than the native active master')
    ranked=base.build_live_target_members_map({pd.Timestamp(day):caps},pd.DatetimeIndex([day]),{},top_n=100)[pd.Timestamp(day)]
    threshold=(_positive(caps[ranked[-1]]),ranked[-1]);outside=[]
    for symbol,value in caps.items():
        row=by_symbol.get(symbol)
        if row is None or abs(_positive(value)-_positive(row['TOTAL_MARKET_CAP']))>Decimal('0.01'):
            raise ValueError('Native capitalization differs from dated whole-market source: '+symbol)
    for row in rows:
        symbol=row['SECURITY_CODE']
        if row['SECUCODE'].endswith('.BJ') or symbol.startswith(('200','900')):continue
        shares=_positive(row['TOTAL_SHARES']);close=_positive(row['CLOSE_PRICE']);cap=_positive(row['TOTAL_MARKET_CAP'])
        if shares!=shares.to_integral_value() or abs(shares*close-cap)>Decimal('0.01'):
            raise ValueError('Whole-market raw-price/share capitalization identity differs: '+symbol)
        if symbol not in set(scope) and (cap,symbol)<=threshold:outside.append(symbol)
    excluded,closure=st_exclusions(root,base,outside,day,authority,spec['directory']+'/lifecycle',
                                  materialize=materialize) if outside else ({},{})
    escaped=sorted(set(outside)-set(excluded))
    if escaped:
        raise ValueError('Recall potentially eligible issuers into the 300-stock roster: '+','.join(escaped))
    return dict(status='native_scope_boundary_passed',date=day,scope_count=300,
        source_rows=len(rows),outside_exclusions=excluded,top100=ranked,authority=list(authority),
        snapshot=spec,source_files={**files,**closure})
