"""Isolated whole-symbol ratio-adjusted return candidates, never formal caches.

Default is a plan. --execute requires explicit --symbols or --all. Provider
factors are research evidence, not a corporate-action completeness certificate.
"""
from pathlib import Path
import argparse,concurrent.futures,json,hashlib,shutil,time,threading,datetime
import numpy as np
import pandas as pd
import requests

BASE=Path(__file__).resolve().parent
ROOT=BASE.parents[1]
OUT=BASE/'return_basis_candidate_20261030'
CUTOFF='2026-10-29'
CALENDAR=BASE/'cross_source_prices/missing_session_review/exchange_sessions_20261028.json'
MASTER=BASE/'cross_source_official/security_master_combined_candidate.csv'
OLD=ROOT/'research_reports/20261029_v23_no_overheat_promotion/audit_sync/isolated_lineage_replay/runtime/.microcap_index_cache/prices_raw'
SCOPE=BASE/'replay/frozen4975_parallel_diagnostic/scope.json'
REUSE=BASE/'cross_source_prices/factor_probe'
STOP=threading.Event()
GENERATOR_SHA256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def dump(p,x):
    p=Path(p);p.parent.mkdir(parents=True,exist_ok=True)
    tmp=p.with_suffix(p.suffix+'.tmp');tmp.write_text(json.dumps(x,ensure_ascii=False,indent=2,allow_nan=False),encoding='utf-8');tmp.replace(p)
def targets():
    old=json.loads(SCOPE.read_text(encoding='utf-8'))['universe']
    result={s:OLD/f'{s}.csv' for s in old}
    extra={p.stem:p for p in (BASE/'cross_source_prices/normalized_raw').glob('*.csv')}
    extra['688287']=BASE/'incremental_688287/prices/688287_raw_candidate.csv'
    assert len(result)==4975 and len(extra)==463 and not set(result)&set(extra)
    result.update(extra)
    assert all(p.exists() for p in result.values())
    return result
def get_factor(symbol,kind):
    folder=OUT/'sources'/symbol;folder.mkdir(parents=True,exist_ok=True)
    path=folder/f'{kind}.js';receipt=folder/f'{kind}.json'
    market=('sh' if symbol.startswith('6') else 'sz')+symbol
    url=f'https://finance.sina.com.cn/realstock/company/{market}/{kind}.js'
    if receipt.exists():
        evidence=json.loads(receipt.read_text(encoding='utf-8'))
        if evidence.get('status')!=200:raise ValueError('previous_factor_fetch_failed_no_retry')
        assert path.exists() and sha(path)==evidence['sha256'] and evidence['url']==url
    elif (REUSE/f'{symbol}_{kind}.js').exists():
        old=REUSE/f'{symbol}_{kind}.js';old_receipt=json.loads(old.with_suffix('.json').read_text(encoding='utf-8'))
        assert old_receipt['status']==200 and old_receipt['url']==url and sha(old)==old_receipt['sha256']
        shutil.copy2(old,path)
        evidence=dict(old_receipt,reused_from=str(old));dump(receipt,evidence)
    else:
        if STOP.is_set() or (OUT/'RATE_LIMIT_STOP.json').exists():raise RuntimeError('group_rate_limit_stop')
        time.sleep(.25)
        try:
            response=requests.get(url,timeout=15)
            path.write_bytes(response.content)
            evidence={'url':url,'status':response.status_code,'sha256':sha(path),'retrieved_at_utc':datetime.datetime.now(datetime.timezone.utc).isoformat()}
            dump(receipt,evidence)
            if response.status_code in (403,429):
                STOP.set();dump(OUT/'RATE_LIMIT_STOP.json',evidence)
            response.raise_for_status()
        except Exception as exc:
            if not receipt.exists():dump(receipt,{'url':url,'status':'transport_error','error':repr(exc)})
            raise
    text=path.read_text(encoding='utf-8')
    if not text.startswith(f'var {market}{kind}='):raise ValueError('factor_response_identity_mismatch')
    obj,_=json.JSONDecoder().raw_decode(text.split('=',1)[1].lstrip())
    data=obj.get('data')
    if not isinstance(data,list) or not data:raise ValueError('empty_factor_history_not_proof_of_no_actions')
    if int(obj['total'])!=len(data):raise ValueError('factor_count_mismatch')
    f=pd.DataFrame(data)[['d','f']].rename(columns={'d':'date','f':kind+'_factor'})
    f.date=pd.to_datetime(f.date,errors='raise');f[kind+'_factor']=pd.to_numeric(f[kind+'_factor'],errors='raise')
    if f.date.duplicated().any() or not np.isfinite(f[kind+'_factor']).all() or not f[kind+'_factor'].gt(0).all():raise ValueError('invalid_factor_history')
    return f.sort_values('date'),sha(path)
def build(symbol,path,calendar,master):
    result_path=OUT/'results'/f'{symbol}.json'
    if result_path.exists():
        existing=json.loads(result_path.read_text(encoding='utf-8'))
        if existing.get('cutoff')==CUTOFF and existing.get('raw_sha256')==sha(path):return existing
        archive=OUT/'prior_results'/existing.get('cutoff','unknown')/result_path.name
        if not archive.exists():dump(archive,existing)
    result={'symbol':symbol,'formal_approved':False,'return_basis':'ratio_adjusted_ex_reference_price_return',
            'cash_dividend_wealth_accounting':False,'cutoff':CUTOFF,'raw_path':str(path),'raw_sha256':sha(path),
            'generator_source_sha256':GENERATOR_SHA256,'calendar_sha256':sha(CALENDAR),'master_sha256':sha(MASTER),
            'latest_target_freshness_certified':False,'raw_tail_refresh':'coordinated_separately_not_claimed_here'}
    try:
        raw=pd.read_csv(path,parse_dates=['date'])[['date','close_raw']]
        if raw.date.isna().any() or raw.date.duplicated().any() or not np.isfinite(raw.close_raw).all() or not raw.close_raw.gt(0).all():raise ValueError('invalid_raw_history')
        raw=raw.sort_values('date');row=master.loc[symbol]
        legal=raw.date.le(CUTOFF)&raw.date.ge(row.list_date)&raw.date.isin(calendar)
        if row.delist_date:legal&=raw.date.lt(row.delist_date)
        result['excluded_raw_dates']=raw.loc[~legal,'date'].dt.strftime('%Y-%m-%d').tolist()
        result['raw_provider_rows']=len(raw)
        raw=raw.loc[legal].copy()
        if raw.empty:raise ValueError('no_legal_sample_rows')
        q,qsha=get_factor(symbol,'qfq');h,hsha=get_factor(symbol,'hfq')
        result.update(qfq_factor_sha256=qsha,hfq_factor_sha256=hsha,qfq_factor_rows=len(q),hfq_factor_rows=len(h),
                      factor_rows_after_cutoff=int(q.date.gt(CUTOFF).sum()+h.date.gt(CUTOFF).sum()))
        q=q.loc[q.date.le(CUTOFF)];h=h.loc[h.date.le(CUTOFF)]
        f=pd.merge_asof(raw,q,on='date',direction='backward');f=pd.merge_asof(f,h,on='date',direction='backward')
        if f[['qfq_factor','hfq_factor']].isna().any().any():raise ValueError('missing_factor_anchor')
        hfq=f.close_raw*f.hfq_factor
        qfq=f.close_raw/f.qfq_factor
        qret=qfq.pct_change(fill_method=None);hret=hfq.pct_change(fill_method=None)
        error=float((qret-hret).abs().max()) if len(f)>1 else 0.
        result['qfq_hfq_return_max_abs_difference']=error
        if error>1e-9:raise ValueError('qfq_hfq_factor_basis_inconsistent')
        f['close_adj']=hfq/f.hfq_factor.iloc[0]
        if not np.isfinite(f.close_adj).all() or not f.close_adj.gt(0).all():raise ValueError('invalid_candidate_adjusted_close')
        # As-of truncation excludes later actions, and same-base rebasing leaves
        # the entire shared prefix unchanged. This is not vendor PIT certification.
        prefix=f.iloc[:max(2,len(f)//2)].copy()
        prefix_h=h.loc[h.date.le(prefix.date.iloc[-1])]
        p=pd.merge_asof(prefix[['date','close_raw']],prefix_h,on='date',direction='backward')
        prefix_close=p.close_raw*p.hfq_factor/p.hfq_factor.iloc[0]
        invariant=float(np.max(np.abs(prefix_close.to_numpy()-f.close_adj.iloc[:len(prefix)].to_numpy())))
        if invariant>1e-10:raise ValueError('fixed_anchor_prefix_invariance_failed')
        const_error=float((f.close_adj.pct_change(fill_method=None)-(f.close_adj*7.3).pct_change(fill_method=None)).abs().max()) if len(f)>1 else 0.
        result.update(fixed_anchor_date=str(f.date.iloc[0].date()),prefix_invariance_max_abs_difference=invariant,
                      constant_rebase_return_max_abs_difference=const_error)
        dest=OUT/'prices_adj'/f'{symbol}.csv';dest.parent.mkdir(exist_ok=True)
        f[['date','close_adj']].to_csv(dest,index=False)
        jumps=f[['date','close_raw','qfq_factor','hfq_factor']].copy()
        jumps['factor_step']=jumps.hfq_factor/jumps.hfq_factor.shift()
        action_path=OUT/'factor_steps'/f'{symbol}.csv';action_path.parent.mkdir(exist_ok=True)
        jumps.loc[jumps.factor_step.notna()&jumps.factor_step.sub(1).abs().gt(1e-12)].to_csv(action_path,index=False)
        result.update(status='candidate_generated_not_certified',rows=len(f),first=str(f.date.min().date()),last=str(f.date.max().date()),
                      candidate_sha256=sha(dest),factor_steps_sha256=sha(action_path),factor_step_count=int(len(pd.read_csv(action_path))),
                      corporate_action_completeness_certified=False,vendor_point_in_time_certified=False)
    except Exception as exc:result.update(status='blocked',error=type(exc).__name__+': '+str(exc))
    dump(result_path,result)
    return result
def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--execute',action='store_true');parser.add_argument('--all',action='store_true');parser.add_argument('--symbols',nargs='*')
    args=parser.parse_args();universe=targets()
    selected=sorted(universe) if args.all else args.symbols or []
    assert set(selected)<=set(universe)
    plan={'universe_with_raw':len(universe),'old_scope':4975,'new_raw':463,'selected':selected,'execute':args.execute,
          'no_sample_trade_symbols':['000787','000805','600003'],'cutoff':CUTOFF,'formal_approved':False,
          'generator_source_sha256':GENERATOR_SHA256,'calendar_sha256':sha(CALENDAR),'master_sha256':sha(MASTER)}
    print(json.dumps(plan,ensure_ascii=False),flush=True)
    if not args.execute:return
    assert selected,'Explicit scope required'
    OUT.mkdir(exist_ok=True);dump(OUT/'last_plan.json',plan)
    cal=pd.to_datetime(json.loads(CALENDAR.read_text()))
    master=pd.read_csv(MASTER,dtype=str).fillna('').set_index('symbol')
    assert set(selected)<=set(master.index)
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        futures=[pool.submit(build,s,universe[s],cal,master) for s in selected]
        for n,future in enumerate(concurrent.futures.as_completed(futures),1):
            r=future.result()
            print(json.dumps({'done':n,'total':len(selected),'symbol':r['symbol'],'status':r['status'],'error':r.get('error')},ensure_ascii=False),flush=True)
    all_results=[json.loads(p.read_text(encoding='utf-8')) for p in sorted((OUT/'results').glob('*.json'))]
    dump(OUT/'summary.json',{'formal_approved':False,'cutoff':CUTOFF,'scope_total':5438,'completed_records':len(all_results),
                            'status_counts':{s:sum(r['status']==s for r in all_results) for s in sorted({r['status'] for r in all_results})},
                            'results':all_results})
if __name__=='__main__':main()
