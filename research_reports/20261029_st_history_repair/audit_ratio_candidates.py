"""Independent offline exhaustive hash/calendar/basis audit of completed candidates."""
import collections,hashlib,json,math
from pathlib import Path
import numpy as np
import pandas as pd
B=Path(__file__).resolve().parent;O=B/'return_basis_candidate_20261030'
def read(p):return json.loads(p.read_text(encoding='utf-8'))
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def audit():
 plan=read(O/'last_plan.json');expected=set(plan['selected']);calendar=set(read(B/'cross_source_prices/missing_session_review/exchange_sessions_20261028.json'))
 master=pd.read_csv(B/'cross_source_official/security_master_combined_candidate.csv',dtype=str).fillna('').set_index('symbol')
 results=[];issues=[];receipts=0;source_hashes=0
 for p in sorted((O/'sources').glob('*/*.json')):
  try:
   r=read(p);receipts+=1;body=p.with_suffix('.js')
   if 'sha256' in r:
    assert body.exists() and sha(body)==r['sha256'],'source hash mismatch'
    source_hashes+=1
  except Exception as e:issues.append(dict(path=str(p),error=str(e)))
 for p in sorted((O/'results').glob('*.json')):
  r=read(p);s=r['symbol'];row=dict(symbol=s,status=r['status'],cutoff=r['cutoff'],errors=[])
  try:
   assert sha(r['raw_path'])==r['raw_sha256'],'raw hash mismatch'
   assert r['cutoff']==plan['cutoff'],'stale cutoff'
   assert r['calendar_sha256']==plan['calendar_sha256'],'calendar lineage mismatch'
   assert r['master_sha256']==plan['master_sha256'],'master lineage mismatch'
   if r['status']=='candidate_generated_not_certified':
    dest=O/'prices_adj'/f'{s}.csv';assert sha(dest)==r['candidate_sha256'],'candidate hash mismatch'
    assert sha(O/'factor_steps'/f'{s}.csv')==r['factor_steps_sha256'],'factor steps hash mismatch'
    f=pd.read_csv(dest);assert len(f)==r['rows'] and f.date.is_unique and f.date.is_monotonic_increasing,'rows/order invalid'
    assert np.isfinite(f.close_adj).all() and f.close_adj.gt(0).all(),'nonpositive/nonfinite'
    assert set(f.date)<=calendar,'non-session candidate'
    m=master.loc[s];assert f.date.ge(m.list_date).all(),'prelisting'
    assert not m.delist_date or f.date.lt(m.delist_date).all(),'postdelisting'
    assert f.date.le(plan['cutoff']).all(),'future candidate'
    raw=pd.read_csv(r['raw_path']);legal=raw.date.isin(calendar)&raw.date.ge(m.list_date)&raw.date.le(plan['cutoff'])
    if m.delist_date:legal &=raw.date.lt(m.delist_date)
    raw=raw.loc[legal].sort_values('date');assert list(raw.date)==list(f.date),'coverage mismatch'
    values={}
    for kind in ('qfq','hfq'):
     src=O/'sources'/s/f'{kind}.js';receipt=read(src.with_suffix('.json'))
     assert receipt['status']==200 and sha(src)==r[kind+'_factor_sha256'],'factor provenance mismatch'
     text=src.read_text(encoding='utf-8');market=('sh' if s.startswith('6') else 'sz')+s
     assert text.startswith(f'var {market}{kind}='),'identity mismatch'
     obj,_=json.JSONDecoder().raw_decode(text.split('=',1)[1].lstrip());data=obj['data']
     assert int(obj['total'])==len(data) and data,'factor count mismatch'
     d=pd.DataFrame(data).rename(columns={'d':'date','f':'factor'});d.factor=pd.to_numeric(d.factor)
     assert d.date.is_unique and np.isfinite(d.factor).all() and d.factor.gt(0).all(),'bad factor'
     d=d.loc[d.date.le(plan['cutoff'])].sort_values('date');raw2=raw[['date','close_raw']].copy();raw2.date=pd.to_datetime(raw2.date);d.date=pd.to_datetime(d.date)
     matched=pd.merge_asof(raw2,d,on='date',direction='backward');assert matched.factor.notna().all(),'uncovered factor'
     values[kind]=matched.close_raw*matched.factor if kind=='hfq' else matched.close_raw/matched.factor
     if kind=='hfq':candidate=values[kind]/matched.factor.iloc[0]
    assert np.allclose(candidate.to_numpy(),f.close_adj.to_numpy(),rtol=1e-13,atol=1e-12),'reconstruction mismatch'
    err=(values['qfq'].pct_change(fill_method=None)-values['hfq'].pct_change(fill_method=None)).abs().max()
    assert pd.isna(err) or err<=1e-9,'basis inconsistent'
    row.update(rows=len(f),first=f.date.iloc[0],last=f.date.iloc[-1],q_h_return_error=0 if pd.isna(err) else float(err))
   else:row['blocked_reason']=r.get('error')
  except Exception as e:row['errors'].append(str(e))
  results.append(row)
 missing=sorted(expected-{r['symbol'] for r in results});bad=[r for r in results if r['errors']]
 out=dict(formal_approved=False,full_scope=len(expected),records=len(results),missing=missing,unexpected=sorted({r['symbol'] for r in results}-expected),status_counts=dict(collections.Counter(r['status'] for r in results)),receipt_count=receipts,source_hash_count=source_hashes,source_issues=issues,record_issues=bad,source_and_reconstruction_audit_pass=not(missing or issues or bad),all_candidates_available=not(missing or issues or bad or any(r['status']=='blocked' for r in results)),results=results)
 (O/'independent_audit.json').write_text(json.dumps(out,ensure_ascii=False,indent=2),encoding='utf-8')
 print(json.dumps({k:v for k,v in out.items() if k not in ('results','missing')},ensure_ascii=False))
if __name__=='__main__':audit()
