"""Independent full readback of normalized-raw candidates and original source receipts."""
from pathlib import Path
import json,hashlib,datetime
import numpy as np
import pandas as pd
B=Path(__file__).resolve().parent;O=B/'fresh_ratio_candidate_20261029';S=B/'return_basis_candidate_20261030/sources'
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
mapping=json.loads((B/'raw_install_normalized_20261029/install_manifest.json').read_text(encoding='utf-8'))
master=pd.read_csv(B/'cross_source_official/security_master_combined_candidate.csv',dtype=str).fillna('').set_index('symbol');calendar=set(json.loads((B/'cross_source_prices/missing_session_review/exchange_sessions_20261028.json').read_text()))
results=[];close=datetime.datetime.fromisoformat('2026-10-29T15:00:00+08:00')
for r in mapping['records']:
 s=r['symbol'];record=dict(symbol=s,status='passed',errors=[])
 try:
  rawpath=Path(r['candidate_path']);assert sha(rawpath)==r['candidate_sha256'],'raw hash mismatch'
  p=O/'prices_adj'/f'{s}.csv';m=json.loads((O/'manifests'/f'{s}.json').read_text());assert sha(p)==m['candidate_sha256'],'candidate hash mismatch';assert m['raw_sha256']==r['candidate_sha256'],'wrong raw layer'
  assert m['as_of']=='2026-10-29' and m['list_date']==master.loc[s,'list_date'],'wrong asof/master'
  assert m['sessions_sha256']==hashlib.sha256('\n'.join(sorted(d for d in calendar if d<=m['as_of'])).encode()).hexdigest(),'calendar scope hash mismatch'
  assert (m['delist_date'] or '')==master.loc[s,'delist_date'],'wrong delist boundary'
  raw=pd.read_csv(rawpath);legal=raw.date.isin(calendar)&raw.date.ge(m['list_date'])&raw.date.le(m['as_of'])
  if m['delist_date']:legal &= raw.date.lt(m['delist_date'])
  raw=raw.loc[legal].sort_values('date');f=pd.read_csv(p)
  assert len(f)==m['rows'] and f.date.is_unique and f.date.is_monotonic_increasing and list(raw.date)==list(f.date),'coverage/calendar/order mismatch'
  assert np.isfinite(f.close_adj).all() and f.close_adj.gt(0).all(),'invalid adjusted prices'
  values={}
  for kind in ['qfq','hfq']:
   src=S/s/(kind+'.js');receipt=json.loads(src.with_suffix('.json').read_text());market=('sh' if s.startswith('6') else 'sz')+s
   assert receipt['status']==200 and sha(src)==receipt['sha256']==m[kind+'_sha256'],'source hash/status mismatch'
   assert receipt['url']==f'https://finance.sina.com.cn/realstock/company/{market}/{kind}.js','source URL mismatch'
   timestamp=datetime.datetime.fromisoformat(receipt['retrieved_at_utc'].replace('Z','+00:00'));assert close<=timestamp<=datetime.datetime.now(datetime.timezone.utc),'invalid source freshness'
   text=src.read_text();prefix=f'var {market}{kind}=';assert text.startswith(prefix),'source identity mismatch'
   data,_=json.JSONDecoder().raw_decode(text[len(prefix):]);assert len(data['data'])==int(data['total']) and data['data'],'factor count mismatch'
   factors=pd.DataFrame(data['data']).rename(columns={'d':'date','f':'factor'});factors.factor=pd.to_numeric(factors.factor);assert factors.date.is_unique and factors.factor.gt(0).all() and np.isfinite(factors.factor).all(),'factor invalid'
   factors=factors[factors.date.le(m['as_of'])].sort_values('date');factors.date=pd.to_datetime(factors.date);rr=raw.copy();rr.date=pd.to_datetime(rr.date)
   joined=pd.merge_asof(rr,factors,on='date',direction='backward');assert joined.factor.notna().all(),'factor coverage missing'
   values[kind]=joined.close_raw*joined.factor if kind=='hfq' else joined.close_raw/joined.factor
   if kind=='hfq':expected=values[kind]/joined.loc[joined.date.eq(pd.Timestamp(m['anchor_date'])),'factor'].iloc[0]
  assert np.allclose(expected,f.close_adj,rtol=1e-13,atol=1e-12),'independent adjusted reconstruction mismatch'
  delta=(values['qfq'].pct_change(fill_method=None)-values['hfq'].pct_change(fill_method=None)).abs().max();assert pd.isna(delta) or delta<=1e-9,'dual-factor returns disagree'
  record.update(rows=len(f),raw_sha256=sha(rawpath),candidate_sha256=sha(p),last=f.date.iloc[-1])
 except Exception as e:record.update(status='failed',errors=[repr(e)])
 results.append(record)
 if len(results)%500==0:print(len(results),flush=True)
out=dict(formal_approved=False,formal_written=False,total=len(results),passed=sum(r['status']=='passed' for r in results),failures=[r for r in results if r['status']!='passed'],results=results,source_mapping_sha256=sha(B/'raw_install_normalized_20261029/install_manifest.json'),master_sha256=sha(B/'cross_source_official/security_master_combined_candidate.csv'))
out['helper_sha256']=sha(B.parents[1]/'scripts/stock_return_basis.py');out['audit_script_sha256']=sha(__file__)
(O/'independent_audit.json').write_text(json.dumps(out,ensure_ascii=False,indent=2),encoding='utf-8');print(out['total'],out['passed'],len(out['failures']))
