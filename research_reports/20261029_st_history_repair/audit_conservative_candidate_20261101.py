"""Read-only audit of the completed conservative candidate; no loader or replay."""
import json,hashlib,argparse
from pathlib import Path
import pandas as pd
import numpy as np

def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def main():
 p=argparse.ArgumentParser();p.add_argument('--candidate',type=Path,required=True);p.add_argument('--receipt',type=Path,required=True);a=p.parse_args();d=a.candidate.resolve();root=d.parent
 r=json.loads((d/'diagnostic_result.json').read_bytes());meta=json.loads((d/'proxy_meta.json').read_bytes());checks={}
 assert sha(d/'proxy_meta.json')==r['proxy_meta.json']['sha256']
 assert meta['delisting_valuation_policy']==meta['core_params']['delisting_valuation_policy']=='delist_zero_writeoff_equal_survivors_v1'
 events=pd.DataFrame(meta['delisting_writeoff_events']);assert not events.empty
 assert events.symbol.is_unique and not events[['symbol','date']].duplicated().any()
 assert events.security_return.eq(-1).all() and events.is_trade.eq(False).all() and events.sell_count.eq(0).all() and events.transaction_cost.eq(0).all()
 assert np.allclose(events.portfolio_return_contribution,-events.writeoff_weight,rtol=0,atol=1e-15)
 calendar=pd.DatetimeIndex(pd.to_datetime(pd.read_csv(d/'proxy.csv').date))
 for row in events.itertuples():assert pd.Timestamp(row.date)==calendar[calendar>=pd.Timestamp(row.delist_date)].min()
 checks['writeoff_unique_nontrade_zero_fee']=len(events)
 members=meta['terminal_effective_members'];assert len(members)==len(set(members))==100
 master=pd.read_csv(root/'.microcap_index_cache/security_master.csv',dtype={'symbol':str}).set_index('symbol')
 for sym in members:
  assert sym in master.index
  date=pd.to_datetime(master.loc[sym,'delist_date'],errors='coerce');assert pd.isna(date) or date>pd.Timestamp(r['as_of'])
 st=pd.read_csv(root/'.microcap_index_cache/current_st.csv',dtype=str);code='symbol' if 'symbol' in st else 'code';assert not set(members)&set(st[code].str.zfill(6))
 checks['terminal_members']={'unique':100,'delisted':0,'current_st_overlap':0}
 streams={}
 for name in ['v2_0','v2_3','v2_5']:
  f=pd.read_csv(d/(name+'.csv'));assert sha(d/(name+'.csv'))==r['streams'][name]['sha256'];dates=pd.DatetimeIndex(pd.to_datetime(f.date));assert dates.is_unique and dates.is_monotonic_increasing and dates.max()==pd.Timestamp(r['as_of'])
  assert dates.equals(calendar[(calendar>=dates.min())&(calendar<=dates.max())]);assert np.isfinite(f[['return_net','nav_net']].to_numpy()).all();assert np.allclose(f.nav_net,(1+f.return_net).cumprod(),rtol=1e-9,atol=1e-9)
  streams[name]={'rows':len(f),'latest':str(dates.max().date()),'sha256':sha(d/(name+'.csv'))}
 report={'status':'PASS','scope':'read_only_candidate_audit_not_formal_delivery','source_manifest_sha256':r['source_manifest_sha256'],'candidate_result_sha256':sha(d/'diagnostic_result.json'),'checks':checks,'streams':streams,'input_identity':{str(p.relative_to(root)):sha(p) for p in [d/'proxy_meta.json',root/'.microcap_index_cache/security_master.csv',root/'.microcap_index_cache/current_st.csv']}}
 if a.receipt.exists():raise RuntimeError('Receipt exists; do not overwrite')
 a.receipt.write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf8');print(json.dumps(report,ensure_ascii=False))
if __name__=='__main__':main()
