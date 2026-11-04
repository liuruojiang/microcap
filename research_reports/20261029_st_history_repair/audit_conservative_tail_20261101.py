"""Independent read-only tail acceptance; no production loader or network."""
import argparse,json,hashlib
from pathlib import Path
import numpy as np
import pandas as pd

def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def main():
 p=argparse.ArgumentParser();p.add_argument('--candidate',type=Path,required=True);p.add_argument('--previous',type=Path,required=True);p.add_argument('--receipt',type=Path,required=True);a=p.parse_args();d=a.candidate.resolve();old=a.previous.resolve();root=d.parent;r=json.loads((d/'diagnostic_result.json').read_bytes());assert r['as_of']=='2026-10-30';assert not r['approved'] and not r['formal_written']
 checks={};identities={}
 for name in ['proxy_meta.json','proxy.csv','turnover.csv','base_costed.csv','effective_members.csv']:
  assert sha(d/name)==r[name]['sha256'];identities[name]=sha(d/name)
 for name,h in r['source_candidate_hashes'].items():assert sha(old/name)==h
 panel=Path(r['panel']['path']);assert sha(panel)==r['panel']['sha256'];assert len(pd.read_csv(panel))==r['panel']['rows']
 cal=Path(r['calendar_binding']['path']);assert sha(cal)==r['calendar_sha256']==r['calendar_binding']['sha256'];dates=pd.DatetimeIndex(pd.to_datetime(pd.read_csv(cal).date))
 meta=json.loads((d/'proxy_meta.json').read_bytes());members=pd.read_csv(d/'effective_members.csv',dtype={'symbol':str});symbols=members.symbol.tolist();assert len(symbols)==len(set(symbols))==100 and members.as_of_date.eq('2026-10-30').all();assert meta['terminal_effective_members']==symbols and meta['terminal_effective_as_of']=='2026-10-30'
 assert meta['delisting_valuation_policy']==meta['core_params']['delisting_valuation_policy']=='delist_zero_writeoff_equal_survivors_v1'
 master=pd.read_csv(root/'.microcap_index_cache/security_master.csv',dtype={'symbol':str}).set_index('symbol');st=pd.read_csv(root/'.microcap_index_cache/current_st.csv',dtype=str);assert not set(symbols)&set(st.code.str.zfill(6))
 for sym in symbols:
  assert sym in master.index;end=pd.to_datetime(master.loc[sym,'delist_date'],errors='coerce');assert pd.isna(end) or end>pd.Timestamp('2026-10-30')
 checks['actual_members']={'unique':100,'current_st_overlap':0,'delisted':0}
 tails=pd.read_csv(d/'member_tail_returns.csv',index_col='date');assert tails.index.tolist()==['2026-10-29','2026-10-30'] and set(tails.columns)==set(symbols);assert tails.loc['2026-10-29'].eq(0).all();assert np.isfinite(tails.to_numpy()).all()
 source_returns={};source_hashes={};basis=root/'.microcap_index_cache/return_basis'
 for sym in symbols:
  mp=basis/'manifests'/(sym+'.json');m=json.loads(mp.read_bytes());adj=basis/'prices_adj'/(sym+'.csv');raw=root/'.microcap_index_cache/prices_raw'/(sym+'.csv');assert m['as_of']=='2026-10-30' and sha(adj)==m['candidate_sha256'] and sha(raw)==m['raw_sha256']
  frame=pd.read_csv(adj,index_col='date');assert frame.index.is_unique and frame.index.is_monotonic_increasing
  # All tail members must have explicit two-session prices for this independent check.
  assert {'2026-10-29','2026-10-30'}<=set(frame.index),sym+' requires suspension-specific independent adjudication'
  ret=float(frame.loc['2026-10-30','close_adj']/frame.loc['2026-10-29','close_adj']-1);assert np.isclose(ret,tails.loc['2026-10-30',sym],rtol=1e-12,atol=1e-12);source_returns[sym]=ret;source_hashes[sym]={'manifest':sha(mp),'adjusted':sha(adj),'raw':sha(raw)}
 proxy=pd.read_csv(d/'proxy.csv',index_col='date');prior=pd.read_csv(old/'proxy.csv',index_col='date');assert proxy.index[:-1].equals(prior.index) and proxy.index[-1]=='2026-10-30'
 for col in ['close','daily_return']:assert np.allclose(proxy.loc[prior.index,col],prior[col],rtol=1e-12,atol=1e-12)
 mean=float(np.mean(list(source_returns.values())));assert np.isclose(proxy.iloc[-1].daily_return,mean,rtol=1e-12,atol=1e-12);assert np.isclose(proxy.iloc[-1].close,prior.iloc[-1].close*(1+mean),rtol=1e-12,atol=1e-12)
 checks['tail']={'members_independently_verified':100,'mean_return':mean,'proxy_return':float(proxy.iloc[-1].daily_return),'previous_proxy_prefix_unchanged':True}
 streams={}
 for name in ['v2_0','v2_3','v2_5']:
  path=d/(name+'.csv');assert sha(path)==r['streams'][name]['sha256'];f=pd.read_csv(path,index_col='date');o=pd.read_csv(old/(name+'.csv'),index_col='date');assert f.index[:-1].equals(o.index);idx=pd.DatetimeIndex(pd.to_datetime(f.index));assert idx.equals(dates[(dates>=idx.min())&(dates<=pd.Timestamp('2026-10-30'))]);assert np.isfinite(f[['return_net','nav_net']].to_numpy()).all()
  delta={c:float(np.max(np.abs(f.loc[o.index,c]-o[c]))) for c in ['return_net','nav_net']};assert all(x<1e-12 for x in delta.values());assert np.allclose(f.nav_net,(1+f.return_net).cumprod(),rtol=1e-9,atol=1e-9);streams[name]={'rows':len(f),'latest':f.index[-1],'sha256':sha(path),'prefix_max_abs_difference':delta}
 report={'status':'PASS','scope':'independent_readonly_tail_audit_not_formal_delivery','candidate_result_sha256':sha(d/'diagnostic_result.json'),'source_manifest_sha256':r['source_manifest_sha256'],'checks':checks,'streams':streams,'artifact_hashes':identities,'member_tail_returns_sha256':sha(d/'member_tail_returns.csv'),'source_hashes':source_hashes}
 assert not a.receipt.exists();a.receipt.write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf8');print(json.dumps({k:v for k,v in report.items() if k!='source_hashes'},ensure_ascii=False))
if __name__=='__main__':main()
