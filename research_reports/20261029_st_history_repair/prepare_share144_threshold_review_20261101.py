"""Conservative affected-issuer screen. Target sets only, never executed NAV certification."""
import argparse,importlib,json,os,sys
from pathlib import Path
import numpy as np
import pandas as pd
from run_three_version_diagnostic_20261101 import digest,isolate_modules,offline_guard,frozen_metadata,bind_frozen_calendar

def threshold_check(selected,baseline_affected,alternative_affected):
 if len(selected)!=100 or len({s for s,c in selected})!=100:raise ValueError('Need100 unique original targets')
 if any(not np.isfinite(c) or c<=0 for s,c in selected):raise ValueError('Invalid original target cap')
 if selected!=sorted(selected,key=lambda x:(x[1],x[0])):raise ValueError('Original ranks disagree with cap/symbol ordering')
 original=dict(selected);cut=(selected[-1][1],selected[-1][0]);safe=True;reasons=[]
 if set(baseline_affected)!=set(alternative_affected):raise ValueError('Scenario symbol mismatch')
 for symbol,old in baseline_affected.items():
  new=alternative_affected[symbol]
  if old is None:
   if symbol in original or new is not None:raise ValueError('Eligibility mismatch')
   continue
  if not np.isfinite(old) or old<=0 or new is None or not np.isfinite(new) or new<=0:raise ValueError('Invalid eligible scenario cap')
  if symbol in original:
   if not np.isclose(old,original[symbol],rtol=1e-12,atol=1e-6):raise ValueError('Actual baseline cap differs from candidate members')
   if (new,symbol)>cut:safe=False;reasons.append(symbol+': selected exceeds old cutoff')
  else:
   if (old,symbol)<=cut:raise ValueError('Original unselected issuer should have ranked inside100')
   if (new,symbol)<=cut:safe=False;reasons.append(symbol+': unselected reaches old cutoff')
 changed=sorted([(s,alternative_affected.get(s,c)) for s,c in selected],key=lambda x:(x[1],x[0]))
 rank_changed=[s for (s,_),(old,_) in zip(changed,selected) if s!=old]
 return dict(target_set_proven_unchanged=safe,requires_full_cross_section=not safe,reasons=reasons,selected_rank_changed=bool(rank_changed),selected_reordered_symbols=[s for s,c in changed],execution_nav_certified=False)

def main():
 p=argparse.ArgumentParser(description=__doc__)
 for name in ('runtime','candidate','calendar','intervals','output'):p.add_argument('--'+name,type=Path,required=True)
 p.add_argument('--calendar-sha256',required=True);p.add_argument('--execute',action='store_true');p.add_argument('--workers',type=int,default=4);a=p.parse_args()
 root=a.runtime.resolve();research=Path(__file__).resolve().parent
 if not root.is_relative_to(research/'replay'):raise ValueError('Research runtime required')
 for name in ('candidate','output'):
  value=getattr(a,name).resolve()
  if not value.is_relative_to(root):raise ValueError('Research output outside runtime')
  setattr(a,name,value)
 a.calendar=a.calendar.resolve();a.intervals=a.intervals.resolve()
 if a.output.exists():raise ValueError('Never overwrite previous screen')
 members=pd.read_csv(a.candidate/'members.csv',dtype={'symbol':str},parse_dates=['rebalance_date']);intervals=pd.read_csv(a.intervals,dtype={'symbol':str},parse_dates=['start_inclusive','end_exclusive'])
 report=json.loads((a.candidate/'diagnostic_result.json').read_text(encoding='utf-8'))
 if report.get('as_of')!='2026-10-29' or report.get('source_manifest_sha256')!=digest(root/'source_manifest.json') or report['members.csv']['sha256']!=digest(a.candidate/'members.csv'):raise ValueError('Candidate identity mismatch')
 if members.empty or not members.groupby('rebalance_date').symbol.agg(['size','nunique']).eq(100).all().all():raise ValueError('Invalid candidate target groups')
 hashes={str(path):digest(path) for path in [a.candidate/'members.csv',a.candidate/'diagnostic_result.json',a.intervals,root/'source_manifest.json',a.calendar]}
 if not a.execute:print('Prepared only: real full-date affected-issuer eligibility required');return
 os.chdir(root);sys.path.insert(0,str(root));sys.dont_write_bytecode=True;os.environ['MPLCONFIGDIR']=str(root/'.mplconfig');os.environ['MPLBACKEND']='Agg';offline_guard(root)
 binding=bind_frozen_calendar(a.calendar,a.calendar_sha256,'2026-10-30')
 v=importlib.import_module('microcap_top100_mom16_biweekly_live_v2_0')
 if not Path(v.__file__).resolve().is_relative_to(root):raise ValueError('External strategy import')
 isolate_modules(v,root);v._sync_embedded_base_config();freq=v.freq_mod
 from scripts.st_history_scope import validated_source_batch
 dates=pd.DatetimeIndex(pd.to_datetime(pd.read_csv(a.calendar).date));dates=dates[(dates>=pd.Timestamp(freq.START_DATE))&(dates<=pd.Timestamp('2026-10-29'))]
 reb=pd.DatetimeIndex(sorted(members.rebalance_date.unique()));affected=pd.DatetimeIndex(sorted({d for r in intervals.itertuples() for d in reb[(reb>=r.start_inclusive)&(reb<r.end_exclusive)]}))
 symbols=sorted(intervals.symbol.str.zfill(6).unique());universe=freq.load_universe()
 if not set(symbols)<=set(universe) or len(universe)!=5438:raise ValueError('Unknown affected issuer')
 with frozen_metadata(freq.SECURITY_META_DIR,universe):
  with validated_source_batch(root):
   _,caps,_,_=freq.load_cache_panels(symbols=symbols,trading_dates=dates,cap_dates=affected,max_workers=a.workers,trade_constraint_mode=v.base_mod.TRADE_CONSTRAINT_MODE,exclude_historical_st_from_caps=True)
 alternative={d:{} for d in affected};baseline={d:{} for d in affected};events=[]
 for r in intervals.itertuples():
  symbol=str(r.symbol).zfill(6);sp=freq.SHARE_DIR/(symbol+'.csv');hashes[str(sp)]=digest(sp);shares=pd.read_csv(sp,parse_dates=['change_date']).sort_values('change_date')
  for d in affected[(affected>=r.start_inclusive)&(affected<r.end_exclusive)]:
   if symbol in baseline[d]:raise ValueError('Overlapping disputed intervals')
   cap=caps[d].get(symbol);baseline[d][symbol]=cap
   if cap is None:new=None;quantity=None
   else:
    quantity=float(shares.loc[shares.change_date.le(d)].iloc[-1].total_shares_10k)*10000
    if not quantity>0 or not float(r.cninfo_shares)>0:raise ValueError('Invalid share quantity')
    new=cap*float(r.cninfo_shares)/quantity
   alternative[d][symbol]=new;events.append(dict(date=str(d.date()),symbol=symbol,eligible=cap is not None,baseline_cap=cap,alternative_cap=new,installed_shares=quantity,alternative_shares=float(r.cninfo_shares)))
 checks=[]
 for d in affected:
  rows=members.loc[members.rebalance_date.eq(d)].sort_values('rank')
  if rows['rank'].tolist()!=list(range(1,101)):raise ValueError('Original rank discontinuity')
  selected=list(zip(rows.symbol,rows.market_cap.astype(float)))
  checks.append(dict(date=str(d.date()),**threshold_check(selected,baseline[d],alternative[d])))
 for path,expected in hashes.items():
  if digest(Path(path))!=expected:raise ValueError('Screen source changed')
 result=dict(approved=False,formal_written=False,scope='conservative target-set threshold only; order changes may alter constrained execution',calendar_binding=binding,input_sha256=hashes,affected_symbols=len(symbols),full_trading_dates=len(dates),dates=checks,set_unchanged_dates=[c['date'] for c in checks if c['target_set_proven_unchanged']],needs_cross_section_dates=[c['date'] for c in checks if c['requires_full_cross_section']],rank_change_dates=[c['date'] for c in checks if c['selected_rank_changed']],intervals_without_sample_rebalance=[int(i) for i,r in intervals.iterrows() if not ((reb>=r.start_inclusive)&(reb<r.end_exclusive)).any()])
 a.output.mkdir();pd.DataFrame(events).to_csv(a.output/'events.csv',index=False);(a.output/'review.json').write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding='utf-8');print(json.dumps({k:result[k] for k in ['affected_symbols','set_unchanged_dates','needs_cross_section_dates','rank_change_dates']}))
if __name__=='__main__':main()
