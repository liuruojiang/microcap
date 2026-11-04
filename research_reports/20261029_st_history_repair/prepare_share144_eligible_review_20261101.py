"""Offline research ranking comparison using real eligibility. No strategy publication."""
import argparse,importlib,json,os,sys
from pathlib import Path
import pandas as pd
from run_three_version_diagnostic_20261101 import digest,isolate_modules,offline_guard,frozen_metadata,bind_frozen_calendar

def main():
 p=argparse.ArgumentParser(description=__doc__)
 for name in ('runtime','candidate','calendar','intervals','output'):p.add_argument('--'+name,type=Path,required=True)
 p.add_argument('--calendar-sha256',required=True);p.add_argument('--execute',action='store_true');p.add_argument('--workers',type=int,default=4);a=p.parse_args()
 root=a.runtime.resolve();research=Path(__file__).resolve().parent
 if not root.is_relative_to(research/'replay'):raise ValueError('Research runtime required')
 for name in ('candidate','output'):
  q=getattr(a,name).resolve()
  if not q.is_relative_to(root):raise ValueError('Candidate/output must be inside runtime')
  setattr(a,name,q)
 a.calendar=a.calendar.resolve();a.intervals=a.intervals.resolve()
 if not all(q.is_relative_to(research) for q in [a.calendar,a.intervals]):raise ValueError('Research evidence required')
 if a.output.exists():raise ValueError('Never overwrite an earlier review')
 members=pd.read_csv(a.candidate/'members.csv',dtype={'symbol':str},parse_dates=['rebalance_date'])
 if members.empty or not members.groupby('rebalance_date').symbol.agg(['size','nunique']).eq(100).all().all():raise ValueError('Incomplete candidate members')
 intervals=pd.read_csv(a.intervals,dtype={'symbol':str},parse_dates=['start_inclusive','end_exclusive'])
 report=json.loads((a.candidate/'diagnostic_result.json').read_text(encoding='utf-8'))
 if report['members.csv']['sha256']!=digest(a.candidate/'members.csv') or report['as_of']!='2026-10-29':raise ValueError('Candidate identity mismatch')
 if not a.execute:print('Prepared only; requires completed new replay, no ranking executed');return
 os.chdir(root);sys.path.insert(0,str(root));sys.dont_write_bytecode=True;os.environ['MPLCONFIGDIR']=str(root/'.mplconfig');os.environ['MPLBACKEND']='Agg';offline_guard(root)
 calendar_binding=bind_frozen_calendar(a.calendar,a.calendar_sha256,'2026-10-30')
 v=importlib.import_module('microcap_top100_mom16_biweekly_live_v2_0')
 if not Path(v.__file__).resolve().is_relative_to(root):raise ValueError('Wrong source root')
 isolate_modules(v,root);v._sync_embedded_base_config();base=v.base_mod;freq=v.freq_mod
 from scripts.st_history_scope import validated_source_batch
 dates=pd.DatetimeIndex(pd.to_datetime(pd.read_csv(a.calendar)['date'],errors='raise'))
 dates=dates[(dates>=pd.Timestamp(freq.START_DATE))&(dates<=pd.Timestamp('2026-10-29'))]
 reb=pd.DatetimeIndex(sorted(members.rebalance_date.unique()))
 affected=sorted({d for r in intervals.itertuples() for d in reb[(reb>=r.start_inclusive)&(reb<r.end_exclusive)]});capdates=pd.DatetimeIndex(affected)
 symbols=freq.load_universe()
 if len(symbols)!=5438 or len(set(symbols))!=5438:raise ValueError('Universe not5438')
 with frozen_metadata(freq.SECURITY_META_DIR,symbols):
  with validated_source_batch(root):
   _,caps,_,_=freq.load_cache_panels(symbols=symbols,trading_dates=dates,cap_dates=capdates,max_workers=a.workers,trade_constraint_mode=base.TRADE_CONSTRAINT_MODE,exclude_historical_st_from_caps=True)
   baseline=base.build_live_target_members_map(caps,capdates,{},top_n=100,exclude_current_st_names=False)
 alternate={d:dict(caps[d]) for d in capdates};events=[];share_hashes={}
 for d in capdates:
  saved=members.loc[members.rebalance_date.eq(d)].sort_values('rank').symbol.tolist()
  if baseline[d]!=saved:raise ValueError('Recomputed eligible ranking differs from new replay '+str(d))
 for r in intervals.itertuples():
  s=str(r.symbol).zfill(6);path=freq.SHARE_DIR/(s+'.csv');share_hashes[s]=digest(path)
  history=pd.read_csv(path,parse_dates=['change_date']).sort_values('change_date')
  for d in capdates[(capdates>=r.start_inclusive)&(capdates<r.end_exclusive)]:
   eligible=s in caps[d];record=dict(symbol=s,rebalance_date=str(d.date()),eligible=eligible)
   if eligible:
    old=history.loc[history.change_date.le(d)].iloc[-1].total_shares_10k*10000
    if not old>0 or not r.cninfo_shares>0:raise ValueError('Invalid actual share quantity')
    alternate[d][s]=caps[d][s]*float(r.cninfo_shares)/float(old)
    record.update(installed_shares=float(old),alternate_shares=float(r.cninfo_shares),baseline_cap=caps[d][s],alternate_cap=alternate[d][s])
   events.append(record)
 alt=base.build_live_target_members_map(alternate,capdates,{},top_n=100,exclude_current_st_names=False)
 changes=[dict(date=str(d.date()),baseline=baseline[d],alternative=alt[d],entered=sorted(set(alt[d])-set(baseline[d])),exited=sorted(set(baseline[d])-set(alt[d]))) for d in capdates if baseline[d]!=alt[d]]
 a.output.mkdir();pd.DataFrame(events).to_csv(a.output/'eligible_share_events.csv',index=False)
 result=dict(calendar_binding=calendar_binding,scope='eligible target-rank diagnostic; executed holdings and returns require separate constrained replay',approved=False,formal_written=False,share_approved=False,candidate_members_sha256=digest(a.candidate/'members.csv'),intervals_sha256=digest(a.intervals),calendar_sha256=digest(a.calendar),installed_share_sha256=share_hashes,affected_dates=len(capdates),changed_dates=len(changes),changes=changes,intervals_without_sample_rebalance=[int(i) for i,r in intervals.iterrows() if not ((reb>=r.start_inclusive)&(reb<r.end_exclusive)).any()])
 (a.output/'review.json').write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding='utf-8');print('Diagnostic changed target dates:',len(changes))
if __name__=='__main__':main()
