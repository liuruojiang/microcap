"""Read-only exact target sensitivity under the verified old-cutoff bound."""
import argparse,json
from pathlib import Path
import numpy as np,pandas as pd
from run_three_version_diagnostic_20261101 import digest
from prepare_share144_threshold_review_20261101 import threshold_check

def refine(selected,baseline,alternative):
 check=threshold_check(selected,baseline,alternative)
 cutoff=(selected[-1][1],selected[-1][0]);old=dict(selected)
 if any((alternative.get(s,c),s)>cutoff for s,c in selected):raise ValueError('Selected issuer exceeds old cutoff; full cross-section required')
 pool={**old,**{s:c for s,c in alternative.items() if c is not None}}
 new=sorted(pool,key=lambda s:(pool[s],s))[:100];oldlist=[s for s,c in selected]
 return dict(entered=sorted(set(new)-set(old)),exited=sorted(set(old)-set(new)),rank_changed=new!=oldlist,original=oldlist,alternative=new,rank_moves=[dict(symbol=s,old_rank=oldlist.index(s)+1 if s in old else None,new_rank=i+1) for i,s in enumerate(new) if s not in old or oldlist.index(s)!=i],execution_nav_certified=False)

def main():
 p=argparse.ArgumentParser();p.add_argument('--review-dir',type=Path,required=True);p.add_argument('--members',type=Path,required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args()
 if a.output.exists():raise ValueError('No overwrite')
 review=json.loads((a.review_dir/'review.json').read_text(encoding='utf-8'));events=pd.read_csv(a.review_dir/'events.csv',dtype={'symbol':str},parse_dates=['date']);members=pd.read_csv(a.members,dtype={'symbol':str},parse_dates=['rebalance_date'])
 for path,expected in review['input_sha256'].items():
  if digest(Path(path))!=expected:raise ValueError('Changed source '+path)
 if review['input_sha256'].get(str(a.members.resolve()))!=digest(a.members):raise ValueError('Members identity unbound')
 intervalpath=next(Path(p) for p in review['input_sha256'] if p.endswith('historical_difference_intervals.csv'));intervals=pd.read_csv(intervalpath,dtype={'symbol':str},parse_dates=['start_inclusive','end_exclusive']);dates=pd.DatetimeIndex(sorted(members.rebalance_date.unique()))
 expected=[(r.symbol,d) for r in intervals.itertuples() for d in dates[(dates>=r.start_inclusive)&(dates<r.end_exclusive)]]
 if len(events)!=len(expected) or set(zip(events.symbol,events.date))!=set(expected) or events.duplicated(['date','symbol']).any():raise ValueError('Event coverage differs from source intervals')
 results=[]
 for d,group in events.groupby('date'):
  selected=members.loc[members.rebalance_date.eq(d)].sort_values('rank');pairs=list(zip(selected.symbol,selected.market_cap.astype(float)))
  baseline={r.symbol:float(r.baseline_cap) if r.eligible else None for r in group.itertuples()};alternative={r.symbol:float(r.alternative_cap) if r.eligible else None for r in group.itertuples()}
  results.append(dict(date=str(d.date()),**refine(pairs,baseline,alternative)))
 result=dict(scope='exact target sensitivity conditional on alternative shares; public completion timing remains unproven; not source adjudication or executed NAV',approved=False,formal_written=False,source_review_sha256=digest(a.review_dir/'review.json'),source_events_sha256=digest(a.review_dir/'events.csv'),members_sha256=digest(a.members),dates=len(results),set_change_dates=[r for r in results if r['entered'] or r['exited']],rank_only_dates=[r for r in results if not r['entered'] and not r['exited'] and r['rank_changed']])
 a.output.write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding='utf-8');print(json.dumps(dict(set_changes=[{k:r[k] for k in ['date','entered','exited']} for r in result['set_change_dates']],rank_only=[dict(date=r['date'],moves=r['rank_moves']) for r in result['rank_only_dates']]),ensure_ascii=False))
if __name__=='__main__':main()
