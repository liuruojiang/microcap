"""Read-only prioritization of real share-source differences; not a new backtest."""
from pathlib import Path
import json,hashlib,io,concurrent.futures
import pandas as pd
import numpy as np
B=Path(__file__).resolve().parent;ROOT=B.parents[1];OUT=B/'share_impact_priority_20261101';OUT.mkdir(exist_ok=True)
sha=lambda body:hashlib.sha256(body).hexdigest()
member_path=ROOT/'outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_members.csv';turn_path=ROOT/'outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_turnover.csv'
inputs={}
for name,path in [('old_members.csv',member_path),('old_turnover.csv',turn_path)]:
 body=path.read_bytes();dest=OUT/name
 if dest.exists():assert dest.read_bytes()==body,'frozen previous inputs changed; use a new run directory'
 else:dest.write_bytes(body)
 inputs[str(path.relative_to(ROOT))]=sha(body)
members=pd.read_csv(OUT/'old_members.csv',dtype={'symbol':str},parse_dates=['rebalance_date']);turn=pd.read_csv(OUT/'old_turnover.csv',parse_dates=['rebalance_date','execution_date','effective_date','return_start_date'])
assert members.groupby('rebalance_date').symbol.nunique().eq(100).all()
all_dates=pd.DatetimeIndex(sorted(members.rebalance_date.unique()));intervalp=B/'new463_cninfo_shares_20261029/historical_difference_intervals.csv';intervals=pd.read_csv(intervalp,dtype={'symbol':str},parse_dates=['start_inclusive','end_exclusive']);inputs[str(intervalp.relative_to(ROOT))]=sha(intervalp.read_bytes())
lookups=[]
for i,r in intervals.iterrows():
 for d in all_dates[(all_dates>=r.start_inclusive)&(all_dates<r.end_exclusive)]:lookups.append(dict(interval_id=int(i),symbol=r.symbol,rebalance_date=d,em_shares=float(r.em_shares),cninfo_shares=float(r.cninfo_shares),relative_difference=float(r.relative_difference),same_event_day_conflict=bool(r.same_event_day_conflict)))
events=pd.DataFrame(lookups);dates=pd.DatetimeIndex(sorted(events.rebalance_date.unique()));date_values=dates.to_numpy(dtype='datetime64[ns]')
rawp=B/'raw_install_normalized_20261029/install_manifest.json';sharep=B/'share_install_20261029/manifest.json';raw=json.loads(rawp.read_text());shares=json.loads(sharep.read_text());rm={r['symbol']:r for r in raw['records']};sm={r['symbol']:r for r in shares['records']};assert set(rm)==set(sm)
masterp=B/'cross_source_official/security_master_combined_candidate.csv';master=pd.read_csv(masterp,dtype={'symbol':str},parse_dates=['list_date','delist_date']).set_index('symbol')
for p in [rawp,sharep,masterp]:inputs[str(p.relative_to(ROOT))]=sha(p.read_bytes())
def one(symbol):
 rb=Path(rm[symbol]['candidate_path']).read_bytes();sb=Path(sm[symbol]['candidate_path']).read_bytes();assert sha(rb)==rm[symbol]['candidate_sha256'];assert sha(sb)==sm[symbol]['candidate_sha256']
 p=pd.read_csv(io.BytesIO(rb),parse_dates=['date']).sort_values('date');s=pd.read_csv(io.BytesIO(sb),parse_dates=['change_date']).sort_values('change_date');assert not p.date.duplicated().any() and not s.change_date.duplicated().any()
 pi=p.date.to_numpy();si=s.change_date.to_numpy();px=np.searchsorted(pi,date_values,side='right')-1;sx=np.searchsorted(si,date_values,side='right')-1
 good=(px>=0)&(sx>=0);close=np.full(len(dates),np.nan);qty=close.copy();age=close.copy();close[good]=p.close_raw.to_numpy(float)[px[good]];qty[good]=s.total_shares_10k.to_numpy(float)[sx[good]]*10000;age[good]=(date_values[good]-pi[px[good]])/np.timedelta64(1,'D')
 row=master.loc[symbol];good &= date_values>=row.list_date.to_datetime64()
 if pd.notna(row.delist_date):good &= date_values<row.delist_date.to_datetime64()
 cap=np.where(good,close*qty,np.nan);cap[~np.isfinite(cap)|(cap<=0)]=np.nan
 return symbol,cap,close,qty,age
symbols=sorted(rm);caps={};prices={};quantities={};ages={}
with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
 for n,(symbol,cap,price,qty,age) in enumerate(pool.map(one,symbols),1):
  caps[symbol]=cap;prices[symbol]=price;quantities[symbol]=qty;ages[symbol]=age
  if n%500==0:print(n,'/',len(symbols),flush=True)
base=pd.DataFrame(caps,index=dates);alternate=base.copy()
for e in lookups:
 d=e['rebalance_date'];s=e['symbol'];j=dates.get_loc(d)
 if np.isfinite(base.at[d,s]):alternate.at[d,s]=prices[s][j]*e['cninfo_shares']
# Rank every legal lifecycle security, without pretending ST/tradeability is known.
rank=base.rank(axis=1,method='min');alt_rank=alternate.rank(axis=1,method='min')
old_lookup=members.set_index(['rebalance_date','symbol']);old_cut=members.groupby('rebalance_date').market_cap.max();records=[]
for e in lookups:
 d=e['rebalance_date'];s=e['symbol'];j=dates.get_loc(d);r=dict(e);basecap=base.at[d,s];newcap=alternate.at[d,s];a=rank.at[d,s];b=alt_rank.at[d,s];key=(d,s)
 r.update(old_target_member=key in old_lookup.index,old_rank=int(old_lookup.loc[key,'rank']) if key in old_lookup.index else None,raw_close=prices[s][j],raw_price_age_days=ages[s][j],baseline_candidate_shares=quantities[s][j],baseline_market_cap=basecap,cninfo_scenario_market_cap=newcap,unfiltered_rank_baseline=a,unfiltered_rank_cninfo_scenario=b,unfiltered_crosses_100=bool(np.isfinite(a) and np.isfinite(b) and ((a<=100)!=(b<=100))),either_unfiltered_top100=bool(np.isfinite(a) and np.isfinite(b) and min(a,b)<=100),old_formal_rank100_cap=float(old_cut.loc[d]),either_cap_below_old_formal_cutoff=bool(np.isfinite(basecap) and np.isfinite(newcap) and min(basecap,newcap)<=old_cut.loc[d]),base_qty_matches_em=bool(np.isfinite(quantities[s][j]) and abs(quantities[s][j]-e['em_shares'])<.01))
 r['priority_score']=100*int(r['old_target_member'])+60*int(r['unfiltered_crosses_100'])+30*int(r['either_cap_below_old_formal_cutoff'])+20*int(r['same_event_day_conflict'])+10*int(r['relative_difference']>=.5)
 records.append(r)
out=pd.DataFrame(records).sort_values(['priority_score','relative_difference'],ascending=False);out.to_csv(OUT/'rebalance_impacts.csv',index=False);out[out.old_target_member].to_csv(OUT/'old_member_impacts.csv',index=False);out[out.unfiltered_crosses_100].to_csv(OUT/'unfiltered_top100_crossings.csv',index=False)
# All intervals are retained even when there is no old saved rebalance within them.
intervals['old_rebalance_events']=intervals.index.map(events.groupby('interval_id').size()).fillna(0).astype(int)
intervals.to_csv(OUT/'all_interval_coverage.csv',index=False)
cols=['baseline_market_cap','cninfo_scenario_market_cap','unfiltered_rank_baseline','unfiltered_rank_cninfo_scenario'];summary=dict(scope='priority diagnostic only; not ST-certified ranking, new formal performance or proof of no impact',approved=False,formal_written=False,source_files=inputs,old_members_rows=len(members),old_rebalance_dates=len(all_dates),old_rebalance_first=str(all_dates.min().date()),old_rebalance_last=str(all_dates.max().date()),full_universe=5438,affected_rebalance_dates=len(dates),intervals=len(intervals),intervals_with_no_saved_rebalance=int(intervals.old_rebalance_events.eq(0).sum()),symbol_rebalance_events=len(out),affected_symbols=int(out.symbol.nunique()),old_member_events=int(out.old_target_member.sum()),old_member_symbols=int(out.loc[out.old_target_member,'symbol'].nunique()),unfiltered_100_crossing_events=int(out.unfiltered_crosses_100.sum()),unfiltered_100_crossing_symbols=int(out.loc[out.unfiltered_crosses_100,'symbol'].nunique()),same_event_day_conflict_events=int(out.same_event_day_conflict.sum()),limits=['Old membership is target list, not a complete executed-lot ledger; nonmembership does not prove no rank impact.','No ST or suspensions/limit eligibility filters applied; unfiltered rank is only a prioritization scenario, not formal Top100.','Lifecycle and original raw price / candidate share date joins follow backward as-of; stale-price ages are explicit.','Alternative scenario replaces all overlapping disputed share intervals together while other candidate inputs stay fixed.','Dates later than the old saved member history, and intervals without an old rebalance, remain unscreened rather than cleared.'])
assert member_path.read_bytes()==(OUT/'old_members.csv').read_bytes() and turn_path.read_bytes()==(OUT/'old_turnover.csv').read_bytes()
(OUT/'summary.json').write_text(json.dumps(summary,ensure_ascii=False,indent=2),encoding='utf-8');print(summary)
