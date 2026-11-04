"""Offline diagnostic tail only. Prepared evidence required; never grants migration approval."""
import argparse,importlib,json,os,sys
from pathlib import Path
import numpy as np
import pandas as pd
from run_three_version_diagnostic_20261101 import digest,contained,isolate_modules,offline_guard,frozen_metadata,bind_frozen_calendar

def append_tail(proxy,returns,effective,dates):
 if len(effective)!=100 or len(set(effective))!=100:raise ValueError('Need exactly 100 executed members')
 if set(returns.columns)!=set(effective):raise ValueError('Missing or extra member returns')
 work=proxy.copy();work['date']=pd.to_datetime(work['date'],errors='raise')
 if work.empty or work.date.duplicated().any() or not work.date.is_monotonic_increasing:raise ValueError('Invalid proxy history')
 if dates[0]!=work.date.iloc[-1]:raise ValueError('Tail anchor mismatch')
 level=float(work.close.iloc[-1]);rows=[]
 for day in dates[1:]:
  values=returns.reindex(index=dates,columns=effective).loc[day]
  if values.isna().any() or not np.isfinite(values).all():raise ValueError('Incomplete finite member returns in no-rebalance tail')
  ret=float(values.mean());level*=1.0+ret
  if not np.isfinite(level) or level<=0:raise ValueError('Invalid tail level')
  rows.append(dict(date=day,close=level,daily_return=ret,holding_count=100,holding_effective=True))
 return pd.concat([work,pd.DataFrame(rows)],ignore_index=True,sort=False)

def derive_terminal_return_start(turnover,calendar,anchor):
 """Fill only an executed terminal close's next return session, on a copy."""
 out=turnover.copy();calendar=pd.DatetimeIndex(calendar);anchor=pd.Timestamp(anchor)
 if calendar.has_duplicates or not calendar.is_monotonic_increasing or anchor not in calendar:raise ValueError('Invalid derived-return calendar')
 future=calendar[calendar>anchor]
 if future.empty:raise ValueError('No proven next exchange session')
 parsed={}
 for col in ('rebalance_date','execution_date','effective_date','return_start_date'):
  raw=out[col];value=pd.to_datetime(raw,errors='coerce')
  if (raw.notna()&value.isna()).any():raise ValueError('Malformed turnover date: '+col)
  if col!='return_start_date' and value.isna().any():raise ValueError('Missing execution identity')
  parsed[col]=value
 missing=parsed['return_start_date'].isna()
 derivation=[]
 if missing.any():
  if missing.sum()!=1:raise ValueError('Multiple unknown return starts')
  index=out.index[missing][0]
  if any(parsed[c].loc[index]!=anchor for c in ('rebalance_date','execution_date','effective_date')) or parsed['rebalance_date'].max()!=anchor:
   raise ValueError('Unknown return start is not the executed terminal anchor')
  if out.loc[index,'execution_timing']!='close' or int(out.loc[index,'holding_count_after'])!=100:raise ValueError('Terminal close execution not established')
  if 'constraint_trade_date' in out and pd.Timestamp(out.loc[index,'constraint_trade_date'])!=anchor:raise ValueError('Terminal constraint date mismatch')
  out.loc[index,'return_start_date']=future[0]
  derivation=[dict(row_index=int(index),field='return_start_date',original=None,derived=str(future[0].date()),reason='actual terminal close executed at anchor; next independently evidenced exchange session')]
 return out,derivation

def actual_members(base,members_path,calendar,anchor,workers):
 targets=base._proxy_target_members_map_from_saved_members(members_path)
 full=pd.DatetimeIndex(calendar[(calendar>=pd.Timestamp(base.freq_mod.START_DATE))&(calendar<=anchor)])
 if full.empty or full.has_duplicates or not full.is_monotonic_increasing or full[-1]!=anchor:
  raise ValueError('Executed reconstruction needs complete ordered anchor calendar')
 rebuild=base._reconstruct_effective_members_from_saved_targets
 namespace=rebuild.__globals__;previous=namespace['_minimal_tradeability_dates']
 # The real status validator requires every observed session, and close-limit
 # checks require the actual previous close. A sparse rebalance calendar fails
 # both contracts. Preserve the actual constrained replay, only widen its input.
 namespace['_minimal_tradeability_dates']=lambda panel_dates,rebalance_dates:full.copy()
 try:
  return rebuild(target_members_map=targets,panel_dates=full,as_of_date=anchor,max_workers=workers)
 finally:
  namespace['_minimal_tradeability_dates']=previous

def main():
 p=argparse.ArgumentParser(description=__doc__)
 for name in ('runtime','candidate','calendar','panel','output'):p.add_argument('--'+name,type=Path,required=True)
 p.add_argument('--calendar-sha256',required=True);p.add_argument('--execute',action='store_true');p.add_argument('--workers',type=int,default=4);a=p.parse_args()
 root=a.runtime.resolve();research=Path(__file__).resolve().parent
 if not root.is_relative_to(research/'replay'):raise ValueError('Research runtime required')
 for name in ('candidate','panel','output'):
  value=getattr(a,name).resolve()
  if not value.is_relative_to(root):raise ValueError(name+' must be inside runtime')
  setattr(a,name,value)
 a.calendar=a.calendar.resolve()
 if not a.calendar.is_relative_to(research):raise ValueError('Research calendar required')
 if a.output.exists():raise ValueError('Output already exists')
 anchor=pd.Timestamp('2026-10-29');target=pd.Timestamp('2026-10-30')
 dates=pd.DatetimeIndex(pd.to_datetime(pd.read_csv(a.calendar)['date'],errors='raise')).normalize()
 if dates.has_duplicates or not dates.is_monotonic_increasing or not dates[(dates>=anchor)&(dates<=target)].equals(pd.DatetimeIndex([anchor,target])):raise ValueError('Invalid independent tail calendar')
 required=['proxy.csv','members.csv','turnover.csv','proxy_meta.json','diagnostic_result.json','v2_0.csv','v2_3.csv','v2_5.csv']
 hashes={name:digest(a.candidate/name) for name in required}
 sm=root/'source_manifest.json';source=json.loads(sm.read_text(encoding='utf-8'))
 if source.get('approved') is not False or source.get('formal_written') is not False:raise ValueError('Invalid diagnostic source manifest')
 for name,sha in source['files'].items():
  if digest(contained(root,name))!=sha:raise ValueError('Frozen source mismatch: '+name)
 report=json.loads((a.candidate/'diagnostic_result.json').read_text(encoding='utf-8'))
 if report.get('source_manifest_sha256')!=digest(sm):raise ValueError('Candidate source mismatch')
 if report.get('as_of')!='2026-10-29' or report.get('approved') is not False:raise ValueError('Need completed 29 diagnostic')
 for name in ['proxy.csv','members.csv','turnover.csv']:
  if report[name]['sha256']!=hashes[name]:raise ValueError('29 artifact changed: '+name)
 for version in ('v2_0','v2_3','v2_5'):
  if report['streams'][version]['sha256']!=hashes[version+'.csv']:raise ValueError('Candidate stream identity changed: '+version)
 turnover=pd.read_csv(a.candidate/'turnover.csv')
 if digest(a.calendar)!=a.calendar_sha256:raise ValueError('Calendar hash mismatch')
 turnover,turnover_derivation=derive_terminal_return_start(turnover,dates,anchor)
 members=pd.read_csv(a.candidate/'members.csv')
 if turnover.empty or members.empty:raise ValueError('Empty member/turnover authority')
 execution={}
 for col in ('execution_date','effective_date','return_start_date'):
  x=pd.to_datetime(turnover[col],errors='raise').dt.normalize()
  if x.isna().any() or not x.isin(dates).all():raise ValueError('Invalid execution calendar')
  if col!='return_start_date' and ((x>anchor)&(x<=target)).any():raise ValueError('Executed rebalance inside tail')
  execution[col]=x
 if ((execution['return_start_date']<execution['execution_date'])|(execution['return_start_date']<execution['effective_date'])).any():raise ValueError('Return precedes execution')
 planned=pd.to_datetime(members.rebalance_date,errors='raise')
 if planned.isna().any() or ((planned>anchor)&(planned<=target)).any():raise ValueError('Saved new rebalance inside tail')
 if not a.execute:print('Prepared input checks passed; no tail executed or network requested');return
 os.chdir(root);sys.path.insert(0,str(root));sys.dont_write_bytecode=True
 os.environ['MPLCONFIGDIR']=str(root/'.mplconfig');os.environ['MPLBACKEND']='Agg'
 offline_guard(root)
 calendar_binding=bind_frozen_calendar(a.calendar,a.calendar_sha256,'2026-10-30')
 v=importlib.import_module('microcap_top100_mom16_biweekly_live_v2_0');v23=importlib.import_module('microcap_top100_mom16_biweekly_live_v2_3');v25=importlib.import_module('microcap_top100_mom16_biweekly_live_v2_5')
 for mod in (v,v23,v25):
  if not Path(mod.__file__).resolve().is_relative_to(root):raise ValueError('External strategy module')
 isolate_modules(v,root);v._sync_embedded_base_config();base=v.base_mod
 from scripts.st_history_scope import validated_source_batch
 from scripts.exchange_calendar import sessions_for_day
 from scripts.stock_tail_returns import load_tail_returns
 from scripts.top100_data_contracts import normalise_turnover_table
 actual_calendar=pd.DatetimeIndex(sessions_for_day(target.date()))
 if not dates[dates<=target].equals(actual_calendar[actual_calendar>=dates.min()]):raise ValueError('Calendar source disagreement')
 scheduled=base.build_biweekly_rebalance_dates(dates[dates<=target])
 if ((scheduled>anchor)&(scheduled<=target)).any():raise ValueError('Scheduled new rebalance inside tail')
 symbols=v.freq_mod.load_universe()
 if len(symbols)!=5438 or len(set(symbols))!=5438:raise ValueError('Universe not 5438')
 with frozen_metadata(v.freq_mod.SECURITY_META_DIR,symbols):
  with validated_source_batch(root):
   if report.get('proxy_meta.json',{}).get('sha256')!=hashes['proxy_meta.json']:raise ValueError('Unbound actual terminal state')
   terminal=json.loads((a.candidate/'proxy_meta.json').read_text(encoding='utf-8'))
   policy='delist_zero_writeoff_equal_survivors_v1'
   if terminal.get('terminal_member_derivation')!='actual_simulate_rebalance_path_final_current_members' or terminal.get('terminal_effective_as_of')!='2026-10-29' or terminal.get('delisting_valuation_policy')!=policy or terminal.get('core_params',{}).get('delisting_valuation_policy')!=policy:raise ValueError('Wrong actual terminal state')
   effective=terminal['terminal_effective_members']
   if len(effective)!=100 or len(set(effective))!=100:raise ValueError('Executed portfolio not exactly 100')
   current=v.freq_mod.load_current_st_name_map()
   if not current or set(effective)&set(current):raise ValueError('Current ST exclusion failed')
   master=v.freq_mod.load_security_master();lifecycle=master.set_index('symbol')['delist_date']
   if any(s not in lifecycle.index or (pd.notna(pd.to_datetime(lifecycle.loc[s],errors='coerce')) and pd.Timestamp(lifecycle.loc[s])<=target) for s in effective):raise ValueError('Terminal member not active through tail')
   taildates=pd.DatetimeIndex([anchor,target])
   returns=pd.DataFrame({symbol:load_tail_returns(root=root,symbol=symbol,
    raw_path=root/'.microcap_index_cache/prices_raw'/(symbol+'.csv'),master=master,tail_dates=taildates) for symbol in effective})
 proxy=append_tail(pd.read_csv(a.candidate/'proxy.csv'),returns,effective,taildates)
 for name,sha in hashes.items():
  if digest(a.candidate/name)!=sha:raise ValueError('29 candidate changed during consumption')
 paneldays=pd.DatetimeIndex(pd.to_datetime(pd.read_csv(a.panel)['date'],errors='raise'))
 if paneldays.has_duplicates or not paneldays.is_monotonic_increasing or paneldays.max()!=target or len(dates[(dates>=pd.Timestamp(v.freq_mod.START_DATE))&(dates<=target)].difference(paneldays)):raise ValueError('Panel does not cover target sessions')
 a.output.mkdir();proxy.to_csv(a.output/'proxy.csv',index=False)
 turnover.to_csv(a.output/'turnover.csv',index=False)
 pd.DataFrame(dict(as_of_date=[str(target.date())]*100,rank=range(1,101),symbol=effective)).to_csv(a.output/'effective_members.csv',index=False)
 returns.to_csv(a.output/'member_tail_returns.csv',index_label='date')
 meta=json.loads((a.candidate/'proxy_meta.json').read_text(encoding='utf-8'))
 proof=meta.get('return_basis_consumption',{})
 if proof.get('full_rebuild_anchor')!='2026-10-29' or proof.get('tail_extensions')!=[]:raise ValueError('Unexpected candidate consumption anchor')
 proof['tail_extensions']=[dict(start_exclusive='2026-10-29',end='2026-10-30',symbols=sorted(effective),effective_members_sha256=digest(a.output/'effective_members.csv'),proxy_members_sha256=hashes['members.csv'],proxy_turnover_sha256=digest(a.output/'turnover.csv'))]
 meta.update(terminal_effective_as_of='2026-10-30',end_date='2026-10-30',scope='diagnostic_only',approved=False,formal_written=False)
 meta.setdefault('core_params',{})['stock_input_evidence_fingerprint']=base.stock_input_evidence_fingerprint()
 if not base.proxy_meta_matches_execution_model(meta):raise ValueError('Tail proxy identity incompatible with actual core/source evidence')
 (a.output/'proxy_meta.json').write_text(json.dumps(meta,ensure_ascii=False,indent=2),encoding='utf-8')
 args=argparse.Namespace(index_csv=a.output/'proxy.csv',costed_nav_csv=a.output/'base_costed.csv')
 base.rebuild_costed_nav_from_proxy_turnover(args,{'proxy_turnover':a.output/'turnover.csv'},a.panel,target_end_date=target)
 close=base.run_signal(base.load_close_df(a.panel,args.index_csv,max_date=target)).sort_index()[['microcap_close','hedge_close']].rename(columns={'microcap_close':'microcap','hedge_close':'hedge'})
 turnover=normalise_turnover_table(turnover)
 gross=base.apply_momentum_gap_exit_buffer(base.run_signal(close).sort_index(),v.V2_0_MOMENTUM_GAP_EXIT_BUFFER)
 out20=v.overlay_mod.apply_v2_0_execution(gross,turnover)
 streams={'v2_0':out20,'v2_3':v23.build_v2_3_result(close,turnover,v23.build_v2_3_common_index(close,official_index=out20.index)),'v2_5':v25.build_v2_5_result(close,turnover,v25.build_v2_5_common_index(close,official_index=out20.index))}
 result=dict(turnover_derivation=turnover_derivation,derived_turnover_sha256=digest(a.output/'turnover.csv'),calendar_binding=calendar_binding,approved=False,formal_written=False,scope='diagnostic_only',share_scope='economic_share_count_basis; public_disclosure_PIT_not_certified',as_of='2026-10-30',source_manifest_sha256=digest(sm),source_candidate_hashes=hashes,calendar_sha256=digest(a.calendar),streams={})
 for name,frame in streams.items():
  if not frame.index.equals(dates[(dates>=frame.index.min())&(dates<=target)]) or frame.index.max()!=target or not np.isfinite(frame[['return_net','nav_net']]).all().all():raise ValueError('Incomplete final stream: '+name)
  old=pd.read_csv(a.candidate/(name+'.csv'),index_col='date',parse_dates=['date'])
  if not old.index.equals(frame.loc[:anchor].index):raise ValueError('Historical stream date rewrite: '+name)
  for col in ('return_net','nav_net'):
   if not np.allclose(old[col],frame.loc[old.index,col],rtol=1e-12,atol=1e-12):raise ValueError('Historical return rewrite: '+name)
  path=a.output/(name+'.csv');frame.to_csv(path,index_label='date');read=pd.read_csv(path)
  if len(read)!=len(frame) or str(pd.to_datetime(read.date).max().date())!='2026-10-30':raise ValueError('CSV readback failed')
  result['streams'][name]=dict(rows=len(read),latest='2026-10-30',sha256=digest(path))
 result['proxy_meta.json']={'sha256':digest(a.output/'proxy_meta.json')}
 for name in ('proxy.csv','turnover.csv','base_costed.csv','effective_members.csv'):
  result[name]={'sha256':digest(a.output/name),'rows':len(pd.read_csv(a.output/name))}
 result['panel']={'path':str(a.panel),'sha256':digest(a.panel),'rows':len(paneldays),'latest':'2026-10-30'}
 (a.output/'diagnostic_result.json').write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding='utf-8')
 print(json.dumps(result,ensure_ascii=False))
if __name__=='__main__':main()
