"""Export actual constrained members after completed 29 replay; offline, not approval."""
import argparse,importlib,json,os,sys
from pathlib import Path
import pandas as pd
from run_isolated_tail_20261030 import actual_members,derive_terminal_return_start
from run_three_version_diagnostic_20261101 import digest,contained,isolate_modules,offline_guard,frozen_metadata,bind_frozen_calendar

def validate_identity(candidate,root):
 report=json.loads((candidate/'diagnostic_result.json').read_text(encoding='utf-8'))
 if report.get('as_of')!='2026-10-29' or report.get('approved') is not False or report.get('formal_written') is not False:raise ValueError('Invalid completed29 identity')
 if report.get('source_manifest_sha256')!=digest(root/'source_manifest.json'):raise ValueError('Candidate source identity mismatch')
 for name in ('members.csv','turnover.csv','proxy.csv'):
  if report[name]['sha256']!=digest(candidate/name):raise ValueError('Candidate artifact identity mismatch: '+name)
 return report

def validate_members(members):
 if len(members)!=100 or len(set(members))!=100 or any(not isinstance(s,str) or len(s)!=6 or not s.isascii() or not s.isdigit() for s in members):raise ValueError('Actual executed portfolio must have100 unique six-digit symbols')

def main():
 p=argparse.ArgumentParser(description=__doc__)
 for name in ('runtime','candidate','calendar'):p.add_argument('--'+name,type=Path,required=True)
 p.add_argument('--direct-final-meta',action='store_true');p.add_argument('--output-name',default='executed100_stage_input');p.add_argument('--calendar-sha256',required=True);p.add_argument('--execute',action='store_true');p.add_argument('--workers',type=int,default=4);a=p.parse_args()
 root=a.runtime.resolve();candidate=a.candidate.resolve();cal=a.calendar.resolve();research=Path(__file__).resolve().parent
 if not root.is_relative_to(research/'replay') or not candidate.is_relative_to(root) or not cal.is_relative_to(research):raise ValueError('Invalid isolated paths')
 out=root/a.output_name
 if out.parent!=root or not a.output_name:raise ValueError('Invalid output name')
 if out.exists():raise ValueError('Never overwrite executed member evidence')
 validate_identity(candidate,root)
 source=json.loads((root/'source_manifest.json').read_text(encoding='utf-8'))
 if source.get('approved') is not False or source.get('formal_written') is not False:raise ValueError('Invalid source scope')
 for name,sha in source['files'].items():
  if digest(contained(root,name))!=sha:raise ValueError('Source bytes changed')
 identities={str(p):digest(p) for p in [root/'source_manifest.json',candidate/'diagnostic_result.json',candidate/'members.csv',candidate/'turnover.csv',candidate/'proxy.csv',cal]}
 if a.direct_final_meta:
  identities[str(candidate/'proxy_meta.json')]=digest(candidate/'proxy_meta.json')
  report=json.loads((candidate/'diagnostic_result.json').read_text(encoding='utf-8'))
  if report.get('proxy_meta.json',{}).get('sha256')!=digest(candidate/'proxy_meta.json'):raise ValueError('Terminal state not bound to completed candidate')
 dates=pd.DatetimeIndex(pd.to_datetime(pd.read_csv(cal)['date'],errors='raise')).normalize();anchor=pd.Timestamp('2026-10-29');target=pd.Timestamp('2026-10-30')
 if dates.has_duplicates or not dates.is_monotonic_increasing or not dates[(dates>=anchor)&(dates<=target)].equals(pd.DatetimeIndex([anchor,target])):raise ValueError('Bad tail calendar')
 turnover=pd.read_csv(candidate/'turnover.csv')
 if digest(cal)!=a.calendar_sha256:raise ValueError('Calendar hash mismatch')
 turnover,turnover_derivation=derive_terminal_return_start(turnover,dates,anchor)
 members=pd.read_csv(candidate/'members.csv',dtype={'symbol':str})
 if turnover.empty or members.empty:raise ValueError('Empty authorities')
 parsed={}
 for col in ('execution_date','effective_date','return_start_date'):
  values=pd.to_datetime(turnover[col],errors='raise').dt.normalize()
  if values.isna().any() or not values.isin(dates).all():raise ValueError('Invalid execution dates')
  if col!='return_start_date' and ((values>anchor)&(values<=target)).any():raise ValueError('New execution inside tail')
  parsed[col]=values
 if ((parsed['return_start_date']<parsed['execution_date'])|(parsed['return_start_date']<parsed['effective_date'])).any():raise ValueError('Return precedes execution')
 planned=pd.to_datetime(members.rebalance_date,errors='raise')
 if planned.isna().any() or not planned.isin(dates).all() or planned.max()>anchor:raise ValueError('Target members extend beyond anchor')
 if not members.groupby('rebalance_date').symbol.agg(['size','nunique']).eq(100).all().all():raise ValueError('Incomplete target periods')
 proxy=pd.read_csv(candidate/'proxy.csv')
 if pd.to_datetime(proxy.date).max()!=anchor:raise ValueError('Proxy not at29')
 if not a.execute:print('Prepared; no actual holdings exported');return
 os.chdir(root);sys.path.insert(0,str(root));sys.dont_write_bytecode=True;os.environ['MPLCONFIGDIR']=str(root/'.mplconfig');os.environ['MPLBACKEND']='Agg';offline_guard(root)
 calendar_binding=bind_frozen_calendar(cal,a.calendar_sha256,'2026-10-30')
 v=importlib.import_module('microcap_top100_mom16_biweekly_live_v2_0')
 if not Path(v.__file__).resolve().is_relative_to(root):raise ValueError('External strategy source')
 isolate_modules(v,root);v._sync_embedded_base_config()
 from scripts.st_history_scope import validated_source_batch
 from scripts.exchange_calendar import sessions_for_day
 official=pd.DatetimeIndex(sessions_for_day(target.date()))
 if not dates[dates<=target].equals(official[official>=dates.min()]):raise ValueError('Calendar disagreement')
 scheduled=v.base_mod.build_biweekly_rebalance_dates(dates[dates<=target])
 if ((scheduled>anchor)&(scheduled<=target)).any():raise ValueError('Scheduled rebalance inside tail')
 universe=v.freq_mod.load_universe()
 if len(universe)!=5438 or len(set(universe))!=5438:raise ValueError('Expected5438 universe')
 with frozen_metadata(v.freq_mod.SECURITY_META_DIR,universe) as metadata:
  with validated_source_batch(root):
   if a.direct_final_meta:
    meta=json.loads((candidate/'proxy_meta.json').read_text(encoding='utf-8'))
    if meta.get('terminal_member_derivation')!='actual_simulate_rebalance_path_final_current_members' or meta.get('terminal_effective_as_of')!='2026-10-29':raise ValueError('Missing actual terminal state')
    policy='delist_zero_writeoff_equal_survivors_v1'
    if meta.get('delisting_valuation_policy')!=policy or meta.get('core_params',{}).get('delisting_valuation_policy')!=policy:raise ValueError('Wrong terminal valuation policy')
    executed=meta['terminal_effective_members']
   else:executed=actual_members(v.base_mod,candidate/'members.csv',dates,anchor,a.workers)
   master=v.freq_mod.load_security_master().copy();master['symbol']=master['symbol'].astype(str).str.zfill(6)
   lifecycle=master.set_index('symbol')['delist_date']
   if any(s not in lifecycle.index or (pd.notna(pd.to_datetime(lifecycle.loc[s],errors='coerce')) and pd.Timestamp(lifecycle.loc[s])<=anchor) for s in executed):raise ValueError('Unknown or already delisted terminal member')
   validate_members(executed)
   current=v.freq_mod.load_current_st_name_map()
   if not current or set(executed)&set(current):raise ValueError('Current ST publication gate failed')
 for name,sha in identities.items():
  if digest(Path(name))!=sha:raise ValueError('Input changed during reconstruction')
 out.mkdir();turnover.to_csv(out/'derived_turnover.csv',index=False);path=out/'effective_members.csv'
 pd.DataFrame(dict(as_of_date=['2026-10-29']*100,rank=range(1,101),symbol=executed)).to_csv(path,index=False)
 proof=dict(turnover_derivation=turnover_derivation,derived_turnover_sha256=digest(out/'derived_turnover.csv'),calendar_binding=calendar_binding,member_derivation=('actual_simulate_rebalance_path_final_current_members' if a.direct_final_meta else 'actual_reconstruct_effective_members_from_saved_targets'),as_of='2026-10-29',target='2026-10-30',approved=False,formal_written=False,effective_members_sha256=digest(path),source_manifest_sha256=digest(root/'source_manifest.json'),input_sha256=identities,metadata_count=len(metadata),metadata_manifest_sha256=__import__('hashlib').sha256(json.dumps(metadata,sort_keys=True,separators=(',',':')).encode()).hexdigest(),source_runtime=str(root))
 proof.update(reconstruction_calendar_mode='complete_sessions_from_strategy_start_through_anchor; temporary selector binding restored',export_runner_sha256=digest(Path(__file__).resolve()),reconstruction_helper_sha256=digest(Path(actual_members.__code__.co_filename).resolve()))
 (out/'execution_proof.json').write_text(json.dumps(proof,ensure_ascii=False,indent=2),encoding='utf-8');print(json.dumps(proof))
if __name__=='__main__':main()
