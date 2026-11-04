"""Research-only as-of replay. No refresh, migration, approval or publication."""
import argparse, hashlib, importlib, json, os, socket, sys, types
from pathlib import Path
from contextlib import contextmanager, nullcontext

def digest(p): return hashlib.sha256(p.read_bytes()).hexdigest()
def canonical_path(value):
 """Compare Windows extended/UNC spellings using the same ordinary namespace."""
 value=os.fsdecode(value)
 if os.name=='nt':
  if value.lower().startswith('\\\\?\\unc\\'):value='\\\\'+value[8:]
  elif value.startswith('\\\\?\\'):value=value[4:]
 return Path(value).resolve()

def contained(root, name):
 root=canonical_path(root)
 path = canonical_path(root / name)
 if not path.is_relative_to(root) or path == root:
  raise RuntimeError('Path escapes isolated root: ' + str(name))
 return path

def isolate_modules(v, root):
 """Embedded SimpleNamespace attributes and function globals are separate stores."""
 disabled = []
 for label in ('base_mod', 'freq_mod', 'fetch_mod', 'cost_mod', 'hedge_mod', 'overlay_mod'):
  mod = getattr(v, label)
  if Path(mod.ROOT).resolve() != root:
   raise RuntimeError('Wrong embedded root: ' + label)
  spaces = [vars(mod)]
  spaces += [f.__globals__ for f in vars(mod).values() if isinstance(f, types.FunctionType)]
  seen = set()
  for ns in spaces:
   if id(ns) in seen: continue
   seen.add(id(ns))
   for key in list(ns):
    if key.startswith('SHARED'):
     if ns[key] is not None: disabled.append(label + '.' + key)
     ns[key] = None
   if 'find_shared_cache_root' in ns:
    ns['find_shared_cache_root'] = lambda: None
 return sorted(set(disabled))

def offline_guard(root):
 """Process-wide guard also catches connect_ex and accidental external writes."""
 root=canonical_path(root)
 def audit(event, args):
  if event in ('socket.connect', 'socket.getaddrinfo', 'subprocess.Popen', 'os.system'):
   raise RuntimeError('Offline replay blocked: ' + event)
  if event == 'open' and isinstance(args[0], (str, bytes, os.PathLike)):
   path = canonical_path(args[0])
   mode, flags = args[1], args[2]
   write = bool(isinstance(mode, str) and any(x in mode for x in 'wax+')) or bool(flags & (os.O_WRONLY | os.O_RDWR | os.O_CREAT | os.O_TRUNC))
   cache_read = any(x.startswith('.microcap') for x in path.parts)
   if (write or cache_read) and not path.is_relative_to(root):
    raise RuntimeError('Isolated replay blocked external file: ' + str(path))
  if event in ('os.remove', 'os.rmdir', 'os.mkdir', 'os.rename'):
   for raw in args[:2] if event == 'os.rename' else args[:1]:
    if isinstance(raw, (str, bytes, os.PathLike)) and not canonical_path(raw).is_relative_to(root):
     raise RuntimeError('Isolated replay blocked mutation: ' + str(raw))
 sys.addaudithook(audit)

@contextmanager
def frozen_metadata(directory, symbols):
 """Forbid repairs during consumption and verify original bytes on both boundaries."""
 directory=canonical_path(directory)
 expected={str(symbol).zfill(6)+'.json' for symbol in symbols}
 def snapshot():
  files={p.name:p for p in directory.glob('*.json')}
  if set(files)!=expected: raise RuntimeError('Installed metadata coverage differs from frozen universe')
  return {name:digest(files[name]) for name in sorted(files)}
 before=snapshot();state={'active':True}
 def guard(event,args):
  if not state['active']: return
  paths=[]
  if event=='open' and isinstance(args[0],(str,bytes,os.PathLike)):
   mode,flags=args[1],args[2]
   if (isinstance(mode,str) and any(x in mode for x in 'wax+')) or (flags & (os.O_WRONLY|os.O_RDWR|os.O_CREAT|os.O_TRUNC)):
    paths=[args[0]]
  elif event in ('os.remove','os.rmdir','os.mkdir','os.rename'):
   paths=args[:2] if event=='os.rename' else args[:1]
  for raw in paths:
   if isinstance(raw,(str,bytes,os.PathLike)) and canonical_path(raw).is_relative_to(directory):
    raise RuntimeError('Frozen metadata update attempted: '+str(raw))
 sys.addaudithook(guard)
 try:
  yield before
 finally:
  try:
   if snapshot()!=before: raise RuntimeError('Installed metadata bytes changed during proxy construction')
  finally: state['active']=False


def bind_frozen_calendar(evidence, expected_sha256, required_through, column='date'):
 """Bind an independently collected, externally hash-pinned calendar offline."""
 import csv,io
 from datetime import date
 evidence=Path(evidence).resolve();raw=evidence.read_bytes()
 if not isinstance(expected_sha256,str) or hashlib.sha256(raw).hexdigest()!=expected_sha256:
  raise ValueError('Frozen calendar source hash mismatch')
 rows=list(csv.DictReader(io.StringIO(raw.decode('utf-8-sig'))))
 if not rows or column not in rows[0]:raise ValueError('Frozen calendar date column missing')
 days=[date.fromisoformat(row[column]) for row in rows]
 if days!=sorted(set(days)):raise ValueError('Frozen calendar must be unique and ordered')
 required=date.fromisoformat(str(required_through)[:10])
 if not days[0]<=required<=days[-1]:raise ValueError('Frozen calendar does not cover required boundary')
 def frozen(day):
  requested=day.date() if hasattr(day,'date') else day
  if not isinstance(requested,date) or not days[0]<=requested<=days[-1]:raise ValueError('Calendar request outside frozen coverage: '+str(day))
  if hashlib.sha256(evidence.read_bytes()).hexdigest()!=expected_sha256:raise ValueError('Frozen calendar source changed during consumption')
  return list(days)
 calendar=importlib.import_module('scripts.exchange_calendar');previous=calendar.sessions_for_day
 calendar.sessions_for_day=frozen
 # Heal already imported `from ... import sessions_for_day` aliases by identity,
 # without permitting any network fallback or changing production module files.
 for module in list(sys.modules.values()):
  if module is None:continue
  for key,value in list(vars(module).items()):
   if value is previous:vars(module)[key]=frozen
 return {'path':str(evidence),'sha256':expected_sha256,'rows':len(days),'first':str(days[0]),'last':str(days[-1])}


def main():
 if sys.flags.optimize: raise RuntimeError('Optimized Python disables evidence assertions; forbidden')
 p=argparse.ArgumentParser();p.add_argument('--runtime',type=Path,required=True);p.add_argument('--contract',type=Path,required=True);p.add_argument('--execute',action='store_true');p.add_argument('--workers',type=int,default=4);p.add_argument('--panels-manifest',type=Path);p.add_argument('--output-name',default='diagnostic_replay_20261029');a=p.parse_args()
 root=a.runtime.resolve(); a.contract=a.contract.resolve(); research=Path(__file__).resolve().parent
 if not root.is_relative_to(research/'replay'): raise RuntimeError('Only isolated research replay roots permitted')
 c=json.loads(a.contract.read_text(encoding='utf-8')); assert c['as_of']=='2026-10-29'
 for entry in c['required_evidence']:
  src=Path(entry['path']).resolve();entry['path']=str(src);assert src.is_relative_to(research);assert digest(src)==entry['sha256'],str(src)
 roles=[e['role'] for e in c['required_evidence']]
 assert len(roles)==len(set(roles)), 'Duplicate evidence role'
 assert {'base_assembly','ratio_install','st_install','independent_sessions'} <= set(roles)
 assert a.workers>=1, 'Worker count must be positive'
 # st_install must be the actual source agent coverage/installation report, not an approval substitute.
 sm=json.loads((root/'source_manifest.json').read_text(encoding='utf-8'))
 assert sm.get('approved') is False and sm.get('formal_written') is False, 'Diagnostic source identity invalid'
 if 'target_root' in sm: assert Path(sm['target_root']).resolve()==root
 for name,expected in sm['files'].items(): assert digest(contained(root,name))==expected,name
 if Path(a.output_name).name!=a.output_name: raise RuntimeError('Invalid output name')
 dest=root/a.output_name
 if a.panels_manifest: a.panels_manifest=a.panels_manifest.resolve()
 if dest.exists(): raise RuntimeError('Use a clean dedicated output directory; never overwrite prior replay')
 if not a.execute: print('Static evidence hash preflight passed; no replay executed');return
 os.chdir(root);sys.path.insert(0,str(root));sys.dont_write_bytecode=True;os.environ['PYTHONDONTWRITEBYTECODE']='1'
 os.environ['MPLCONFIGDIR']=str(root/'.mplconfig');os.environ['MPLBACKEND']='Agg'
 os.environ['TEMP']=str(root/'.tmp');os.environ['TMP']=str(root/'.tmp');(root/'.tmp').mkdir(exist_ok=True)
 offline_guard(root)
 for key in list(os.environ):
  if 'SHARED' in key and ('MICROCAP' in key or 'TOP100' in key): raise RuntimeError('Shared-cache environment forbidden: '+key)
 def no_network(*args,**kwargs): raise RuntimeError('Offline historical replay forbids network access')
 socket.socket.connect=no_network;socket.create_connection=no_network
 import pandas as pd
 import numpy as np
 sessions_entry=next(e for e in c['required_evidence'] if e['role']=='independent_sessions')
 calendar_binding=bind_frozen_calendar(sessions_entry['path'],sessions_entry['sha256'],c['as_of'],c.get('session_column','date'))
 v=importlib.import_module('microcap_top100_mom16_biweekly_live_v2_0');v23=importlib.import_module('microcap_top100_mom16_biweekly_live_v2_3');v25=importlib.import_module('microcap_top100_mom16_biweekly_live_v2_5')
 for mod in (v,v23,v25):
  assert Path(mod.__file__).resolve().is_relative_to(root),mod.__name__
 for module_name, module in list(sys.modules.items()):
  source_file=getattr(module,'__file__',None)
  if source_file and not (module_name in ('__main__','__mp_main__') and Path(source_file).resolve()==Path(__file__).resolve()):
   source_path=Path(source_file).resolve()
   if source_path.is_relative_to(research.parents[1]) and not source_path.is_relative_to(root):
    raise RuntimeError('Production module leaked into replay: '+module_name+' '+str(source_path))
 disabled_shared=isolate_modules(v,root)
 v._sync_embedded_base_config();base=v.base_mod
 sessions_entry=next(e for e in c['required_evidence'] if e['role']=='independent_sessions')
 sess=pd.read_csv(Path(sessions_entry['path']).resolve() if Path(sessions_entry['path']).is_absolute() else research.parents[1]/sessions_entry['path']);dates=pd.DatetimeIndex(pd.to_datetime(sess[c.get('session_column','date')],errors='raise')).normalize()
 assert dates.is_unique and dates.is_monotonic_increasing
 dates=dates[dates<=pd.Timestamp(c['as_of'])];assert dates.max()==pd.Timestamp(c['as_of'])
 panel=root/'mnt_strategy_data_cn.csv';paneldates=pd.DatetimeIndex(pd.to_datetime(pd.read_csv(panel,usecols=['date'])['date'],errors='raise'))
 assert paneldates.max()==dates.max();assert not dates[dates>=pd.Timestamp(v.freq_mod.START_DATE)].difference(paneldates).size
 symbols=v.freq_mod.load_universe();assert len(symbols)==5438 and len(set(symbols))==5438,len(symbols)
 dest.mkdir();args=argparse.Namespace(max_workers=a.workers,index_csv=dest/'proxy.csv',costed_nav_csv=dest/'base_costed.csv')
 from scripts.st_history_scope import validated_source_batch
 with frozen_metadata(v.freq_mod.SECURITY_META_DIR,symbols) as metadata_hashes:
  with validated_source_batch(root):
   cache_context=nullcontext()
   if a.panels_manifest:
    import parallel_panels_cache_20261101 as cache
    # Parquet caps are a serialized date->symbol mapping; pandas from_dict may
    # reorder rows while aligning sparse columns. Restore only chronological keys.
    original_merge=cache.merge_blocks
    def merge_with_ordered_caps(*args,**kwargs):
     original_read=cache.pd.read_parquet
     def ordered_read(path,*read_args,**read_kwargs):
      frame=original_read(path,*read_args,**read_kwargs)
      return frame.sort_index() if Path(path).name=='caps.parquet' else frame
     cache.pd.read_parquet=ordered_read
     try:return original_merge(*args,**kwargs)
     finally:cache.pd.read_parquet=original_read
    cache.merge_blocks=merge_with_ordered_caps
    cache_context=cache.consume(root,a.panels_manifest,v,metadata_hashes)
   with cache_context:
    index,members,turnover,meta=base.build_local_proxy_bundle(args,dates,symbols=symbols)
 # Only publish diagnostic artifacts after both source and metadata exit checks pass.
 index.to_csv(args.index_csv,index=False);members.to_csv(dest/'members.csv',index=False);turnover.to_csv(dest/'turnover.csv',index=False)
 (dest/'proxy_meta.json').write_text(json.dumps(meta,ensure_ascii=False,indent=2,default=str),encoding='utf-8')
 base.rebuild_costed_nav_from_proxy_turnover(args,{'proxy_turnover':dest/'turnover.csv'},panel,target_end_date=pd.Timestamp(c['as_of']))
 raw_close=base.load_close_df(panel,args.index_csv,max_date=pd.Timestamp(c['as_of']))
 cached_gross=base.run_signal(raw_close).sort_index()
 close=cached_gross[['microcap_close','hedge_close']].rename(columns={'microcap_close':'microcap','hedge_close':'hedge'})
 from scripts.top100_data_contracts import normalise_turnover_table
 turnover=normalise_turnover_table(pd.read_csv(dest/'turnover.csv'))
 gross=base.apply_momentum_gap_exit_buffer(base.run_signal(close).sort_index(),v.V2_0_MOMENTUM_GAP_EXIT_BUFFER)
 out20=v.overlay_mod.apply_v2_0_execution(gross,turnover)
 out23=v23.build_v2_3_result(close,turnover,v23.build_v2_3_common_index(close,official_index=out20.index))
 out25=v25.build_v2_5_result(close,turnover,v25.build_v2_5_common_index(close,official_index=out20.index))
 assert v23.OVERHEAT_ENABLED is False
 result={'calendar_binding':calendar_binding,'metadata_count':len(metadata_hashes),'metadata_manifest_sha256':hashlib.sha256(json.dumps(metadata_hashes,sort_keys=True,separators=(',',':')).encode()).hexdigest(),'scope':'diagnostic_only_not_approved_for_production','as_of':c['as_of'],'runner_sha256':digest(Path(__file__).resolve()),'contract_sha256':digest(a.contract),'source_manifest_sha256':digest(root/'source_manifest.json'),'streams':{},'disabled_shared_fallbacks':disabled_shared,'approved':False,'formal_written':False}
 if a.panels_manifest: result['panels_manifest']={'path':str(a.panels_manifest),'sha256':digest(a.panels_manifest),'caps_mapping_readback':'sort date keys only; values and file hashes unchanged'}
 for name,frame in [('v2_0',out20),('v2_3',out23),('v2_5',out25)]:
  assert frame.index.is_unique and frame.index.is_monotonic_increasing and frame.index.max()==pd.Timestamp(c['as_of'])
  assert frame.index.equals(dates[(dates>=frame.index.min()) & (dates<=frame.index.max())]), name+' missing sessions'
  assert np.isfinite(frame['return_net']).all() and np.isfinite(frame['nav_net']).all()
  path=dest/(name+'.csv');frame.to_csv(path,index_label='date');read=pd.read_csv(path);assert len(read)==len(frame)
  assert pd.DatetimeIndex(pd.to_datetime(read['date'])).equals(frame.index)
  for col in ['return_net','nav_net']:
   assert np.allclose(read[col],frame[col],rtol=1e-12,atol=1e-12),name+' CSV readback '+col
  result['streams'][name]={'rows':len(read),'latest':str(read['date'].iloc[-1]),'sha256':digest(path)}
 for name in ['proxy.csv','turnover.csv','base_costed.csv','members.csv']:
  f=pd.read_csv(dest/name);datecol='date' if 'date' in f else ('rebalance_date' if 'rebalance_date' in f else None)
  latest=str(pd.to_datetime(f[datecol]).max().date()) if datecol else None
  if name in ['proxy.csv','base_costed.csv']: assert latest==c['as_of'],name+' stale'
  result[name]={'rows':len(f),'latest_observation':latest,'date_column':datecol,'sha256':digest(dest/name)}
 result['proxy_meta.json']={'sha256':digest(dest/'proxy_meta.json')}
 result['panel']={'rows':len(paneldates),'latest':str(paneldates.max().date()),'sha256':digest(panel)}
 (dest/'diagnostic_result.json').write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding='utf-8');print(json.dumps(result,ensure_ascii=False))
if __name__=='__main__': main()
