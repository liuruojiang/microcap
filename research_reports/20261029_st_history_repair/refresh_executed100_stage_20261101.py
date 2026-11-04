"""Explicit real-source refresh of an isolated seeded100 stage. Never installs production."""
import argparse,datetime,hashlib,importlib,json,os,sys,traceback,uuid
from pathlib import Path
from run_three_version_diagnostic_20261101 import canonical_path,contained,digest,bind_frozen_calendar,offline_guard

def write_json(path,value):
 temporary=path.with_name(path.name+'.'+uuid.uuid4().hex+'.tmp')
 temporary.write_text(json.dumps(value,ensure_ascii=False,indent=2,allow_nan=False),encoding='utf-8');temporary.replace(path)

def stage_write_guard(stage):
 stage=canonical_path(stage)
 def audit(event,args):
  if event in ('subprocess.Popen','os.system'):raise RuntimeError('Stage refresh cannot spawn external processes')
  paths=[]
  if event=='open' and isinstance(args[0],(str,bytes,os.PathLike)):
   mode,flags=args[1],args[2]
   if (isinstance(mode,str) and any(c in mode for c in 'wax+')) or (flags&(os.O_WRONLY|os.O_RDWR|os.O_CREAT|os.O_TRUNC)):paths=[args[0]]
  elif event in ('os.remove','os.rmdir','os.mkdir','os.rename'):paths=args[:2] if event=='os.rename' else args[:1]
  for raw in paths:
   if isinstance(raw,(str,bytes,os.PathLike)) and not canonical_path(raw).is_relative_to(stage):raise RuntimeError('Write outside isolated stage')
 sys.addaudithook(audit)

def main():
 p=argparse.ArgumentParser(description=__doc__)
 for name in ('runtime','stage','proof','calendar'):p.add_argument('--'+name,type=Path,required=True)
 p.add_argument('--calendar-sha256',required=True);p.add_argument('--execute',action='store_true');p.add_argument('--resume',action='store_true');a=p.parse_args()
 root=canonical_path(a.runtime);stage=canonical_path(a.stage);proof_path=a.proof.resolve();calendar=a.calendar.resolve();research=Path(__file__).resolve().parent
 if not root.is_relative_to(research/'replay') or not stage.is_relative_to(research) or stage.is_relative_to(root) or root.is_relative_to(stage):raise ValueError('Separate research stage required')
 seed_path=stage/'daily_stage_seed.json';seed=json.loads(seed_path.read_text(encoding='utf-8'));seed['files']={Path(k).as_posix():v for k,v in seed['files'].items()};proof=json.loads(proof_path.read_text(encoding='utf-8'));symbols=seed['symbols']
 if len(symbols)!=100 or len(set(symbols))!=100 or seed.get('target')!='2026-10-30' or seed.get('approved') is not False:raise ValueError('Invalid stage seed')
 if digest(proof_path)!=seed['execution_proof_sha256'] or proof.get('member_derivation') not in ('actual_reconstruct_effective_members_from_saved_targets','actual_simulate_rebalance_path_final_current_members') or proof.get('effective_members_sha256')!=seed['executed_members_sha256']:raise ValueError('Execution proof differs from seed')
 source_manifest=root/'source_manifest.json';source=json.loads(source_manifest.read_text(encoding='utf-8'))
 if source.get('approved') is not False or source.get('formal_written') is not False:raise ValueError('Invalid frozen source scope')
 for name,expected in source['files'].items():
  if digest(contained(root,name))!=expected:raise ValueError('Frozen source changed: '+name)
 def check_source():
  for name,expected in seed['frozen_source_identity'].items():
   if digest(root/name)!=expected:raise ValueError('Frozen ST/code identity changed')
  if digest(seed_path)!=seed_sha or digest(proof_path)!=proof_sha:raise ValueError('Seed/proof changed')
 seed_sha=digest(seed_path);proof_sha=digest(proof_path);check_source()
 state_path=stage/'daily_refresh_state.json'
 if state_path.exists() and not a.resume:raise ValueError('Prior attempt exists; inspect and explicitly --resume')
 state=json.loads(state_path.read_text(encoding='utf-8')) if state_path.exists() else dict(schema_version=1,approved=False,formal_written=False,target='2026-10-30',seed_sha256=seed_sha,execution_proof_sha256=proof_sha,raw={},ratio={},attempts=[])
 if state['seed_sha256']!=seed_sha:raise ValueError('Cannot resume another seed')
 if digest(calendar)!=a.calendar_sha256:raise ValueError('Calendar evidence hash mismatch')
 if not a.execute:print('Inputs prepared; no real source request, no data-ready claim');return
 os.chdir(stage);sys.path.insert(0,str(root));sys.dont_write_bytecode=True;os.environ['MPLCONFIGDIR']=str(stage/'.mplconfig');os.environ['MPLBACKEND']='Agg';stage_write_guard(stage)
 binding=bind_frozen_calendar(calendar,a.calendar_sha256,'2026-10-30')
 import pandas as pd
 from scripts.stock_daily_inputs import refresh_daily_inputs
 from scripts.stock_return_pipeline import load_return_prices
 from scripts.stock_tail_returns import load_tail_returns
 for module_name in ('scripts.stock_daily_inputs','scripts.stock_return_pipeline','scripts.stock_return_basis_store','scripts.stock_tail_returns'):
  if not Path(sys.modules[module_name].__file__).resolve().is_relative_to(root):raise ValueError('External implementation imported')
 cache=stage/'.microcap_index_cache';master=pd.read_csv(cache/'security_master.csv',dtype={'symbol':str},parse_dates=['list_date','delist_date'])
 if digest(cache/'security_master.csv')!=seed['files']['.microcap_index_cache/security_master.csv']:raise ValueError('Master changed')
 attempt=dict(started_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),status='running',phase='raw');state['attempts'].append(attempt);write_json(state_path,state)
 try:
  # First inspect/validate existing source evidence without network, including resume.
  pending=[]
  for symbol in symbols:
   try:result=refresh_daily_inputs(root=stage,symbols=[symbol],as_of='2026-10-30',allow_network=False)[0]
   except ValueError as exc:
    if str(exc)!='Daily trade status requires a real source refresh':raise
    pending.append(symbol)
   else:state['raw'][symbol]=result
  write_json(state_path,state)
  # One bounded provider connection; its per-symbol attempts persist before publication.
  if pending:
   for result in refresh_daily_inputs(root=stage,symbols=pending,as_of='2026-10-30',allow_network=True):state['raw'][result['symbol']]=result
  write_json(state_path,state);attempt['phase']='ratio'
  for symbol in symbols:
   frame=load_return_prices(root=stage,symbol=symbol,raw_path=cache/'prices_raw'/(symbol+'.csv'),master=master,as_of='2026-10-30',allow_network=True)
   state['ratio'][symbol]=dict(rows=len(frame),manifest_sha256=digest(cache/'return_basis/manifests'/(symbol+'.json')))
   write_json(state_path,state)
  # Every subsequent validation is now process-wide offline.
  attempt['phase']='offline_validation';write_json(state_path,state)
  def deny_network(event,args):
   if event in ('socket.connect','socket.getaddrinfo','subprocess.Popen','os.system'):raise RuntimeError('Offline final stage validation forbids network/process')
  sys.addaudithook(deny_network)
  records=[];taildates=pd.to_datetime(['2026-10-29','2026-10-30'])
  for symbol in symbols:
   raw=cache/'prices_raw'/(symbol+'.csv');ratio=cache/'return_basis';oldpath=root/'.microcap_index_cache/return_basis/manifests'/(symbol+'.json')
   old=json.loads(oldpath.read_bytes());newpath=ratio/'manifests'/(symbol+'.json');new=json.loads(newpath.read_bytes())
   if digest(oldpath)!=seed['files']['.microcap_index_cache/return_basis/manifests/'+symbol+'.json']:raise ValueError('Original29 ratio identity changed')
   oldbundle=ratio/'bundles'/symbol/hashlib.sha256(json.dumps(old,sort_keys=True).encode()).hexdigest();oldraw=pd.read_csv(oldbundle/'raw.csv',dtype={'date':str});newraw=pd.read_csv(raw,dtype={'date':str})
   original=oldraw.set_index('date').close_raw;current=newraw.set_index('date').close_raw
   if not original.index.isin(current.index).all() or not (original.to_numpy()==current.reindex(original.index).to_numpy()).all():raise ValueError('Raw historical prefix changed')
   if new['anchor_date']!=old['anchor_date'] or new['as_of']!='2026-10-30' or new['return_basis_revision']!=old['return_basis_revision']:raise ValueError('Ratio basis identity changed')
   ret=load_tail_returns(root=stage,symbol=symbol,raw_path=raw,master=master,tail_dates=taildates)
   records.append(dict(symbol=symbol,return_20261030=float(ret.iloc[-1]),raw_sha256=digest(raw),status_sha256=digest(cache/'trade_status'/(symbol+'.json')),ratio_manifest_sha256=digest(newpath),old_raw_prefix_unchanged=True))
  check_source()
  # Include every portable status binding and active ratio bundle/source; lock/attempt files are not authority.
  files={}
  def add(path):files[str(path.relative_to(stage))]=digest(path)
  def bound(value,base):
   if isinstance(value,dict):
    if 'path' in value and 'sha256' in value:
     path=(base/value['path']).resolve()
     if not path.is_relative_to(base.resolve()) or digest(path)!=value['sha256']:raise ValueError('Status evidence hash mismatch')
     add(path)
    else:
     for item in value.values():bound(item,base)
   elif isinstance(value,list):
    for item in value:bound(item,base)
  add(cache/'security_master.csv')
  for symbol in symbols:
   for rel in ['prices_raw/'+symbol+'.csv','trade_status/'+symbol+'.json','return_basis/manifests/'+symbol+'.json','return_basis/prices_adj/'+symbol+'.csv']:add(cache/rel)
   bound(json.loads((cache/'trade_status'/(symbol+'.json')).read_bytes()),cache/'trade_status')
   m=json.loads((cache/'return_basis/manifests'/(symbol+'.json')).read_bytes());bundle=cache/'return_basis/bundles'/symbol/hashlib.sha256(json.dumps(m,sort_keys=True).encode()).hexdigest()
   for name in ('raw.csv','candidate.csv','manifest.json','qfq.js','qfq.json','hfq.js','hfq.json'):add(bundle/name)
   for name in ('qfq.js','qfq.json','hfq.js','hfq.json'):add(cache/'return_basis/sources'/symbol/name)
  final=dict(scope='isolated real-source100 inputs verified; not installed production',approved=False,formal_written=False,share_approved=False,target='2026-10-30',seed_sha256=seed_sha,execution_proof_sha256=proof_sha,calendar_binding=binding,frozen_source_identity=seed['frozen_source_identity'],symbols=symbols,records=records,portable_files=files)
  write_json(stage/'daily_inputs_verified.json',final);attempt.update(status='verified',completed_utc=datetime.datetime.now(datetime.timezone.utc).isoformat());write_json(state_path,state);print('Verified100 isolated input closure; no production installation')
 except BaseException as exc:
  attempt.update(status='failed',error=repr(exc),completed_utc=datetime.datetime.now(datetime.timezone.utc).isoformat());write_json(state_path,state);raise
if __name__=='__main__':main()
