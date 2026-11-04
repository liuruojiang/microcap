"""Isolated research cache of actual load_cache_panels outputs; never production authority."""
import argparse,hashlib,importlib,json,os,subprocess,sys
from pathlib import Path
from contextlib import contextmanager
import pandas as pd
HERE=Path(__file__).resolve().parent

def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def objsha(x):return hashlib.sha256(json.dumps(x,sort_keys=True,separators=(',',':')).encode()).hexdigest()
def write(p,x):Path(p).write_text(json.dumps(x,ensure_ascii=False,indent=2),encoding='utf8')
def inputs(root):
 result={}
 for p in sorted((root/'.microcap_index_cache').rglob('*')):
  if not p.is_file() or any(x in ('security_meta','st_notice_sources','__pycache__') for x in p.relative_to(root).parts):continue
  result[p.relative_to(root).as_posix()]=sha(p)
 for name in ('mnt_strategy_data_cn.csv',):result[name]=sha(root/name)
 return result

def verify_files(root,files):
 for name,h in files.items():
  p=(root/name).resolve()
  if not p.is_relative_to(root) or sha(p)!=h:raise RuntimeError('Cache input/source identity changed: '+name)

def setup(root,contract):
 sys.path.insert(0,str(root));sys.dont_write_bytecode=True
 os.environ['PYTHONDONTWRITEBYTECODE']='1';os.environ['MPLCONFIGDIR']=str(root/'.mplconfig');os.environ['MPLBACKEND']='Agg';os.environ['TEMP']=str(root/'.tmp');os.environ['TMP']=str(root/'.tmp');(root/'.tmp').mkdir(exist_ok=True)
 import run_three_version_diagnostic_20261101 as runner
 c=json.loads(contract.read_bytes())
 for entry in c['required_evidence']:
  if sha(entry['path'])!=entry['sha256']:raise RuntimeError('Contract evidence changed')
 sm=json.loads((root/'source_manifest.json').read_bytes());verify_files(root,sm['files'])
 runner.offline_guard(root)
 calendar=next(x for x in c['required_evidence'] if x['role']=='independent_sessions');runner.bind_frozen_calendar(calendar['path'],calendar['sha256'],c['as_of'])
 v=importlib.import_module('microcap_top100_mom16_biweekly_live_v2_0')
 if Path(v.__file__).resolve().parent!=root:raise RuntimeError('Wrong actual strategy module')
 runner.isolate_modules(v,root);v._sync_embedded_base_config()
 days=pd.DatetimeIndex(pd.to_datetime(pd.read_csv(calendar['path']).date));days=days[(days>=pd.Timestamp(v.freq_mod.START_DATE))&(days<=pd.Timestamp(c['as_of']))]
 caps=v.base_mod.build_biweekly_rebalance_dates(days);symbols=sorted(v.freq_mod.load_universe())
 return runner,c,v,days,caps,symbols,calendar

def save_block(folder,panels,symbols,identity):
 folder.mkdir(exist_ok=False);ret,caps,buy,sell=panels;stats=dict(ret.attrs.get('symbol_load_stats',{}));ret.attrs={}
 files={}
 for name,frame in [('returns',ret),('buy',buy),('sell',sell),('caps',pd.DataFrame.from_dict(caps,orient='index'))]:
  p=folder/(name+'.parquet');frame.to_parquet(p);read=pd.read_parquet(p);pd.testing.assert_frame_equal(read,frame);files[p.name]=sha(p)
 out=dict(schema=1,status='verified_research_block',symbols=symbols,identity=identity,files=files,symbol_load_stats=stats);write(folder/'block.json',out);return out

def validate_partition(blocks,expected):
 seen=[]
 for b in blocks:seen+=b['symbols']
 if len(seen)!=len(set(seen)) or sorted(seen)!=sorted(expected):raise RuntimeError('Blocks overlap or omit universe symbols')

def merge_blocks(root,manifest,identity,dates,capdates,symbols):
 m=json.loads(Path(manifest).read_bytes())
 if m['identity']!=identity:raise RuntimeError('Panel cache identity mismatch')
 blocks=[];loaded=[];frames={k:[] for k in ['returns','buy','sell','caps']};failures={};unavailable=[]
 for spec in m['blocks']:
  p=(root/spec['path']).resolve()
  if not p.is_relative_to(root) or sha(p)!=spec['sha256']:raise RuntimeError('Block manifest hash/path mismatch')
  b=json.loads(p.read_bytes());blocks.append(b)
  if b['identity']!=identity or b['status']!='verified_research_block':raise RuntimeError('Mixed panel block identities')
  stats=b['symbol_load_stats']
  if stats.get('failure_count') or stats.get('failures'):raise RuntimeError('Failed symbol block')
  unavailable+=stats['legitimately_unavailable'];blockcols=None
  for key in frames:
   fp=p.parent/(key+'.parquet')
   if sha(fp)!=b['files'][fp.name]:raise RuntimeError('Panel bytes changed')
   f=pd.read_parquet(fp);expected_dates=capdates if key=='caps' else dates
   if not f.index.equals(expected_dates) or not f.columns.is_unique or not set(f.columns)<=set(b['symbols']):raise RuntimeError('Panel axes mismatch')
   if key=='returns':blockcols=set(f.columns);loaded+=list(f.columns)
   if key in ('buy','sell') and set(f.columns)!=blockcols:raise RuntimeError('Tradeability coverage differs')
   frames[key].append(f)
  if stats['requested_count']!=len(b['symbols']) or stats['loaded_count']!=len(blockcols) or blockcols|set(stats['legitimately_unavailable'])!=set(b['symbols']) or blockcols&set(stats['legitimately_unavailable']):raise RuntimeError('Block symbol accounting mismatch')
 validate_partition(blocks,symbols)
 if len(loaded)!=len(set(loaded)) or set(loaded)|set(unavailable)!=set(symbols):raise RuntimeError('Merged symbol coverage mismatch')
 merged={k:pd.concat(v,axis=1).reindex(columns=sorted(set().union(*(set(f.columns) for f in v)))) for k,v in frames.items()}
 ret=merged['returns'];ret.attrs['symbol_load_stats']=dict(requested_count=len(symbols),loaded_count=len(loaded),legitimately_unavailable_count=len(unavailable),legitimately_unavailable=sorted(unavailable),failure_count=0,failures={})
 caps={pd.Timestamp(d):{s:float(x) for s,x in row.items() if pd.notna(x)} for d,row in merged['caps'].iterrows()}
 return ret,caps,merged['buy'],merged['sell']

@contextmanager
def consume(root,manifest,v,metadata_hashes):
 m=json.loads(Path(manifest).read_bytes());identity=m['identity'];before=sha(manifest)
 if sha(root/'source_manifest.json')!=identity['source_manifest_sha256'] or objsha(metadata_hashes)!=identity['metadata_digest']:raise RuntimeError('Source/metadata differs from cached panels')
 if sha(__file__)!=identity['producer_sha256']:raise RuntimeError('Cache producer source changed')
 verify_files(root,identity['input_files']);verify_files(root,json.loads((root/'source_manifest.json').read_bytes())['files'])
 if sha(identity['calendar_path'])!=identity['calendar_sha256']:raise RuntimeError('Cache calendar changed')
 old=v.freq_mod.load_cache_panels;calls=[]
 def cached(symbols,trading_dates,cap_dates,max_workers=8,trade_constraint_mode=None,exclude_historical_st_from_caps=True):
  if list(trading_dates.strftime('%Y-%m-%d'))!=identity['dates'] or list(cap_dates.strftime('%Y-%m-%d'))!=identity['capdates'] or trade_constraint_mode!=identity['trade_constraint_mode'] or exclude_historical_st_from_caps is not True:raise RuntimeError('Cache request differs from actual base loader contract')
  calls.append(1);return merge_blocks(root,manifest,identity,trading_dates,cap_dates,sorted(symbols))
 v.freq_mod.load_cache_panels=cached
 try:yield
 finally:
  v.freq_mod.load_cache_panels=old
  if len(calls)!=1 or sha(manifest)!=before:raise RuntimeError('Cache consumption changed or not used exactly once')
  verify_files(root,identity['input_files'])

def main():
 p=argparse.ArgumentParser();p.add_argument('--runtime',type=Path,required=True);p.add_argument('--contract',type=Path,required=True);p.add_argument('--output-name',required=True);p.add_argument('--processes',type=int,default=4);p.add_argument('--worker',type=int);p.add_argument('--execute',action='store_true');a=p.parse_args();root=a.runtime.resolve();contract=a.contract.resolve()
 if not root.is_relative_to(HERE/'replay') or Path(a.output_name).name!=a.output_name or a.processes<1:raise RuntimeError('Invalid isolated cache destination')
 dest=root/a.output_name
 if not a.execute:print('Prepared only; no panel calculation or cache creation');return
 if a.worker is None:
  if dest.exists():raise RuntimeError('Never overwrite a cache generation')
  dest.mkdir();procs=[]
  for i in range(a.processes):
   log=(dest/('worker'+str(i)+'.log')).open('w',encoding='utf8');procs.append((subprocess.Popen([sys.executable,'-B','-X','utf8',__file__,'--runtime',str(root),'--contract',str(contract),'--output-name',a.output_name,'--processes',str(a.processes),'--worker',str(i),'--execute'],stdout=log,stderr=subprocess.STDOUT),log))
  exits=[]
  for proc,log in procs:exits.append(proc.wait());log.close()
  if any(exits):raise RuntimeError('Worker failed; no cache manifest published: '+str(exits))
  specs=[dict(path=(dest/('block'+str(i))/'block.json').relative_to(root).as_posix(),sha256=sha(dest/('block'+str(i))/'block.json')) for i in range(a.processes)];blocks=[json.loads((root/x['path']).read_bytes()) for x in specs];identity=blocks[0]['identity']
  if any(b['identity']!=identity for b in blocks):raise RuntimeError('Workers observed mixed inputs')
  validate_partition(blocks,identity['universe']);verify_files(root,identity['input_files']);write(dest/'manifest.json',dict(schema=1,scope='research_only_not_formal_refresh_or_approval',identity=identity,blocks=specs));print(dest/'manifest.json');return
 runner,c,v,dates,caps,symbols,calendar=setup(root,contract);assigned=symbols[a.worker::a.processes]
 from scripts.st_history_scope import validated_source_batch,resolve_archive_reference
 with runner.frozen_metadata(v.freq_mod.SECURITY_META_DIR,symbols) as mh:
  with validated_source_batch(root):
   inp=inputs(root);identity=dict(source_manifest_sha256=sha(root/'source_manifest.json'),producer_sha256=sha(__file__),contract_sha256=sha(contract),metadata_digest=objsha(mh),input_files=inp,archive_reference=resolve_archive_reference(root),calendar_path=calendar['path'],calendar_sha256=calendar['sha256'],dates=list(dates.strftime('%Y-%m-%d')),capdates=list(caps.strftime('%Y-%m-%d')),universe=symbols,trade_constraint_mode=v.base_mod.TRADE_CONSTRAINT_MODE)
   panels=v.freq_mod.load_cache_panels(assigned,dates,caps,max_workers=1,trade_constraint_mode=v.base_mod.TRADE_CONSTRAINT_MODE,exclude_historical_st_from_caps=True);verify_files(root,inp)
 # Cache block is written only after the immutable source and metadata contexts exit successfully.
 save_block(dest/('block'+str(a.worker)),panels,assigned,identity)
if __name__=='__main__':main()
