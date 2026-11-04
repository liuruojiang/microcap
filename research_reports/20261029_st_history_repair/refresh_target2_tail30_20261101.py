"""Two target-only source refresh, isolated and backed up; no NAV/authority writes."""
import argparse,hashlib,json,os,shutil,sys
from pathlib import Path
import pandas as pd
ROOT=Path(__file__).resolve().parents[2];HERE=Path(__file__).resolve().parent;STAGE=HERE/'target2_tail30_stage_20261101';SYMS=['003042','300514'];sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
def write(p,x):p.write_text(json.dumps(x,ensure_ascii=False,indent=2),encoding='utf8')
def closure(root):
 cache=root/'.microcap_index_cache';files={}
 def add(p):files[p.relative_to(root).as_posix()]=sha(p)
 def bound(v):
  if isinstance(v,dict):
   if 'path' in v and 'sha256' in v:
    p=(cache/'trade_status'/v['path']).resolve();assert p.is_relative_to((cache/'trade_status').resolve()) and sha(p)==v['sha256'];add(p)
   else:
    for x in v.values():bound(x)
  elif isinstance(v,list):
   for x in v:bound(x)
 for s in SYMS:
  for rel in [f'prices_raw/{s}.csv',f'trade_status/{s}.json',f'return_basis/manifests/{s}.json',f'return_basis/prices_adj/{s}.csv']:add(cache/rel)
  bound(json.loads((cache/'trade_status'/f'{s}.json').read_bytes()));m=json.loads((cache/'return_basis/manifests'/f'{s}.json').read_bytes());b=cache/'return_basis/bundles'/s/hashlib.sha256(json.dumps(m,sort_keys=True).encode()).hexdigest()
  for n in ['raw.csv','candidate.csv','manifest.json','qfq.js','qfq.json','hfq.js','hfq.json']:
   if (b/n).exists():add(b/n)
   elif n=='raw.csv':raise RuntimeError('Missing original raw bundle snapshot')
  for n in ['qfq.js','qfq.json','hfq.js','hfq.json']:add(cache/'return_basis/sources'/s/n)
 return files

def main():
 a=argparse.ArgumentParser();a.add_argument('phase',choices=['refresh','install']);a=a.parse_args();sys.path.insert(0,str(ROOT));sys.dont_write_bytecode=True
 if a.phase=='refresh':
  assert not STAGE.exists() or not any(STAGE.iterdir());STAGE.mkdir(exist_ok=True);files=closure(ROOT);files['.microcap_index_cache/security_master.csv']=sha(ROOT/'.microcap_index_cache/security_master.csv');navs={p.relative_to(ROOT).as_posix():sha(p) for p in (ROOT/'outputs').glob('*v2*nav.csv')}
  for rel,h in files.items():
   p=STAGE/rel;p.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(ROOT/rel,p);assert sha(p)==h
  write(STAGE/'seed.json',{'symbols':SYMS,'files':files,'formal_navs':navs});os.environ['MPLCONFIGDIR']=str(STAGE/'.mplconfig');os.chdir(STAGE)
  from refresh_executed100_stage_20261101 import stage_write_guard
  from run_three_version_diagnostic_20261101 import bind_frozen_calendar
  stage_write_guard(STAGE);bind_frozen_calendar(HERE/'replay/independent_sessions_20261030.csv','e602ed766beaf8aab4c45cdfc2d8cad6ea182bcc08024142598a4bee96bb1fe2','2026-10-30')
  from scripts.stock_daily_inputs import refresh_daily_inputs
  from scripts.stock_return_pipeline import load_return_prices
  from scripts.stock_tail_returns import load_tail_returns
  cache=STAGE/'.microcap_index_cache';master=pd.read_csv(cache/'security_master.csv',dtype={'symbol':str},parse_dates=['list_date','delist_date']);rawresults=refresh_daily_inputs(root=STAGE,symbols=SYMS,as_of='2026-10-30',allow_network=True);records=[]
  for s in SYMS:
   frame=load_return_prices(root=STAGE,symbol=s,raw_path=cache/'prices_raw'/f'{s}.csv',master=master,as_of='2026-10-30',allow_network=True)
   old=pd.read_csv(ROOT/'.microcap_index_cache/prices_raw'/f'{s}.csv',index_col='date');new=pd.read_csv(cache/'prices_raw'/f'{s}.csv',index_col='date');assert old.index.isin(new.index).all();pd.testing.assert_frame_equal(old,new.loc[old.index],check_exact=True)
   oldadj=pd.read_csv(ROOT/'.microcap_index_cache/return_basis/prices_adj'/f'{s}.csv',index_col='date');newadj=frame.set_index('date');newadj.index=newadj.index.strftime('%Y-%m-%d');assert oldadj.index.isin(newadj.index).all();assert abs(oldadj.close_adj-newadj.loc[oldadj.index].close_adj).max()<1e-10
   ret=load_tail_returns(root=STAGE,symbol=s,raw_path=cache/'prices_raw'/f'{s}.csv',master=master,tail_dates=pd.to_datetime(['2026-10-29','2026-10-30']));m=json.loads((cache/'return_basis/manifests'/f'{s}.json').read_bytes());assert m['as_of']=='2026-10-30';records.append({'symbol':s,'rows':len(frame),'last':str(frame.date.max().date()),'raw_prefix_exact':True,'adjusted_prefix_max_abs':float(abs(oldadj.close_adj-newadj.loc[oldadj.index].close_adj).max()),'tail_return':float(ret.iloc[-1]),'manifest':m})
  output=closure(STAGE);write(STAGE/'verified.json',{'status':'PASS','symbols':SYMS,'target':'2026-10-30','raw_results':rawresults,'records':records,'files':output,'formal_written':False});print('isolated two-symbol source refresh PASS')
 else:
  seed=json.loads((STAGE/'seed.json').read_bytes());verified=json.loads((STAGE/'verified.json').read_bytes());assert verified['status']=='PASS' and verified['symbols']==SYMS;backup=STAGE/'production_before_install';backup.mkdir();changes={}
  for rel,h in seed['formal_navs'].items():assert sha(ROOT/rel)==h
  for rel,h in verified['files'].items():
   assert sha(STAGE/rel)==h
   p=ROOT/rel;old=sha(p) if p.exists() else None
   if old==h:continue
   if rel in seed['files']:assert old==seed['files'][rel]
   if p.exists():
    b=backup/rel;b.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(p,b);assert sha(b)==old
   changes[rel]={'before':old,'after':h}
  write(STAGE/'install_preflight.json',changes)
  for rel,item in changes.items():
   p=ROOT/rel;p.parent.mkdir(parents=True,exist_ok=True);t=p.with_name(p.name+'.target2-tmp');shutil.copyfile(STAGE/rel,t);assert sha(t)==item['after'];os.replace(t,p)
  for rel,h in verified['files'].items():assert sha(ROOT/rel)==h
  for rel,h in seed['formal_navs'].items():assert sha(ROOT/rel)==h
  write(STAGE/'installed.json',{'status':'PASS','symbols':SYMS,'changed_files':changes,'formal_navs_unchanged':True,'meta_authority_changed':False});print('Installed two-symbol sources only; changes',len(changes))
if __name__=='__main__':main()
