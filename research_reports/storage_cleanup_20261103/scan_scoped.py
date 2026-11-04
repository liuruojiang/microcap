import os,json,time,datetime
from pathlib import Path
out=Path(__file__).resolve().parent;repo=out.parents[1]
roots=[p for p in (repo/'research_reports').iterdir() if p.is_dir() and p.name.startswith(('20261024','20261029','20261102','20261103')) and p!=out]
roots += [p for p in Path('D:/Codex/workspaces').glob('microcap-*') if p.is_dir() and 'tests-' in p.name]
roots += [Path('D:/Codex/workspaces/microcap-no-overheat-daily/.tmp-native-20261102')]
roots += [p for p in Path('D:/Codex/tmp').glob('microcap-*20261102*') if p.is_dir()]
stats={};errors=[];links=[];started=time.time()
for root in roots:
 if not root.exists():continue
 native='\\\\?\\'+str(root.resolve());stack=[native];total=count=0
 while stack:
  folder=stack.pop()
  try:
   with os.scandir(folder) as entries:
    for e in entries:
     try:
      s=e.stat(follow_symlinks=False)
      if getattr(s,'st_file_attributes',0)&0x400:links.append(e.path);continue
      if e.is_dir(follow_symlinks=False):stack.append(e.path);continue
      if not e.is_file(follow_symlinks=False):continue
      total+=s.st_size;count+=1
      parts=Path(os.path.relpath(e.path,native)).parts
      for depth in range(min(4,len(parts))):
       key=str(root.joinpath(*parts[:depth]));v=stats.setdefault(key,{'bytes':0,'files':0,'latest_mtime':0,'hardlinked_files':0});v['bytes']+=s.st_size;v['files']+=1;v['latest_mtime']=max(v['latest_mtime'],s.st_mtime);v['hardlinked_files']+=s.st_nlink>1
     except OSError as ex:errors.append([e.path,str(ex)])
  except OSError as ex:errors.append([folder,str(ex)])
 print(root.name,round(total/2**30,3),'GiB',count,'files',flush=True)
 result={'created_at':datetime.datetime.now().isoformat(),'elapsed_seconds':time.time()-started,'roots':[str(p) for p in roots],'stats':stats,'errors':errors,'skipped_reparse_points':links}
 (out/'scan.json').write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding='utf-8')
print('TOP DIRECTORIES')
for k,v in sorted(stats.items(),key=lambda x:x[1]['bytes'],reverse=True)[:35]:print(round(v['bytes']/2**30,3),v['files'],k,flush=True)
