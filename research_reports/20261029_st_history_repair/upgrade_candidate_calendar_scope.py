"""Uniform pre-release metadata-only upgrade, preserving all old evidence."""
from pathlib import Path
import json,hashlib,os,shutil
B=Path(__file__).resolve().parent
sessions=json.loads((B/'cross_source_prices/missing_session_review/exchange_sessions_20261028.json').read_text())
def h(data):return hashlib.sha256(data).hexdigest()
def write(path,data):
 path.parent.mkdir(parents=True,exist_ok=True);tmp=path.with_suffix('.upgrade.tmp');tmp.write_bytes(data);tmp.replace(path)
report=[]
for name in ['fresh_ratio_candidate_20261029','installable_return_basis_20261029/return_basis']:
 root=B/name
 for mp in sorted((root/'manifests').glob('*.json')):
  before=mp.read_bytes();m=json.loads(before);expected=h('\n'.join(sorted(set(d for d in sessions if d<=m['as_of']))).encode())
  if m['sessions_sha256']==expected:continue
  assert m['sessions_sha256']==h('\n'.join(sorted(set(sessions))).encode()),'unknown calendar lineage'
  candidate=root/'prices_adj'/(mp.stem+'.csv');candidate_bytes=candidate.read_bytes();assert h(candidate_bytes)==m['candidate_sha256']
  archive=root/'pre_calendar_scope_upgrade/manifests'/mp.name;write(archive,before)
  old_identity=h(json.dumps(m,sort_keys=True).encode());m['sessions_sha256']=expected;new_identity=h(json.dumps(m,sort_keys=True).encode())
  if (root/'bundles').exists():
   old=root/'bundles'/mp.stem/old_identity;new=root/'bundles'/mp.stem/new_identity
   assert (old/'raw.csv').exists() and h((old/'raw.csv').read_bytes())==m['raw_sha256']
   new.mkdir(parents=True,exist_ok=True)
   for p in old.iterdir():
    if p.name=='manifest.json':continue
    target=new/p.name
    if target.exists():assert target.read_bytes()==p.read_bytes()
    else:
     try:os.link(p,target)
     except OSError:shutil.copy2(p,target)
   write(new/'manifest.json',json.dumps(m,ensure_ascii=False,sort_keys=True,indent=2).encode())
  write(mp,json.dumps(m,ensure_ascii=False,sort_keys=True,indent=2).encode())
  assert candidate.read_bytes()==candidate_bytes
  report.append(dict(root=name,symbol=mp.stem,old_manifest_sha256=h(before),new_manifest_sha256=h(mp.read_bytes()),candidate_sha256=h(candidate_bytes),values_unchanged=True))
 if root.exists():write(root/'calendar_scope_upgrade.json',json.dumps(dict(formal_written=False,records=[r for r in report if r['root']==name]),ensure_ascii=False,indent=2).encode())
print('metadata upgraded',len(report),'candidate bytes unchanged')
