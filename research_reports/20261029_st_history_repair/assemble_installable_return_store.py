"""No-network installable store in research; no formal workspace mutation."""
from pathlib import Path
import json,hashlib,sys,shutil
import pandas as pd
B=Path(__file__).resolve().parent;sys.path.insert(0,str(B.parents[1]))
from scripts.stock_return_basis_store import load_or_build,_extended
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
mapping_path=B/'raw_install_normalized_20261029/install_manifest.json';mapping=json.loads(mapping_path.read_text(encoding='utf-8'))
assert sha(mapping_path)=='3f7f8f3aab8b97dddd219f3da140295efa70af2cc5cb0a12a128b470714e3416'
masterpath=B/'cross_source_official/security_master_combined_candidate.csv';assert sha(masterpath)==mapping['master_sha256']
master=pd.read_csv(masterpath,dtype={'symbol':str}).set_index('symbol');sessions=json.loads((B/'cross_source_prices/missing_session_review/exchange_sessions_20261028.json').read_text())
root=_extended(B/'installable_return_basis_20261029/return_basis');source=B/'return_basis_candidate_20261030/sources';records=[]
for r in mapping['records']:
 s=r['symbol'];folder=source/s;result=dict(symbol=s,status='pending_source')
 if not all((folder/(k+ext)).exists() for k in ['qfq','hfq'] for ext in ['.js','.json']):records.append(result);continue
 try:
  raw=Path(r['candidate_path']);assert sha(raw)==r['candidate_sha256'];row=master.loc[s]
  target=root/'sources'/s;target.mkdir(parents=True,exist_ok=True)
  for kind in ['qfq','hfq']:
   receipt=json.loads((folder/(kind+'.json')).read_text());assert receipt.get('retrieved_at_utc') and receipt['status']==200 and sha(folder/(kind+'.js'))==receipt['sha256']
   for ext in ['.js','.json']:
    old=folder/(kind+ext);dest=target/(kind+ext)
    if dest.exists():assert dest.read_bytes()==old.read_bytes(),'source changed after store assembly'
    else:shutil.copy2(old,dest)
  frame,manifest=load_or_build(basis_root=root,symbol=s,raw_path=raw,as_of='2026-10-29',sessions=sessions,list_date=str(row.list_date),delist_date=None if pd.isna(row.delist_date) else str(row.delist_date),allow_network=False)
  mp=root/'manifests'/f'{s}.json';bundle=root/'bundles'/s/hashlib.sha256(json.dumps(manifest,sort_keys=True).encode()).hexdigest()
  assert sha(bundle/'raw.csv')==r['candidate_sha256']
  result.update(status='assembled_candidate_not_approved',raw_sha256=r['candidate_sha256'],manifest_sha256=sha(mp),candidate_sha256=manifest['candidate_sha256'],rows=len(frame),last=manifest['last'],bundle=str(bundle))
 except Exception as exc:result.update(status='blocked',error=repr(exc))
 records.append(result)
 if len(records)%500==0:print(len(records),flush=True)
assert sha(masterpath)==mapping['master_sha256']
report=dict(formal_written=False,approved=False,network_requests=0,total=len(records),assembled=sum(r['status']=='assembled_candidate_not_approved' for r in records),records=records,raw_install_manifest_sha256=sha(mapping_path),master_sha256=sha(masterpath))
path=B/'installable_return_basis_20261029/assembly_manifest.json';path.write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf-8');print(report['total'],report['assembled'],len([r for r in records if r['status']=='blocked']))
