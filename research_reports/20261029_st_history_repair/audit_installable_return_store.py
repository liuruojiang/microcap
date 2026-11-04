"""Read-only store audit against independently reconstructed final candidates."""
from pathlib import Path
import json,hashlib,datetime
B=Path(__file__).resolve().parent;root=B/'installable_return_basis_20261029/return_basis';fresh=B/'fresh_ratio_candidate_20261029'
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
mapping_path=B/'raw_install_normalized_20261029/install_manifest.json';mapping=json.loads(mapping_path.read_text(encoding='utf-8'))
audit=json.loads((fresh/'independent_audit.json').read_text(encoding='utf-8'));assert audit['passed']==5438 and not audit['failures']
records=[]
for r in mapping['records']:
 s=r['symbol'];entry=dict(symbol=s,status='passed')
 try:
  mp=root/'manifests'/f'{s}.json';before=mp.read_bytes();manifest=json.loads(before);expected=json.loads((fresh/'manifests'/f'{s}.json').read_text())
  receipts={k:json.loads((root/'sources'/s/(k+'.json')).read_text()) for k in ['qfq','hfq']}
  expected['source_package_verified_through']=None;expected['source_receipts_sha256']={k:hashlib.sha256(json.dumps(v,sort_keys=True,ensure_ascii=False).encode()).hexdigest() for k,v in receipts.items()}
  assert expected==manifest,'store/full independently audited candidate manifest differs'
  candidate=root/'prices_adj'/f'{s}.csv';assert sha(candidate)==manifest['candidate_sha256']==sha(fresh/'prices_adj'/f'{s}.csv')
  bundle=root/'bundles'/s/hashlib.sha256(json.dumps(manifest,sort_keys=True).encode()).hexdigest()
  assert sha(bundle/'raw.csv')==manifest['raw_sha256']==r['candidate_sha256']==sha(r['candidate_path'])
  assert json.loads((bundle/'manifest.json').read_text())==manifest and sha(bundle/'candidate.csv')==sha(candidate)
  paths=[mp,candidate,*bundle.iterdir()]
  for k in receipts:
   source=root/'sources'/s/(k+'.js');receipt=root/'sources'/s/(k+'.json');paths.extend([source,receipt])
   assert sha(source)==manifest[k+'_sha256']==sha(bundle/(k+'.js'))
   assert json.loads((bundle/(k+'.json')).read_text())==receipts[k]
   stamp=datetime.datetime.fromisoformat(receipts[k]['retrieved_at_utc'].replace('Z','+00:00'))
   assert datetime.datetime.fromisoformat('2026-10-29T15:00:00+08:00')<=stamp<=datetime.datetime.now(datetime.timezone.utc)
  files={str(p.relative_to(root)):sha(p) for p in paths};assert mp.read_bytes()==before
  assert all(sha(root/p)==digest for p,digest in files.items()),'store changed during audit'
  entry.update(manifest_sha256=sha(mp),candidate_sha256=sha(candidate),raw_sha256=manifest['raw_sha256'],bundle_identity=bundle.name,file_hashes=files)
 except Exception as exc:entry.update(status='failed',error=repr(exc))
 records.append(entry)
locks=list((root/'locks').glob('*.lock'))
report=dict(formal_written=False,approved=False,network_requests=0,total=len(records),passed=sum(r['status']=='passed' for r in records),failures=[r for r in records if r['status']!='passed'],locks=[str(p) for p in locks],records=records,raw_install_manifest_sha256=sha(mapping_path),master_sha256=sha(B/'cross_source_official/security_master_combined_candidate.csv'),independent_price_audit_sha256=sha(fresh/'independent_audit.json'))
p=B/'installable_return_basis_20261029/final_independent_audit.json';p.write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf-8');print(report['total'],report['passed'],len(report['failures']),len(locks))
if report['passed']==5438 and not locks:
 assembly=root.parent/'assembly_manifest.json';old=assembly.read_bytes();backup=root.parent/'assembly_manifest_before_calendar_scope_upgrade.json'
 if not backup.exists():backup.write_bytes(old)
 state=json.loads(old);lookup={r['symbol']:r for r in records}
 for row in state['records']:
  certified=lookup[row['symbol']];row['manifest_sha256']=certified['manifest_sha256'];row['bundle']=str(root/'bundles'/row['symbol']/certified['bundle_identity'])
 state['calendar_scope']='sessions_through_as_of_only';state['final_independent_audit_sha256']=sha(p)
 assembly.write_text(json.dumps(state,ensure_ascii=False,indent=2),encoding='utf-8')
