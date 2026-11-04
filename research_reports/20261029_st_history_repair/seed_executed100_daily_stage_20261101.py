"""Copy only an evidenced executed-100 input closure to a fresh research stage. No network."""
import argparse,hashlib,json,shutil
from pathlib import Path
import pandas as pd
sha=lambda b:hashlib.sha256(b).hexdigest()
def main():
 p=argparse.ArgumentParser(description=__doc__)
 for name in ('runtime','members','proof','stage'):p.add_argument('--'+name,type=Path,required=True)
 p.add_argument('--execute',action='store_true');a=p.parse_args()
 root=a.runtime.resolve();stage=a.stage.resolve();research=Path(__file__).resolve().parent
 if not root.is_relative_to(research/'replay') or not stage.is_relative_to(research) or stage.is_relative_to(root) or root.is_relative_to(stage):raise ValueError('Separate isolated research stage required')
 if stage.exists():raise ValueError('Use a new empty stage')
 proof=json.loads(a.proof.read_text(encoding='utf-8'));body=a.members.read_bytes()
 # This record must come from actual reconstructed members_after, not a target-list relabel.
 if proof.get('member_derivation') not in ('actual_reconstruct_effective_members_from_saved_targets','actual_simulate_rebalance_path_final_current_members') or proof.get('effective_members_sha256')!=sha(body) or proof.get('as_of')!='2026-10-29':raise ValueError('Executed member derivation proof missing')
 sm=root/'source_manifest.json'
 if proof.get('source_manifest_sha256')!=sha(sm.read_bytes()):raise ValueError('Execution source identity mismatch')
 members=pd.read_csv(a.members,dtype={'symbol':str})
 if len(members)!=100 or members.symbol.nunique()!=100 or set(members['rank'])!=set(range(1,101)) or set(members.as_of_date.astype(str))!={'2026-10-29'}:raise ValueError('Need100 actual dated ranked members')
 symbols=members.sort_values('rank').symbol.tolist()
 if any(len(s)!=6 or not s.isascii() or not s.isdigit() for s in symbols):raise ValueError('Invalid symbol')
 cache=root/'.microcap_index_cache';files={};bound={}
 def add(path):
  path=path.resolve()
  if not path.is_relative_to(root):raise ValueError('Escaping source closure')
  data=path.read_bytes();files[str(path.relative_to(root))]=sha(data)
 def walk(value,base):
  if isinstance(value,dict):
   if 'path' in value and 'sha256' in value:
    path=(base/value['path']).resolve()
    if not path.is_relative_to(base.resolve()) or sha(path.read_bytes())!=value['sha256']:raise ValueError('Bad status binding')
    add(path)
   else:
    for v in value.values():walk(v,base)
  elif isinstance(value,list):
   for v in value:walk(v,base)
 add(cache/'security_master.csv')
 for s in symbols:
  add(cache/'prices_raw'/f'{s}.csv');side=cache/'trade_status'/f'{s}.json';add(side);walk(json.loads(side.read_bytes()),cache/'trade_status')
  basis=cache/'return_basis';mp=basis/'manifests'/f'{s}.json';add(mp);m=json.loads(mp.read_bytes())
  if m.get('as_of')!='2026-10-29' or m.get('return_basis_revision')!='sina_ratio_ex_reference_v1':raise ValueError('Unexpected ratio seed')
  bundle=basis/'bundles'/s/sha(json.dumps(m,sort_keys=True).encode())
  for name in ('candidate.csv','manifest.json','raw.csv','qfq.js','qfq.json','hfq.js','hfq.json'):add(bundle/name)
  if sha((bundle/'raw.csv').read_bytes())!=m['raw_sha256']:raise ValueError('Raw anchor mismatch')
  add(basis/'prices_adj'/f'{s}.csv')
  for name in ('qfq.js','qfq.json','hfq.js','hfq.json'):add(basis/'sources'/s/name)
 for rel in ('source_manifest.json','.microcap_index_cache/st_notice_archive_pointer.json'):
  path=root/rel;bound[rel]=sha(path.read_bytes())
 report=dict(scope='research daily input seed only',approved=False,formal_written=False,as_of='2026-10-29',target='2026-10-30',symbols=symbols,executed_members_sha256=sha(body),execution_proof_sha256=sha(a.proof.read_bytes()),frozen_source_identity=bound,files=files,source_runtime=str(root))
 if not a.execute:print(json.dumps(dict(prepared_files=len(files),symbols=len(symbols),executed=False)));return
 stage.mkdir()
 for rel,expected in files.items():
  src=root/rel;dst=stage/rel
  if sha(src.read_bytes())!=expected:raise ValueError('Source changed during stage copy')
  dst.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(src,dst)
  if sha(dst.read_bytes())!=expected:raise ValueError('Copy verification failed')
 for rel,expected in bound.items():
  if sha((root/rel).read_bytes())!=expected:raise ValueError('Frozen source identity changed')
 (stage/'daily_stage_seed.json').write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf-8')
 print(json.dumps(dict(stage=str(stage),copied_files=len(files),symbols=100,network=False)))
if __name__=='__main__':main()
