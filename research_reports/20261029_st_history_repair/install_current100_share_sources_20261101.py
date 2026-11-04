"""Install completed share provenance without changing selected share CSVs or NAVs."""
from pathlib import Path
import json,hashlib,shutil,sys,os,copy
ROOT=Path(__file__).resolve().parents[2];sys.path.insert(0,str(ROOT))
import pandas as pd
import microcap_top100_mom16_biweekly_live_v2_0 as v
from scripts.share_change_source import verify_share_cache
from scripts.top100_delivery import COSTED
TASK=Path(__file__).resolve().parent;OUT=TASK/'current100_share_completion_20261101';STAGE=OUT/'stage'
sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
def write(p,data):
 p.parent.mkdir(parents=True,exist_ok=True);tmp=p.with_name(p.name+'.share-proof.tmp');tmp.write_text(json.dumps(data,ensure_ascii=False,indent=2)+'\n',encoding='utf-8');os.replace(tmp,p)
def main():
 progress=json.loads((OUT/'progress.json').read_text(encoding='utf-8'))
 if progress.get('status')!='all100_verified_no_formal_csv_change' or len(progress['refreshed'])!=100:raise ValueError('Incomplete current100 source validation')
 symbols=list(progress['refreshed']);sources=STAGE/'.microcap_index_cache/share_sources';destination=ROOT/'.microcap_index_cache/share_sources'
 inputs={ROOT/'.microcap_index_cache/share_change'/f'{s}.csv':sha(ROOT/'.microcap_index_cache/share_change'/f'{s}.csv') for s in symbols}
 inputs.update({ROOT/'outputs'/name:sha(ROOT/'outputs'/name) for name in COSTED.values()})
 for symbol in symbols:
  verify_share_cache(root=STAGE,symbol=symbol,as_of='2026-10-30')
  if sha(STAGE/'.microcap_index_cache/share_change'/f'{symbol}.csv')!=inputs[ROOT/'.microcap_index_cache/share_change'/f'{symbol}.csv']:raise ValueError('Selected share bytes differ')
 files={p.relative_to(sources).as_posix():sha(p) for p in sources.rglob('*') if p.is_file()}
 if any(key.startswith('locks/') or key.startswith('incomplete/') or key=='RATE_LIMIT_STOP.json' for key in files):raise ValueError('Incomplete share source stage')
 for rel,expected in files.items():
  dst=destination/rel
  if dst.exists() and sha(dst)!=expected:raise ValueError('Existing formal source conflict '+rel)
 meta_path=ROOT/'outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_meta.json';auth_path=ROOT/'outputs/microcap_top100_mom16_biweekly_live_v2_0_base_frozen_tail_authority.json'
 backup=OUT/'formal_before';backup.mkdir(exist_ok=False)
 shutil.copy2(meta_path,backup/'proxy_meta.json');shutil.copy2(auth_path,backup/'authority.json')
 write(backup/'plan.json',{'installed_source_files':files,'unchanged_input_sha256':{str(p.relative_to(ROOT)):h for p,h in inputs.items()},'previous_state_files':sorted(p.name for p in destination.glob('*.json'))})
 for rel,expected in files.items():
  src=sources/rel;dst=destination/rel
  if not dst.exists():
   dst.parent.mkdir(parents=True,exist_ok=True);tmp=dst.with_name(dst.name+'.share-install.tmp');shutil.copy2(src,tmp)
   if sha(tmp)!=expected:raise ValueError('Source copy changed')
   os.replace(tmp,dst)
 verified={s:verify_share_cache(root=ROOT,symbol=s,as_of='2026-10-30') for s in symbols}
 if any(sha(p)!=h for p,h in inputs.items()):raise ValueError('Share CSV or NAV changed during source installation')
 meta=json.loads(meta_path.read_bytes());authority=json.loads(auth_path.read_bytes());before_meta=copy.deepcopy(meta)
 v._sync_embedded_base_config();fingerprint=v.base_mod.stock_input_evidence_fingerprint();old=meta['core_params']['stock_input_evidence_fingerprint']
 if fingerprint['code']!=old['code'] or fingerprint['file_count']!=old['file_count']+100:raise ValueError('Expected only100 new share source state identities')
 if sha(meta_path)!=sha(backup/'proxy_meta.json') or sha(auth_path)!=sha(backup/'authority.json'):raise ValueError('Formal authority changed concurrently')
 meta['core_params']['stock_input_evidence_fingerprint']=fingerprint
 if not v.base_mod.proxy_meta_matches_execution_model(meta):raise ValueError('Proxy identity incompatible')
 write(meta_path,meta)
 review={'scope':'Current100 existing economic share baseline provenance plus real contiguous CNInfo 2026-10-30 queries; no selected share CSV or NAV changes','approved_by':'assistant executing existing explicit user data repair and local/cloud daily deployment authorization','user_separately_reviewed_generated_hashes':False,'previous_review_path':authority['approved_review_path'],'previous_review_sha256':authority['approved_review_sha256'],'previous_authority_sha256':sha(backup/'authority.json'),'before_meta_sha256':sha(backup/'proxy_meta.json'),'after_meta_sha256':sha(meta_path),'source_code_unchanged':True,'before_stock_input_fingerprint':old,'after_stock_input_fingerprint':fingerprint,'baseline_approval_sha256':progress['approval_sha256'],'baseline_approval_transport_path':'.microcap_index_cache/share_sources/baselines/'+progress['approval_sha256']+'/approval.json','verified_count':len(verified),'verified_cutoff':'2026-10-30','source_states':{s:{'candidate_sha256':x['candidate_sha256'],'source_state_sha256':sha(destination/(s+'.json'))} for s,x in verified.items()},'selected_share_csv_and_nav_unchanged':True,'limitations':['95 selected historical baselines plus independently bound matching-anchor EM tails; not complete original-history source certification.','5 selected EM histories retained with CNInfo29 absence-of-new-event evidence. Existing historical cross-source differences for000929/002529 are retained, not relabelled resolved.','Economic share-count convention remains distinct from complete public-disclosure PIT certification.'],'whole_delivery_status':'pending final refresh-all/check/cloud validation; no additional email or trades'}
 review_path=ROOT/'docs/release_evidence_20261101/current100_share_provenance_review.json';write(review_path,review)
 authority['previous_authority_sha256']=sha(backup/'authority.json');authority['approved_review_path']=review_path.relative_to(ROOT).as_posix();authority['approved_review_sha256']=sha(review_path);authority['seed_file_sha256']['proxy_meta']=v.base_mod._file_sha256(meta_path);write(auth_path,authority)
 for key,value in authority['seed_file_sha256'].items():
  if key!='proxy_meta' and value!=json.loads((backup/'authority.json').read_bytes())['seed_file_sha256'][key]:raise ValueError('Nonmeta seed changed')
 result={'status':'PASS','verified_current100':len(verified),'cutoff':'2026-10-30','installed_source_file_count':len(files),'selected_csv_and_nav_hashes_unchanged':True,'meta_sha256':sha(meta_path),'authority_sha256':sha(auth_path),'review_sha256':sha(review_path),'pending_commit':['outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_meta.json','outputs/microcap_top100_mom16_biweekly_live_v2_0_base_frozen_tail_authority.json','docs/release_evidence_20261101/current100_share_provenance_review.json']}
 write(OUT/'installation_receipt.json',result);print(json.dumps(result))
if __name__=='__main__':main()
