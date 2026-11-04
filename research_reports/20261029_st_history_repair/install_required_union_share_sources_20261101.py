"""Install completed share provenance without changing selected share CSVs or NAVs."""
from pathlib import Path
import json,hashlib,shutil,sys,os,copy
ROOT=Path(__file__).resolve().parents[2];sys.path.insert(0,str(ROOT))
import pandas as pd
import microcap_top100_mom16_biweekly_live_v2_0 as v
from scripts.share_change_source import verify_share_cache
from scripts.top100_delivery import COSTED
TASK=Path(__file__).resolve().parent;OUT=TASK/'required_union_share_completion_20261101';STAGE=OUT/'stage'
sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
def write(p,data):
 p.parent.mkdir(parents=True,exist_ok=True);tmp=p.with_name(p.name+'.share-proof.tmp');tmp.write_text(json.dumps(data,ensure_ascii=False,indent=2)+'\n',encoding='utf-8');os.replace(tmp,p)
def main():
 progress=json.loads((OUT/'progress.json').read_text(encoding='utf-8'))
 receipt=json.loads((OUT/'share_only_installation_receipt.json').read_text(encoding='utf-8'))
 if receipt['status']!='PASS' or receipt['verified_required_union']!=102:raise ValueError('Union verification incomplete')
 verified={s:verify_share_cache(root=ROOT,symbol=s,as_of='2026-10-30') for s in receipt['verified_symbols']}
 destination=ROOT/'.microcap_index_cache/share_sources';files=receipt['installed_source_files']
 meta_path=ROOT/'outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_meta.json';auth_path=ROOT/'outputs/microcap_top100_mom16_biweekly_live_v2_0_base_frozen_tail_authority.json'
 backup=OUT/'formal_before_binding';backup.mkdir(exist_ok=False)
 shutil.copy2(meta_path,backup/'proxy_meta.json');shutil.copy2(auth_path,backup/'authority.json')
 price_receipt=TASK/'target2_tail30_stage_20261101/installed.json'
 if not price_receipt.exists():raise ValueError('Missing installed price evidence')
 meta=json.loads(meta_path.read_bytes());authority=json.loads(auth_path.read_bytes());before_meta=copy.deepcopy(meta)
 v._sync_embedded_base_config();fingerprint=v.base_mod.stock_input_evidence_fingerprint();old=meta['core_params']['stock_input_evidence_fingerprint']
 if fingerprint['code']!=old['code']:raise ValueError('Unexpected source code change')
 if sha(meta_path)!=sha(backup/'proxy_meta.json') or sha(auth_path)!=sha(backup/'authority.json'):raise ValueError('Formal authority changed concurrently')
 meta['core_params']['stock_input_evidence_fingerprint']=fingerprint
 if not v.base_mod.proxy_meta_matches_execution_model(meta):raise ValueError('Proxy identity incompatible')
 write(meta_path,meta)
 review={'scope':'Required102 member union: additional003042/300514 share provenance and real30 raw/status/adjusted-price source extension; formal shareCSV/NAV unchanged','approved_by':'assistant executing existing explicit user data repair and local/cloud daily deployment authorization','user_separately_reviewed_generated_hashes':False,'previous_review_path':authority['approved_review_path'],'previous_review_sha256':authority['approved_review_sha256'],'previous_authority_sha256':sha(backup/'authority.json'),'before_meta_sha256':sha(backup/'proxy_meta.json'),'after_meta_sha256':sha(meta_path),'source_code_unchanged':True,'price_install_receipt_path':str(price_receipt.relative_to(ROOT)),'price_install_receipt_sha256':sha(price_receipt),'share_install_receipt_sha256':sha(OUT/'share_only_installation_receipt.json'),'before_stock_input_fingerprint':old,'after_stock_input_fingerprint':fingerprint,'baseline_approval_sha256':progress['approval_sha256'],'baseline_approval_transport_path':'.microcap_index_cache/share_sources/baselines/'+progress['approval_sha256']+'/approval.json','verified_count':len(verified),'verified_cutoff':'2026-10-30','source_states':{s:{'candidate_sha256':x['candidate_sha256'],'source_state_sha256':sha(destination/(s+'.json'))} for s,x in verified.items()},'selected_share_csv_and_nav_unchanged':True,'limitations':['Two additional selected historical baselines plus independently bound matching-anchor EM tails; not complete original-history source certification.','Prior100 baseline review remains unchanged; this supplement closes required executed/target/static membership union to102.','Economic share-count convention remains distinct from complete public-disclosure PIT certification.'],'whole_delivery_status':'pending final refresh-all/check/cloud validation; no additional email or trades'}
 review_path=ROOT/'docs/release_evidence_20261101/required_union_share_provenance_review.json';write(review_path,review)
 authority['previous_authority_sha256']=sha(backup/'authority.json');authority['approved_review_path']=review_path.relative_to(ROOT).as_posix();authority['approved_review_sha256']=sha(review_path);authority['seed_file_sha256']['proxy_meta']=v.base_mod._file_sha256(meta_path);write(auth_path,authority)
 for key,value in authority['seed_file_sha256'].items():
  if key!='proxy_meta' and value!=json.loads((backup/'authority.json').read_bytes())['seed_file_sha256'][key]:raise ValueError('Nonmeta seed changed')
 result={'status':'PASS','verified_required_union':len(verified),'cutoff':'2026-10-30','installed_source_file_count':len(files),'selected_csv_and_nav_hashes_unchanged':True,'meta_sha256':sha(meta_path),'authority_sha256':sha(auth_path),'review_sha256':sha(review_path),'pending_commit':['outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_meta.json','outputs/microcap_top100_mom16_biweekly_live_v2_0_base_frozen_tail_authority.json','docs/release_evidence_20261101/required_union_share_provenance_review.json']}
 write(OUT/'installation_receipt.json',result);print(json.dumps(result))
if __name__=='__main__':main()
