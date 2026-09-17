import copy,datetime,hashlib,io,json,shutil,sys,zipfile
from pathlib import Path
import pandas as pd
ROOT=Path(__file__).resolve().parents[2];sys.path.insert(0,str(ROOT))
import microcap_top100_mom16_biweekly_live_v2_0 as v
from scripts.restore_approved_top100_seed import BASE_FILES
D=Path(__file__).parent; C=D/'candidate'; review=C/'exact_hash_migration_review.json'
sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest()
r=json.loads(review.read_text(encoding='utf-8')); a=json.loads((C/'user_approval.json').read_text(encoding='utf-8'))
assert a['approved'] and a['approved_report_sha256']==sha(review)
for x in r['candidates'].values():assert sha(x['path'])==x['sha256']
for name,digest in r['diagnostic_artifact_hashes'].items():assert sha(C/name)==digest
v._sync_embedded_base_config();b=v.base_mod; args=v._build_base_args();paths=b.build_output_paths(b.DEFAULT_OUTPUT_PREFIX)
backup=ROOT/'.codex_backups'/('approved_migration_'+datetime.datetime.now().strftime('%Y%m%d_%H%M%S'));backup.mkdir(parents=True)
files=list((ROOT/'outputs').glob('microcap_top100*'))+[args.index_csv,ROOT/'outputs/top100_delivery_manifest.json']+list((ROOT/'.microcap_index_cache/realtime').glob('*static*'))
manifest={}
for src in files:
 if not src.is_file():continue
 rel=src.relative_to(ROOT);dst=backup/rel;dst.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(src,dst);assert sha(src)==sha(dst);manifest[str(rel)]=sha(src)
(backup/'manifest.json').write_text(json.dumps(manifest,indent=2),encoding='utf-8')
panel=paths['panel_refreshed'] if 'panel_refreshed' in paths else ROOT/'outputs/microcap_top100_mom16_biweekly_live_v2_0_base_panel_refreshed.csv'
# Derive the base costed stream using formal functions and approved candidate inputs.
temp_args=copy.copy(args);temp_args.index_csv=C/'candidate_proxy.csv';temp_args.costed_nav_csv=D/'approved_base_costed_candidate.csv'
temp_paths=dict(paths);temp_paths['proxy_turnover']=C/'candidate_turnover.csv'
b.rebuild_costed_nav_from_proxy_turnover(temp_args,temp_paths,panel,pd.Timestamp('2026-09-17'))
with zipfile.ZipFile(ROOT/'outputs/repair_20260917_cloud_evidence/cloud_20260916_whole.zip') as z:
 old=pd.read_csv(io.BytesIO(z.read('outputs/'+BASE_FILES['costed_nav'])),index_col='date',parse_dates=True)
new=pd.read_csv(temp_args.costed_nav_csv,index_col='date',parse_dates=True)
pd.testing.assert_frame_equal(old.loc[:'2026-09-03'],new.loc[:'2026-09-03'],check_dtype=False,rtol=1e-10,atol=1e-12)
for source,target in [(C/'candidate_proxy.csv',args.index_csv),(C/'candidate_turnover.csv',paths['proxy_turnover']),(C/'candidate_effective_members.csv',paths['proxy_effective_members']),(temp_args.costed_nav_csv,args.costed_nav_csv)]:
 shutil.copy2(source,target);assert sha(source)==sha(target)
meta=json.loads(paths['proxy_meta'].read_text(encoding='utf-8'))
assert b.proxy_meta_matches_execution_model(meta),'Metadata fingerprint drift: stop before authority update'
meta.update(source_used='approved_executed_member_continuation',continuation_bridge_date='2026-09-03',end_date='2026-09-17',approved_migration_review_sha256=sha(review))
paths['proxy_meta'].write_text(json.dumps(meta,ensure_ascii=False,indent=2),encoding='utf-8')
result={'approved_review_sha256':sha(review),'backup':str(backup),'base_frozen_prefix_through':'2026-09-03','base_frozen_prefix_unchanged':True,'files':{k:{'path':str(ROOT/'outputs'/name),'sha256':sha(ROOT/'outputs'/name)} for k,name in BASE_FILES.items()}}
(D/'base_migration_execution.json').write_text(json.dumps(result,indent=2),encoding='utf-8');print(json.dumps(result,indent=2))
