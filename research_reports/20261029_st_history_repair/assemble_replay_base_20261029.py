from pathlib import Path
import json,hashlib,shutil,concurrent.futures,time
B=Path(__file__).resolve().parent;ROOT=B.parents[1];R=B/'replay/runtime_20261029';C=R/'.microcap_index_cache';R.mkdir(parents=True,exist_ok=True)
sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
rawp=B/'raw_install_normalized_20261029/install_manifest.json';sharep=B/'share_install_20261029/manifest.json';statusp=B/'trade_status_candidate/manifest.json'
raw=json.loads(rawp.read_text());shares=json.loads(sharep.read_text());status=json.loads(statusp.read_text());jobs=[]
for item in raw['records']:jobs.append((Path(item['candidate_path']),C/'prices_raw'/f"{item['symbol']}.csv",item['candidate_sha256']))
for item in shares['records']:jobs.append((Path(item['candidate_path']),C/'share_change'/f"{item['symbol']}.csv",item['candidate_sha256']))
base=B/'trade_status_candidate/.microcap_index_cache/trade_status'
for rel,digest in status['source_files'].items():jobs.append((base/rel,C/'trade_status'/rel,digest))
jobs.append((Path(status['master_path']),C/'security_master.csv',status['master_sha256']))
for name in ['active_universe.csv','current_st.csv','cninfo_a_org_map.csv','sz_name_change_short.csv']:
 src=ROOT/'.microcap_index_cache'/name
 jobs.append((src,C/name,sha(src)))
panel=ROOT/'outputs/microcap_top100_mom16_biweekly_live_v2_0_base_panel_refreshed.csv';jobs.append((panel,R/'mnt_strategy_data_cn.csv',sha(panel)))
def one(job):
 src,dest,digest=job;assert sha(src)==digest,(str(src),'source hash changed')
 dest.parent.mkdir(parents=True,exist_ok=True)
 if dest.exists():assert sha(dest)==digest,('existing runtime differs',str(dest))
 else:shutil.copyfile(src,dest)
 assert sha(dest)==digest
 return dict(path=dest.relative_to(R).as_posix(),sha256=digest,source_path=str(src.resolve()))
records=[]
with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
 for result in pool.map(one,jobs):
  records.append(result)
  if len(records)%1000==0:print(len(records),'/',len(jobs),flush=True)
report=dict(approved=False,formal_written=False,scope='isolated diagnostic base inputs only; ST and ratio supplied separately; share144 unresolved explicitly retained',target='2026-10-29',records=records,manifests={str(p.relative_to(B)):sha(p) for p in [rawp,sharep,statusp]},notes=['mnt_strategy_data_cn.csv is copied from local refreshed panel dated 2026-10-29; no stale official NAV is imported.','Current ST and name-map caches are dated inherited source inputs, not claimed freshly collected for 10/30.'])
p=R/'base_input_assembly.json';p.write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf-8');print({'complete':len(records),'manifest_sha256':sha(p)},flush=True)
