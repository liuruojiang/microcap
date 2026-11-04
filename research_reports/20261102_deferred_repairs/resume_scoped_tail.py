from pathlib import Path
import copy,datetime,hashlib,io,json,os,runpy,sys
import pandas as pd
root=Path.cwd();sys.path.insert(0,str(root));os.environ['MICROCAP_DAILY_STOCK_PROVIDER']='tencent'
import microcap_top100_mom16_biweekly_live_v2_0 as v
from scripts import top100_delivery as delivery
from scripts.st_metadata_refresh import complete_terminal_return_dates,preserve_verified_replay_prefix,_atomic
from scripts.top100_data_contracts import normalise_turnover_table
v._sync_embedded_base_config();b=v.base_mod;a=v._build_base_args(max_workers=8);paths=b.build_output_paths(b.DEFAULT_OUTPUT_PREFIX)
stage=root/'.microcap_index_cache/scoped_proxy_rebuild/54349c3e368f478eb2b360b784b4e50e';s=copy.copy(a);s.index_csv=stage/a.index_csv.name;s.costed_nav_csv=stage/a.costed_nav_csv.name;sp={k:stage/p.name for k,p in paths.items()}
keys=('proxy_members','proxy_turnover','proxy_meta','proxy_effective_members');originals={p:(stage/('before_'+p.name)).read_bytes() for p in [a.index_csv,a.costed_nav_csv,*[paths[k] for k in keys]]}
for p,raw in originals.items():assert p.read_bytes()==raw,'Primary output changed'
meta=json.loads(sp['proxy_meta'].read_bytes());old_code=meta['core_params']['stock_input_evidence_fingerprint']['code'];new_code=b.stock_input_evidence_fingerprint()['code'];assert {k:x for k,x in old_code.items() if k!='scripts/st_metadata_refresh.py'}=={k:x for k,x in new_code.items() if k!='scripts/st_metadata_refresh.py'}
assert meta['core_params']['security_meta_cache_fingerprint']==b.security_meta_cache_fingerprint()
panel=root/'outputs/microcap_top100_mom16_biweekly_live_v2_0_base_panel_refreshed.csv';dates=pd.DatetimeIndex(pd.to_datetime(pd.read_csv(panel)['date']));anchor=pd.Timestamp('2026-10-29')
for oldp,newp in [(a.index_csv,s.index_csv),(a.costed_nav_csv,s.costed_nav_csv)]:
 preserved=stage/('recomputed_'+newp.name)
 if not preserved.exists():preserved.write_bytes(newp.read_bytes())
 preserve_verified_replay_prefix(b,originals[oldp],newp,proxy=(oldp==a.index_csv))
old=pd.read_csv(io.BytesIO(originals[paths['proxy_members']]));new=pd.read_csv(sp['proxy_members']);pd.testing.assert_frame_equal(old.iloc[:len(new)].reset_index(drop=True),new,check_exact=True,check_dtype=False)
t=complete_terminal_return_dates(pd.read_csv(sp['proxy_turnover']),dates,anchor);old=pd.read_csv(io.BytesIO(originals[paths['proxy_turnover']]));pd.testing.assert_frame_equal(normalise_turnover_table(old),normalise_turnover_table(t),check_exact=True,check_dtype=False)
_atomic(sp['proxy_turnover'],originals[paths['proxy_turnover']])
print('CACHED_REBUILD_EXACT_PREFIX_PASS',datetime.datetime.now().isoformat(),flush=True)
lock=root/delivery.LOCK
with os.fdopen(os.open(lock,os.O_CREAT|os.O_EXCL|os.O_WRONLY),'w') as f:f.write(str(os.getpid()))
try:
 assert b.try_extend_proxy_index_without_rebalance(s,sp,panel,pd.Timestamp('2026-11-02'))
 preserve_verified_replay_prefix(b,originals[a.index_csv],s.index_csv,proxy=True)
 assert b.try_extend_costed_nav_without_turnover(s,panel,pd.Timestamp('2026-11-02'),sp['proxy_turnover'])
 preserve_verified_replay_prefix(b,originals[a.costed_nav_csv],s.costed_nav_csv)
 installs=[(a.index_csv,s.index_csv),(a.costed_nav_csv,s.costed_nav_csv)]+[(paths[k],sp[k]) for k in keys]
 for oldp,newp in installs[:2]+[(paths[k],sp[k]) for k in ('proxy_members','proxy_turnover')]:
  old=pd.read_csv(io.BytesIO(originals[oldp]));new=pd.read_csv(newp);pd.testing.assert_frame_equal(old,new.iloc[:len(old)].reset_index(drop=True),check_exact=True,check_dtype=False)
 for p,raw in originals.items():assert p.read_bytes()==raw
 for target,source in installs:_atomic(target,source.read_bytes())
 (stage/'receipt.json').write_text(json.dumps(dict(status='exact_historical_parity_pass',anchor='2026-10-29',target='2026-11-02',outputs={p.name:hashlib.sha256(q.read_bytes()).hexdigest() for p,q in installs}),indent=2),encoding='utf-8')
 print('SCOPED_REPLAY_PASS',datetime.datetime.now().isoformat(),flush=True)
finally:lock.unlink()
for name in ('finish_current_primary.py','finish_current_pack.py'):
 print('STAGE_START',name,datetime.datetime.now().isoformat(),flush=True)
 runpy.run_path(str(root/'research_reports/20261102_deferred_repairs'/name),run_name='__main__')
 print('STAGE_END',name,datetime.datetime.now().isoformat(),flush=True)
