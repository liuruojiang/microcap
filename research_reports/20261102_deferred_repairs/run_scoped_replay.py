from pathlib import Path
import os,sys
root=Path.cwd();sys.path.insert(0,str(root));os.environ['MICROCAP_DAILY_STOCK_PROVIDER']='tencent'
import microcap_top100_mom16_biweekly_live_v2_0 as v
from scripts.st_metadata_refresh import try_cached_history_rebuild
from scripts import top100_delivery as delivery
v._sync_embedded_base_config();b=v.base_mod;args=v._build_base_args(max_workers=8);paths=b.build_output_paths(b.DEFAULT_OUTPUT_PREFIX)
lock=root/delivery.LOCK
with os.fdopen(os.open(lock,os.O_CREAT|os.O_EXCL|os.O_WRONLY),'w') as f:f.write(str(os.getpid()))
try:
 assert try_cached_history_rebuild(root=root,base=b,args=args,paths=paths,panel_path=root/'outputs/microcap_top100_mom16_biweekly_live_v2_0_base_panel_refreshed.csv',target='2026-11-02')
 print('SCOPED_REPLAY_PASS',flush=True)
finally:lock.unlink()
