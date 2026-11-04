"""Rebuild certified historical inputs locally, then use native member-only tail."""
from pathlib import Path
import copy,datetime,hashlib,json,os,sys
import pandas as pd
root=Path.cwd();sys.path.insert(0,str(root));os.environ['MICROCAP_DAILY_STOCK_PROVIDER']='tencent'
import microcap_top100_mom16_biweekly_live_v2_0 as v
from scripts import top100_delivery as delivery
from scripts.st_metadata_refresh import _history_equal
v._sync_embedded_base_config();b=v.base_mod
args=v._build_base_args(max_workers=8);paths=b.build_output_paths(b.DEFAULT_OUTPUT_PREFIX)
e=root/'research_reports/20261102_deferred_repairs';stage=e/'scoped_replay';stage.mkdir(exist_ok=True)
lock=root/delivery.LOCK
with os.fdopen(os.open(lock,os.O_CREAT|os.O_EXCL|os.O_WRONLY),'w') as f:f.write(str(os.getpid()))
try:
    marker=json.loads((root/'.microcap_index_cache/st_metadata_refresh.json').read_bytes())
    for symbol,sha in marker['new_hashes'].items():
        raw=(root/'.microcap_index_cache/security_meta'/f'{symbol}.json').read_bytes();assert hashlib.sha256(raw).hexdigest()==sha
        old=json.loads((root/marker['work']/'backup'/f'{symbol}.json').read_bytes());assert _history_equal(old,json.loads(raw),'2026-11-02'),symbol
    original={p:p.read_bytes() for p in [args.index_csv,args.costed_nav_csv,*[paths[k] for k in ('proxy_members','proxy_turnover','proxy_meta','proxy_effective_members')]]}
    for p,raw in original.items(): (stage/('before_'+p.name)).write_bytes(raw)
    prior=json.loads(original[paths['proxy_meta']]);anchor=prior['return_basis_consumption']['full_rebuild_anchor']
    assert anchor=='2026-10-29'
    panel=paths['panel_refreshed'] if 'panel_refreshed' in paths else root/'outputs/microcap_top100_mom16_biweekly_live_v2_0_base_panel_refreshed.csv'
    dates=pd.to_datetime(pd.read_csv(panel)['date']);dates=pd.DatetimeIndex(dates[dates<=pd.Timestamp(anchor)])
    staged_args=copy.copy(args);staged_args.index_csv=stage/args.index_csv.name;staged_args.costed_nav_csv=stage/args.costed_nav_csv.name
    staged_paths={k:stage/p.name for k,p in paths.items()}
    print('CACHED_HISTORY_REBUILD_START',datetime.datetime.now().isoformat(),anchor,flush=True)
    index,members,turnover,meta=b.build_local_proxy_bundle(staged_args,dates)
    for frame,p in [(index,staged_args.index_csv),(members,staged_paths['proxy_members']),(turnover,staged_paths['proxy_turnover'])]:b._atomic_to_csv(frame,p,index=False,encoding='utf-8')
    b._atomic_write_json(staged_paths['proxy_meta'],meta,encoding='utf-8')
    b._write_proxy_effective_members(staged_paths['proxy_effective_members'],pd.Timestamp(anchor),meta['terminal_effective_members'])
    b.rebuild_costed_nav_from_proxy_turnover(staged_args,staged_paths,panel,target_end_date=pd.Timestamp(anchor))
    print('CACHED_HISTORY_REBUILD_PASS',datetime.datetime.now().isoformat(),flush=True)
    assert b.try_extend_proxy_index_without_rebalance(staged_args,staged_paths,panel,pd.Timestamp('2026-11-02')),'native scoped proxy tail rejected'
    assert b.try_extend_costed_nav_without_turnover(staged_args,panel,pd.Timestamp('2026-11-02'),staged_paths['proxy_turnover']),'native costed tail rejected'
    print('SCOPED_TAIL_PASS',datetime.datetime.now().isoformat(),flush=True)
    import io
    checks=[]
    for oldp,newp in [(args.index_csv,staged_args.index_csv),(args.costed_nav_csv,staged_args.costed_nav_csv),(paths['proxy_members'],staged_paths['proxy_members']),(paths['proxy_turnover'],staged_paths['proxy_turnover'])]:
        old=pd.read_csv(io.BytesIO(original[oldp]));new=pd.read_csv(newp)
        pd.testing.assert_frame_equal(old,new.iloc[:len(old)].reset_index(drop=True),check_exact=True,check_dtype=False)
        checks.append(dict(file=oldp.name,historical_rows=len(old),new_rows=len(new)-len(old)))
    for p,raw in original.items():assert p.read_bytes()==raw,'Concurrent output change: '+str(p)
    installs=[(args.index_csv,staged_args.index_csv),(args.costed_nav_csv,staged_args.costed_nav_csv)]+[(paths[k],staged_paths[k]) for k in ('proxy_members','proxy_turnover','proxy_meta','proxy_effective_members')]
    for target,source in installs:
        temporary=target.with_suffix(target.suffix+'.scoped.tmp');temporary.write_bytes(source.read_bytes());temporary.replace(target)
    (stage/'receipt.json').write_text(json.dumps(dict(status='native_replay_and_exact_history_parity_pass',full_rebuild_anchor=anchor,target='2026-11-02',checks=checks),indent=2),encoding='utf-8')
    print('SCOPED_INSTALL_PASS',flush=True)
finally:lock.unlink()
