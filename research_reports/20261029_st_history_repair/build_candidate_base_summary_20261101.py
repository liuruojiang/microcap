"""Derive a fresh base summary from completed real replay artifacts, offline."""
import argparse
import hashlib
import importlib
import json
import os
from pathlib import Path
import sys


def sha(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--runtime',type=Path,required=True)
    p.add_argument('--candidate',type=Path,required=True)
    p.add_argument('--candidate29',type=Path,required=True)
    p.add_argument('--calendar',type=Path,required=True)
    p.add_argument('--calendar-sha256',required=True)
    p.add_argument('--execute',action='store_true')
    a=p.parse_args();root=a.runtime.resolve();dest=a.candidate.resolve();old=a.candidate29.resolve();a.calendar=a.calendar.resolve()
    research=Path(__file__).resolve().parent
    if not root.is_relative_to(research/'replay') or not dest.is_relative_to(root) or not old.is_relative_to(root):
        raise ValueError('Only isolated replay roots permitted')
    report=json.loads((dest/'diagnostic_result.json').read_bytes())
    prior=json.loads((old/'diagnostic_result.json').read_bytes())
    if report.get('as_of')!='2026-10-30' or report.get('approved') is not False or prior.get('as_of')!='2026-10-29':
        raise ValueError('Need completed, unpromoted 29/30 diagnostic')
    bindings={dest/'diagnostic_result.json':sha(dest/'diagnostic_result.json')}
    for name in ('proxy.csv','turnover.csv','proxy_meta.json','base_costed.csv'):
        if sha(dest/name)!=report[name]['sha256']:raise ValueError('Candidate hash mismatch: '+name)
        bindings[dest/name]=sha(dest/name)
    members=old/'members.csv'
    if sha(members)!=prior['members.csv']['sha256'] or sha(members)!=report['source_candidate_hashes']['members.csv']:
        raise ValueError('Member identity mismatch')
    bindings[members]=sha(members)
    panel=Path(report['panel']['path']).resolve()
    if not panel.is_relative_to(root) or sha(panel)!=report['panel']['sha256']:raise ValueError('Panel mismatch')
    bindings[panel]=sha(panel)
    manifest=root/'source_manifest.json'
    if sha(manifest)!=report['source_manifest_sha256']:raise ValueError('Source generation mismatch')
    for name,digest in json.loads(manifest.read_bytes())['files'].items():
        path=(root/name).resolve()
        if not path.is_relative_to(root) or sha(path)!=digest:raise ValueError('Source changed: '+name)
    output=dest/'base_summary.json'
    if output.exists():raise ValueError('Summary already exists; inspect its receipt instead of overwriting')
    if not a.execute:
        print('Bound candidate preflight passed; no summary written');return
    from run_three_version_diagnostic_20261101 import offline_guard,bind_frozen_calendar,isolate_modules
    os.chdir(root);sys.path.insert(0,str(root));sys.dont_write_bytecode=True
    os.environ['MPLCONFIGDIR']=str(root/'.mplconfig');os.environ['MPLBACKEND']='Agg'
    offline_guard(root)
    bind_frozen_calendar(a.calendar.resolve(),a.calendar_sha256,'2026-10-30','date')
    import pandas as pd
    v=importlib.import_module('microcap_top100_mom16_biweekly_live_v2_0')
    if not Path(v.__file__).resolve().is_relative_to(root):raise ValueError('Wrong strategy import')
    isolate_modules(v,root);v._sync_embedded_base_config();base=v.base_mod
    from scripts.realtime_state_bundle import persist_reference_summary
    target=pd.Timestamp('2026-10-30')
    close=base.load_close_df(panel,dest/'proxy.csv',max_date=target)
    result=base.run_signal(close).sort_index()
    if result.index[-1]!=target:raise ValueError('Base result stale')
    latest,previous,next_date,effective=base.locate_rebalance_dates(close.index)
    table=pd.read_csv(members,dtype={'symbol':str});table['rebalance_date']=pd.to_datetime(table['rebalance_date'])
    selected=table[table.rebalance_date==latest].copy()
    prev=table[table.rebalance_date==previous].copy() if previous is not None else None
    if len(selected)!=100 or selected.symbol.nunique()!=100:raise ValueError('Latest target member period incomplete')
    names_path=root/'.microcap_index_cache/active_universe.csv'
    names=pd.read_csv(names_path,dtype={'code':str})
    if names.code.duplicated().any():raise ValueError('Current name source has duplicate codes')
    mapping=names.set_index('code')['name']
    if not selected.symbol.isin(mapping.index).all():raise ValueError('Current target name missing')
    selected['name']=selected.symbol.map(mapping)
    bindings[names_path]=sha(names_path)
    changes=base.build_change_table(prev,selected)
    signal=base.enrich_signal_frame(base.hedge_mod.build_latest_signal(result),result)
    signal=base.augment_signal_with_member_rebalance(signal,changes)
    context=dict(result=result,close_df=close,latest_signal=signal,target_members=selected,
                 latest_rebalance=latest,prev_rebalance=previous,next_rebalance=next_date,changes_df=changes)
    args=v._build_base_args()
    persist_reference_summary(v,args,{'summary':output},context,target.date())
    summary=json.loads(output.read_bytes())
    if summary['latest_trade_date']!='2026-10-30' or summary['summary_version_key']!='hedge_0.8' or summary['target_members']['count']!=100:
        raise ValueError('Summary readback failed')
    for path,digest in bindings.items():
        if sha(path)!=digest:raise ValueError('Input changed while building summary')
    receipt={'approved':False,'formal_written':False,'scope':'derived_summary_only',
             'generator':'scripts.realtime_state_bundle.persist_reference_summary',
             'source_manifest_sha256':sha(manifest),'runner_sha256':sha(Path(__file__)),
             'summary_sha256':sha(output),'as_of':'2026-10-30',
             'inputs':{str(path):digest for path,digest in bindings.items()},'path_rewrites':[],
             'current_member_display_names':'verified active_universe snapshot; historical member file unchanged'}
    (dest/'base_summary_receipt.json').write_text(json.dumps(receipt,ensure_ascii=False,indent=2),encoding='utf-8')
    print(json.dumps({'summary':str(output),'sha256':sha(output),'formal_written':False}))


if __name__=='__main__':main()
