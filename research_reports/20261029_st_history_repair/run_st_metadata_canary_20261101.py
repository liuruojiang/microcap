"""Actual isolated builder checks before the full pool; no formal writes."""
import json, os, sys, argparse, re
from pathlib import Path
from run_three_version_diagnostic_20261101 import digest, isolate_modules, offline_guard

research=Path(__file__).resolve().parent
parser=argparse.ArgumentParser();parser.add_argument('--attempt',default='initial');args=parser.parse_args()
if re.fullmatch('[a-z0-9_]+',args.attempt) is None:raise ValueError('Invalid canary attempt label')
root=(research/'replay/runtime_20261029').resolve()
manifest=json.loads((root/'source_manifest.json').read_text(encoding='utf-8'))
for name,h in manifest['files'].items():
    assert digest(root/name)==h, name
os.chdir(root);sys.path.insert(0,str(root));sys.dont_write_bytecode=True
os.environ['MPLCONFIGDIR']=str(root/'.mplconfig');os.environ['MPLBACKEND']='Agg'
os.environ['TEMP']=str(root/'.tmp');os.environ['TMP']=str(root/'.tmp');(root/'.tmp').mkdir(exist_ok=True)
offline_guard(root)
import microcap_top100_mom16_biweekly_live_v2_0 as v
isolate_modules(v,root);v._sync_embedded_base_config()
from scripts.st_history_scope import validated_source_batch
stage=root/('st_metadata_canary_20261101' if args.attempt=='initial' else 'st_metadata_canary_20261101_'+args.attempt)
if stage.exists():raise RuntimeError('Retain previous canary evidence; do not overwrite')
stage.mkdir();v.freq_mod.SECURITY_META_DIR=stage
v.freq_mod.build_security_meta.__globals__['SECURITY_META_DIR']=stage
symbols=['000001','000018','000430','002072','600000','600083','600180','600385','600556','600633','600831']
results=[]
with validated_source_batch(root):
    for s in symbols:
        try:
            meta=v.freq_mod.build_security_meta(s,as_of='2026-10-30')
            assert meta and meta['notice_query_status']=='ok' and meta['current_st_history_resolved']
            assert all('current_name_snapshot' not in str(x.get('source')) for x in meta['st_intervals'])
            results.append(dict(symbol=s,status='pass',first_trade=meta['first_trade_date'],sample_start=meta['st_history_sample_start'],notice_end=meta['notice_query_end_date'],sha256=digest(stage/(s+'.json'))))
        except Exception as e:results.append(dict(symbol=s,status='blocked',error=type(e).__name__+': '+str(e)))
        print(json.dumps(results[-1],ensure_ascii=False),flush=True)
report=dict(scope='actual isolated metadata canaries only',approved=False,formal_written=False,source_manifest_sha256=digest(root/'source_manifest.json'),attempt=args.attempt,results=results)
(stage/'result.json').write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf-8')
assert all(r['status']=='pass' for r in results), 'Canary blockers remain'
